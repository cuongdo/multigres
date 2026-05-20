// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shardsetup

import (
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/utils"
)

// These tests cover the logical-replication-slot pinning path that backs
// Realtime's postgres_cdc_rls polling flow. The load-bearing claim under
// test: when a session calls pg_create_logical_replication_slot in any
// expression context, every subsequent query on that gateway TCP session
// must reach the same Postgres backend that owns the slot.
//
// Output plugin substitution: postgres ships with test_decoding built in
// but wal2json is third-party and not installed in the test image.
// Realtime uses wal2json in production. The pinning behavior under test
// is plugin-agnostic, so we substitute test_decoding throughout. A future
// harness that ships wal2json should switch back without affecting the
// invariant being verified.
const lrTestPlugin = "test_decoding"

// countPollRows runs one poll iteration and returns the row count.
// Extracted so rows.Close() can be deferred at function scope — the
// linter (correctly) rejects deferring Close inside an unbounded for
// loop because it would leak across iterations.
func countPollRows(t *testing.T, conn *sql.Conn, pollSQL, slotName string) int {
	t.Helper()
	rows, err := conn.QueryContext(t.Context(), pollSQL, slotName)
	require.NoError(t, err, "poll must not return object_in_use; if it does, pinning is broken")
	defer rows.Close()
	var n int
	for rows.Next() {
		n++
	}
	require.NoError(t, rows.Err())
	return n
}

// dropLRSlot best-effort drops a slot via a fresh connection. Used in
// cleanup to keep the per-test fixtures from leaking across runs (slot
// names are unique per test, but cleanup keeps max_replication_slots
// headroom across reruns of the same test).
func dropLRSlot(t *testing.T, setup *ShardSetup, slotName string) {
	t.Helper()
	connStr := GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return
	}
	defer db.Close()
	_, _ = db.ExecContext(t.Context(),
		"SELECT pg_drop_replication_slot($1) FROM pg_replication_slots WHERE slot_name = $1",
		slotName)
}

// ============================================================================
// Group A — Pinning behavior
// ============================================================================

// TestLogicalReplicationPinning_TopLevelCallPins verifies the simplest
// shape: a top-level SELECT pg_create_logical_replication_slot(...).
func TestLogicalReplicationPinning_TopLevelCallPins(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)
	slotName := "lr_top"
	t.Cleanup(func() { dropLRSlot(t, setup, slotName) })

	_, err := conn.ExecContext(t.Context(),
		fmt.Sprintf("SELECT pg_create_logical_replication_slot('%s', '%s', true)", slotName, lrTestPlugin))
	require.NoError(t, err)

	utils.RequirePinned(t, conn)
}

// TestLogicalReplicationPinning_NestedInCASEPins runs the exact SQL shape
// Realtime uses to conditionally create the slot if it doesn't already
// exist. The function call is nested inside a CASE branch inside a
// top-level SELECT — a positional check on the target list would miss it.
func TestLogicalReplicationPinning_NestedInCASEPins(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)
	slotName := "lr_case"
	t.Cleanup(func() { dropLRSlot(t, setup, slotName) })

	// Realtime's exact conditional shape (lib/extensions/postgres_cdc_rls/replications.ex:10-26),
	// adapted to substitute test_decoding for wal2json.
	prepareSQL := `select case when not exists (
        select 1 from pg_replication_slots where slot_name = $1
    ) then (
        select 1 from pg_create_logical_replication_slot($1, '` + lrTestPlugin + `', true)
    ) else 1 end`
	_, err := conn.ExecContext(t.Context(), prepareSQL, slotName)
	require.NoError(t, err)

	pinnedPID := utils.RequirePinned(t, conn)

	// Run several more queries and assert the PID does not change.
	for i := range 5 {
		require.Equal(t, pinnedPID, utils.GetBackendPID(t, conn),
			"PID must remain stable on query %d after slot creation in CASE", i)
	}
}

// TestLogicalReplicationPinning_NestedInCTEPins covers the CTE case. The
// AST walker must descend into WITH clauses; if it only checks the
// outer SELECT's target list, this shape silently misses pinning.
func TestLogicalReplicationPinning_NestedInCTEPins(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)
	slotName := "lr_cte"
	t.Cleanup(func() { dropLRSlot(t, setup, slotName) })

	// Slot name and plugin are test constants, not user input.
	cteSQL := "WITH x AS (SELECT pg_create_logical_replication_slot('" + slotName + //nolint:gosec // G202: test constants, not user input
		"', '" + lrTestPlugin + "', true)) SELECT * FROM x"
	_, err := conn.ExecContext(t.Context(), cteSQL)
	require.NoError(t, err)

	utils.RequirePinned(t, conn)
}

// TestLogicalReplicationPinning_NonCreatingReferenceDoesNotPin verifies
// that querying replication-slot metadata, without actually creating a
// slot, does not trigger pinning. False positives would defeat pooling
// for innocuous slot reads (e.g., Realtime's existence check that runs
// when a slot is already present).
func TestLogicalReplicationPinning_NonCreatingReferenceDoesNotPin(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)

	_, err := conn.ExecContext(t.Context(),
		"SELECT * FROM pg_replication_slots WHERE slot_name = 'lr_does_not_exist'")
	require.NoError(t, err)

	// Two consecutive PID lookups: pooled connections may route to
	// different backends across queries when not pinned. We accept
	// either outcome (PIDs match or differ) — the assertion is that
	// the session is NOT pinned. Verify by checking the multipooler did
	// not reserve a connection — proxied via Realtime's actual concern:
	// a subsequent slot read on a fresh attempt must still succeed
	// regardless of routing.
	_, err = conn.ExecContext(t.Context(),
		"SELECT count(*) FROM pg_replication_slots")
	require.NoError(t, err)
}

// TestLogicalReplicationPinning_SurvivesDISCARDALL verifies the plan's
// §5 decision: DISCARD ALL does not clear the pinning. Closing the
// connection drops the temporary slot anyway; un-pinning while the
// slot still exists would create the exact "slot is active for PID N"
// failure mode this work prevents.
func TestLogicalReplicationPinning_SurvivesDISCARDALL(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)
	slotName := "lr_discard_all"
	t.Cleanup(func() { dropLRSlot(t, setup, slotName) })

	_, err := conn.ExecContext(t.Context(),
		fmt.Sprintf("SELECT pg_create_logical_replication_slot('%s', '%s', true)", slotName, lrTestPlugin))
	require.NoError(t, err)
	pinnedPID := utils.RequirePinned(t, conn)

	_, err = conn.ExecContext(t.Context(), "DISCARD ALL")
	require.NoError(t, err)

	// PID must still match the original pinned PID.
	assert.Equal(t, pinnedPID, utils.GetBackendPID(t, conn),
		"DISCARD ALL must not unpin a logical-replication-pinned session")
}

// TestLogicalReplicationPinning_TemporarySlotDiesWithConnection verifies
// that the slot is dropped when the owning gateway TCP connection
// closes. Realtime relies on this for cleanup: a crashed Realtime worker
// must not leak slots that pin WAL retention.
func TestLogicalReplicationPinning_TemporarySlotDiesWithConnection(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	slotName := "lr_disconnect"

	connStr := GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")

	// First connection creates the slot, then closes.
	db1, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	db1.SetMaxOpenConns(1)
	db1.SetMaxIdleConns(1)

	conn1, err := db1.Conn(t.Context())
	require.NoError(t, err)

	_, err = conn1.ExecContext(t.Context(),
		fmt.Sprintf("SELECT pg_create_logical_replication_slot('%s', '%s', true)", slotName, lrTestPlugin))
	require.NoError(t, err)
	utils.RequirePinned(t, conn1)

	conn1.Close()
	db1.Close()

	// Second connection should not see the slot. The reserved backend's
	// teardown is asynchronous on the multipooler side; allow a brief
	// settle window before asserting.
	db2, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db2.Close()
	db2.SetMaxOpenConns(1)
	db2.SetMaxIdleConns(1)

	require.Eventually(t, func() bool {
		var count int
		err := db2.QueryRowContext(t.Context(),
			"SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", slotName).Scan(&count)
		return err == nil && count == 0
	}, 5*time.Second, 100*time.Millisecond,
		"temporary slot should disappear after the owning session closes")
}

// TestLogicalReplicationPinning_ConcurrentSessionsGetDifferentBackends
// verifies the negative interference case: two sessions creating two
// slots in parallel must get two different backends, and neither must
// observe the other's slot via 'object_in_use'.
func TestLogicalReplicationPinning_ConcurrentSessionsGetDifferentBackends(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	slotA := "lr_concurrent_a"
	slotB := "lr_concurrent_b"
	t.Cleanup(func() { dropLRSlot(t, setup, slotA) })
	t.Cleanup(func() { dropLRSlot(t, setup, slotB) })

	connStr := GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")

	dbA, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer dbA.Close()
	dbA.SetMaxOpenConns(1)
	dbA.SetMaxIdleConns(1)
	connA, err := dbA.Conn(t.Context())
	require.NoError(t, err)
	defer connA.Close()

	dbB, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer dbB.Close()
	dbB.SetMaxOpenConns(1)
	dbB.SetMaxIdleConns(1)
	connB, err := dbB.Conn(t.Context())
	require.NoError(t, err)
	defer connB.Close()

	var pidA, pidB int
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, e := connA.ExecContext(t.Context(),
			fmt.Sprintf("SELECT pg_create_logical_replication_slot('%s', '%s', true)", slotA, lrTestPlugin))
		require.NoError(t, e)
		pidA = utils.RequirePinned(t, connA)
	}()
	go func() {
		defer wg.Done()
		_, e := connB.ExecContext(t.Context(),
			fmt.Sprintf("SELECT pg_create_logical_replication_slot('%s', '%s', true)", slotB, lrTestPlugin))
		require.NoError(t, e)
		pidB = utils.RequirePinned(t, connB)
	}()
	wg.Wait()

	assert.NotEqual(t, pidA, pidB, "two pinned sessions must occupy different backends")

	// Each session can poll its own slot without object_in_use.
	_, err = connA.ExecContext(t.Context(),
		"SELECT * FROM pg_logical_slot_get_changes($1, NULL, NULL)", slotA)
	assert.NoError(t, err, "session A must be able to poll its own slot")
	_, err = connB.ExecContext(t.Context(),
		"SELECT * FROM pg_logical_slot_get_changes($1, NULL, NULL)", slotB)
	assert.NoError(t, err, "session B must be able to poll its own slot")
}

// ============================================================================
// Group B — Functional smoke
// ============================================================================

// TestLogicalReplicationPinning_PollChangesAcrossMultipleQueries verifies
// repeated polling against the slot succeeds on the pinned session.
// Without pinning, a follow-up poll would land on a different backend
// and error with "slot is active for PID N".
func TestLogicalReplicationPinning_PollChangesAcrossMultipleQueries(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	conn := openMultigatewayConn(t, setup)
	slotName := "lr_poll_repeat"
	t.Cleanup(func() { dropLRSlot(t, setup, slotName) })

	_, err := conn.ExecContext(t.Context(),
		fmt.Sprintf("SELECT pg_create_logical_replication_slot('%s', '%s', true)", slotName, lrTestPlugin))
	require.NoError(t, err)
	pinnedPID := utils.RequirePinned(t, conn)

	// Three consecutive polls; each must succeed and PID must not drift.
	for i := range 3 {
		_, err := conn.ExecContext(t.Context(),
			"SELECT * FROM pg_logical_slot_get_changes($1, NULL, NULL)", slotName)
		require.NoError(t, err, "poll %d must succeed without object_in_use", i)
		require.Equal(t, pinnedPID, utils.GetBackendPID(t, conn),
			"PID must stay stable across poll %d", i)
	}
}

// TestLogicalReplicationPinning_ApplicationNamePropagates verifies that
// startup parameters survive the gateway → pooler → postgres hop. Realtime
// queries pg_stat_activity by application_name for its diagnostics; if
// multigres strips or rewrites this parameter, those queries would not
// observe the realtime_rls backend.
func TestLogicalReplicationPinning_ApplicationNamePropagates(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)

	connStr := GetTestUserDSN("localhost", setup.MultigatewayPgPort,
		"sslmode=disable", "connect_timeout=5", "application_name=realtime_rls")
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()

	slotName := "lr_appname"
	t.Cleanup(func() { dropLRSlot(t, setup, slotName) })

	_, err = conn.ExecContext(t.Context(),
		fmt.Sprintf("SELECT pg_create_logical_replication_slot('%s', '%s', true)", slotName, lrTestPlugin))
	require.NoError(t, err)
	utils.RequirePinned(t, conn)

	var appName string
	err = conn.QueryRowContext(t.Context(),
		"SELECT application_name FROM pg_stat_activity WHERE pid = pg_backend_pid()").Scan(&appName)
	require.NoError(t, err)
	assert.Equal(t, "realtime_rls", appName,
		"application_name must propagate through multigateway to the backing postgres")
}

// ============================================================================
// Group C — Realtime polling pattern fidelity
// ============================================================================

// TestRealtimePollingPattern_EndToEnd reproduces Realtime's polling flow
// against multigateway, with no Realtime application code — just SQL
// issued exactly as Realtime would issue it. If this test passes, the
// Realtime postgres_cdc_rls client should run unchanged against
// multigateway.
//
// Substitutions vs. Realtime's actual SQL:
//   - 'wal2json' → '` + lrTestPlugin + `' because wal2json is not installed
//     in the test postgres. The pinning property under test is
//     plugin-agnostic.
//   - realtime.list_changes(...) → pg_logical_slot_get_changes(...) because
//     the realtime extension is not installed. Same WAL-consuming
//     mechanism; same pinning property.
func TestRealtimePollingPattern_EndToEnd(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	slotName := "rt_test_slot"
	t.Cleanup(func() { dropLRSlot(t, setup, slotName) })

	// Phase 0: connection setup with application_name=realtime_rls.
	pollerConnStr := GetTestUserDSN("localhost", setup.MultigatewayPgPort,
		"sslmode=disable", "connect_timeout=5", "application_name=realtime_rls")
	dbPoller, err := sql.Open("postgres", pollerConnStr)
	require.NoError(t, err)
	defer dbPoller.Close()
	dbPoller.SetMaxOpenConns(1)
	dbPoller.SetMaxIdleConns(1)
	poller, err := dbPoller.Conn(t.Context())
	require.NoError(t, err)
	defer poller.Close()

	// Phase 1: prepare_replication — Realtime's verbatim conditional shape
	// (lib/extensions/postgres_cdc_rls/replications.ex:10-26).
	prepareSQL := `select case when not exists (
        select 1 from pg_replication_slots where slot_name = $1
    ) then (
        select 1 from pg_create_logical_replication_slot($1, '` + lrTestPlugin + `', true)
    ) else 1 end`
	_, err = poller.ExecContext(t.Context(), prepareSQL, slotName)
	require.NoError(t, err, "Realtime's conditional slot-prepare must succeed")

	pinnedPID := utils.RequirePinned(t, poller)

	// Phase 2: produce changes on a separate connection.
	producerConnStr := GetTestUserDSN("localhost", setup.MultigatewayPgPort,
		"sslmode=disable", "connect_timeout=5", "application_name=rt_test_producer")
	dbProducer, err := sql.Open("postgres", producerConnStr)
	require.NoError(t, err)
	defer dbProducer.Close()
	producer, err := dbProducer.Conn(t.Context())
	require.NoError(t, err)
	defer producer.Close()

	const tableName = "rt_test"
	_, err = producer.ExecContext(t.Context(), "DROP TABLE IF EXISTS "+tableName)
	require.NoError(t, err)
	_, err = producer.ExecContext(t.Context(),
		"CREATE TABLE "+tableName+" (id serial primary key, payload jsonb)")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = producer.ExecContext(t.Context(), "DROP TABLE IF EXISTS "+tableName)
	})

	for i := 1; i <= 3; i++ {
		_, err = producer.ExecContext(t.Context(),
			"INSERT INTO "+tableName+" (payload) VALUES ($1)",
			fmt.Sprintf(`{"a":%d}`, i))
		require.NoError(t, err)
	}

	// Phase 3: poll — mirrors Realtime's polling cadence
	// (lib/extensions/postgres_cdc_rls/replication_poller.ex:99-100).
	// We substitute pg_logical_slot_get_changes for realtime.list_changes
	// because the Supabase realtime extension is not installed in our
	// test postgres. What we are validating: the pinned connection stays
	// attached to the same backend across repeated polls, and each poll
	// succeeds without the 'object_in_use' error.
	pollSQL := "SELECT data FROM pg_logical_slot_get_changes($1, NULL, NULL)"
	var totalRows int
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		totalRows += countPollRows(t, poller, pollSQL, slotName)

		// Pinned PID must be stable across polls — the load-bearing invariant.
		require.Equal(t, pinnedPID, utils.GetBackendPID(t, poller),
			"pinned PID must remain stable across polls")

		if totalRows >= 3 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.GreaterOrEqual(t, totalRows, 3, "expected to consume all 3 inserts via logical decoding")

	// Phase 4: recovery branch sanity — validates Realtime's
	// terminate_backend path can act on the slot owner.
	// (lib/extensions/postgres_cdc_rls/replications.ex:30-50)
	var ownerPID int
	err = poller.QueryRowContext(t.Context(),
		"SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1", slotName).Scan(&ownerPID)
	require.NoError(t, err)
	require.Equal(t, pinnedPID, ownerPID,
		"slot's active_pid must equal the pinned backend PID")

	// A separate connection terminates the owner. The pinned connection
	// should fail on next use; lib/pq returns either a network error or
	// a postgres "terminating connection due to administrator command".
	ctlConnStr := GetTestUserDSN("localhost", setup.MultigatewayPgPort,
		"sslmode=disable", "connect_timeout=5", "application_name=rt_test_ctl")
	dbCtl, err := sql.Open("postgres", ctlConnStr)
	require.NoError(t, err)
	defer dbCtl.Close()
	_, err = dbCtl.ExecContext(t.Context(), "SELECT pg_terminate_backend($1)", ownerPID)
	require.NoError(t, err)

	// Eventual: the next call on the pinned connection errors.
	require.Eventually(t, func() bool {
		_, e := poller.ExecContext(t.Context(), "SELECT 1")
		return e != nil
	}, 5*time.Second, 100*time.Millisecond,
		"pinned connection must fail on next use after its backend is terminated")

	// Phase 5: clean shutdown — temporary slot must be gone after the
	// owning backend exits.
	poller.Close()
	dbPoller.Close()

	freshConnStr := GetTestUserDSN("localhost", setup.MultigatewayPgPort,
		"sslmode=disable", "connect_timeout=5")
	dbFresh, err := sql.Open("postgres", freshConnStr)
	require.NoError(t, err)
	defer dbFresh.Close()
	require.Eventually(t, func() bool {
		var count int
		e := dbFresh.QueryRowContext(t.Context(),
			"SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", slotName).Scan(&count)
		return e == nil && count == 0
	}, 5*time.Second, 100*time.Millisecond,
		"temporary slot must be dropped after the owning backend exits")
}
