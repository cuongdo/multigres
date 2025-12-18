// Copyright 2025 Supabase, Inc.
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

package multiorch

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/utils"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// TestBootstrapStandbyFailureNoRetry reproduces a bug where standby initialization
// failures during bootstrap are not retried.
//
// The bug: When pgbackrest restore fails (e.g., lock contention), the standby:
// - Remains registered as UNKNOWN type (ChangeType never called)
// - Auto-restore skips because poolerType != REPLICA
// - ShardNeedsBootstrapAnalyzer skips because primary exists
// - Result: Standby stays uninitialized forever
//
// This test documents the current (buggy) behavior. When the bug is fixed,
// the final assertion should change from assert.False to assert.True.
func TestBootstrapStandbyFailureNoRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end bootstrap retry test (short mode)")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end bootstrap retry test (no postgres binaries)")
	}

	// Setup test environment
	env := setupMultiOrchTestEnv(t, testEnvConfig{
		tempDirPrefix:    "brty*",
		cellName:         "retry-cell",
		database:         "postgres",
		shardID:          "retry-shard-01",
		tableGroup:       "test",
		durabilityPolicy: "ANY_2",
		stanzaName:       "retry-test",
	})

	// Create 3 nodes:
	// - Node 0: Will become primary (no fault injection)
	// - Node 1: Standby with fault injection (will fail restore initially)
	// - Node 2: Normal standby (no fault injection)
	//
	// We inject faults on node 1 to simulate pgbackrest lock contention.
	// The fault clears after 2 attempts (but auto-restore won't retry).
	nodes := env.createNodesWithFaultInjection(3, []int{1}, 2 /* maxFailures */)

	// Verify all nodes start uninitialized with UNKNOWN type
	for i, node := range nodes {
		status := checkInitializationStatus(t, node)
		require.False(t, status.IsInitialized, "Node %d should not be initialized", i)
		require.Equal(t, clustermetadatapb.PoolerType_UNKNOWN, status.PoolerType,
			"Node %d should have UNKNOWN type initially", i)
	}

	env.setupPgBackRest()
	env.registerNodes()

	// Start multiorch - it will detect uninitialized shard and attempt bootstrap
	env.startMultiOrch()

	// Wait for primary to be elected (this should succeed)
	t.Log("Waiting for primary to be bootstrapped...")
	primaryNode := waitForShardPrimary(t, nodes, 60*time.Second)
	require.NotNil(t, primaryNode, "Primary should be initialized")
	t.Logf("Primary is %s", primaryNode.name)

	// Wait for at least one standby to initialize (node 2 should succeed)
	t.Log("Waiting for at least 1 standby to initialize...")
	waitForStandbysInitialized(t, nodes, primaryNode.name, 1, 60*time.Second)

	// At this point:
	// - Primary (node 0) is initialized and type=PRIMARY
	// - Node 2 (no fault injection) should be initialized and type=REPLICA
	// - Node 1 (with fault injection) should have FAILED to initialize

	// Verify node 1 is NOT initialized (this is the bug we're reproducing)
	node1 := nodes[1]
	status1 := checkInitializationStatus(t, node1)
	t.Logf("Node 1 status after bootstrap: IsInitialized=%v, PoolerType=%s, PostgresRunning=%v",
		status1.IsInitialized, status1.PoolerType, status1.PostgresRunning)

	// BUG ASSERTION: Node 1 should NOT be initialized because:
	// 1. Its pgbackrest restore failed due to simulated lock
	// 2. ChangeType was never called (only called after successful init)
	// 3. Its type is still UNKNOWN
	assert.False(t, status1.IsInitialized,
		"Node 1 should NOT be initialized (restore failed due to fault injection)")
	assert.Equal(t, clustermetadatapb.PoolerType_UNKNOWN, status1.PoolerType,
		"Node 1 should still be UNKNOWN type (ChangeType not called)")

	// Wait additional time to see if any retry mechanism kicks in.
	// The fault injection allows success after 2 failures, so if there's a retry
	// mechanism, the 3rd attempt would succeed. With recovery-cycle-interval=500ms,
	// any retry would trigger within a few seconds.
	t.Log("Waiting 10s to check if retry mechanism exists...")
	time.Sleep(10 * time.Second)

	// Check node 1 status again
	status1After := checkInitializationStatus(t, node1)
	t.Logf("Node 1 status after waiting: IsInitialized=%v, PoolerType=%s, PostgresRunning=%v",
		status1After.IsInitialized, status1After.PoolerType, status1After.PostgresRunning)

	// BUG DOCUMENTATION: This assertion documents the current buggy behavior.
	// Node 1 should STILL NOT be initialized because:
	// - Auto-restore skips (poolerType != REPLICA)
	// - ShardNeedsBootstrapAnalyzer skips (primary exists)
	// - No other retry mechanism exists
	//
	// WHEN THE BUG IS FIXED: Change this to assert.True and verify the standby
	// eventually initializes via a retry mechanism.
	assert.False(t, status1After.IsInitialized,
		"BUG: Node 1 remains uninitialized - no retry mechanism for failed standby bootstrap")
}
