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

	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/test/utils"
)

// TestBootstrapStandbyFailureRetry verifies that standby initialization failures
// during bootstrap are retried until successful.
//
// Scenario: When pgbackrest restore fails (e.g., lock contention), the standby
// should eventually initialize after the transient failure clears.
func TestBootstrapStandbyFailureRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end bootstrap retry test (short mode)")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end bootstrap retry test (no postgres binaries)")
	}

	// Setup test environment
	// Note: Use DefaultTableGroup and DefaultShard to match what multipooler actually uses.
	// The test helpers currently hardcode these values in the multipooler command.
	env := setupMultiOrchTestEnv(t, testEnvConfig{
		tempDirPrefix:    "brty*",
		cellName:         "retry-cell",
		database:         "postgres",
		shardID:          constants.DefaultShard,
		tableGroup:       constants.DefaultTableGroup,
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

	// Node 1 should be REPLICA type (set during bootstrap attempt)
	node1 := nodes[1]
	status1 := checkInitializationStatus(t, node1)
	t.Logf("Node 1 status after bootstrap: IsInitialized=%v, PoolerType=%s, PostgresRunning=%v",
		status1.IsInitialized, status1.PoolerType, status1.PostgresRunning)

	assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, status1.PoolerType,
		"Node 1 should be REPLICA type")

	// Wait for node 1 to eventually initialize via retry.
	// The fault injection allows success after 2 failures.
	t.Log("Waiting for node 1 to initialize via retry...")

	require.Eventually(t, func() bool {
		status := checkInitializationStatus(t, node1)
		return status.IsInitialized
	}, 30*time.Second, 500*time.Millisecond,
		"Node 1 should eventually be initialized via retry")

	status1After := checkInitializationStatus(t, node1)
	t.Logf("Node 1 successfully initialized: IsInitialized=%v, PoolerType=%s, PostgresRunning=%v",
		status1After.IsInitialized, status1After.PoolerType, status1After.PostgresRunning)
}
