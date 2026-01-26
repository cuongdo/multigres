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

package multipooler

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// filesystemSetupManager manages the shared test setup for filesystem backend tests.
var filesystemSetupManager = shardsetup.NewSharedSetupManager(func(t *testing.T) *shardsetup.ShardSetup {
	// Create a 2-node cluster for testing (primary + standby) with filesystem backup
	return shardsetup.New(t, shardsetup.WithMultipoolerCount(2))
})

// minioSetupManager manages the shared test setup for MinIO/S3 backend tests.
var minioSetupManager = shardsetup.NewSharedSetupManager(func(t *testing.T) *shardsetup.ShardSetup {
	// Create a 2-node cluster for testing (primary + standby) with S3 backup
	minioEndpoint := os.Getenv("MULTIGRES_MINIO_ENDPOINT")
	return shardsetup.New(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithS3Backup("multigres", "us-east-1", minioEndpoint),
	)
})

// TestMain sets the path and cleans up after all tests.
func TestMain(m *testing.M) {
	exitCode := shardsetup.RunTestMain(m)
	if exitCode != 0 {
		filesystemSetupManager.DumpLogs()
		minioSetupManager.DumpLogs()
	}
	filesystemSetupManager.Cleanup()
	minioSetupManager.Cleanup()
	os.Exit(exitCode) //nolint:forbidigo // TestMain() is allowed to call os.Exit
}

// getSharedSetup returns the shared setup for tests (filesystem backend).
func getSharedSetup(t *testing.T) *shardsetup.ShardSetup {
	t.Helper()
	return filesystemSetupManager.Get(t)
}

// backendConfig holds configuration for a backup backend.
type backendConfig struct {
	name     string // "filesystem" or "minio"
	setupOpt shardsetup.SetupOption
}

// getAvailableBackends returns the list of backup backends available for testing.
// Filesystem backend always available. MinIO backend available if MULTIGRES_MINIO_ENDPOINT is set.
func getAvailableBackends(t *testing.T) []backendConfig {
	t.Helper()

	backends := []backendConfig{
		{
			name:     "filesystem",
			setupOpt: nil, // Uses default filesystem backup
		},
	}

	// Check if MinIO is available
	minioEndpoint := os.Getenv("MULTIGRES_MINIO_ENDPOINT")
	if minioEndpoint != "" {
		backends = append(backends, backendConfig{
			name:     "minio",
			setupOpt: shardsetup.WithS3Backup("multigres", "us-east-1", minioEndpoint),
		})
		t.Logf("MinIO backend available at %s", minioEndpoint)
	} else {
		t.Log("MinIO backend not available (MULTIGRES_MINIO_ENDPOINT not set)")
	}

	return backends
}

// getSetupForBackend returns the appropriate shared setup for the given backend.
func getSetupForBackend(t *testing.T, backend backendConfig) *MultipoolerTestSetup {
	t.Helper()

	var setup *shardsetup.ShardSetup
	if backend.name == "minio" {
		setup = minioSetupManager.Get(t)
	} else {
		setup = filesystemSetupManager.Get(t)
	}

	return newMultipoolerTestSetup(setup)
}

// restoreAfterEmergencyDemotion restores a pooler to a working state after emergency demotion.
// Emergency demotion stops postgres and disables monitoring but doesn't update topology.
// This helper:
// 1. Restarts postgres as standby
// 2. Updates topology to REPLICA
// 3. Restarts the multipooler to pick up topology changes
// 4. Resets synchronous replication configuration (clears synchronous_standby_names)
func restoreAfterEmergencyDemotion(t *testing.T, setup *MultipoolerTestSetup, pgctld *ProcessInstance, multipooler *ProcessInstance, multipoolerName string) {
	t.Helper()

	// Step 1: Restart postgres as standby (emergency demotion stopped it)
	pgctldClient, err := shardsetup.NewPgctldClient(pgctld.GrpcPort)
	require.NoError(t, err)
	defer pgctldClient.Close()

	t.Logf("Restarting stopped postgres as standby for pooler %s...", multipoolerName)
	restartCtx, restartCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer restartCancel()
	_, err = pgctldClient.Restart(restartCtx, &pgctldpb.RestartRequest{
		Mode:      "fast",
		AsStandby: true,
	})
	require.NoError(t, err, "Restart as standby should succeed on pooler: %s", multipoolerName)

	// Wait for postgres to be running
	require.Eventually(t, func() bool {
		statusResp, err := pgctldClient.Status(context.Background(), &pgctldpb.StatusRequest{})
		return err == nil && statusResp.Status == pgctldpb.ServerStatus_RUNNING
	}, 10*time.Second, 1*time.Second, "Postgres should be running after restart on pooler: %s", multipoolerName)

	// Step 2: Update topology to REPLICA (emergency demotion doesn't update topology)
	t.Logf("Updating topology to REPLICA for pooler %s...", multipoolerName)
	multipoolerRecord, err := setup.TopoServer.GetMultiPooler(context.Background(), &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      setup.CellName,
		Name:      multipoolerName,
	})
	require.NoError(t, err)
	multipoolerRecord.Type = clustermetadatapb.PoolerType_REPLICA
	err = setup.TopoServer.UpdateMultiPooler(context.Background(), multipoolerRecord)
	require.NoError(t, err, "Should update topology to REPLICA for pooler: %s", multipoolerName)

	// Step 3: Restart multipooler so it picks up the topology change
	t.Logf("Restarting multipooler %s to pick up topology change...", multipoolerName)
	multipooler.Stop()
	err = multipooler.Start(restartCtx, t)
	require.NoError(t, err, "Multipooler should restart successfully: %s", multipoolerName)

	// Wait for manager to be ready
	waitForManagerReady(t, setup, multipooler)

	// Step 4: Re-enable the monitor (emergency demotion disabled it)
	t.Logf("Re-enabling monitor for pooler %s...", multipoolerName)
	multipoolerClient, err := shardsetup.NewMultipoolerClient(multipooler.GrpcPort)
	require.NoError(t, err)
	defer multipoolerClient.Close()

	setMonitorCtx, setMonitorCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer setMonitorCancel()
	_, err = multipoolerClient.Manager.SetMonitor(setMonitorCtx, &multipoolermanagerdatapb.SetMonitorRequest{
		Enabled: true,
	})
	require.NoError(t, err, "Should re-enable monitor on pooler: %s", multipoolerName)

	// Step 5: Reset synchronous replication configuration
	// Clear synchronous_standby_names that may have been set when this was primary
	t.Logf("Resetting synchronous replication config for pooler %s...", multipoolerName)
	poolerClient, err := shardsetup.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", multipooler.GrpcPort))
	require.NoError(t, err)
	defer poolerClient.Close()

	_, err = poolerClient.ExecuteQuery(context.Background(), "ALTER SYSTEM SET synchronous_standby_names = ''", 1)
	require.NoError(t, err, "Should clear synchronous_standby_names on pooler: %s", multipoolerName)

	_, err = poolerClient.ExecuteQuery(context.Background(), "SELECT pg_reload_conf()", 1)
	require.NoError(t, err, "Should reload postgres config on pooler: %s", multipoolerName)

	t.Logf("Pooler %s restored after emergency demotion", multipoolerName)
}
