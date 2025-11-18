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

// TODO: Move these tests to a separate multiadmin test directory once the test infrastructure
// supports importing from multipooler package (currently all files in multipooler/ are test files)
//
// Note: These tests focus on the MultiAdmin orchestration layer (topology lookup, job tracking,
// multi-pooler aggregation). The underlying backup/restore functionality is also tested in the
// pooler cluster tests (backup_test.go), which exercise the MultiPoolerManager APIs directly.

package multipooler

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	adminserver "github.com/multigres/multigres/go/admin/server"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
)

// TestMultiAdminBackupRestore tests the full backup/restore workflow through MultiAdmin APIs
func TestMultiAdminBackupRestore(t *testing.T) {
	// Setup cluster with primary and standby using the shared setup
	setup := getSharedTestSetup(t)
	setupPoolerTest(t, setup)

	// Create MultiAdmin server
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	adminServer := adminserver.NewMultiAdminServer(setup.TopoServer, logger)

	ctx := context.Background()

	t.Run("CreateBackup", func(t *testing.T) {
		// Create a backup using MultiAdmin API
		backupReq := &multiadminpb.BackupRequest{
			Database:     "postgres",
			TableGroup:   "test",
			Shard:        "", // Empty shard for non-sharded deployment
			Type:         "full",
			ForcePrimary: true,
		}

		resp, err := adminServer.Backup(ctx, backupReq)
		require.NoError(t, err, "Backup should succeed")
		require.NotEmpty(t, resp.JobId, "Job ID should be returned")

		t.Logf("Backup job created: %s", resp.JobId)

		// Poll for backup completion
		var backupID string
		require.Eventually(t, func() bool {
			statusReq := &multiadminpb.GetBackupJobStatusRequest{
				JobId: resp.JobId,
			}
			statusResp, err := adminServer.GetBackupJobStatus(ctx, statusReq)
			if err != nil {
				t.Logf("Error checking backup status: %v", err)
				return false
			}

			if statusResp.Status == multiadminpb.GetBackupJobStatusResponse_FAILED {
				t.Fatalf("Backup job failed: %s", statusResp.ErrorMessage)
			}

			if statusResp.Status == multiadminpb.GetBackupJobStatusResponse_COMPLETED {
				backupID = statusResp.BackupId
				return true
			}

			return false
		}, 60*time.Second, 1*time.Second, "Backup should complete within 60 seconds")

		t.Logf("Backup completed successfully: %s", backupID)

		// Verify we can get the job status again
		statusReq := &multiadminpb.GetBackupJobStatusRequest{
			JobId: resp.JobId,
		}
		statusResp, err := adminServer.GetBackupJobStatus(ctx, statusReq)
		require.NoError(t, err)
		require.Equal(t, multiadminpb.GetBackupJobStatusResponse_COMPLETED, statusResp.Status)
		require.Equal(t, backupID, statusResp.BackupId)
		require.Equal(t, multiadminpb.GetBackupJobStatusResponse_BACKUP, statusResp.JobType)
	})

	t.Run("GetBackups", func(t *testing.T) {
		// List backups using MultiAdmin API
		getBackupsReq := &multiadminpb.GetBackupsRequest{
			Database:   "postgres",
			TableGroup: "test",
			Shard:      "", // Empty shard
			Limit:      0,  // No limit
		}

		resp, err := adminServer.GetBackups(ctx, getBackupsReq)
		require.NoError(t, err, "GetBackups should succeed")
		require.NotEmpty(t, resp.Backups, "Should have at least one backup")

		t.Logf("Found %d backup(s)", len(resp.Backups))

		// Verify backup info
		backup := resp.Backups[0]
		require.NotEmpty(t, backup.BackupId)
		require.Equal(t, "postgres", backup.Database)
		require.Equal(t, "test", backup.TableGroup)
		require.Equal(t, "", backup.Shard)
		require.Equal(t, multiadminpb.BackupInfo_COMPLETE, backup.Status)
	})

	t.Run("GetBackupsWithLimit", func(t *testing.T) {
		// Test GetBackups with limit
		getBackupsReq := &multiadminpb.GetBackupsRequest{
			Database:   "postgres",
			TableGroup: "test",
			Shard:      "",
			Limit:      1,
		}

		resp, err := adminServer.GetBackups(ctx, getBackupsReq)
		require.NoError(t, err)
		require.LessOrEqual(t, len(resp.Backups), 1, "Should respect limit")
	})

	t.Run("RestoreFromBackup", func(t *testing.T) {
		// First, get a backup ID to restore
		getBackupsReq := &multiadminpb.GetBackupsRequest{
			Database:   "postgres",
			TableGroup: "test",
			Shard:      "",
			Limit:      1,
		}

		backupsResp, err := adminServer.GetBackups(ctx, getBackupsReq)
		require.NoError(t, err)
		require.NotEmpty(t, backupsResp.Backups)

		backupID := backupsResp.Backups[0].BackupId
		t.Logf("Restoring from backup: %s", backupID)

		// Restore to standby using MultiAdmin API
		restoreReq := &multiadminpb.RestoreFromBackupRequest{
			Database:   "postgres",
			TableGroup: "test",
			Shard:      "",
			BackupId:   backupID,
			PoolerId: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "test-cell",
				Name:      "standby-multipooler",
			},
			AsStandby: true, // Restore as standby
		}

		resp, err := adminServer.RestoreFromBackup(ctx, restoreReq)
		require.NoError(t, err, "RestoreFromBackup should succeed")
		require.NotEmpty(t, resp.JobId, "Job ID should be returned")

		t.Logf("Restore job created: %s", resp.JobId)

		// Poll for restore completion
		require.Eventually(t, func() bool {
			statusReq := &multiadminpb.GetBackupJobStatusRequest{
				JobId: resp.JobId,
			}
			statusResp, err := adminServer.GetBackupJobStatus(ctx, statusReq)
			if err != nil {
				t.Logf("Error checking restore status: %v", err)
				return false
			}

			if statusResp.Status == multiadminpb.GetBackupJobStatusResponse_FAILED {
				t.Fatalf("Restore job failed: %s", statusResp.ErrorMessage)
			}

			return statusResp.Status == multiadminpb.GetBackupJobStatusResponse_COMPLETED
		}, 120*time.Second, 2*time.Second, "Restore should complete within 120 seconds")

		t.Log("Restore completed successfully")

		// Verify we can get the job status again
		statusReq := &multiadminpb.GetBackupJobStatusRequest{
			JobId: resp.JobId,
		}
		statusResp, err := adminServer.GetBackupJobStatus(ctx, statusReq)
		require.NoError(t, err)
		require.Equal(t, multiadminpb.GetBackupJobStatusResponse_COMPLETED, statusResp.Status)
		require.Equal(t, multiadminpb.GetBackupJobStatusResponse_RESTORE, statusResp.JobType)
	})

	t.Run("ErrorCases", func(t *testing.T) {
		// Test invalid backup request
		t.Run("BackupWithEmptyDatabase", func(t *testing.T) {
			backupReq := &multiadminpb.BackupRequest{
				Database:   "",
				TableGroup: "test",
				Type:       "full",
			}
			_, err := adminServer.Backup(ctx, backupReq)
			require.Error(t, err, "Should fail with empty database")
			require.Contains(t, err.Error(), "database cannot be empty")
		})

		// Test invalid restore request
		t.Run("RestoreWithEmptyPoolerID", func(t *testing.T) {
			restoreReq := &multiadminpb.RestoreFromBackupRequest{
				Database:   "postgres",
				TableGroup: "test",
				BackupId:   "test-backup",
				PoolerId:   nil,
			}
			_, err := adminServer.RestoreFromBackup(ctx, restoreReq)
			require.Error(t, err, "Should fail with nil pooler_id")
			require.Contains(t, err.Error(), "pooler_id cannot be empty")
		})

		// Test invalid job status request
		t.Run("GetStatusForNonExistentJob", func(t *testing.T) {
			statusReq := &multiadminpb.GetBackupJobStatusRequest{
				JobId: "non-existent-job-id",
			}
			_, err := adminServer.GetBackupJobStatus(ctx, statusReq)
			require.Error(t, err, "Should fail for non-existent job")
			require.Contains(t, err.Error(), "job not found")
		})

		// Test invalid GetBackups request
		t.Run("GetBackupsWithEmptyDatabase", func(t *testing.T) {
			getBackupsReq := &multiadminpb.GetBackupsRequest{
				Database:   "",
				TableGroup: "test",
			}
			_, err := adminServer.GetBackups(ctx, getBackupsReq)
			require.Error(t, err, "Should fail with empty database")
			require.Contains(t, err.Error(), "database cannot be empty")
		})
	})
}
