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
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/test/utils"

	backupservicepb "github.com/multigres/multigres/go/pb/multipoolerbackupservice"
)

func TestBackup_CreateAndList(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	// Wait for manager to be ready
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)

	// Create backup client connection
	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	backupClient := backupservicepb.NewMultiPoolerBackupServiceClient(conn)

	t.Run("CreateFullBackup", func(t *testing.T) {
		t.Log("Creating full backup...")

		req := &backupservicepb.BackupShardRequest{
			TableGroup:   "test",
			Shard:        "default",
			ForcePrimary: false,
			Type:         "full",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		resp, err := backupClient.BackupShard(ctx, req)
		require.NoError(t, err, "Full backup should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		// Verify backup ID format
		assert.NotEmpty(t, resp.BackupId, "Backup ID should not be empty")

		// Backup ID should match pgbackrest format: YYYYMMDD-HHMMSSF
		// F indicates full backup, D indicates differential, I indicates incremental
		backupIDPattern := regexp.MustCompile(`^\d{8}-\d{6}F$`)
		assert.True(t, backupIDPattern.MatchString(resp.BackupId),
			"Backup ID should match format YYYYMMDD-HHMMSSF, got: %s", resp.BackupId)

		t.Logf("Full backup created successfully with ID: %s", resp.BackupId)

		// Store backup ID for next subtest
		fullBackupID := resp.BackupId

		t.Run("ListBackups_VerifyFullBackup", func(t *testing.T) {
			t.Log("Listing backups to verify full backup...")

			listReq := &backupservicepb.GetShardBackupsRequest{
				Limit: 10,
			}

			listCtx := utils.WithShortDeadline(t)
			listResp, err := backupClient.GetShardBackups(listCtx, listReq)
			require.NoError(t, err, "Listing backups should succeed")
			require.NotNil(t, listResp, "List response should not be nil")

			// Verify at least one backup exists
			assert.NotEmpty(t, listResp.Backups, "Should have at least one backup")

			// Find our backup in the list
			var foundBackup *backupservicepb.BackupMetadata
			for _, backup := range listResp.Backups {
				if backup.BackupId == fullBackupID {
					foundBackup = backup
					break
				}
			}

			require.NotNil(t, foundBackup, "Our backup should be in the list")

			// Verify backup metadata
			assert.Equal(t, fullBackupID, foundBackup.BackupId, "Backup ID should match")
			assert.Equal(t, backupservicepb.BackupMetadata_COMPLETE, foundBackup.Status,
				"Backup status should be COMPLETE")

			t.Logf("Backup verified in list: ID=%s, Status=%s",
				foundBackup.BackupId, foundBackup.Status)
		})

		t.Run("ListBackups_WithoutLimit", func(t *testing.T) {
			t.Log("Listing backups without limit...")

			listReq := &backupservicepb.GetShardBackupsRequest{
				Limit: 0, // No limit
			}

			listCtx := utils.WithShortDeadline(t)
			listResp, err := backupClient.GetShardBackups(listCtx, listReq)
			require.NoError(t, err, "Listing backups without limit should succeed")
			require.NotNil(t, listResp, "List response should not be nil")

			// Should have at least our backup
			assert.NotEmpty(t, listResp.Backups, "Should have at least one backup")

			t.Logf("Listed %d backup(s) without limit", len(listResp.Backups))
		})

		t.Run("ListBackups_WithSmallLimit", func(t *testing.T) {
			t.Log("Listing backups with limit=1...")

			listReq := &backupservicepb.GetShardBackupsRequest{
				Limit: 1,
			}

			listCtx := utils.WithShortDeadline(t)
			listResp, err := backupClient.GetShardBackups(listCtx, listReq)
			require.NoError(t, err, "Listing backups with limit should succeed")
			require.NotNil(t, listResp, "List response should not be nil")

			// Should return at most 1 backup
			assert.LessOrEqual(t, len(listResp.Backups), 1,
				"Should return at most 1 backup when limit=1")

			t.Logf("Listed %d backup(s) with limit=1", len(listResp.Backups))
		})
	})

	t.Run("CreateDifferentialBackup", func(t *testing.T) {
		t.Log("Creating differential backup...")

		req := &backupservicepb.BackupShardRequest{
			TableGroup:   "test",
			Shard:        "default",
			ForcePrimary: false,
			Type:         "differential",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		resp, err := backupClient.BackupShard(ctx, req)
		require.NoError(t, err, "Differential backup should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		assert.NotEmpty(t, resp.BackupId, "Backup ID should not be empty")

		// Differential backup ID should contain reference to full backup
		// Format: YYYYMMDD-HHMMSSF_YYYYMMDD-HHMMSSD
		assert.Contains(t, resp.BackupId, "D",
			"Differential backup ID should contain 'D'")

		t.Logf("Differential backup created successfully with ID: %s", resp.BackupId)

		// Verify differential backup appears in list
		listReq := &backupservicepb.GetShardBackupsRequest{
			Limit: 10,
		}

		listCtx := utils.WithShortDeadline(t)
		listResp, err := backupClient.GetShardBackups(listCtx, listReq)
		require.NoError(t, err, "Listing backups should succeed")

		// Should now have at least 2 backups (full + differential)
		assert.GreaterOrEqual(t, len(listResp.Backups), 2,
			"Should have at least 2 backups (full + differential)")

		t.Logf("Verified %d total backups exist", len(listResp.Backups))
	})

	t.Run("CreateIncrementalBackup", func(t *testing.T) {
		t.Log("Creating incremental backup...")

		req := &backupservicepb.BackupShardRequest{
			TableGroup:   "test",
			Shard:        "default",
			ForcePrimary: false,
			Type:         "incremental",
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		resp, err := backupClient.BackupShard(ctx, req)
		require.NoError(t, err, "Incremental backup should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		assert.NotEmpty(t, resp.BackupId, "Backup ID should not be empty")

		// Incremental backup ID should contain reference to full backup
		// Format: YYYYMMDD-HHMMSSF_YYYYMMDD-HHMMSSI
		assert.Contains(t, resp.BackupId, "I",
			"Incremental backup ID should contain 'I'")

		t.Logf("Incremental backup created successfully with ID: %s", resp.BackupId)

		// Verify incremental backup appears in list
		listReq := &backupservicepb.GetShardBackupsRequest{
			Limit: 10,
		}

		listCtx := utils.WithShortDeadline(t)
		listResp, err := backupClient.GetShardBackups(listCtx, listReq)
		require.NoError(t, err, "Listing backups should succeed")

		// Should now have at least 3 backups (full + differential + incremental)
		assert.GreaterOrEqual(t, len(listResp.Backups), 3,
			"Should have at least 3 backups (full + differential + incremental)")

		t.Logf("Verified %d total backups exist", len(listResp.Backups))
	})
}

func TestBackup_ValidationErrors(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	// Wait for manager to be ready
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)

	// Create backup client connection
	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	backupClient := backupservicepb.NewMultiPoolerBackupServiceClient(conn)

	t.Run("MissingTableGroup", func(t *testing.T) {
		req := &backupservicepb.BackupShardRequest{
			TableGroup:   "", // Missing
			Shard:        "default",
			ForcePrimary: false,
			Type:         "full",
		}

		ctx := utils.WithShortDeadline(t)
		resp, err := backupClient.BackupShard(ctx, req)

		assert.Error(t, err, "Should return error for missing table_group")
		assert.Nil(t, resp, "Response should be nil on error")
		assert.Contains(t, err.Error(), "table_group", "Error should mention table_group")
	})

	t.Run("MissingShard", func(t *testing.T) {
		req := &backupservicepb.BackupShardRequest{
			TableGroup:   "test",
			Shard:        "", // Missing
			ForcePrimary: false,
			Type:         "full",
		}

		ctx := utils.WithShortDeadline(t)
		resp, err := backupClient.BackupShard(ctx, req)

		assert.Error(t, err, "Should return error for missing shard")
		assert.Nil(t, resp, "Response should be nil on error")
		assert.Contains(t, err.Error(), "shard", "Error should mention shard")
	})

	t.Run("MissingType", func(t *testing.T) {
		req := &backupservicepb.BackupShardRequest{
			TableGroup:   "test",
			Shard:        "default",
			ForcePrimary: false,
			Type:         "", // Missing
		}

		ctx := utils.WithShortDeadline(t)
		resp, err := backupClient.BackupShard(ctx, req)

		assert.Error(t, err, "Should return error for missing type")
		assert.Nil(t, resp, "Response should be nil on error")
		assert.Contains(t, err.Error(), "type", "Error should mention type")
	})

	t.Run("InvalidType", func(t *testing.T) {
		req := &backupservicepb.BackupShardRequest{
			TableGroup:   "test",
			Shard:        "default",
			ForcePrimary: false,
			Type:         "invalid", // Invalid type
		}

		ctx := utils.WithShortDeadline(t)
		resp, err := backupClient.BackupShard(ctx, req)

		assert.Error(t, err, "Should return error for invalid type")
		assert.Nil(t, resp, "Response should be nil on error")
		assert.Contains(t, err.Error(), "invalid", "Error should mention invalid type")
	})
}
