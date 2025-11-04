// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backup

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	"github.com/multigres/multigres/go/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// RestoreOptions contains options for performing a restore
type RestoreOptions struct {
	BackupID string // If empty, restore from the latest backup
}

// RestoreResult contains the result of a restore operation
type RestoreResult struct {
	// Currently empty, but can be extended with metadata about the restore
}

// RestoreShardFromBackup restores a shard from a backup
func RestoreShardFromBackup(ctx context.Context, pgctldClient pgctldpb.PgCtldClient, configPath, stanzaName string, opts RestoreOptions) (*RestoreResult, error) {
	// TODO: Implement restore logic
	// This should:
	// 1. Determine which backup to restore from
	//    - If opts.BackupID is empty, query pgBackRest to find the latest backup
	//    - Use "pgbackrest info --output=json" to list available backups
	//    - Select the most recent backup if BackupID is not specified
	//
	// 2. Stop the PostgreSQL server if it's running
	//    - Check if PostgreSQL is running (e.g., via pg_ctl status or process check)
	//    - If running, issue a graceful shutdown: pg_ctl stop -D <data_dir> -m fast
	//    - Wait for the server to stop completely
	//    - Handle timeout scenarios
	//
	// 3. Execute pgBackRest restore command
	//    - Construct the restore command:
	//      pgbackrest --stanza=<stanza_name> --delta restore
	//    - If BackupID is specified, add: --set=<backup_id>
	//    - The --delta flag allows pgBackRest to preserve valid files and only restore changed/missing ones
	//    - Execute the command using exec.CommandContext
	//    - Capture stdout/stderr for logging
	//    - Monitor for errors and handle appropriately
	//
	// 4. Start the PostgreSQL server after restore completes
	//    - Execute: pg_ctl start -D <data_dir>
	//    - Wait for PostgreSQL to be ready to accept connections
	//    - Use a retry mechanism with exponential backoff
	//
	// 5. Verify the restore was successful
	//    - Connect to PostgreSQL and run a simple query (e.g., SELECT 1)
	//    - Check the restored data directory size/structure
	//    - Verify that WAL replay is working if this is a standby
	//    - Log success or failure with detailed information
	//
	// 6. Handle error scenarios and rollback if necessary
	//    - If restore fails, attempt to restart with the old data (if possible)
	//    - Log all errors with sufficient context for debugging
	//    - Return appropriate error codes (INTERNAL, FAILED_PRECONDITION, etc.)

	// Validate required configuration
	if pgctldClient == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "pgctld_client is required")
	}
	if configPath == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "config_path is required")
	}
	if stanzaName == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "stanza_name is required")
	}

	// Step 1: Stop PostgreSQL server
	stopCtx, stopCancel := context.WithTimeout(ctx, 2*time.Minute)
	defer stopCancel()

	_, err := pgctldClient.Stop(stopCtx, &pgctldpb.StopRequest{
		Mode: "fast", // Fast shutdown mode
	})
	if err != nil {
		return nil, mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("failed to stop PostgreSQL: %v", err))
	}

	// Step 2: Execute pgBackRest restore command
	restoreCtx, restoreCancel := context.WithTimeout(ctx, 30*time.Minute) // Restores can take a long time
	defer restoreCancel()

	args := []string{
		"--stanza=" + stanzaName,
		"--config=" + configPath,
		"--delta", // Preserve valid files and only restore changed/missing ones
	}

	// If a specific backup ID is specified, add the --set flag
	if opts.BackupID != "" {
		args = append(args, "--set="+opts.BackupID)
	}

	args = append(args, "restore")

	cmd := exec.CommandContext(restoreCtx, "pgbackrest", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Attempt to restart PostgreSQL even if restore failed
		startCtx, startCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer startCancel()
		_, _ = pgctldClient.Start(startCtx, &pgctldpb.StartRequest{})

		return nil, mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("pgbackrest restore failed: %v\nOutput: %s", err, string(output)))
	}

	// Step 3: Start PostgreSQL server after successful restore
	startCtx, startCancel := context.WithTimeout(ctx, 2*time.Minute)
	defer startCancel()

	_, err = pgctldClient.Start(startCtx, &pgctldpb.StartRequest{})
	if err != nil {
		return nil, mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("failed to start PostgreSQL after restore: %v", err))
	}

	return &RestoreResult{}, nil
}
