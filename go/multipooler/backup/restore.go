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
	// Validate required pgctld client, config path, and stanza name
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
		"--log-level-console=info", // Verbose logging to see what pgBackRest is doing
		"--delta",                  // Preserve valid files and only restore changed/missing ones
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
