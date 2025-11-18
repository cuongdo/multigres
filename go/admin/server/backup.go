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

package server

import (
	"context"
	"fmt"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	clustermetadata "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Backup starts an async backup of a specific shard
func (s *MultiAdminServer) Backup(ctx context.Context, req *multiadminpb.BackupRequest) (*multiadminpb.BackupResponse, error) {
	s.logger.DebugContext(ctx, "Backup request received",
		"database", req.Database,
		"table_group", req.TableGroup,
		"shard", req.Shard,
		"type", req.Type,
		"force_primary", req.ForcePrimary)

	// Validate request
	if req.Database == "" {
		return nil, status.Error(codes.InvalidArgument, "database cannot be empty")
	}
	if req.TableGroup == "" {
		return nil, status.Error(codes.InvalidArgument, "table_group cannot be empty")
	}
	if req.Shard == "" {
		return nil, status.Error(codes.InvalidArgument, "shard cannot be empty")
	}

	// Create job
	jobID := s.jobTracker.CreateJob(multiadminpb.GetBackupJobStatusResponse_BACKUP, req.Database, req.TableGroup, req.Shard)

	// Start backup in background
	go func() {
		if err := s.executeBackup(context.Background(), jobID, req); err != nil {
			s.logger.ErrorContext(context.Background(), "Backup failed", "job_id", jobID, "error", err)
			s.jobTracker.FailJob(jobID, err.Error())
		}
	}()

	return &multiadminpb.BackupResponse{
		JobId: jobID,
	}, nil
}

// executeBackup performs the actual backup operation
func (s *MultiAdminServer) executeBackup(ctx context.Context, jobID string, req *multiadminpb.BackupRequest) error {
	s.jobTracker.UpdateJobStatus(jobID, multiadminpb.GetBackupJobStatusResponse_RUNNING)

	// Find the primary pooler for this shard
	poolerID, err := s.findPrimaryPoolerID(ctx, req.Database, req.TableGroup, req.Shard)
	if err != nil {
		return fmt.Errorf("failed to find primary pooler: %w", err)
	}

	// Get pooler manager client
	client, conn, err := s.getPoolerManagerClient(ctx, poolerID)
	if err != nil {
		return fmt.Errorf("failed to get pooler manager client: %w", err)
	}
	defer conn.Close()

	// Call backup on the pooler
	backupReq := &multipoolermanagerdata.BackupRequest{
		Type:         req.Type,
		ForcePrimary: req.ForcePrimary,
	}

	resp, err := client.Backup(ctx, backupReq)
	if err != nil {
		return fmt.Errorf("pooler backup failed: %w", err)
	}

	// Mark job as completed
	s.jobTracker.CompleteJob(jobID, resp.BackupId)
	s.logger.InfoContext(ctx, "Backup completed", "job_id", jobID, "backup_id", resp.BackupId)

	return nil
}

// RestoreFromBackup starts an async restore of a specific shard from a backup
func (s *MultiAdminServer) RestoreFromBackup(ctx context.Context, req *multiadminpb.RestoreFromBackupRequest) (*multiadminpb.RestoreFromBackupResponse, error) {
	s.logger.DebugContext(ctx, "RestoreFromBackup request received",
		"database", req.Database,
		"table_group", req.TableGroup,
		"shard", req.Shard,
		"backup_id", req.BackupId,
		"pooler_id", req.PoolerId)

	// Validate request
	if req.Database == "" {
		return nil, status.Error(codes.InvalidArgument, "database cannot be empty")
	}
	if req.TableGroup == "" {
		return nil, status.Error(codes.InvalidArgument, "table_group cannot be empty")
	}
	if req.Shard == "" {
		return nil, status.Error(codes.InvalidArgument, "shard cannot be empty")
	}
	if req.PoolerId == nil {
		return nil, status.Error(codes.InvalidArgument, "pooler_id cannot be empty")
	}

	// Create job
	jobID := s.jobTracker.CreateJob(multiadminpb.GetBackupJobStatusResponse_RESTORE, req.Database, req.TableGroup, req.Shard)

	// Start restore in background
	go func() {
		if err := s.executeRestore(context.Background(), jobID, req); err != nil {
			s.logger.ErrorContext(context.Background(), "Restore failed", "job_id", jobID, "error", err)
			s.jobTracker.FailJob(jobID, err.Error())
		}
	}()

	return &multiadminpb.RestoreFromBackupResponse{
		JobId: jobID,
	}, nil
}

// executeRestore performs the actual restore operation
func (s *MultiAdminServer) executeRestore(ctx context.Context, jobID string, req *multiadminpb.RestoreFromBackupRequest) error {
	s.jobTracker.UpdateJobStatus(jobID, multiadminpb.GetBackupJobStatusResponse_RUNNING)

	// Get pooler manager client for the specified pooler
	client, conn, err := s.getPoolerManagerClient(ctx, req.PoolerId)
	if err != nil {
		s.jobTracker.FailJob(jobID, fmt.Sprintf("failed to get pooler manager client: %v", err))
		return fmt.Errorf("failed to get pooler manager client: %w", err)
	}
	defer conn.Close()

	// Call restore on the pooler
	restoreReq := &multipoolermanagerdata.RestoreFromBackupRequest{
		BackupId: req.BackupId,
	}

	_, err = client.RestoreFromBackup(ctx, restoreReq)
	if err != nil {
		s.jobTracker.FailJob(jobID, fmt.Sprintf("pooler restore failed: %v", err))
		return fmt.Errorf("pooler restore failed: %w", err)
	}

	// Mark job as completed
	// Note: RestoreFromBackup doesn't return a backup_id, so we use the input backup_id
	s.jobTracker.CompleteJob(jobID, req.BackupId)
	s.logger.InfoContext(ctx, "Restore completed", "job_id", jobID, "backup_id", req.BackupId)

	return nil
}

// GetBackupJobStatus checks the status of a backup or restore job
func (s *MultiAdminServer) GetBackupJobStatus(ctx context.Context, req *multiadminpb.GetBackupJobStatusRequest) (*multiadminpb.GetBackupJobStatusResponse, error) {
	s.logger.DebugContext(ctx, "GetBackupJobStatus request received", "job_id", req.JobId)

	// Validate request
	if req.JobId == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id cannot be empty")
	}

	// Get job status from tracker
	jobStatus, err := s.jobTracker.GetJobStatus(req.JobId)
	if err != nil {
		s.logger.DebugContext(ctx, "Job not found", "job_id", req.JobId, "error", err)
		return nil, status.Errorf(codes.NotFound, "job not found: %s", req.JobId)
	}

	s.logger.DebugContext(ctx, "GetBackupJobStatus completed",
		"job_id", req.JobId,
		"status", jobStatus.Status,
		"job_type", jobStatus.JobType)

	return jobStatus, nil
}

// GetBackups lists backup artifacts with optional filtering
func (s *MultiAdminServer) GetBackups(ctx context.Context, req *multiadminpb.GetBackupsRequest) (*multiadminpb.GetBackupsResponse, error) {
	s.logger.DebugContext(ctx, "GetBackups request received",
		"database", req.Database,
		"table_group", req.TableGroup,
		"shard", req.Shard,
		"limit", req.Limit)

	// Validate request
	if req.Database == "" {
		return nil, status.Error(codes.InvalidArgument, "database cannot be empty")
	}
	if req.TableGroup == "" {
		return nil, status.Error(codes.InvalidArgument, "table_group cannot be empty")
	}
	if req.Shard == "" {
		return nil, status.Error(codes.InvalidArgument, "shard cannot be empty")
	}

	// Get all cells from topology
	cells, err := s.ts.GetCellNames(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get cells: %v", err)
	}

	// Aggregate backups from all matching poolers
	var allBackups []*multiadminpb.BackupInfo
	var errors []error

	for _, cell := range cells {
		// Check if we've reached the limit
		if req.Limit > 0 && len(allBackups) >= int(req.Limit) {
			break
		}

		// Find poolers in this cell matching the criteria
		opt := &topo.GetMultiPoolersByCellOptions{
			DatabaseShard: &topo.DatabaseShard{
				Database:   req.Database,
				TableGroup: req.TableGroup,
				Shard:      req.Shard,
			},
		}

		poolers, err := s.ts.GetMultiPoolersByCell(ctx, cell, opt)
		if err != nil {
			s.logger.DebugContext(ctx, "Failed to get poolers in cell", "cell", cell, "error", err)
			errors = append(errors, fmt.Errorf("cell %s: %w", cell, err))
			continue
		}

		// Query each pooler for its backups
		for _, pooler := range poolers {
			// Check if we've reached the limit
			if req.Limit > 0 && len(allBackups) >= int(req.Limit) {
				break
			}

			// Calculate remaining limit
			remainingLimit := req.Limit
			if req.Limit > 0 {
				remainingLimit = req.Limit - uint32(len(allBackups))
			}

			backups, err := s.getBackupsFromPooler(ctx, pooler.Id, remainingLimit)
			if err != nil {
				s.logger.DebugContext(ctx, "Failed to get backups from pooler",
					"pooler", pooler.Id,
					"error", err)
				errors = append(errors, fmt.Errorf("pooler %s: %w", pooler.Id, err))
				continue
			}
			allBackups = append(allBackups, backups...)
		}
	}

	s.logger.DebugContext(ctx, "GetBackups completed",
		"backup_count", len(allBackups),
		"error_count", len(errors))

	return &multiadminpb.GetBackupsResponse{
		Backups: allBackups,
	}, nil
}

// getBackupsFromPooler retrieves backup information from a specific pooler
func (s *MultiAdminServer) getBackupsFromPooler(ctx context.Context, poolerID *clustermetadata.ID, limit uint32) ([]*multiadminpb.BackupInfo, error) {
	// Get pooler info to extract database name
	poolerInfo, err := s.ts.GetMultiPooler(ctx, poolerID)
	if err != nil {
		return nil, fmt.Errorf("failed to get pooler info: %w", err)
	}

	// Get pooler manager client
	client, conn, err := s.getPoolerManagerClient(ctx, poolerID)
	if err != nil {
		return nil, fmt.Errorf("failed to get pooler manager client: %w", err)
	}
	defer conn.Close()

	// Query backups from pooler
	getBackupsReq := &multipoolermanagerdata.GetBackupsRequest{
		Limit: limit,
	}

	resp, err := client.GetBackups(ctx, getBackupsReq)
	if err != nil {
		return nil, fmt.Errorf("pooler getBackups failed: %w", err)
	}

	// Convert multipoolermanagerdata.BackupMetadata to multiadminpb.BackupInfo
	backups := make([]*multiadminpb.BackupInfo, len(resp.Backups))
	for i, b := range resp.Backups {
		// Map BackupMetadata_Status to BackupInfo_BackupStatus
		var backupStatus multiadminpb.BackupInfo_BackupStatus
		switch b.Status {
		case multipoolermanagerdata.BackupMetadata_COMPLETE:
			backupStatus = multiadminpb.BackupInfo_COMPLETE
		case multipoolermanagerdata.BackupMetadata_INCOMPLETE:
			backupStatus = multiadminpb.BackupInfo_INCOMPLETE
		default:
			backupStatus = multiadminpb.BackupInfo_UNKNOWN
		}

		backups[i] = &multiadminpb.BackupInfo{
			BackupId:   b.BackupId,
			Database:   poolerInfo.Database,
			TableGroup: b.TableGroup,
			Shard:      b.Shard,
			Status:     backupStatus,
			// Type, BackupTime, and BackupSizeBytes are not available from BackupMetadata
			// These would need to be added to the multipoolermanagerdata.BackupMetadata proto
			// or queried separately from pgBackRest info command
		}
	}

	return backups, nil
}
