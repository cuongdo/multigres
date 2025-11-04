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

// Package grpcbackupservice implements the gRPC server for MultiPoolerBackupService
package grpcbackupservice

import (
	"context"

	"github.com/multigres/multigres/go/mterrors"
	"github.com/multigres/multigres/go/multipooler/backup"
	"github.com/multigres/multigres/go/multipooler/manager"
	backupservicepb "github.com/multigres/multigres/go/pb/multipoolerbackupservice"
	"github.com/multigres/multigres/go/servenv"
)

// backupService is the gRPC wrapper for backup operations
type backupService struct {
	backupservicepb.UnimplementedMultiPoolerBackupServiceServer
	manager *manager.MultiPoolerManager
}

// RegisterBackupServices registers the backup service with the gRPC server
func RegisterBackupServices(senv *servenv.ServEnv, grpc *servenv.GrpcServer) {
	// Register ourselves to be invoked when the manager starts
	manager.RegisterPoolerManagerServices = append(manager.RegisterPoolerManagerServices, func(pm *manager.MultiPoolerManager) {
		if grpc.CheckServiceMap("backup", senv) {
			srv := &backupService{
				manager: pm,
			}
			backupservicepb.RegisterMultiPoolerBackupServiceServer(grpc.Server, srv)
		}
	})
}

// BackupShard performs a backup on a specific shard
func (s *backupService) BackupShard(ctx context.Context, req *backupservicepb.BackupShardRequest) (*backupservicepb.BackupShardResponse, error) {
	configPath := s.manager.GetBackupConfigPath()
	stanzaName := s.manager.GetBackupStanzaName()

	result, err := backup.BackupShard(ctx, configPath, stanzaName, backup.BackupOptions{
		TableGroup:   req.TableGroup,
		Shard:        req.Shard,
		ForcePrimary: req.ForcePrimary,
		Type:         req.Type,
	})
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	return &backupservicepb.BackupShardResponse{
		BackupId: result.BackupID,
	}, nil
}

// RestoreShardFromBackup restores a shard from a backup
func (s *backupService) RestoreShardFromBackup(ctx context.Context, req *backupservicepb.RestoreShardFromBackupRequest) (*backupservicepb.RestoreShardFromBackupResponse, error) {
	pgctldClient := s.manager.GetPgCtldClient()
	configPath := s.manager.GetBackupConfigPath()
	stanzaName := s.manager.GetBackupStanzaName()

	_, err := backup.RestoreShardFromBackup(ctx, pgctldClient, configPath, stanzaName, backup.RestoreOptions{
		BackupID: req.BackupId,
	})
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	return &backupservicepb.RestoreShardFromBackupResponse{}, nil
}

// GetShardBackups retrieves backup information for a shard
func (s *backupService) GetShardBackups(ctx context.Context, req *backupservicepb.GetShardBackupsRequest) (*backupservicepb.GetShardBackupsResponse, error) {
	configPath := s.manager.GetBackupConfigPath()
	stanzaName := s.manager.GetBackupStanzaName()

	result, err := backup.GetShardBackups(ctx, configPath, stanzaName, backup.ListOptions{
		Limit: req.Limit,
	})
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}

	return &backupservicepb.GetShardBackupsResponse{
		Backups: result.Backups,
	}, nil
}
