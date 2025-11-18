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
	"testing"

	"github.com/stretchr/testify/require"

	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
)

func TestJobTracker_CreateAndGetJob(t *testing.T) {
	tracker := NewJobTracker()

	jobID := tracker.CreateJob(multiadminpb.GetBackupJobStatusResponse_BACKUP, "postgres", "test", "0")
	require.NotEmpty(t, jobID)

	status, err := tracker.GetJobStatus(jobID)
	require.NoError(t, err)
	require.Equal(t, multiadminpb.GetBackupJobStatusResponse_PENDING, status.Status)
	require.Equal(t, multiadminpb.GetBackupJobStatusResponse_BACKUP, status.JobType)
}

func TestJobTracker_UpdateJobStatus(t *testing.T) {
	tracker := NewJobTracker()
	jobID := tracker.CreateJob(multiadminpb.GetBackupJobStatusResponse_RESTORE, "postgres", "test", "0")

	tracker.UpdateJobStatus(jobID, multiadminpb.GetBackupJobStatusResponse_RUNNING)
	status, err := tracker.GetJobStatus(jobID)
	require.NoError(t, err)
	require.Equal(t, multiadminpb.GetBackupJobStatusResponse_RUNNING, status.Status)

	tracker.CompleteJob(jobID, "test-backup-id")
	status, err = tracker.GetJobStatus(jobID)
	require.NoError(t, err)
	require.Equal(t, multiadminpb.GetBackupJobStatusResponse_COMPLETED, status.Status)
	require.Equal(t, "test-backup-id", status.BackupId)
}

func TestJobTracker_FailJob(t *testing.T) {
	tracker := NewJobTracker()
	jobID := tracker.CreateJob(multiadminpb.GetBackupJobStatusResponse_BACKUP, "postgres", "test", "0")

	tracker.FailJob(jobID, "backup failed: disk full")
	status, err := tracker.GetJobStatus(jobID)
	require.NoError(t, err)
	require.Equal(t, multiadminpb.GetBackupJobStatusResponse_FAILED, status.Status)
	require.Equal(t, "backup failed: disk full", status.ErrorMessage)
}

func TestJobTracker_NonExistentJob(t *testing.T) {
	tracker := NewJobTracker()

	_, err := tracker.GetJobStatus("non-existent-job-id")
	require.Error(t, err)
}
