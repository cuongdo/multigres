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
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
)

func TestGetBackupJobStatus_Success(t *testing.T) {
	logger := slog.Default()
	server := NewMultiAdminServer(nil, logger)

	// Create a job directly in tracker
	jobID := server.jobTracker.CreateJob(multiadminpb.GetBackupJobStatusResponse_BACKUP, "postgres", "test", "0")

	req := &multiadminpb.GetBackupJobStatusRequest{
		JobId: jobID,
	}

	resp, err := server.GetBackupJobStatus(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, jobID, resp.JobId)
	require.Equal(t, multiadminpb.GetBackupJobStatusResponse_PENDING, resp.Status)
}

func TestGetBackupJobStatus_NotFound(t *testing.T) {
	logger := slog.Default()
	server := NewMultiAdminServer(nil, logger)

	req := &multiadminpb.GetBackupJobStatusRequest{
		JobId: "non-existent-job-id",
	}

	_, err := server.GetBackupJobStatus(context.Background(), req)
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.NotFound, st.Code())
}

func TestGetBackupJobStatus_EmptyJobID(t *testing.T) {
	logger := slog.Default()
	server := NewMultiAdminServer(nil, logger)

	req := &multiadminpb.GetBackupJobStatusRequest{
		JobId: "",
	}

	_, err := server.GetBackupJobStatus(context.Background(), req)
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, st.Code())
}
