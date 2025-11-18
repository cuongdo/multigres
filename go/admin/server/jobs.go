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
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
)

// jobMetadata stores additional metadata not in the proto
type jobMetadata struct {
	response    *multiadminpb.GetBackupJobStatusResponse
	createdAt   time.Time
	updatedAt   time.Time
	completedAt *time.Time
}

// JobTracker manages async backup/restore jobs
type JobTracker struct {
	mu   sync.RWMutex
	jobs map[string]*jobMetadata
}

// NewJobTracker creates a new job tracker
func NewJobTracker() *JobTracker {
	return &JobTracker{
		jobs: make(map[string]*jobMetadata),
	}
}

// CreateJob creates a new job and returns its ID
func (jt *JobTracker) CreateJob(jobType multiadminpb.GetBackupJobStatusResponse_JobType, database, tableGroup, shard string) string {
	jt.mu.Lock()
	defer jt.mu.Unlock()

	jobID := uuid.New().String()
	now := time.Now()

	jt.jobs[jobID] = &jobMetadata{
		response: &multiadminpb.GetBackupJobStatusResponse{
			JobId:      jobID,
			JobType:    jobType,
			Status:     multiadminpb.GetBackupJobStatusResponse_PENDING,
			Database:   database,
			TableGroup: tableGroup,
			Shard:      shard,
		},
		createdAt: now,
		updatedAt: now,
	}

	return jobID
}

// GetJobStatus retrieves the status of a job
func (jt *JobTracker) GetJobStatus(jobID string) (*multiadminpb.GetBackupJobStatusResponse, error) {
	jt.mu.RLock()
	defer jt.mu.RUnlock()

	job, exists := jt.jobs[jobID]
	if !exists {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}

	return job.response, nil
}

// UpdateJobStatus updates the status of a job
func (jt *JobTracker) UpdateJobStatus(jobID string, status multiadminpb.GetBackupJobStatusResponse_JobStatus) {
	jt.mu.Lock()
	defer jt.mu.Unlock()

	if job, exists := jt.jobs[jobID]; exists {
		job.response.Status = status
		job.updatedAt = time.Now()
	}
}

// CompleteJob marks a job as completed with results
func (jt *JobTracker) CompleteJob(jobID string, backupID string) {
	jt.mu.Lock()
	defer jt.mu.Unlock()

	if job, exists := jt.jobs[jobID]; exists {
		job.response.Status = multiadminpb.GetBackupJobStatusResponse_COMPLETED
		job.response.BackupId = backupID
		now := time.Now()
		job.updatedAt = now
		job.completedAt = &now
	}
}

// FailJob marks a job as failed with error message
func (jt *JobTracker) FailJob(jobID string, errorMsg string) {
	jt.mu.Lock()
	defer jt.mu.Unlock()

	if job, exists := jt.jobs[jobID]; exists {
		job.response.Status = multiadminpb.GetBackupJobStatusResponse_FAILED
		job.response.ErrorMessage = errorMsg
		now := time.Now()
		job.updatedAt = now
		job.completedAt = &now
	}
}

// TODO: Add periodic cleanup to expire old jobs (e.g., completed jobs older than 24 hours)
// to prevent unbounded memory growth
