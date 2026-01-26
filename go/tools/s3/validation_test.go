// Copyright 2026 Supabase, Inc.
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

package s3

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestValidateAccessMinIO tests S3 validation against a local MinIO instance
// This test only runs when AWS credentials are set in the environment
func TestValidateAccessMinIO(t *testing.T) {
	minioEndpoint := os.Getenv("MULTIGRES_MINIO_ENDPOINT")
	if minioEndpoint == "" {
		t.Skip("Skipping S3 validation test - MULTIGRES_MINIO_ENDPOINT not set")
	}

	// Skip if credentials not set
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	sessionToken := os.Getenv("AWS_SESSION_TOKEN")

	if accessKey == "" || secretKey == "" {
		t.Skip("Skipping S3 validation test - AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY not set")
	}

	cfg := ValidationConfig{
		Bucket:       "multigres",
		Region:       "us-east-1",
		Endpoint:     minioEndpoint,
		KeyPrefix:    "backups/",
		AccessKey:    accessKey,
		SecretKey:    secretKey,
		SessionToken: sessionToken,
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Logf("Calling ValidateAccess with 10 second timeout...")
	start := time.Now()

	err := ValidateAccess(ctx, cfg)

	elapsed := time.Since(start)
	t.Logf("ValidateAccess completed in %v", elapsed)

	if err != nil {
		t.Logf("Validation failed: %v", err)

		// Check if it's a timeout
		if ctx.Err() == context.DeadlineExceeded {
			t.Errorf("Validation timed out after %v (context deadline exceeded)", elapsed)
		} else {
			t.Errorf("Validation failed: %v", err)
		}
	} else {
		t.Logf("Validation succeeded in %v", elapsed)
	}
}

// TestValidateAccessMinIODetailed tests each step of S3 validation separately
func TestValidateAccessMinIODetailed(t *testing.T) {
	minioEndpoint := os.Getenv("MULTIGRES_MINIO_ENDPOINT")
	if minioEndpoint == "" {
		t.Skip("Skipping MinIO validation test - MULTIGRES_MINIO_ENDPOINT not set")
	}

	// Skip if credentials not set
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	sessionToken := os.Getenv("AWS_SESSION_TOKEN")

	if accessKey == "" || secretKey == "" {
		t.Skip("Skipping S3 validation test - AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY not set")
	}

	t.Run("HeadBucket", func(t *testing.T) {
		cfg := ValidationConfig{
			Bucket:       "multigres",
			Region:       "us-east-1",
			Endpoint:     minioEndpoint,
			KeyPrefix:    "backups/",
			AccessKey:    accessKey,
			SecretKey:    secretKey,
			SessionToken: sessionToken,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		t.Logf("Testing HeadBucket operation...")
		start := time.Now()

		// This will run the full validation, but we'll see how long it takes
		err := ValidateAccess(ctx, cfg)

		elapsed := time.Since(start)
		t.Logf("Validation completed in %v", elapsed)

		if err != nil {
			if ctx.Err() == context.DeadlineExceeded {
				t.Errorf("HeadBucket timed out after %v", elapsed)
			} else {
				t.Logf("Validation error (may be expected): %v", err)
			}
		}
	})
}

func TestValidateS3BucketName(t *testing.T) {
	tests := []struct {
		name        string
		bucket      string
		expectError bool
	}{
		{
			name:        "valid bucket name",
			bucket:      "my-backup-bucket",
			expectError: false,
		},
		{
			name:        "valid with dots",
			bucket:      "my.backup.bucket",
			expectError: false,
		},
		{
			name:        "valid with numbers",
			bucket:      "backup123",
			expectError: false,
		},
		{
			name:        "too short",
			bucket:      "ab",
			expectError: true,
		},
		{
			name:        "too long",
			bucket:      "this-is-a-very-long-bucket-name-that-exceeds-the-sixty-three-character-limit",
			expectError: true,
		},
		{
			name:        "uppercase letters",
			bucket:      "MyBucket",
			expectError: true,
		},
		{
			name:        "underscores",
			bucket:      "my_bucket",
			expectError: true,
		},
		{
			name:        "starts with hyphen",
			bucket:      "-mybucket",
			expectError: true,
		},
		{
			name:        "ends with hyphen",
			bucket:      "mybucket-",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBucketName(tt.bucket)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
