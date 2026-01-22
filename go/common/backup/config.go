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

package backup

import (
	"errors"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// Config provides a unified interface for backup operations
type Config struct {
	proto *clustermetadatapb.BackupLocation
}

// NewConfig creates a Config from a BackupLocation proto
func NewConfig(loc *clustermetadatapb.BackupLocation) (*Config, error) {
	if loc == nil {
		return nil, errors.New("backup location cannot be nil")
	}

	if err := validate(loc); err != nil {
		return nil, err
	}

	return &Config{proto: loc}, nil
}

// Type returns the backup location type for logging/metrics
func (c *Config) Type() string {
	switch c.proto.Location.(type) {
	case *clustermetadatapb.BackupLocation_Filesystem:
		return "filesystem"
	case *clustermetadatapb.BackupLocation_S3:
		return "s3"
	default:
		return "unknown"
	}
}

// validate checks that the backup location is properly configured
func validate(loc *clustermetadatapb.BackupLocation) error {
	if loc.Location == nil {
		return errors.New("no backup location configured")
	}

	switch v := loc.Location.(type) {
	case *clustermetadatapb.BackupLocation_Filesystem:
		if v.Filesystem == nil {
			return errors.New("filesystem backup config is nil")
		}
		if v.Filesystem.Path == "" {
			return errors.New("filesystem path is required")
		}
	case *clustermetadatapb.BackupLocation_S3:
		if v.S3 == nil {
			return errors.New("s3 backup config is nil")
		}
		if v.S3.Bucket == "" {
			return errors.New("s3 bucket is required")
		}
		if v.S3.Region == "" {
			return errors.New("s3 region is required")
		}
	default:
		return errors.New("unknown backup location type")
	}

	return nil
}
