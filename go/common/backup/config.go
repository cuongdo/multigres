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
	"strings"

	"github.com/multigres/multigres/go/common/safepath"
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

// FullPath returns the complete backup path for a database/tablegroup/shard
func (c *Config) FullPath(database, tableGroup, shard string) (string, error) {
	if database == "" {
		return "", errors.New("database cannot be empty")
	}
	if tableGroup == "" {
		return "", errors.New("table group cannot be empty")
	}
	if shard == "" {
		return "", errors.New("shard cannot be empty")
	}

	switch loc := c.proto.Location.(type) {
	case *clustermetadatapb.BackupLocation_Filesystem:
		return filesystemFullPath(loc.Filesystem.Path, database, tableGroup, shard)
	case *clustermetadatapb.BackupLocation_S3:
		return s3FullPath(loc.S3, database, tableGroup, shard)
	default:
		return "", errors.New("unknown backup location type")
	}
}

// filesystemFullPath builds a filesystem backup path
func filesystemFullPath(basePath, database, tableGroup, shard string) (string, error) {
	return safepath.Join(basePath, database, tableGroup, shard)
}

// s3FullPath builds an S3 backup path
func s3FullPath(s3 *clustermetadatapb.S3Backup, database, tableGroup, shard string) (string, error) {
	// Start with bucket
	path := "s3://" + s3.Bucket + "/"

	// Add prefix if set
	if s3.KeyPrefix != "" {
		path += strings.TrimSuffix(s3.KeyPrefix, "/") + "/"
	}

	// Add database/tablegroup/shard
	path += database + "/" + tableGroup + "/" + shard

	return path, nil
}

// PgBackRestConfig returns pgBackRest-specific configuration
func (c *Config) PgBackRestConfig(stanzaName string) (map[string]string, error) {
	switch loc := c.proto.Location.(type) {
	case *clustermetadatapb.BackupLocation_Filesystem:
		return map[string]string{
			"repo1-type": "posix",
			"repo1-path": loc.Filesystem.Path,
		}, nil

	case *clustermetadatapb.BackupLocation_S3:
		config := map[string]string{
			"repo1-type":        "s3",
			"repo1-s3-bucket":   loc.S3.Bucket,
			"repo1-s3-region":   loc.S3.Region,
			"repo1-s3-key-type": "auto", // Use IAM role or env vars
		}

		// Optional endpoint for S3-compatible storage
		if loc.S3.Endpoint != "" {
			config["repo1-s3-endpoint"] = loc.S3.Endpoint
		}

		// Repo path includes prefix if set
		path := "/" + stanzaName
		if loc.S3.KeyPrefix != "" {
			path = "/" + strings.TrimSuffix(loc.S3.KeyPrefix, "/") + path
		}
		config["repo1-path"] = path

		return config, nil

	default:
		return nil, errors.New("unknown backup location type")
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
