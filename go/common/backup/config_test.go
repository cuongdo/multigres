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

package backup_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/backup"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestNewConfig_Filesystem(t *testing.T) {
	loc := &clustermetadatapb.BackupLocation{
		Location: &clustermetadatapb.BackupLocation_Filesystem{
			Filesystem: &clustermetadatapb.FilesystemBackup{
				Path: "/var/backups",
			},
		},
	}

	cfg, err := backup.NewConfig(loc)
	require.NoError(t, err)
	assert.Equal(t, "filesystem", cfg.Type())
}

func TestConfig_FullPath_Filesystem(t *testing.T) {
	loc := &clustermetadatapb.BackupLocation{
		Location: &clustermetadatapb.BackupLocation_Filesystem{
			Filesystem: &clustermetadatapb.FilesystemBackup{
				Path: "/var/backups",
			},
		},
	}

	cfg, err := backup.NewConfig(loc)
	require.NoError(t, err)

	path, err := cfg.FullPath("mydb", "default", "0")
	require.NoError(t, err)
	assert.Equal(t, "/var/backups/mydb/default/0", path)
}

func TestConfig_FullPath_S3(t *testing.T) {
	loc := &clustermetadatapb.BackupLocation{
		Location: &clustermetadatapb.BackupLocation_S3{
			S3: &clustermetadatapb.S3Backup{
				Bucket:    "my-backups",
				Region:    "us-east-1",
				KeyPrefix: "prod/",
			},
		},
	}

	cfg, err := backup.NewConfig(loc)
	require.NoError(t, err)

	path, err := cfg.FullPath("mydb", "default", "0")
	require.NoError(t, err)
	assert.Equal(t, "s3://my-backups/prod/mydb/default/0", path)
}

func TestConfig_PgBackRestConfig_Filesystem(t *testing.T) {
	loc := &clustermetadatapb.BackupLocation{
		Location: &clustermetadatapb.BackupLocation_Filesystem{
			Filesystem: &clustermetadatapb.FilesystemBackup{
				Path: "/var/backups",
			},
		},
	}

	cfg, err := backup.NewConfig(loc)
	require.NoError(t, err)

	pgbrCfg, err := cfg.PgBackRestConfig("multigres")
	require.NoError(t, err)

	assert.Equal(t, "posix", pgbrCfg["repo1-type"])
	assert.Equal(t, "/var/backups", pgbrCfg["repo1-path"])
}

func TestConfig_PgBackRestConfig_S3_Basic(t *testing.T) {
	loc := &clustermetadatapb.BackupLocation{
		Location: &clustermetadatapb.BackupLocation_S3{
			S3: &clustermetadatapb.S3Backup{
				Bucket: "my-backups",
				Region: "us-east-1",
			},
		},
	}

	cfg, err := backup.NewConfig(loc)
	require.NoError(t, err)

	pgbrCfg, err := cfg.PgBackRestConfig("multigres")
	require.NoError(t, err)

	assert.Equal(t, "s3", pgbrCfg["repo1-type"])
	assert.Equal(t, "my-backups", pgbrCfg["repo1-s3-bucket"])
	assert.Equal(t, "us-east-1", pgbrCfg["repo1-s3-region"])
	assert.Equal(t, "auto", pgbrCfg["repo1-s3-key-type"])
	assert.Equal(t, "/multigres", pgbrCfg["repo1-path"])
}

func TestConfig_PgBackRestConfig_S3_WithPrefix(t *testing.T) {
	loc := &clustermetadatapb.BackupLocation{
		Location: &clustermetadatapb.BackupLocation_S3{
			S3: &clustermetadatapb.S3Backup{
				Bucket:    "my-backups",
				Region:    "us-east-1",
				KeyPrefix: "prod/",
			},
		},
	}

	cfg, err := backup.NewConfig(loc)
	require.NoError(t, err)

	pgbrCfg, err := cfg.PgBackRestConfig("multigres")
	require.NoError(t, err)

	assert.Equal(t, "/prod/multigres", pgbrCfg["repo1-path"])
}

func TestConfig_PgBackRestConfig_S3_WithEndpoint(t *testing.T) {
	loc := &clustermetadatapb.BackupLocation{
		Location: &clustermetadatapb.BackupLocation_S3{
			S3: &clustermetadatapb.S3Backup{
				Bucket:   "my-backups",
				Region:   "us-east-1",
				Endpoint: "https://minio.example.com",
			},
		},
	}

	cfg, err := backup.NewConfig(loc)
	require.NoError(t, err)

	pgbrCfg, err := cfg.PgBackRestConfig("multigres")
	require.NoError(t, err)

	assert.Equal(t, "https://minio.example.com", pgbrCfg["repo1-s3-endpoint"])
}
