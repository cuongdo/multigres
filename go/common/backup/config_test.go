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

func TestNewConfig_Validation(t *testing.T) {
	tests := []struct {
		name    string
		loc     *clustermetadatapb.BackupLocation
		wantErr string
	}{
		{
			name:    "nil location",
			loc:     nil,
			wantErr: "backup location cannot be nil",
		},
		{
			name:    "no location set",
			loc:     &clustermetadatapb.BackupLocation{},
			wantErr: "no backup location configured",
		},
		{
			name: "filesystem missing path",
			loc: &clustermetadatapb.BackupLocation{
				Location: &clustermetadatapb.BackupLocation_Filesystem{
					Filesystem: &clustermetadatapb.FilesystemBackup{
						Path: "",
					},
				},
			},
			wantErr: "filesystem path is required",
		},
		{
			name: "s3 missing bucket",
			loc: &clustermetadatapb.BackupLocation{
				Location: &clustermetadatapb.BackupLocation_S3{
					S3: &clustermetadatapb.S3Backup{
						Region: "us-east-1",
					},
				},
			},
			wantErr: "s3 bucket is required",
		},
		{
			name: "s3 missing region",
			loc: &clustermetadatapb.BackupLocation{
				Location: &clustermetadatapb.BackupLocation_S3{
					S3: &clustermetadatapb.S3Backup{
						Bucket: "my-backups",
					},
				},
			},
			wantErr: "s3 region is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := backup.NewConfig(tt.loc)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestConfig_FullPath_Validation(t *testing.T) {
	loc := &clustermetadatapb.BackupLocation{
		Location: &clustermetadatapb.BackupLocation_Filesystem{
			Filesystem: &clustermetadatapb.FilesystemBackup{
				Path: "/var/backups",
			},
		},
	}

	cfg, err := backup.NewConfig(loc)
	require.NoError(t, err)

	tests := []struct {
		name       string
		database   string
		tableGroup string
		shard      string
		wantErr    string
	}{
		{
			name:       "empty database",
			database:   "",
			tableGroup: "default",
			shard:      "0",
			wantErr:    "database cannot be empty",
		},
		{
			name:       "empty tableGroup",
			database:   "mydb",
			tableGroup: "",
			shard:      "0",
			wantErr:    "table group cannot be empty",
		},
		{
			name:       "empty shard",
			database:   "mydb",
			tableGroup: "default",
			shard:      "",
			wantErr:    "shard cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := cfg.FullPath(tt.database, tt.tableGroup, tt.shard)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
