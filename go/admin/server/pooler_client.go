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

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	clustermetadata "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanager "github.com/multigres/multigres/go/pb/multipoolermanager"
)

// TODO: Investigate keeping persistent gRPC connections instead of creating new
// connections for each operation. This would improve performance by avoiding
// connection overhead and could use a connection pool pattern.

// getPoolerManagerClient creates a gRPC client for a pooler's manager service
func (s *MultiAdminServer) getPoolerManagerClient(ctx context.Context, poolerID *clustermetadata.ID) (multipoolermanager.MultiPoolerManagerClient, *grpc.ClientConn, error) {
	// Get pooler info from topology
	poolerInfo, err := s.ts.GetMultiPooler(ctx, poolerID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get pooler info: %w", err)
	}

	// Connect to pooler's manager service (uses grpc port)
	conn, err := grpc.NewClient(poolerInfo.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create pooler manager client: %w", err)
	}

	client := multipoolermanager.NewMultiPoolerManagerClient(conn)
	return client, conn, nil
}

// findPrimaryPoolerID finds the pooler ID for the primary pooler of a given shard
func (s *MultiAdminServer) findPrimaryPoolerID(ctx context.Context, database, tableGroup, shard string) (*clustermetadata.ID, error) {
	// Get all cells from topology
	cells, err := s.ts.GetCellNames(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get cells: %w", err)
	}

	// Search each cell for poolers matching the database/tableGroup/shard
	for _, cell := range cells {
		// Use GetMultiPoolersByCell with filtering options
		opt := &topo.GetMultiPoolersByCellOptions{
			DatabaseShard: &topo.DatabaseShard{
				Database:   database,
				TableGroup: tableGroup,
				Shard:      shard,
			},
		}

		poolers, err := s.ts.GetMultiPoolersByCell(ctx, cell, opt)
		if err != nil {
			s.logger.DebugContext(ctx, "Failed to get poolers in cell", "cell", cell, "error", err)
			continue
		}

		// Find the primary pooler
		for _, pooler := range poolers {
			if pooler.Type == clustermetadata.PoolerType_PRIMARY {
				return pooler.Id, nil
			}
		}
	}

	return nil, fmt.Errorf("no primary pooler found for database=%s, tableGroup=%s, shard=%s", database, tableGroup, shard)
}
