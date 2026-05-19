// Copyright 2026 Supabase, Inc.
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

package engine

import (
	"context"
	"fmt"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// LogicalReplicationSlotRoute routes a query that creates a logical
// replication slot (via pg_create_logical_replication_slot) through a
// reserved connection. It sets PendingLogicalReplicationReservation on the
// state so that ScatterConn's StreamExecute creates a reserved connection
// with ReasonLogicalReplication. Mirrors TempTableRoute for symmetry.
//
// The slot, once created on a Postgres backend, is owned by that backend's
// PID. Subsequent calls to pg_logical_slot_get_changes must land on the
// same backend, or postgres rejects them with "replication slot is active
// for PID N". Pinning the session via the reservation mechanism guarantees
// stickiness for the lifetime of the connection.
type LogicalReplicationSlotRoute struct {
	TableGroup string
	Shard      string
	Query      string
}

// NewLogicalReplicationSlotRoute creates a new LogicalReplicationSlotRoute primitive.
func NewLogicalReplicationSlotRoute(tableGroup, shard, sql string) *LogicalReplicationSlotRoute {
	return &LogicalReplicationSlotRoute{TableGroup: tableGroup, Shard: shard, Query: sql}
}

// StreamExecute sets the logical-replication reservation flag and delegates
// to StreamExecute. ScatterConn will see the flag and create a reserved
// connection with ReasonLogicalReplication.
func (t *LogicalReplicationSlotRoute) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	_ []*ast.A_Const,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	state.PendingLogicalReplicationReservation = true
	return exec.StreamExecute(ctx, conn, t.TableGroup, t.Shard, t.Query, nil, state, callback)
}

// PortalStreamExecute delegates to StreamExecute. The extended-protocol path
// can reach here only via a composed primitive; current dispatch routes
// pg_create_logical_replication_slot through the simple-query Plan path.
func (t *LogicalReplicationSlotRoute) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	_ *preparedstatement.PortalInfo,
	_ int32,
	_ bool,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return t.StreamExecute(ctx, exec, conn, state, nil, callback)
}

func (t *LogicalReplicationSlotRoute) GetTableGroup() string { return t.TableGroup }

// GetQuery returns the SQL query.
func (t *LogicalReplicationSlotRoute) GetQuery() string { return t.Query }

// String returns a description of the primitive for debugging.
func (t *LogicalReplicationSlotRoute) String() string {
	return fmt.Sprintf("LogicalReplicationSlotRoute(%s)", t.Query)
}

// Ensure LogicalReplicationSlotRoute implements Primitive interface.
var _ Primitive = (*LogicalReplicationSlotRoute)(nil)
