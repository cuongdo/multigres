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
// reserved connection. The session is pinned to a single Postgres
// backend for the lifetime of the connection so subsequent calls to
// pg_logical_slot_get_changes / realtime.list_changes land on the same
// backend that owns the slot — otherwise postgres rejects them with
// "replication slot is active for PID N".
//
// Behaviorally, this is a Route with one side effect: it sets
// PendingLogicalReplicationReservation on the session state so ScatterConn
// adds ReasonLogicalReplication when it mints (or augments) the reserved
// connection for this request. Embedding Route reuses its NormalizedAST
// reconstruction and portal-binding handling, both of which are
// load-bearing for cacheable statement shapes (SELECT / INSERT / UPDATE /
// DELETE) where literals like the slot name have been normalized to
// ParamRefs and must be reconstituted before the SQL reaches postgres.
//
// Known limitation: wrapped EXECUTE forms (EXPLAIN EXECUTE,
// CREATE TABLE ... AS EXECUTE) referencing a PREPAREd slot-creation
// statement are not pinned. The walker that detects the FuncCall runs
// before unwrap, when the prepared statement's body is still just a name
// reference, so the trigger fires only if the wrapped EXECUTE is later
// unwrapped and re-walked. Realtime does not use this shape; documenting
// rather than fixing.
type LogicalReplicationSlotRoute struct {
	Route
}

// NewLogicalReplicationSlotRoute creates a LogicalReplicationSlotRoute.
// astStmt is the (normalized) statement and is stored for SQL
// reconstruction when bindVars are supplied at execution time. Pass nil
// for non-cached plans where literals were never stripped.
func NewLogicalReplicationSlotRoute(tableGroup, shard, sql string, astStmt ast.Stmt) *LogicalReplicationSlotRoute {
	return &LogicalReplicationSlotRoute{
		Route: Route{
			TableGroup:    tableGroup,
			Shard:         shard,
			Query:         sql,
			NormalizedAST: astStmt,
		},
	}
}

// StreamExecute sets the pending logical-replication reservation flag and
// delegates to the embedded Route. ScatterConn consumes the flag and
// includes ReasonLogicalReplication in the reservation it mints (or
// augments) for this request.
func (t *LogicalReplicationSlotRoute) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	bindVars []*ast.A_Const,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	state.PendingLogicalReplicationReservation = true
	return t.Route.StreamExecute(ctx, exec, conn, state, bindVars, callback)
}

// PortalStreamExecute sets the pending reservation flag and delegates to
// the embedded Route's portal path. This preserves the wire-format Bind
// values that the portal carries; without it, parameterized slot
// creation via the extended protocol would lose its bindings and fail.
func (t *LogicalReplicationSlotRoute) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	maxRows int32,
	includeDescribe bool,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	state.PendingLogicalReplicationReservation = true
	return t.Route.PortalStreamExecute(ctx, exec, conn, state, portalInfo, maxRows, includeDescribe, callback)
}

// String returns a description of the primitive for debugging and
// observability. Overrides the embedded Route.String() so logs and
// span attributes distinguish the two.
func (t *LogicalReplicationSlotRoute) String() string {
	return fmt.Sprintf("LogicalReplicationSlotRoute(%s)", t.Query)
}

// Ensure LogicalReplicationSlotRoute implements Primitive interface.
var _ Primitive = (*LogicalReplicationSlotRoute)(nil)
