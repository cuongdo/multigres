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

package planner

import (
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

// logicalReplicationSlotCreator is the function name (lowercase) whose
// presence anywhere in a statement's expression tree triggers session
// pinning. Detection is folded into inspectExpressionFuncCalls so the
// existing FuncCall walk covers it at no extra traversal cost; see
// expressionCheckResult.HasLogicalReplicationSlotCreation.
const logicalReplicationSlotCreator = "pg_create_logical_replication_slot"

// planLogicalReplicationSlotCreation creates a plan that routes the
// statement through a reserved connection with ReasonLogicalReplication.
// The reservation pins the session to a single Postgres backend so
// subsequent polls (pg_logical_slot_get_changes / realtime.list_changes)
// reach the same backend that owns the slot.
//
// astStmt is the (possibly normalized) statement; passed through to the
// route primitive so literals stripped to ParamRefs can be reconstituted
// from bindVars at execution time. Plain Route uses the same mechanism;
// see engine.LogicalReplicationSlotRoute.
func (p *Planner) planLogicalReplicationSlotCreation(sql string, astStmt ast.Stmt) (*engine.Plan, error) {
	p.logger.Debug("planning logical replication slot creation", "sql", sql)
	route := engine.NewLogicalReplicationSlotRoute(p.defaultTableGroup, constants.DefaultShard, sql, astStmt)
	return engine.NewPlan(sql, route), nil
}
