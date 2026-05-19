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
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

// TestInspectExpressionFuncCalls_DetectsLogicalReplicationSlotCreation
// covers the AST-walk detection of pg_create_logical_replication_slot in
// any expression context. The check is folded into the existing FuncCall
// walker so the cost is one string compare per FuncCall — no second walk.
// Realtime nests the call inside CASE / scalar subquery / etc., so the
// walker must recurse; positional checks on the top-level target list
// would miss it.
func TestInspectExpressionFuncCalls_DetectsLogicalReplicationSlotCreation(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want bool
	}{
		{
			name: "top-level bare call",
			sql:  "SELECT pg_create_logical_replication_slot('s', 'wal2json', true)",
			want: true,
		},
		{
			name: "top-level pg_catalog-qualified call",
			sql:  "SELECT pg_catalog.pg_create_logical_replication_slot('s', 'wal2json', true)",
			want: true,
		},
		{
			name: "nested inside CASE (Realtime shape)",
			sql: `select case when not exists (
                  select 1 from pg_replication_slots where slot_name = 's'
                ) then (
                  select 1 from pg_create_logical_replication_slot('s', 'wal2json', 'true')
                ) else 1 end`,
			want: true,
		},
		{
			name: "nested inside CTE",
			sql:  "WITH x AS (SELECT pg_create_logical_replication_slot('s', 'wal2json', true)) SELECT * FROM x",
			want: true,
		},
		{
			name: "nested inside scalar subquery",
			sql:  "SELECT (SELECT pg_create_logical_replication_slot('s', 'wal2json', true))",
			want: true,
		},
		{
			name: "nested inside WHERE expression",
			sql:  "SELECT 1 WHERE (SELECT pg_create_logical_replication_slot('s', 'wal2json', true)) IS NOT NULL",
			want: true,
		},
		{
			name: "INSERT VALUES with slot creation",
			sql:  "INSERT INTO log (info) VALUES ((pg_create_logical_replication_slot('s', 'wal2json', true)).slot_name)",
			want: true,
		},
		{
			name: "UPDATE SET with slot creation",
			sql:  "UPDATE t SET v = (pg_create_logical_replication_slot('s', 'wal2json', true)).slot_name",
			want: true,
		},
		{
			name: "WITH+INSERT (CTE feeding DML)",
			sql:  "WITH x AS (SELECT pg_create_logical_replication_slot('s', 'wal2json', true)) INSERT INTO log SELECT (x).slot_name FROM x",
			want: true,
		},
		{
			name: "non-creating reference: read pg_replication_slots",
			sql:  "SELECT * FROM pg_replication_slots WHERE slot_name = 'non_existent'",
			want: false,
		},
		{
			name: "non-creating reference: pg_logical_slot_get_changes",
			sql:  "SELECT * FROM pg_logical_slot_get_changes('s', NULL, NULL)",
			want: false,
		},
		{
			name: "plain SELECT",
			sql:  "SELECT 1",
			want: false,
		},
		{
			name: "different function with similar prefix",
			sql:  "SELECT pg_create_physical_replication_slot('s')",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseOne(t, tt.sql)
			res, err := inspectExpressionFuncCalls(stmt)
			require.NoError(t, err)
			assert.Equal(t, tt.want, res.HasLogicalReplicationSlotCreation)
		})
	}
}

// TestPlan_LogicalReplicationSlotCreation_ProducesRoute verifies the
// planner end-to-end: a SELECT containing pg_create_logical_replication_slot
// (in any position) yields a LogicalReplicationSlotRoute primitive.
func TestPlan_LogicalReplicationSlotCreation_ProducesRoute(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "top-level call",
			sql:  "SELECT pg_create_logical_replication_slot('s', 'wal2json', true)",
		},
		{
			name: "pg_catalog-qualified",
			sql:  "SELECT pg_catalog.pg_create_logical_replication_slot('s', 'wal2json', true)",
		},
		{
			name: "nested inside CASE (Realtime shape)",
			sql: `select case when not exists (
                  select 1 from pg_replication_slots where slot_name = 's'
                ) then (
                  select 1 from pg_create_logical_replication_slot('s', 'wal2json', 'true')
                ) else 1 end`,
		},
		{
			name: "CTE",
			sql:  "WITH x AS (SELECT pg_create_logical_replication_slot('s', 'wal2json', true)) SELECT * FROM x",
		},
		{
			name: "INSERT VALUES with slot creation",
			sql:  "INSERT INTO log (info) VALUES ((pg_create_logical_replication_slot('s', 'wal2json', true)).slot_name)",
		},
		{
			name: "UPDATE SET with slot creation",
			sql:  "UPDATE t SET v = (pg_create_logical_replication_slot('s', 'wal2json', true)).slot_name",
		},
		{
			name: "WITH+INSERT (CTE feeding DML)",
			sql:  "WITH x AS (SELECT pg_create_logical_replication_slot('s', 'wal2json', true)) INSERT INTO log SELECT (x).slot_name FROM x",
		},
	}

	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseOne(t, tt.sql)
			plan, err := p.Plan(tt.sql, stmt, testConn.Conn)
			require.NoError(t, err)
			require.NotNil(t, plan)
			_, ok := plan.Primitive.(*engine.LogicalReplicationSlotRoute)
			assert.True(t, ok, "expected LogicalReplicationSlotRoute, got %T", plan.Primitive)
		})
	}
}

// TestPlan_NonSlotCreatingSelect_DoesNotPin verifies that queries that read
// replication-slot metadata or poll slots — but do not create them — fall
// through to a regular Route. Querying pg_replication_slots or calling
// pg_logical_slot_get_changes must NOT trigger pinning.
func TestPlan_NonSlotCreatingSelect_DoesNotPin(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"read pg_replication_slots", "SELECT * FROM pg_replication_slots"},
		{"pg_logical_slot_get_changes call", "SELECT * FROM pg_logical_slot_get_changes('s', NULL, NULL)"},
		{"plain SELECT", "SELECT 1"},
	}

	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseOne(t, tt.sql)
			plan, err := p.Plan(tt.sql, stmt, testConn.Conn)
			require.NoError(t, err)
			require.NotNil(t, plan)
			_, isLR := plan.Primitive.(*engine.LogicalReplicationSlotRoute)
			assert.False(t, isLR, "expected non-LR route, got %T", plan.Primitive)
		})
	}
}
