// Copyright (C) 2024 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

//! Integration tests for stateright models.
//!
//! Each test runs exhaustive BFS model checking against the small TLA+ config
//! equivalents and verifies all invariants hold.
//!
//! Requires `--features model-checking` to compile and run.

#![cfg(feature = "model-checking")]

use quickwit_dst::models::{
    parquet_data_model::DataModelModel, sort_schema::SortSchemaModel,
    time_windowed_compaction::CompactionModel,
};
use stateright::{Checker, Model};

/// SS-1..SS-5: Sort schema invariants (ADR-002).
/// Mirrors SortSchema_small.cfg: Columns={c1}, RowsPerSplitMax=2,
/// SplitsMax=2, SchemaChangesMax=1.
#[test]
fn exhaustive_sort_schema() {
    let model = SortSchemaModel::small();
    let result = model.checker().spawn_bfs().join();
    result.assert_properties();
    println!(
        "SortSchema: states={}, unique={}",
        result.state_count(),
        result.unique_state_count()
    );
}

/// TW-1..TW-3, CS-1..CS-3, MC-1..MC-4: Compaction invariants (ADR-003).
/// Mirrors TimeWindowedCompaction_small.cfg.
#[test]
fn exhaustive_compaction() {
    let model = CompactionModel::small();
    let result = model.checker().spawn_bfs().join();
    result.assert_properties();
    println!(
        "Compaction: states={}, unique={}",
        result.state_count(),
        result.unique_state_count()
    );
}

/// DM-1..DM-5: Data model invariants (ADR-001).
/// Mirrors ParquetDataModel_small.cfg: Nodes={n1}, MetricNames={m1},
/// TagSets={tags1}, Timestamps={1}, RequestCountMax=3.
#[test]
fn exhaustive_data_model() {
    let model = DataModelModel::small();
    let result = model.checker().spawn_bfs().join();
    result.assert_properties();
    println!(
        "DataModel: states={}, unique={}",
        result.state_count(),
        result.unique_state_count()
    );
}
