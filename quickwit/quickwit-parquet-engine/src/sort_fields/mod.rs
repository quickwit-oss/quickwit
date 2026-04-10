// Copyright 2021-Present Datadog, Inc.
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

//! Sort fields types, parsing, and time-window arithmetic for metrics compaction.
//!
//! The sort fields define how rows are ordered within a Parquet split,
//! which determines merge and compaction behavior. The parser is a direct
//! port of Husky's Go `schemautils.go` for cross-system interoperability.

// TableConfig.effective_sort_fields() is wired into ParquetWriter at construction
// time. The writer resolves sort field names to physical ParquetField columns;
// columns not yet in the schema (e.g., timeseries_id) are skipped during sort
// but recorded in metadata.
//
// TODO(Phase 32): Wire per-index TableConfig into IndexConfig so each index can
// override the default sort fields. Currently all metrics indexes use
// ProductType::Metrics default.
//
// When accepting user-supplied sort_fields for metrics indexes, validation MUST
// reject schemas that do not include timeseries_id__i immediately before timestamp.
// The timeseries_id tiebreaker is mandatory for metrics to ensure deterministic
// sort order across splits with identical tag combinations.

pub mod column_type;
pub mod display;
pub mod equivalence;
pub mod parser;
pub mod validation;
pub mod window;

#[cfg(test)]
mod tests;

// Public API re-exports.
pub use column_type::ColumnTypeId;
pub use display::{schema_to_string, schema_to_string_short};
pub use equivalence::{equivalent_schemas, equivalent_schemas_for_compaction};
pub use parser::parse_sort_fields;
pub use quickwit_proto::SortFieldsError;
pub use validation::validate_schema;
pub use window::{validate_window_duration, window_start};
