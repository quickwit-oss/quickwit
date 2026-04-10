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

//! Sort schema equivalence comparison -- direct port of Go `EquivalentSchemas`
//! and `EquivalentSchemasForCompaction`.

use quickwit_proto::sortschema::SortSchema;

/// Base comparison: checks sort_version and all columns match (name, type, direction).
///
/// Hand-rolled comparison (not proto equality) because the Go compactor calls this
/// in tight loops on 10s-100s of thousands of fragments and proto.Equal allocates.
fn equivalent_schemas_base(a: &SortSchema, b: &SortSchema) -> bool {
    if a.sort_version != b.sort_version {
        return false;
    }
    if a.column.len() != b.column.len() {
        return false;
    }
    for (a_col, b_col) in a.column.iter().zip(b.column.iter()) {
        if a_col.name != b_col.name {
            return false;
        }
        if a_col.column_type != b_col.column_type {
            return false;
        }
        if a_col.sort_direction != b_col.sort_direction {
            return false;
        }
    }
    true
}

/// Check if two schemas are equivalent, ignoring names and versioned schema.
///
/// Compares columns, sort_version, and `lsm_comparison_cutoff`.
///
/// Direct port of Go `EquivalentSchemas`.
pub fn equivalent_schemas(a: &SortSchema, b: &SortSchema) -> bool {
    equivalent_schemas_base(a, b) && a.lsm_comparison_cutoff == b.lsm_comparison_cutoff
}

/// Check if two schemas are equivalent for compaction purposes.
///
/// Same as `equivalent_schemas` but ignores `lsm_comparison_cutoff`, providing
/// backward compatibility when old cplanners send steps without LSM cutoff info.
///
/// Direct port of Go `EquivalentSchemasForCompaction`.
pub fn equivalent_schemas_for_compaction(a: &SortSchema, b: &SortSchema) -> bool {
    equivalent_schemas_base(a, b)
}
