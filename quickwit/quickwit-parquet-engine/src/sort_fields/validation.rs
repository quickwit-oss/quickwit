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

//! Sort schema validation -- direct port of Go `ValidateSchema`.

use std::collections::HashSet;

use quickwit_proto::sortschema::{SortColumnDirection, SortSchema};

use super::SortFieldsError;
use super::column_type::{ColumnTypeId, TIEBREAKER, default_is_descending};

/// Name used for the special skip-builder schema that does not require timestamp.
const DEFAULT_SKIP_BUILDER_SCHEMA_NAME: &str = "defaultSkipBuilderSchema";

/// Check if a bare column name is a timestamp column (defaults to descending).
fn is_timestamp_column(name: &str) -> bool {
    default_is_descending(name)
}

/// Validate a sort schema, enforcing all rules from Go `ValidateSchema`.
///
/// Rules:
/// - Schema must have at least one column.
/// - No duplicate column names.
/// - Sort direction must not be Unknown.
/// - `timestamp` must be present (unless schema name is `defaultSkipBuilderSchema`).
/// - `timestamp` must be Int64 and descending (unless it's a msgid schema).
/// - `timestamp` must come before `tiebreaker`.
/// - No non-tiebreaker columns may appear after `timestamp`.
pub fn validate_schema(schema: &SortSchema) -> Result<(), SortFieldsError> {
    if schema.column.is_empty() {
        return Err(SortFieldsError::ValidationError("empty schema".to_string()));
    }

    let mut seen: HashSet<&str> = HashSet::new();
    let is_msgid = schema.version == 2 || schema.name == "defaultMsgIDsSchema";

    for col in &schema.column {
        let name = col.name.as_str();

        if seen.contains(name) {
            return Err(SortFieldsError::ValidationError(format!(
                "column {} is duplicated in schema",
                name
            )));
        }
        seen.insert(name);

        if col.sort_direction == SortColumnDirection::SortDirectionUnknown as i32 {
            return Err(SortFieldsError::ValidationError(format!(
                "column {} does not specify a sort direction in schema",
                name
            )));
        }

        let has_seen_timestamp = seen.iter().any(|s| is_timestamp_column(s));

        if is_timestamp_column(name) {
            if seen.contains(TIEBREAKER) {
                return Err(SortFieldsError::ValidationError(format!(
                    "{} column must come before {} in schema",
                    name, TIEBREAKER
                )));
            }
            if col.sort_direction != SortColumnDirection::SortDirectionDescending as i32
                && !is_msgid
            {
                return Err(SortFieldsError::ValidationError(format!(
                    "{} column must sorted in descending order in schema",
                    name
                )));
            }
            if col.column_type != ColumnTypeId::Int64 as u64 {
                return Err(SortFieldsError::ValidationError(format!(
                    "{} column must be of type int64 in schema",
                    name
                )));
            }
        } else if name == TIEBREAKER {
            if !has_seen_timestamp {
                return Err(SortFieldsError::ValidationError(format!(
                    "timestamp column must come before {} in schema",
                    TIEBREAKER
                )));
            }
        } else if has_seen_timestamp && !is_msgid {
            return Err(SortFieldsError::ValidationError(format!(
                "column {} is after timestamp but timestamp must be the last schema column",
                name
            )));
        }
    }

    let has_timestamp = schema.column.iter().any(|c| is_timestamp_column(&c.name));
    if !has_timestamp && schema.name != DEFAULT_SKIP_BUILDER_SCHEMA_NAME {
        return Err(SortFieldsError::ValidationError(
            "timestamp column is required, but is missing from schema".to_string(),
        ));
    }

    Ok(())
}
