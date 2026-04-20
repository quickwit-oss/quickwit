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

//! Sort schema string parser -- direct port of Go `StringToSchema`.
//!
//! Parses Husky-style sort schema strings like:
//!   `"metric_name|host|env|timeseries_id|timestamp/V2"`
//! into a `SortSchema` proto with correct column names, types, directions, and version.

use quickwit_proto::sortschema::{SortColumn, SortColumnDirection, SortSchema};

use super::SortFieldsError;
use super::column_type::{ColumnTypeId, default_is_descending};
use super::validation::validate_schema;

/// The minimum sort version we accept. V0 (INCORRECT_TRIM) and V1 (TRIMMED_WITH_BUDGET)
/// are rejected per the strict V2-only decision.
const MINIMUM_SORT_VERSION: i32 = 2;

/// Parse a sort schema string into a `SortSchema` proto.
///
/// Direct port of Go `StringToSchema`. Accepts the format:
///   `[name=]column[|column...][/V#]`
///
/// Each column can be:
///   - `name` (1-part): infer type from suffix, default direction
///   - `+name` or `name+` (1-part with prefix/suffix direction)
///   - `name:+/-` (2-part): infer type from suffix, explicit direction
///   - `name:type:+/-` (3-part): explicit type + verify matches suffix
///
/// Direction (`+`/`-`) may appear as a prefix (`+name`), suffix (`name+`),
/// or after a colon (`name:+`). It is an error for direction to appear in
/// more than one position for a given column (e.g., `+name+` or `+name:+`).
///
/// The `&` marker before a column name indicates the LSM comparison cutoff.
///
/// **V2-only enforcement**: Only sort_version >= 2 is accepted. Unversioned
/// strings (default to 0), V0, and V1 are rejected.
pub fn parse_sort_fields(s: &str) -> Result<SortSchema, SortFieldsError> {
    let mut schema = SortSchema::default();
    let mut input = s;

    // Split on `=` for optional name prefix.
    if let Some((name, rest)) = split_once_max2(input, '=', s)? {
        schema.name = name.to_string();
        input = rest;
    }

    // Split on `/` for version suffix.
    let sort_version = parse_version_suffix(&mut input, s)?;
    if sort_version < MINIMUM_SORT_VERSION {
        return Err(SortFieldsError::UnsupportedVersion {
            version: sort_version,
            minimum: MINIMUM_SORT_VERSION,
        });
    }
    schema.sort_version = sort_version;

    // Parse columns.
    let mut cutoff_marker_count = 0;

    for (i, col_str) in input.split('|').enumerate() {
        let col_remaining = parse_cutoff_marker(col_str, i, &mut cutoff_marker_count, &mut schema)?;

        let (prefix_dir, after_prefix) = strip_direction_prefix(col_remaining);
        let parts: Vec<&str> = after_prefix.split(':').collect();

        let column = match parts.len() {
            3 => parse_3part(parts, prefix_dir, col_str)?,
            2 => parse_2part(parts, prefix_dir, col_str)?,
            1 => {
                schema
                    .column
                    .push(parse_1part(parts[0], prefix_dir, col_str)?);
                continue;
            }
            _ => {
                return Err(SortFieldsError::InvalidColumnFormat(format!(
                    "columns should be of the form 'name:type:+/-' or 'name:+/-' or 'name', \
                     found: {}",
                    col_str
                )));
            }
        };

        schema.column.push(column);
    }

    if cutoff_marker_count > 0 && schema.column.len() < 2 {
        return Err(SortFieldsError::InvalidCutoffPlacement(
            "LSM cutoff marker (&) requires at least 2 columns".to_string(),
        ));
    }

    validate_schema(&schema)?;
    Ok(schema)
}

/// Parse the `/V#` version suffix, updating `input` to point at the columns portion.
/// Returns 0 if no version suffix is present.
fn parse_version_suffix(input: &mut &str, original: &str) -> Result<i32, SortFieldsError> {
    let Some((columns, version_str)) = split_once_max2(input, '/', original)? else {
        return Ok(0);
    };
    let version_str = version_str.strip_prefix('V').ok_or_else(|| {
        SortFieldsError::BadSortVersion(format!(
            "mal-formatted sort schema '{}' -- bad sort version",
            *input
        ))
    })?;
    let version = version_str.parse::<i32>().map_err(|_| {
        SortFieldsError::BadSortVersion(format!(
            "mal-formatted sort schema '{}' parsing sort version",
            *input
        ))
    })?;
    *input = columns;
    Ok(version)
}

/// Handle the `&` LSM cutoff marker at the start of a column string.
/// Returns the remaining column string after stripping `&`.
fn parse_cutoff_marker<'a>(
    col_str: &'a str,
    column_index: usize,
    cutoff_count: &mut usize,
    schema: &mut SortSchema,
) -> Result<&'a str, SortFieldsError> {
    let Some(rest) = col_str.strip_prefix('&') else {
        if col_str.contains('&') {
            return Err(SortFieldsError::MalformedSchema(format!(
                "LSM cutoff marker (&) must appear at the beginning of column name, found in \
                 middle of: {}",
                col_str
            )));
        }
        return Ok(col_str);
    };

    *cutoff_count += 1;
    if *cutoff_count > 1 {
        return Err(SortFieldsError::InvalidCutoffPlacement(
            "only one LSM cutoff marker (&) is allowed per schema".to_string(),
        ));
    }
    schema.lsm_comparison_cutoff = column_index as i32;
    if rest.is_empty() {
        return Err(SortFieldsError::InvalidCutoffPlacement(
            "LSM cutoff marker (&) must be followed by a valid column name".to_string(),
        ));
    }
    if column_index == 0 {
        return Err(SortFieldsError::InvalidCutoffPlacement(
            "LSM cutoff marker (&) cannot be used on the first column as it would ignore all \
             columns"
                .to_string(),
        ));
    }
    if rest.contains('&') {
        return Err(SortFieldsError::MalformedSchema(format!(
            "LSM cutoff marker (&) must appear at the beginning of column name, found in middle \
             of: {}",
            rest
        )));
    }
    Ok(rest)
}

/// Resolve bare name and type from a column name string via `ColumnTypeId::from_column_name`.
fn resolve_name_type(name: &str) -> Result<(&str, ColumnTypeId), SortFieldsError> {
    ColumnTypeId::from_column_name(name).map_err(|_| {
        SortFieldsError::UnknownColumnType(format!(
            "error determining type for column {} from suffix",
            name
        ))
    })
}

/// Parse a 3-part column: `name__suffix:type:+/-`.
fn parse_3part(
    parts: Vec<&str>,
    prefix_dir: Option<i32>,
    col_str: &str,
) -> Result<SortColumn, SortFieldsError> {
    let explicit_type: ColumnTypeId = parts[1].parse().map_err(|_| {
        SortFieldsError::UnknownColumnType(format!(
            "error determining type for column {}: unknown type '{}'",
            parts[0], parts[1]
        ))
    })?;
    let (bare_name, suffix_type) = resolve_name_type(parts[0])?;
    if explicit_type != suffix_type {
        return Err(SortFieldsError::TypeMismatch {
            column: parts[0].to_string(),
            from_suffix: suffix_type.to_string(),
            explicit: explicit_type.to_string(),
        });
    }
    let colon_dir = parse_direction(parts[2])?;
    if prefix_dir.is_some() {
        return Err(SortFieldsError::DuplicateDirection(col_str.to_string()));
    }
    Ok(SortColumn {
        name: bare_name.to_string(),
        column_type: explicit_type as u64,
        sort_direction: colon_dir,
    })
}

/// Parse a 2-part column: `name__suffix:+/-`.
fn parse_2part(
    parts: Vec<&str>,
    prefix_dir: Option<i32>,
    col_str: &str,
) -> Result<SortColumn, SortFieldsError> {
    // Reject direction suffix embedded in the name part: `name-:-` has
    // direction in both the name suffix and the colon position.
    let (embedded_suffix_dir, name_without_suffix) = strip_direction_suffix(parts[0]);
    if embedded_suffix_dir.is_some() {
        return Err(SortFieldsError::DuplicateDirection(col_str.to_string()));
    }
    let (bare_name, col_type) = resolve_name_type(name_without_suffix)?;
    let colon_dir = parse_direction(parts[1])?;
    if prefix_dir.is_some() {
        return Err(SortFieldsError::DuplicateDirection(col_str.to_string()));
    }
    Ok(SortColumn {
        name: bare_name.to_string(),
        column_type: col_type as u64,
        sort_direction: colon_dir,
    })
}

/// Parse a 1-part column: `name__suffix` with optional direction prefix/suffix.
fn parse_1part(
    part: &str,
    prefix_dir: Option<i32>,
    col_str: &str,
) -> Result<SortColumn, SortFieldsError> {
    let (suffix_dir, suffixed_name) = strip_direction_suffix(part);
    if prefix_dir.is_some() && suffix_dir.is_some() {
        return Err(SortFieldsError::DuplicateDirection(col_str.to_string()));
    }
    let (bare_name, col_type) = resolve_name_type(suffixed_name)?;
    let direction = prefix_dir.or(suffix_dir).unwrap_or_else(|| {
        if default_is_descending(bare_name) {
            SortColumnDirection::SortDirectionDescending as i32
        } else {
            SortColumnDirection::SortDirectionAscending as i32
        }
    });
    Ok(SortColumn {
        name: bare_name.to_string(),
        column_type: col_type as u64,
        sort_direction: direction,
    })
}

/// Split `input` on the first `sep`, returning None if no separator.
/// Errors if there are more than 2 parts (i.e., multiple separators).
fn split_once_max2<'a>(
    input: &'a str,
    sep: char,
    original: &str,
) -> Result<Option<(&'a str, &'a str)>, SortFieldsError> {
    let mut iter = input.splitn(3, sep);
    let first = iter.next().unwrap(); // always present
    let second = match iter.next() {
        Some(s) => s,
        None => return Ok(None),
    };
    if iter.next().is_some() {
        return Err(SortFieldsError::MalformedSchema(format!(
            "mal-formatted sort schema '{}'",
            original
        )));
    }
    Ok(Some((first, second)))
}

/// Strip a leading `+` or `-` from a string, returning the direction and remainder.
fn strip_direction_prefix(s: &str) -> (Option<i32>, &str) {
    if let Some(rest) = s.strip_prefix('+') {
        (
            Some(SortColumnDirection::SortDirectionAscending as i32),
            rest,
        )
    } else if let Some(rest) = s.strip_prefix('-') {
        (
            Some(SortColumnDirection::SortDirectionDescending as i32),
            rest,
        )
    } else {
        (None, s)
    }
}

/// Strip a trailing `+` or `-` from a string, returning the direction and trimmed name.
fn strip_direction_suffix(s: &str) -> (Option<i32>, &str) {
    if s.len() > 1 {
        if let Some(rest) = s.strip_suffix('+') {
            return (
                Some(SortColumnDirection::SortDirectionAscending as i32),
                rest,
            );
        }
        if let Some(rest) = s.strip_suffix('-') {
            return (
                Some(SortColumnDirection::SortDirectionDescending as i32),
                rest,
            );
        }
    }
    (None, s)
}

/// Parse a sort direction string ("+" or "-") into the proto enum value.
fn parse_direction(s: &str) -> Result<i32, SortFieldsError> {
    match s {
        "+" => Ok(SortColumnDirection::SortDirectionAscending as i32),
        "-" => Ok(SortColumnDirection::SortDirectionDescending as i32),
        _ => Err(SortFieldsError::UnknownSortDirection(format!(
            "unknown sort direction '{}'",
            s
        ))),
    }
}
