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

//! Column type identification from name suffixes and string names.
//!
//! Type can be specified via Husky-convention suffixes (`__s`, `__i`, `__nf`)
//! or inferred from well-known bare names. The discriminant values match
//! the Go iota exactly for cross-system interoperability.

use std::str::FromStr;

use super::SortFieldsError;

/// Well-known column name for timestamps.
pub const TIMESTAMP: &str = "timestamp";

/// Well-known column name for tiebreaker.
pub const TIEBREAKER: &str = "tiebreaker";

/// Well-known column name for timeseries ID hash.
pub const TIMESERIES_ID: &str = "timeseries_id";

/// Well-known column name for metric value.
pub const METRIC_VALUE: &str = "metric_value";

/// Column type IDs matching Go `types.TypeID` iota values.
///
/// Only the types that appear in sort schemas are included here.
/// The discriminant values MUST match Go exactly for cross-system interop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum ColumnTypeId {
    Int64 = 2,
    Float64 = 10,
    String = 14,
    Sketch = 17,
    CpcSketch = 20,
    ItemSketch = 22,
}

impl ColumnTypeId {
    /// The Husky-convention suffix for this column type.
    ///
    /// Used when serializing back to the string format with explicit types.
    pub fn suffix(self) -> &'static str {
        match self {
            Self::Int64 => "__i",
            Self::Float64 => "__nf",
            Self::String => "__s",
            Self::Sketch => "__sk",
            Self::CpcSketch => "__cpcsk",
            Self::ItemSketch => "__isk",
        }
    }

    /// Human-readable type name matching Go `TypeID.String()`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Int64 => "dense-int64",
            Self::Float64 => "dense-float64",
            Self::String => "dense-string",
            Self::Sketch => "dense-sketch",
            Self::CpcSketch => "dense-cpc-sketch",
            Self::ItemSketch => "dense-item-sketch",
        }
    }

    /// Resolve column type from a column name, stripping any type suffix.
    ///
    /// Returns `(bare_name, type)`. Type resolution order:
    /// 1. Explicit suffix (`__s`, `__i`, `__nf`, etc.) — stripped, type from suffix
    /// 2. Well-known bare name defaults:
    ///    - `timestamp`, `tiebreaker`, `timeseries_id` → Int64
    ///    - `metric_value` → Float64
    ///    - everything else → String
    pub fn from_column_name(name: &str) -> Result<(&str, Self), SortFieldsError> {
        // Try explicit suffixes first (longest match first to avoid ambiguity).
        if let Some(bare) = name.strip_suffix("__isk") {
            return Ok((bare, Self::ItemSketch));
        }
        if let Some(bare) = name.strip_suffix("__cpcsk") {
            return Ok((bare, Self::CpcSketch));
        }
        if let Some(bare) = name.strip_suffix("__sk") {
            return Ok((bare, Self::Sketch));
        }
        if let Some(bare) = name.strip_suffix("__nf") {
            return Ok((bare, Self::Float64));
        }
        if let Some(bare) = name.strip_suffix("__i") {
            return Ok((bare, Self::Int64));
        }
        if let Some(bare) = name.strip_suffix("__s") {
            return Ok((bare, Self::String));
        }

        // No suffix — use well-known name defaults.
        Ok((name, default_type_for_name(name)))
    }
}

/// Default column type and sort direction for a bare column name.
///
/// This is the single source of truth for well-known column defaults.
/// Used by the parser (type inference, default direction), display
/// (suffix omission, direction omission), and validation.
pub struct ColumnDefaults {
    pub column_type: ColumnTypeId,
    /// True if the default sort direction is descending.
    pub descending: bool,
}

/// Well-known name → default type and sort direction lookup table.
///
/// Columns not in this table default to String, ascending.
static WELL_KNOWN_COLUMNS: &[(&str, ColumnDefaults)] = &[
    (
        TIMESTAMP,
        ColumnDefaults {
            column_type: ColumnTypeId::Int64,
            descending: true,
        },
    ),
    (
        "timestamp_secs",
        ColumnDefaults {
            column_type: ColumnTypeId::Int64,
            descending: true,
        },
    ),
    (
        TIEBREAKER,
        ColumnDefaults {
            column_type: ColumnTypeId::Int64,
            descending: false,
        },
    ),
    (
        TIMESERIES_ID,
        ColumnDefaults {
            column_type: ColumnTypeId::Int64,
            descending: false,
        },
    ),
    (
        METRIC_VALUE,
        ColumnDefaults {
            column_type: ColumnTypeId::Float64,
            descending: false,
        },
    ),
    (
        "value",
        ColumnDefaults {
            column_type: ColumnTypeId::Float64,
            descending: false,
        },
    ),
];

const DEFAULT_COLUMN: ColumnDefaults = ColumnDefaults {
    column_type: ColumnTypeId::String,
    descending: false,
};

/// Look up default type and direction for a bare column name.
pub fn column_defaults(name: &str) -> &'static ColumnDefaults {
    WELL_KNOWN_COLUMNS
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, d)| d)
        .unwrap_or(&DEFAULT_COLUMN)
}

/// Default column type for a bare name (convenience wrapper).
pub fn default_type_for_name(name: &str) -> ColumnTypeId {
    column_defaults(name).column_type
}

/// Whether this bare name defaults to descending sort.
pub fn default_is_descending(name: &str) -> bool {
    column_defaults(name).descending
}

impl std::fmt::Display for ColumnTypeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Parse a type name string (e.g., "dense-int64") into a `ColumnTypeId`.
impl FromStr for ColumnTypeId {
    type Err = SortFieldsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "dense-int64" => Ok(Self::Int64),
            "dense-float64" => Ok(Self::Float64),
            "dense-string" => Ok(Self::String),
            "dense-sketch" => Ok(Self::Sketch),
            "dense-cpc-sketch" => Ok(Self::CpcSketch),
            "dense-item-sketch" => Ok(Self::ItemSketch),
            _ => Err(SortFieldsError::UnknownColumnType(format!(
                "unknown column type '{}'",
                s
            ))),
        }
    }
}

/// Convert a proto `column_type` u64 back to a `ColumnTypeId`.
impl TryFrom<u64> for ColumnTypeId {
    type Error = SortFieldsError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            2 => Ok(Self::Int64),
            10 => Ok(Self::Float64),
            14 => Ok(Self::String),
            17 => Ok(Self::Sketch),
            20 => Ok(Self::CpcSketch),
            22 => Ok(Self::ItemSketch),
            _ => Err(SortFieldsError::UnknownColumnType(format!(
                "unknown column type id: {}",
                value
            ))),
        }
    }
}
