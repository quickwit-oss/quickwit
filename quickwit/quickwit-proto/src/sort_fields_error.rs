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

//! Error types for sort fields parsing and validation.

/// Errors arising from sort fields parsing, validation, and time-window arithmetic.
#[derive(Debug, thiserror::Error)]
pub enum SortFieldsError {
    /// Window duration does not meet the divisibility constraint.
    #[error("invalid window duration {duration_secs}s: {reason}")]
    InvalidWindowDuration {
        duration_secs: u32,
        reason: &'static str,
    },

    /// Schema string is syntactically malformed.
    #[error("{0}")]
    MalformedSchema(String),

    /// Version suffix could not be parsed.
    #[error("{0}")]
    BadSortVersion(String),

    /// Sort version is below the minimum accepted (V2-only enforcement).
    #[error("unsupported sort version {version}, minimum is {minimum}")]
    UnsupportedVersion { version: i32, minimum: i32 },

    /// Invalid placement or usage of the `&` LSM cutoff marker.
    #[error("{0}")]
    InvalidCutoffPlacement(String),

    /// Column specification has wrong number of parts.
    #[error("{0}")]
    InvalidColumnFormat(String),

    /// Unknown column type (from suffix or explicit name).
    #[error("{0}")]
    UnknownColumnType(String),

    /// Explicit column type does not match the type inferred from the suffix.
    #[error(
        "column type doesn't match type deduced from suffix for {column}, deduced={from_suffix}, \
         explicit={explicit}"
    )]
    TypeMismatch {
        column: String,
        from_suffix: String,
        explicit: String,
    },

    /// Unrecognized sort direction string.
    #[error("{0}")]
    UnknownSortDirection(String),

    /// Sort direction specified in multiple places (e.g., both prefix and suffix,
    /// or prefix/suffix combined with colon-separated direction).
    #[error("sort direction specified multiple times for column '{0}'")]
    DuplicateDirection(String),

    /// Schema is structurally invalid (missing timestamp, wrong order, etc.).
    #[error("{0}")]
    ValidationError(String),

    /// window_start timestamp cannot be represented as a DateTime.
    #[error("window_start timestamp {timestamp_secs} is out of representable range")]
    WindowStartOutOfRange { timestamp_secs: i64 },
}
