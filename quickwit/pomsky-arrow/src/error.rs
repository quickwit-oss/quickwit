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

use arrow::datatypes::DataType;

/// Errors produced while mapping schemas or reading fast-field columns.
///
/// This type is the entire decoupling from DataFusion: the originating
/// `tantivy-datafusion` modules returned `datafusion::common::Result` /
/// `DataFusionError`, and re-pointing them at this crate is a matter of adding
/// an `impl From<ArrowError> for DataFusionError` on the consumer side.
#[derive(thiserror::Error, Debug)]
pub enum PomskyArrowError {
    /// An error surfaced by the Arrow library (e.g. building a `RecordBatch`).
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// An error surfaced by tantivy while reading columnar data.
    #[error("tantivy error: {0}")]
    Tantivy(#[from] tantivy::TantivyError),

    /// A requested Arrow output type has no fast-field reader implementation.
    #[error("unsupported arrow data type: {0}")]
    UnsupportedType(DataType),

    /// An invariant was violated while reading columns (with context).
    #[error("{0}")]
    Internal(String),
}

/// Result type used throughout `pomsky-arrow`.
pub type Result<T> = std::result::Result<T, PomskyArrowError>;
