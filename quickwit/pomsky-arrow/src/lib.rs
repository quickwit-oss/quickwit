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

//! `pomsky-arrow` turns Pomsky's columnar storage (tantivy fast fields) into
//! Arrow [`RecordBatch`](arrow::record_batch::RecordBatch)es.
//!
//! It is a DataFusion-free extraction of the core of `tantivy-datafusion`
//! (`fast_field_reader`). Two independent consumers share this small surface:
//! the Trino columnar `SearchSplit` endpoint and the full DataFusion / SQL
//! connector.
//!
//! # What this crate owns
//!
//! - **Column reading** — given an already-opened `SegmentReader`, a projected Arrow schema, and a
//!   [`DocSelection`], read *only* those columns for *only* those docs into a `RecordBatch`
//!   ([`read_segment_columns`]).
//!
//! # What this crate does NOT own (the caller's job)
//!
//! - Opening splits / storage / caches (caller passes an opened `SegmentReader`).
//! - Query parsing / execution (the caller's query engine produces the doc-id set).
//! - gRPC / Arrow-IPC framing, split listing, metastore, DataFusion.
//!
//! This boundary keeps the crate dependency-light (`tantivy` + `arrow` +
//! `thiserror`), signal-agnostic (metrics / traces / logs), and independently
//! testable.

pub mod error;
pub mod fast_field_reader;
pub mod warmup;

use arrow::datatypes::Field;
pub use error::{ArrowError, Result};
pub use fast_field_reader::{DictCache, DocSelection, read_segment_columns};
pub use warmup::warm_up_fast_fields;

/// Arrow field metadata key used when a projected column should read from a
/// different physical tantivy fast-field name than its logical Arrow name.
pub const FAST_FIELD_READ_NAME_METADATA_KEY: &str = "pomsky_arrow.read_name";

/// Returns the physical tantivy fast-field name to read for `field`.
///
/// Defaults to the field's own name, unless overridden via the
/// [`FAST_FIELD_READ_NAME_METADATA_KEY`] metadata entry.
pub fn fast_field_read_name(field: &Field) -> &str {
    match field.metadata().get(FAST_FIELD_READ_NAME_METADATA_KEY) {
        Some(read_name) => read_name.as_str(),
        None => field.name().as_str(),
    }
}
