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

//! Metrics indexing pipeline actors.
//!
//! This module contains the Parquet/DataFusion-based pipeline for time-series
//! metrics data. The pipeline bypasses Tantivy and instead produces Parquet
//! split files:
//!
//! ```text
//! Source → ParquetDocProcessor → ParquetIndexer → ParquetPackager → ParquetUploader → Publisher
//! ```

mod parquet_doc_processor;
mod parquet_indexer;
mod parquet_packager;
mod parquet_uploader;
mod pipeline;

#[cfg(test)]
#[allow(
    clippy::disallowed_methods,
    clippy::needless_borrow,
    clippy::unnecessary_map_or
)]
mod parquet_e2e_test;

pub use parquet_doc_processor::{
    ParquetDocProcessor, ParquetDocProcessorCounters, ParquetDocProcessorError, is_arrow_ipc,
};
pub use parquet_indexer::{ParquetIndexer, ParquetIndexerCounters, ParquetSplitBatch};
pub use parquet_packager::{ParquetBatchForPackager, ParquetPackager, ParquetPackagerCounters};
pub use parquet_uploader::ParquetUploader;
pub use pipeline::MetricsPipeline;
