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

//! Parquet ingest pipeline.
//!
//! Provides high-throughput metrics ingestion by converting Arrow IPC batches
//! directly to Parquet splits, bypassing Tantivy entirely.

pub mod arrow_sketches;
pub mod processor;
pub mod sketch_processor;

pub use arrow_sketches::{ArrowSketchBatchBuilder, SketchDataPoint};
pub use processor::{IngestError, ParquetIngestProcessor, record_batch_to_ipc};
pub use sketch_processor::SketchParquetIngestProcessor;
