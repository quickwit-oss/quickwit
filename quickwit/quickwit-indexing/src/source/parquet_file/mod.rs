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

//! Parallel local Parquet source for `quickwit tool local-ingest`.
//! See `docs/internals/parquet-bulk-load.md`.

mod plan;
mod source;
#[cfg(any(test, feature = "testsuite"))]
mod testsuite;

pub use plan::{DEFAULT_PARQUET_BATCH_NUM_ROWS, ParquetLoadPlan};
pub use source::{ParquetSource, ParquetSourceFactory, record_batch_to_ndjson_docs};
#[cfg(any(test, feature = "testsuite"))]
pub use testsuite::{write_f64_values_as_parquet_file, write_json_docs_as_parquet_file};

#[cfg(test)]
mod conversion_tests;
#[cfg(test)]
mod tests;
