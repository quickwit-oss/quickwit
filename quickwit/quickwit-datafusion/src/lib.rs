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

//! DataFusion-based query execution for Quickwit.
//!
//! ## Architecture
//!
//! The crate is split into two layers:
//!
//! **Generic execution layer** (no data-source-specific code):
//! - [`data_source`] — `QuickwitDataSource` trait (the extension point)
//! - [`session`] — `DataFusionSessionBuilder`: builds sessions from a list of sources
//! - [`catalog`] — `QuickwitSchemaProvider`: routes `table(name)` to the right source
//! - [`flight`] — `QuickwitWorkerSessionBuilder` + `build_quickwit_worker()`
//! - [`resolver`] — `QuickwitWorkerResolver`: `SearcherPool` → Flight URLs
//! - [`task_estimator`] — `QuickwitTaskEstimator`: split-count based task sizing
//! - [`storage`] — `QuickwitObjectStore`: `Storage` → DataFusion `ObjectStore` bridge
//!
//! **Data source implementations** (`sources/`):
//! - [`sources::metrics`] — `MetricsDataSource` for OSS parquet metrics
//!
//! Pomsky adds its own data sources and wraps `DataFusionSessionBuilder` in
//! the `CloudPremService.SubstraitSearch` handler — no Pomsky code needed here.

pub mod catalog;
pub mod data_source;
pub mod flight;
pub mod resolver;
pub mod session;
pub mod sources;
pub mod storage;
pub mod task_estimator;

// Re-export the top-level worker builder for use in grpc.rs.
// Callers get a gRPC service via `worker.into_worker_server()`.
pub use flight::build_quickwit_worker;
pub use session::DataFusionSessionBuilder;

#[cfg(any(test, feature = "testsuite"))]
pub mod test_utils;
