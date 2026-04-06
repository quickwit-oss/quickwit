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
//! - [`worker`] — `QuickwitWorkerSessionBuilder` + `build_quickwit_worker()`
//! - [`resolver`] — `QuickwitWorkerResolver`: default `SearcherPool`-backed worker URL resolver
//! - [`task_estimator`] — `QuickwitTaskEstimator`: split-count based task sizing
//! - [`storage_bridge`] — `QuickwitObjectStore`: `quickwit_storage::Storage` → `object_store::ObjectStore` adapter
//! - [`substrait`] — `QuickwitSubstraitConsumer`: routes Substrait `ReadRel` to data sources
//!
//! **Data source implementations** (`sources/`):
//! - [`sources::metrics`] — `MetricsDataSource` for OSS parquet metrics
//!
//! ## Worker URL resolution
//!
//! The default worker resolver (`QuickwitWorkerResolver`) maps `SearcherPool`
//! socket addresses to `http[s]://` URLs.  downstream callers or other deployments with
//! different service discovery (e.g., Consul, DD-internal DNS) can supply their
//! own resolver via `DataFusionSessionBuilder::with_worker_resolver()`.

pub(crate) mod catalog;
pub mod data_source;
pub(crate) mod resolver;
pub mod service;
pub mod session;
pub mod sources;
pub(crate) mod storage_bridge;
pub(crate) mod substrait;
pub(crate) mod task_estimator;
pub(crate) mod worker;

// Re-export the top-level types for use in quickwit-serve and downstream callers.
pub use resolver::QuickwitWorkerResolver;
pub use service::DataFusionService;
pub use session::DataFusionSessionBuilder;
pub use worker::build_quickwit_worker;

#[cfg(any(test, feature = "testsuite"))]
pub mod test_utils;
