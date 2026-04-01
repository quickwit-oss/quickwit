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

//! DataFusion-based query execution for Quickwit parquet metrics.
//!
//! Uses DF's standard `ParquetSource` for reading — no custom physical nodes.
//! The only custom parts are:
//! - `MetricsTableProvider`: queries metastore for split discovery + pruning
//! - `MetricsSchemaProvider`: lazy catalog resolution
//! - `MetricsSplitProvider`: trait abstracting metastore split queries
//! - `MetricsWorkerResolver`: worker discovery via SearcherPool

pub mod catalog;
pub mod flight;
pub mod metastore_provider;
pub mod metastore_resolver;
pub mod predicate;
pub mod resolver;
pub mod session;
pub mod storage;
pub mod table_factory;
pub mod table_provider;
pub mod task_estimator;

#[cfg(any(test, feature = "testsuite"))]
pub mod test_utils;
