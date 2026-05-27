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

//! Quickwit Parquet Engine
//!
//! High-throughput metrics storage and query engine using DataFusion/Parquet.
//! Replaces Tantivy for metrics workloads while maintaining full compatibility
//! with Tantivy for logs/traces.

#![deny(clippy::disallowed_methods)]

pub mod index;
pub mod ingest;
pub mod merge;
pub mod metrics;
pub mod row_keys;
pub mod schema;
pub mod sort_fields;
pub mod sorted_series;
pub mod split;
pub mod storage;
pub mod table_config;
pub mod timeseries_id;
pub mod zonemap;

#[cfg(any(test, feature = "testsuite"))]
pub mod test_helpers;
