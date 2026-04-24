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

//! Generic DataFusion runtime framework.
//!
//! Provides the session builder, query service, distributed worker, and
//! Substrait consumer with no Quickwit domain coupling. Quickwit-specific
//! integrations (metrics, tantivy, ...) live in downstream crates that
//! implement the runtime and Substrait extension traits from [`data_source`]
//! and register native DataFusion catalogs/schemas with the session builder.
//!
//! ## Features
//!
//! - `grpc` (off by default) — compiles the `proto` module (generated from `datafusion.proto`) and
//!   the `grpc` module (tonic server adapter for [`service::DataFusionService`]). Enable this only
//!   when building the Quickwit server; downstream connector crates should leave it off.

pub mod data_source;
pub mod service;
pub mod session;
pub mod substrait;
pub mod task_estimator;
pub mod worker;

#[cfg(feature = "grpc")]
pub mod grpc;
#[cfg(feature = "grpc")]
pub mod proto;

pub use data_source::{
    QuickwitRuntimePlugin, QuickwitRuntimeRegistration, QuickwitSubstraitConsumerExt,
};
pub use datafusion::execution::SendableRecordBatchStream;
pub use datafusion_distributed::{Worker, WorkerResolver};
pub use service::DataFusionService;
pub use session::DataFusionSessionBuilder;
pub use task_estimator::DataSourceExecPartitionEstimator;
pub use worker::build_worker;
