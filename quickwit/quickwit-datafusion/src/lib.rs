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

pub(crate) mod catalog;
pub mod data_source;
pub(crate) mod resolver;
pub mod service;
pub mod session;
pub(crate) mod storage_bridge;
pub(crate) mod task_estimator;
pub(crate) mod worker;

pub use resolver::QuickwitWorkerResolver;
pub use service::DataFusionService;
pub use session::DataFusionSessionBuilder;
pub use worker::build_quickwit_worker;
