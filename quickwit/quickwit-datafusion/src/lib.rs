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

pub mod object_store_registry;
pub(crate) mod storage_bridge;
pub mod worker_resolver;

pub use object_store_registry::QuickwitObjectStoreRegistry;
pub use quickwit_df_core::proto;
// Re-export the framework so consumers (serve, integration tests) can keep
// using `quickwit_datafusion::…` paths.
pub use quickwit_df_core::*;
pub use storage_bridge::QuickwitObjectStore;
pub use worker_resolver::QuickwitWorkerResolver;
