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

mod debug;

#[cfg_attr(not(feature = "jemalloc-profiled"), path = "heap_prof_disabled.rs")]
mod heap_prof;
mod log_level;
#[cfg_attr(not(feature = "pprof"), path = "pprof_disabled.rs")]
mod pprof;
mod server;

use axum::routing::get;
use axum::{Extension, Router};
use debug::debug_handler;
use heap_prof::heap_prof_routes;
use log_level::log_level_routes;
use pprof::pprof_routes;
use quickwit_cluster::Cluster;
pub(crate) use server::DeveloperApiServer;

use crate::EnvFilterReloadFn;

#[derive(utoipa::OpenApi)]
#[openapi(paths())]
pub struct DeveloperApi;

/// Creates routes for developer API endpoints
pub(crate) fn developer_routes(
    cluster: Cluster,
    env_filter_reload_fn: EnvFilterReloadFn,
) -> Router {
    Router::new()
        .route("/debug", get(debug_handler))
        .merge(log_level_routes())
        .merge(pprof_routes())
        .merge(heap_prof_routes())
        .layer(Extension(cluster))
        .layer(Extension(env_filter_reload_fn))
}
