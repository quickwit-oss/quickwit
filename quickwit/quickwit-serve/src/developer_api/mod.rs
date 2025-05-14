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

use debug::debug_handler;
use heap_prof::heap_prof_handlers;
use log_level::log_level_handler;
use pprof::pprof_handlers;
use quickwit_cluster::Cluster;
pub(crate) use server::DeveloperApiServer;
use warp::{Filter, Rejection};

use crate::EnvFilterReloadFn;
use crate::rest::recover_fn;

#[derive(utoipa::OpenApi)]
#[openapi(paths(debug::debug_handler, log_level::log_level_handler))]
pub struct DeveloperApi;

pub(crate) fn developer_api_routes(
    cluster: Cluster,
    env_filter_reload_fn: EnvFilterReloadFn,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("api" / "developer" / ..)
        .and(
            debug_handler(cluster.clone())
                .or(log_level_handler(env_filter_reload_fn.clone()).boxed())
                .or(pprof_handlers())
                .or(heap_prof_handlers()),
        )
        .recover(recover_fn)
}
