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

use warp::Filter;

fn not_implemented_handler() -> impl warp::Reply {
    warp::reply::with_status(
        "Quickwit was compiled without the `pprof` feature",
        warp::http::StatusCode::NOT_IMPLEMENTED,
    )
}

/// pprof/start disabled
/// pprof/flamegraph disabled
pub fn pprof_handlers() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
{
    let start_profiler = { warp::path!("pprof" / "start").map(not_implemented_handler) };
    let stop_profiler = { warp::path!("pprof" / "flamegraph").map(not_implemented_handler) };
    start_profiler.or(stop_profiler)
}
