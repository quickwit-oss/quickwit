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

use quickwit_common::jemalloc_profiled::{start_profiling, stop_profiling};
use serde::Deserialize;
use warp::Filter;
use warp::reply::Reply;

pub fn heap_prof_handlers()
-> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    #[derive(Deserialize)]
    struct ProfilerQueryParams {
        min_alloc_size: Option<u64>,
        backtrace_every: Option<u64>,
    }

    let start_profiler = {
        warp::path!("heap-prof" / "start")
            .and(warp::query::<ProfilerQueryParams>())
            .and_then(move |params: ProfilerQueryParams| start_profiler_handler(params))
    };

    let stop_profiler = { warp::path!("heap-prof" / "stop").and_then(stop_profiler_handler) };

    async fn start_profiler_handler(
        params: ProfilerQueryParams,
    ) -> Result<warp::reply::Response, warp::Rejection> {
        start_profiling(params.min_alloc_size, params.backtrace_every);
        let response =
            warp::reply::with_status("Heap profiling started", warp::http::StatusCode::OK)
                .into_response();
        Ok(response)
    }

    async fn stop_profiler_handler() -> Result<warp::reply::Response, warp::Rejection> {
        stop_profiling();
        let response =
            warp::reply::with_status("Heap profiling stopped", warp::http::StatusCode::OK)
                .into_response();
        Ok(response)
    }

    start_profiler.or(stop_profiler)
}
