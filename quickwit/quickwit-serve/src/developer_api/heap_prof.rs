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

use std::time::Duration;

use quickwit_common::jemalloc_profiled::{
    start_profiling, stop_profiling, DEFAULT_MIN_ALLOC_SIZE_FOR_BACKTRACE,
};
use serde::Deserialize;
use warp::reply::Reply;
use warp::Filter;

pub fn heap_prof_handlers(
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    #[derive(Deserialize)]
    struct ProfilerQueryParams {
        duration: Option<u64>,
        backtrace_alloc_size: Option<usize>,
    }

    let start_profiler = {
        warp::path!("heap-prof" / "start")
            .and(warp::query::<ProfilerQueryParams>())
            .and_then(move |params: ProfilerQueryParams| start_profiler_handler(params))
    };

    let stop_profiler = { warp::path!("heap-prof" / "stop").and_then(stop_profiler_handler) };

    async fn start_profiler_handler(
        params: ProfilerQueryParams,
    ) -> Result<hyper::Response<hyper::Body>, warp::Rejection> {
        let min_alloc_size_for_backtrace = params
            .backtrace_alloc_size
            .unwrap_or(DEFAULT_MIN_ALLOC_SIZE_FOR_BACKTRACE);
        if start_profiling(min_alloc_size_for_backtrace).is_err() {
            return Ok(warp::reply::with_status(
                "Heap profiling already running",
                warp::http::StatusCode::SERVICE_UNAVAILABLE,
            )
            .into_response());
        }
        if let Some(duration_secs) = params.duration {
            tokio::time::sleep(Duration::from_secs(duration_secs)).await;
            stop_profiler_handler().await
        } else {
            Ok(
                warp::reply::with_status("Heap profiling started", warp::http::StatusCode::OK)
                    .into_response(),
            )
        }
    }

    async fn stop_profiler_handler() -> Result<hyper::Response<hyper::Body>, warp::Rejection> {
        if let Ok(data) = stop_profiling() {
            Ok(warp::reply::with_header(
                serde_json::to_string_pretty(&data).unwrap(),
                "Content-Type",
                "application/json",
            )
            .into_response())
        } else {
            Ok(warp::reply::with_status(
                "Heap profiling not running",
                warp::http::StatusCode::SERVICE_UNAVAILABLE,
            )
            .into_response())
        }
    }

    start_profiler.or(stop_profiler)
}
