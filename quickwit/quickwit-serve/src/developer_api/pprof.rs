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

use std::sync::OnceLock;

use regex::Regex;
use warp::Filter;

fn remove_trailing_numbers(thread_name: &mut String) {
    static REMOVE_TRAILING_NUMBER_PTN: OnceLock<Regex> = OnceLock::new();
    let captures_opt = REMOVE_TRAILING_NUMBER_PTN
        .get_or_init(|| Regex::new(r"^(.*?)[-\d]+$").unwrap())
        .captures(thread_name);
    if let Some(captures) = captures_opt {
        *thread_name = captures[1].to_string();
    }
}

fn frames_post_processor(frames: &mut pprof::Frames) {
    remove_trailing_numbers(&mut frames.thread_name);
}

/// pprof/start to start cpu profiling.
/// pprof/start?duration=5&sampling=1000 to start a short high frequency cpu profiling
/// pprof/flamegraph to stop the current cpu profiling and return a flamegraph or return the last
/// flamegraph
///
/// Query parameters:
/// - duration: duration of the profiling in seconds, default is 30 seconds. max value is 300
/// - sampling: the sampling rate, default is 100, max value is 1000
pub fn pprof_handlers() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
{
    use std::sync::{Arc, Mutex};

    use pprof::ProfilerGuard;
    use serde::Deserialize;
    use tokio::time::{self, Duration};
    use warp::reply::Reply;

    struct ProfilerState {
        profiler_guard: Option<ProfilerGuard<'static>>,
        // We will keep the latest flamegraph and return it at the flamegraph endpoint
        // A new run will overwrite the flamegraph_data
        flamegraph_data: Option<Vec<u8>>,
    }

    let profiler_state = Arc::new(Mutex::new(ProfilerState {
        profiler_guard: None,
        flamegraph_data: None,
    }));

    #[derive(Deserialize)]
    struct ProfilerQueryParams {
        duration: Option<u64>, // max allowed value is 300 seconds, default is 30 seconds
        sampling: Option<i32>, // max value is 1000, default is 100
    }

    let start_profiler = {
        let profiler_state = Arc::clone(&profiler_state);
        warp::path!("pprof" / "start")
            .and(warp::query::<ProfilerQueryParams>())
            .and_then(move |params: ProfilerQueryParams| {
                start_profiler_handler(profiler_state.clone(), params)
            })
    };

    let stop_profiler = {
        let profiler_state = Arc::clone(&profiler_state);
        warp::path!("pprof" / "flamegraph")
            .and_then(move || get_flamegraph_handler(Arc::clone(&profiler_state)))
    };

    async fn start_profiler_handler(
        profiler_state: Arc<Mutex<ProfilerState>>,
        params: ProfilerQueryParams,
    ) -> Result<impl warp::Reply, warp::Rejection> {
        let mut state = profiler_state.lock().unwrap();

        if state.profiler_guard.is_none() {
            let duration = params.duration.unwrap_or(30).min(300);
            let sampling = params.sampling.unwrap_or(100).min(1000);
            state.profiler_guard = Some(pprof::ProfilerGuard::new(sampling).unwrap());
            let profiler_state = Arc::clone(&profiler_state);
            tokio::spawn(async move {
                time::sleep(Duration::from_secs(duration)).await;
                save_flamegraph(profiler_state).await;
            });
            Ok(warp::reply::with_status(
                "CPU profiling started",
                warp::http::StatusCode::OK,
            ))
        } else {
            Ok(warp::reply::with_status(
                "CPU profiling is already running",
                warp::http::StatusCode::BAD_REQUEST,
            ))
        }
    }

    async fn get_flamegraph_handler(
        profiler_state: Arc<Mutex<ProfilerState>>,
    ) -> Result<impl warp::Reply, warp::Rejection> {
        let state = profiler_state.lock().unwrap();

        if let Some(data) = state.flamegraph_data.clone() {
            Ok(warp::reply::with_header(data, "Content-Type", "image/svg+xml").into_response())
        } else {
            Ok(warp::reply::with_status(
                "flamegraph is not available",
                warp::http::StatusCode::BAD_REQUEST,
            )
            .into_response())
        }
    }

    async fn save_flamegraph(profiler_state: Arc<Mutex<ProfilerState>>) {
        let handle = quickwit_common::thread_pool::run_cpu_intensive(move || {
            let mut state = profiler_state.lock().unwrap();
            if let Some(profiler) = state.profiler_guard.take()
                && let Ok(report) = profiler
                    .report()
                    .frames_post_processor(frames_post_processor)
                    .build()
            {
                let mut buffer = Vec::new();
                if report.flamegraph(&mut buffer).is_ok() {
                    state.flamegraph_data = Some(buffer);
                }
            }
        });
        let _ = handle.await;
    }

    start_profiler.or(stop_profiler)
}

#[cfg(test)]
mod tests {
    use super::remove_trailing_numbers;

    #[track_caller]
    fn test_remove_trailing_numbers_aux(thread_name: &str, expected: &str) {
        let mut thread_name = thread_name.to_string();
        remove_trailing_numbers(&mut thread_name);
        assert_eq!(&thread_name, expected);
    }

    #[test]
    fn test_remove_trailing_numbers() {
        test_remove_trailing_numbers_aux("thread-12", "thread");
        test_remove_trailing_numbers_aux("thread12", "thread");
        test_remove_trailing_numbers_aux("thread-", "thread");
        test_remove_trailing_numbers_aux("thread-1-2", "thread");
        test_remove_trailing_numbers_aux("thread-1-2", "thread");
        test_remove_trailing_numbers_aux("12-aa", "12-aa");
    }
}
