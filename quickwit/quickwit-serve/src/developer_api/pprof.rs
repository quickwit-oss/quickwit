// Copyright (C) 2024 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use warp::Filter;

/// pprof/start disabled
/// pprof/flamegraph disabled
#[cfg(not(feature = "pprof"))]
pub fn pprof_handlers() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
{
    let not_implemented_handler = || {
        warp::reply::with_status(
            "Quickwit was compiled without the `pprof` feature",
            warp::http::StatusCode::NOT_IMPLEMENTED,
        )
    };
    let start_profiler = { warp::path!("pprof" / "start").map(not_implemented_handler) };
    let stop_profiler = { warp::path!("pprof" / "flamegraph").map(not_implemented_handler) };
    start_profiler.or(stop_profiler)
}

/// pprof/start to start cpu profiling.
/// pprof/start?duration=5&sampling=1000 to start a short high frequency cpu profiling
/// pprof/flamegraph to stop the current cpu profiling and return a flamegraph or return the last
/// flamegraph
///
/// Query parameters:
/// - duration: duration of the profiling in seconds, default is 30 seconds. max value is 300
/// - sampling: the sampling rate, default is 100, max value is 1000
#[cfg(feature = "pprof")]
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
            if let Some(profiler) = state.profiler_guard.take() {
                if let Ok(report) = profiler.report().build() {
                    let mut buffer = Vec::new();
                    if report.flamegraph(&mut buffer).is_ok() {
                        state.flamegraph_data = Some(buffer);
                    }
                }
            }
        });
        let _ = handle.await;
    }

    start_profiler.or(stop_profiler)
}
