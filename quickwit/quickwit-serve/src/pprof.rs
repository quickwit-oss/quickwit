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

/// pprof/start to start cpu profiling
/// pprof/stop to stop cpu profiling and return a flamegraph
#[cfg(not(feature = "pprof"))]
pub fn pprof_routes() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let start_profiler = {
        warp::path!("pprof" / "start").map(move || {
            warp::reply::with_status(
                "not compiled with pprof feature",
                warp::http::StatusCode::BAD_REQUEST,
            )
        })
    };

    let stop_profiler = {
        warp::path!("pprof" / "stop").map(move || {
            warp::reply::with_status(
                "not compiled with pprof feature",
                warp::http::StatusCode::BAD_REQUEST,
            )
        })
    };

    start_profiler.or(stop_profiler)
}

/// pprof/start to start cpu profiling
/// pprof/stop to stop cpu profiling and return a flamegraph
#[cfg(feature = "pprof")]
pub fn pprof_routes() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    use std::sync::{Arc, Mutex};

    use pprof::ProfilerGuard;
    use warp::reply::Reply;
    let profiler_guard: Arc<Mutex<Option<ProfilerGuard<'_>>>> = Arc::new(Mutex::new(None));

    let start_profiler = {
        let profiler_guard = Arc::clone(&profiler_guard);
        warp::path!("pprof" / "start").map(move || {
            let mut guard = profiler_guard.lock().unwrap();
            if guard.is_none() {
                *guard = Some(pprof::ProfilerGuard::new(100).unwrap());
                warp::reply::with_status("CPU profiling started", warp::http::StatusCode::OK)
            } else {
                warp::reply::with_status(
                    "CPU profiling is already running",
                    warp::http::StatusCode::BAD_REQUEST,
                )
            }
        })
    };

    let stop_profiler = {
        let profiler_guard = Arc::clone(&profiler_guard);
        warp::path!("pprof" / "stop").map(move || {
            let profiler_guard = Arc::clone(&profiler_guard);
            get_flamegraph(profiler_guard)
        })
    };

    fn get_flamegraph(profiler_guard: Arc<Mutex<Option<ProfilerGuard>>>) -> impl warp::Reply {
        let mut guard = profiler_guard.lock().unwrap();
        if let Some(profiler) = guard.take() {
            if let Ok(report) = profiler.report().build() {
                let mut buffer = Vec::new();
                if report.flamegraph(&mut buffer).is_ok() {
                    return warp::reply::with_header(buffer, "Content-Type", "image/svg+xml")
                        .into_response();
                }
            }
            warp::reply::with_status(
                "Failed to generate flamegraph",
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
            )
            .into_response()
        } else {
            warp::reply::with_status(
                "CPU profiling is not running",
                warp::http::StatusCode::BAD_REQUEST,
            )
            .into_response()
        }
    }

    start_profiler.or(stop_profiler)
}
