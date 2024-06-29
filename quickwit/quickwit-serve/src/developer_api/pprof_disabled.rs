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
