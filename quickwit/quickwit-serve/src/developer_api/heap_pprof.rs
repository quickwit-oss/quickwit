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

use warp::reply::Reply;
use warp::Filter;

pub fn heap_pprof_handler(
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("heap-pprof" / "dump").and_then(|| async move {
        let Some(prof_ctl_mutex) = jemalloc_pprof::PROF_CTL.as_ref() else {
            return Ok(warp::reply::with_status(
                "PROF_CTL not found",
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
            )
            .into_response());
        };
        let mut prof_ctl = prof_ctl_mutex.lock().await;
        assert!(prof_ctl.activated());
        let pprof: Vec<u8> = prof_ctl.dump_flamegraph().unwrap();
        Ok::<_, warp::Rejection>(
            warp::reply::with_header(pprof, "Content-Type", "image/svg+xml").into_response(),
        )
    })
}
