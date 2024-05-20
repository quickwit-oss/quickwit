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

mod debug;
mod log_level;
mod pprof;
mod server;

use debug::debug_handler;
use log_level::log_level_handler;
use pprof::pprof_handlers;
use quickwit_cluster::Cluster;
pub(crate) use server::DeveloperApiServer;
use warp::{Filter, Rejection};

use crate::rest::recover_fn;
use crate::EnvFilterReloadFn;

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
            .or(log_level_handler(env_filter_reload_fn.clone()))
            .or(pprof_handlers()),
        )
        .recover(recover_fn)
}
