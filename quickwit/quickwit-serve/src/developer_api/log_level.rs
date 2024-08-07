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

use serde::Deserialize;
use tracing::{error, info};
use warp::{Filter, Rejection};

use crate::{with_arg, EnvFilterReloadFn};

#[derive(Deserialize)]
struct EnvFilter {
    filter: String,
}

/// Dynamically Quickwit's log level
#[utoipa::path(get, tag = "Debug", path = "/log-level")]
pub fn log_level_handler(
    env_filter_reload_fn: EnvFilterReloadFn,
) -> impl warp::Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path("log-level")
        .and(warp::get().or(warp::post()).unify())
        .and(warp::path::end())
        .and(with_arg(env_filter_reload_fn))
        .and(warp::query::<EnvFilter>())
        .then(
            |env_filter_reload_fn: EnvFilterReloadFn, env_filter: EnvFilter| async move {
                match env_filter_reload_fn(&env_filter.filter) {
                    Ok(_) => {
                        info!(filter = env_filter.filter, "setting log level");
                        warp::reply::with_status(
                            format!("set log level to:[{}]", env_filter.filter),
                            warp::http::StatusCode::OK,
                        )
                    }
                    Err(_) => {
                        error!(filter = env_filter.filter, "invalid log level");
                        warp::reply::with_status(
                            format!("invalid log level:[{}]", env_filter.filter),
                            warp::http::StatusCode::BAD_REQUEST,
                        )
                    }
                }
            },
        )
}
