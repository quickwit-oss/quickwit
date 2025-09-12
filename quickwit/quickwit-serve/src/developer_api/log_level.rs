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

use serde::Deserialize;
use tracing::{error, info};
use warp::hyper::StatusCode;
use warp::{Filter, Rejection};

use crate::{EnvFilterReloadFn, with_arg};

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
                            format!("set log level to: [{}]", env_filter.filter),
                            StatusCode::OK,
                        )
                    }
                    Err(err) => {
                        error!(filter = env_filter.filter, %err, "failed to set log level");
                        warp::reply::with_status(
                            format!(
                                "failed to set log level to: [{}], {}",
                                env_filter.filter, err
                            ),
                            StatusCode::BAD_REQUEST,
                        )
                    }
                }
            },
        )
}
