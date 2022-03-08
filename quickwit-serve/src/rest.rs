// Copyright (C) 2021 Quickwit, Inc.
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

use std::net::SocketAddr;
use std::sync::Arc;

use quickwit_cluster::service::ClusterServiceImpl;
use quickwit_common::metrics;
use quickwit_search::SearchServiceImpl;
use tracing::info;
use warp::{Filter, Rejection, Reply};

use crate::cluster_api::cluster_handler;
use crate::error::ApiError;
use crate::health_check_api::liveness_check_handler;
use crate::search_api::{search_get_handler, search_post_handler, search_stream_handler};
use crate::Format;

/// Start REST service given a HTTP address and a search service.
pub async fn start_rest_service(
    rest_addr: SocketAddr,
    search_service: Arc<SearchServiceImpl>,
    cluster_service: Arc<ClusterServiceImpl>,
) -> anyhow::Result<()> {
    info!(rest_addr=?rest_addr, "Starting REST service.");
    let request_counter = warp::log::custom(|_| {
        crate::COUNTERS.num_requests.inc();
    });
    let metrics_service = warp::path("metrics")
        .and(warp::get())
        .map(metrics::metrics_handler);
    let rest_routes = liveness_check_handler()
        .or(cluster_handler(cluster_service))
        .or(search_get_handler(search_service.clone()))
        .or(search_post_handler(search_service.clone()))
        .or(search_stream_handler(search_service))
        .or(metrics_service)
        .with(request_counter)
        .recover(recover_fn);
    warp::serve(rest_routes).run(rest_addr).await;
    Ok(())
}

/// This function returns a formated error based on the given rejection reason.
pub async fn recover_fn(rejection: Rejection) -> Result<impl Reply, Rejection> {
    // TODO handle more errors.
    match rejection.find::<serde_qs::Error>() {
        Some(err) => {
            // The querystring was incorrect.
            Ok(
                Format::PrettyJson.make_reply(Err::<(), ApiError>(ApiError::InvalidArgument(
                    err.to_string(),
                ))),
            )
        }
        None => Ok(Format::PrettyJson.make_reply(Err::<(), ApiError>(ApiError::NotFound))),
    }
}
