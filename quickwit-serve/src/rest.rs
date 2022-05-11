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

use quickwit_common::metrics;
use tracing::{error, info};
use warp::{Filter, Rejection, Reply};

use crate::cluster_api::cluster_handler;
use crate::error::ServiceErrorCode;
use crate::format::FormatError;
use crate::health_check_api::liveness_check_handler;
use crate::index_api::index_management_handlers;
use crate::indexing_api::indexing_get_handler;
use crate::push_api::{bulk_handler, elastic_bulk_handler, ingest_handler, tail_handler};
use crate::search_api::{search_get_handler, search_post_handler, search_stream_handler};
use crate::ui_handler::ui_handler;
use crate::{Format, QuickwitServices};

/// Start REST service given a HTTP address and a search service.
pub(crate) async fn start_rest_server(
    rest_addr: SocketAddr,
    quickwit_services: &QuickwitServices,
) -> anyhow::Result<()> {
    info!(rest_addr=?rest_addr, "Starting REST service.");
    let request_counter = warp::log::custom(|_| {
        crate::COUNTERS.num_requests.inc();
    });
    let metrics_service = warp::path("metrics")
        .and(warp::get())
        .map(metrics::metrics_handler);
    let api_v1_root_url = warp::path!("api" / "v1" / ..);
    let api_v1_routes = cluster_handler(quickwit_services.cluster_service.clone())
        .or(indexing_get_handler(
            quickwit_services.indexer_service.clone(),
        ))
        .or(search_get_handler(quickwit_services.search_service.clone()))
        .or(search_post_handler(
            quickwit_services.search_service.clone(),
        ))
        .or(search_stream_handler(
            quickwit_services.search_service.clone(),
        ))
        .or(ingest_handler(quickwit_services.push_api_service.clone()))
        .or(tail_handler(quickwit_services.push_api_service.clone()))
        .or(bulk_handler(quickwit_services.push_api_service.clone()))
        .or(elastic_bulk_handler(
            quickwit_services.push_api_service.clone(),
        ))
        .or(index_management_handlers(
            quickwit_services.index_service.clone(),
        ));
    let rest_routes = api_v1_root_url
        .and(api_v1_routes)
        .or(ui_handler())
        .or(liveness_check_handler())
        .or(metrics_service)
        .with(request_counter)
        .recover(recover_fn);

    info!("Searcher ready to accept requests at http://{rest_addr}/");
    warp::serve(rest_routes).run(rest_addr).await;
    Ok(())
}

/// This function returns a formatted error based on the given rejection reason.
pub async fn recover_fn(rejection: Rejection) -> Result<impl Reply, Rejection> {
    if rejection.is_not_found() {
        Ok(Format::PrettyJson.make_reply_for_err(FormatError {
            code: ServiceErrorCode::NotFound,
            error: "Route not found".to_string(),
        }))
    } else if let Some(error) = rejection.find::<warp::filters::body::BodyDeserializeError>() {
        // Happens when the request body could not be deserialized correctly.
        Ok(Format::PrettyJson.make_reply_for_err(FormatError {
            code: ServiceErrorCode::BadRequest,
            error: error.to_string(),
        }))
    } else if let Some(error) = rejection.find::<serde_qs::Error>() {
        Ok(Format::PrettyJson.make_reply_for_err(FormatError {
            code: ServiceErrorCode::BadRequest,
            error: error.to_string(),
        }))
    } else if let Some(error) = rejection.find::<crate::push_api::BulkApiError>() {
        Ok(Format::PrettyJson.make_reply_for_err(FormatError {
            code: ServiceErrorCode::BadRequest,
            error: error.to_string(),
        }))
    } else {
        error!("REST server error: {:?}", rejection);
        Ok(Format::PrettyJson.make_reply_for_err(FormatError {
            code: ServiceErrorCode::Internal,
            error: "Internal server error.".to_string(),
        }))
    }
    // TODO: handle more error types like Rejection([UnsupportedMediaType, MethodNotAllowed])
}
