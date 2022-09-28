// Copyright (C) 2022 Quickwit, Inc.
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

use hyper::http;
use prometheus::{Encoder, TextEncoder};
use quickwit_proto::ServiceErrorCode;
use tracing::{error, info};
use warp::{redirect, Filter, Rejection, Reply};

use crate::cluster_api::cluster_handler;
use crate::delete_task_api::delete_task_api_handlers;
use crate::format::FormatError;
use crate::health_check_api::health_check_handlers;
use crate::index_api::index_management_handlers;
use crate::indexing_api::indexing_get_handler;
use crate::ingest_api::{elastic_bulk_handler, ingest_handler, tail_handler};
use crate::node_info_handler::node_info_handler;
use crate::search_api::{search_get_handler, search_post_handler, search_stream_handler};
use crate::ui_handler::ui_handler;
use crate::{Format, QuickwitServices};

fn metrics_handler() -> impl warp::Reply {
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    let _ = encoder.encode(&metric_families, &mut buffer); // TODO avoid ignoring the error.
    String::from_utf8_lossy(&buffer).to_string()
}

/// Starts REST service given a HTTP address and a search service.
pub(crate) async fn start_rest_server(
    rest_listen_addr: SocketAddr,
    quickwit_services: &QuickwitServices,
) -> anyhow::Result<()> {
    info!(rest_listen_addr = %rest_listen_addr, "Starting REST server.");
    let request_counter = warp::log::custom(|_| {
        crate::SERVE_METRICS.http_requests_total.inc();
    });
    let metrics_service = warp::path("metrics").and(warp::get()).map(metrics_handler);
    let api_v1_root_url = warp::path!("api" / "v1" / ..);
    let api_v1_routes = cluster_handler(quickwit_services.cluster.clone())
        .or(node_info_handler(
            quickwit_services.build_info.clone(),
            quickwit_services.config.clone(),
        ))
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
        .or(ingest_handler(quickwit_services.ingest_api_service.clone()))
        .or(tail_handler(quickwit_services.ingest_api_service.clone()))
        .or(elastic_bulk_handler(
            quickwit_services.ingest_api_service.clone(),
        ))
        .or(index_management_handlers(
            quickwit_services.index_service.clone(),
        ))
        .or(delete_task_api_handlers(
            quickwit_services.metastore.clone(),
            quickwit_services
                .janitor_service
                .as_ref()
                .map(|service| service.delete_task_service_mailbox().clone()),
        ))
        .or(health_check_handlers(quickwit_services.cluster.clone()));
    let api_v1_root_route = api_v1_root_url.and(api_v1_routes);
    let redirect_root_to_ui_route =
        warp::path::end().map(|| redirect(http::Uri::from_static("/ui/search")));
    let rest_routes = api_v1_root_route
        .or(redirect_root_to_ui_route)
        .or(ui_handler())
        .or(metrics_service)
        .with(request_counter)
        .recover(recover_fn);

    info!("Searcher ready to accept requests at http://{rest_listen_addr}/");
    warp::serve(rest_routes).run(rest_listen_addr).await;
    Ok(())
}

/// This function returns a formatted error based on the given rejection reason.
/// The ordering of rejection processing is very important, we need to start
/// with the most specific rejections and end with the most generic. If not, Quickwit
/// will return useless errors to the user.
// TODO: we may want in the future revamp rejections as our usage does not exactly
// match rejection behaviour. When a filter returns a rejection, it means that it
// did not match, but maybe another filter can. Consequently warp will continue
// to try to match other filters. Once a filter is matched, we can enter into
// our own logic and return a proper reply.
// More on this here: https://github.com/seanmonstar/warp/issues/388.
// We may use this work on the PR is merged: https://github.com/seanmonstar/warp/pull/909.
pub async fn recover_fn(rejection: Rejection) -> Result<impl Reply, Rejection> {
    let err = get_status_with_error(rejection);
    Ok(Format::PrettyJson.make_reply_for_err(err))
}

fn get_status_with_error(rejection: Rejection) -> FormatError {
    if rejection.is_not_found() {
        FormatError {
            code: ServiceErrorCode::NotFound,
            error: "Route not found".to_string(),
        }
    } else if let Some(error) = rejection.find::<serde_qs::Error>() {
        FormatError {
            code: ServiceErrorCode::BadRequest,
            error: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::filters::body::BodyDeserializeError>() {
        // Happens when the request body could not be deserialized correctly.
        FormatError {
            code: ServiceErrorCode::BadRequest,
            error: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::UnsupportedMediaType>() {
        FormatError {
            code: ServiceErrorCode::UnsupportedMediaType,
            error: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::InvalidQuery>() {
        FormatError {
            code: ServiceErrorCode::BadRequest,
            error: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::LengthRequired>() {
        FormatError {
            code: ServiceErrorCode::BadRequest,
            error: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::MissingHeader>() {
        FormatError {
            code: ServiceErrorCode::BadRequest,
            error: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::InvalidHeader>() {
        FormatError {
            code: ServiceErrorCode::BadRequest,
            error: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::MethodNotAllowed>() {
        FormatError {
            code: ServiceErrorCode::MethodNotAllowed,
            error: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::PayloadTooLarge>() {
        FormatError {
            code: ServiceErrorCode::BadRequest,
            error: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<crate::ingest_api::BulkApiError>() {
        FormatError {
            code: ServiceErrorCode::BadRequest,
            error: error.to_string(),
        }
    } else {
        error!("REST server error: {:?}", rejection);
        FormatError {
            code: ServiceErrorCode::Internal,
            error: "Internal server error.".to_string(),
        }
    }
}
