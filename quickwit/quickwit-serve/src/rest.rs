// Copyright (C) 2023 Quickwit, Inc.
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
use quickwit_common::metrics;
use quickwit_proto::ServiceErrorCode;
use tower::make::Shared;
use tower::ServiceBuilder;
use tower_http::compression::predicate::{DefaultPredicate, Predicate, SizeAbove};
use tower_http::compression::CompressionLayer;
use tracing::{error, info};
use warp::{redirect, Filter, Rejection, Reply};

use crate::cluster_api::cluster_handler;
use crate::delete_task_api::delete_task_api_handlers;
use crate::elastic_search_api::elastic_api_handlers;
use crate::format::ApiError;
use crate::health_check_api::health_check_handlers;
use crate::index_api::index_management_handlers;
use crate::indexing_api::indexing_get_handler;
use crate::ingest_api::ingest_api_handlers;
use crate::node_info_handler::node_info_handler;
use crate::search_api::{search_get_handler, search_post_handler, search_stream_handler};
use crate::ui_handler::ui_handler;
use crate::{BodyFormat, QuickwitServices};

/// The minimum size a response body must be in order to
/// be automatically compressed with gzip.
const MINIMUM_RESPONSE_COMPRESSION_SIZE: u16 = 10 << 10;

/// Starts REST services.
pub(crate) async fn start_rest_server(
    rest_listen_addr: SocketAddr,
    quickwit_services: &QuickwitServices,
) -> anyhow::Result<()> {
    info!(rest_listen_addr = %rest_listen_addr, "Starting REST server.");
    let request_counter = warp::log::custom(|_| {
        crate::SERVE_METRICS.http_requests_total.inc();
    });

    // Docs routes
    let api_doc = warp::path("openapi.json")
        .and(warp::get())
        .map(|| warp::reply::json(&crate::openapi::build_docs()));

    // `/health/*` routes.
    let health_check_routes = health_check_handlers(
        quickwit_services.cluster.clone(),
        quickwit_services.indexing_service.clone(),
        quickwit_services.janitor_service.clone(),
    );

    // `/metrics` route.
    let metrics_routes = warp::path("metrics")
        .and(warp::get())
        .map(metrics::metrics_handler);

    let ingest_service = quickwit_services.ingest_service.clone();

    // `/api/v1/*` routes.
    let api_v1_root_url = warp::path!("api" / "v1" / ..);
    let api_v1_routes = cluster_handler(quickwit_services.cluster.clone())
        .or(node_info_handler(
            quickwit_services.build_info,
            quickwit_services.config.clone(),
        ))
        .or(indexing_get_handler(
            quickwit_services.indexing_service.clone(),
        ))
        .or(search_get_handler(quickwit_services.search_service.clone()))
        .or(search_post_handler(
            quickwit_services.search_service.clone(),
        ))
        .or(search_stream_handler(
            quickwit_services.search_service.clone(),
        ))
        .or(ingest_api_handlers(ingest_service.clone()))
        .or(index_management_handlers(
            quickwit_services.index_service.clone(),
            quickwit_services.config.clone(),
        ))
        .or(delete_task_api_handlers(
            quickwit_services.metastore.clone(),
        ))
        .or(elastic_api_handlers());

    let api_v1_root_route = api_v1_root_url.and(api_v1_routes);
    let redirect_root_to_ui_route = warp::path::end()
        .and(warp::get())
        .map(|| redirect(http::Uri::from_static("/ui/search")));

    // Combine all the routes together.
    let rest_routes = api_v1_root_route
        .or(api_doc)
        .or(redirect_root_to_ui_route)
        .or(ui_handler())
        .or(health_check_routes)
        .or(metrics_routes)
        .with(request_counter)
        .recover(recover_fn)
        .boxed();

    let warp_service = warp::service(rest_routes);
    let compression_predicate =
        DefaultPredicate::new().and(SizeAbove::new(MINIMUM_RESPONSE_COMPRESSION_SIZE));

    let service = ServiceBuilder::new()
        .layer(
            CompressionLayer::new()
                .gzip(true)
                .compress_when(compression_predicate),
        )
        .service(warp_service);

    info!("Searcher ready to accept requests at http://{rest_listen_addr}/");

    hyper::Server::bind(&rest_listen_addr)
        .serve(Shared::new(service))
        .await?;
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
    Ok(BodyFormat::PrettyJson.make_reply_for_err(err))
}

fn get_status_with_error(rejection: Rejection) -> ApiError {
    if let Some(error) = rejection.find::<crate::index_api::UnsupportedContentType>() {
        ApiError {
            code: ServiceErrorCode::UnsupportedMediaType,
            message: error.to_string(),
        }
    } else if rejection.is_not_found() {
        ApiError {
            code: ServiceErrorCode::NotFound,
            message: "Route not found".to_string(),
        }
    } else if let Some(error) = rejection.find::<serde_qs::Error>() {
        ApiError {
            code: ServiceErrorCode::BadRequest,
            message: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::filters::body::BodyDeserializeError>() {
        // Happens when the request body could not be deserialized correctly.
        ApiError {
            code: ServiceErrorCode::BadRequest,
            message: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::UnsupportedMediaType>() {
        ApiError {
            code: ServiceErrorCode::UnsupportedMediaType,
            message: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::InvalidQuery>() {
        ApiError {
            code: ServiceErrorCode::BadRequest,
            message: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::LengthRequired>() {
        ApiError {
            code: ServiceErrorCode::BadRequest,
            message: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::MissingHeader>() {
        ApiError {
            code: ServiceErrorCode::BadRequest,
            message: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::InvalidHeader>() {
        ApiError {
            code: ServiceErrorCode::BadRequest,
            message: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::MethodNotAllowed>() {
        ApiError {
            code: ServiceErrorCode::MethodNotAllowed,
            message: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::PayloadTooLarge>() {
        ApiError {
            code: ServiceErrorCode::BadRequest,
            message: error.to_string(),
        }
    } else {
        error!("REST server error: {:?}", rejection);
        ApiError {
            code: ServiceErrorCode::Internal,
            message: "Internal server error.".to_string(),
        }
    }
}
