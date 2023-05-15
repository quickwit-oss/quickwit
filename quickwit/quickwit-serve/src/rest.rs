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
use std::sync::Arc;

use hyper::http::HeaderValue;
use hyper::{http, Method};
use quickwit_common::metrics;
use quickwit_common::tower::BoxFutureInfaillible;
use quickwit_proto::ServiceErrorCode;
use tower::make::Shared;
use tower::ServiceBuilder;
use tower_http::compression::predicate::{DefaultPredicate, Predicate, SizeAbove};
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tracing::{error, info};
use warp::{redirect, Filter, Rejection, Reply};

use crate::cluster_api::cluster_handler;
use crate::delete_task_api::delete_task_api_handlers;
use crate::elastic_search_api::elastic_api_handlers;
use crate::health_check_api::health_check_handlers;
use crate::index_api::index_management_handlers;
use crate::indexing_api::indexing_get_handler;
use crate::ingest_api::ingest_api_handlers;
use crate::json_api_response::{ApiError, JsonApiResponse};
use crate::node_info_handler::node_info_handler;
use crate::search_api::{search_get_handler, search_post_handler, search_stream_handler};
use crate::ui_handler::ui_handler;
use crate::{BodyFormat, QuickwitServices};

/// The minimum size a response body must be in order to
/// be automatically compressed with gzip.
const MINIMUM_RESPONSE_COMPRESSION_SIZE: u16 = 10 << 10;

#[derive(Debug)]
pub(crate) struct InvalidJsonRequest(pub serde_json::Error);

impl warp::reject::Reject for InvalidJsonRequest {}

#[derive(Debug)]
pub(crate) struct InvalidArgument(pub String);

impl warp::reject::Reject for InvalidArgument {}

/// Starts REST services.
pub(crate) async fn start_rest_server(
    rest_listen_addr: SocketAddr,
    quickwit_services: Arc<QuickwitServices>,
    readiness_trigger: BoxFutureInfaillible<()>,
    shutdown_signal: BoxFutureInfaillible<()>,
) -> anyhow::Result<()> {
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
        .or(elastic_api_handlers(
            quickwit_services.search_service.clone(),
        ));

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
    let cors = build_cors(&quickwit_services.config.rest_cors_allow_origins);

    let service = ServiceBuilder::new()
        .layer(
            CompressionLayer::new()
                .gzip(true)
                .compress_when(compression_predicate),
        )
        .layer(cors)
        .service(warp_service);

    info!(
        rest_listen_addr=?rest_listen_addr,
        "Starting REST server listening on {rest_listen_addr}."
    );
    let serve_fut = hyper::Server::bind(&rest_listen_addr)
        .serve(Shared::new(service))
        .with_graceful_shutdown(shutdown_signal);

    let (serve_res, _trigger_res) = tokio::join!(serve_fut, readiness_trigger);
    serve_res?;
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
    let status_code = err.service_code.to_http_status_code();
    Ok(JsonApiResponse::new::<(), _>(
        &Err(err),
        status_code,
        &BodyFormat::default(),
    ))
}

fn get_status_with_error(rejection: Rejection) -> ApiError {
    if let Some(error) = rejection.find::<crate::index_api::UnsupportedContentType>() {
        ApiError {
            service_code: ServiceErrorCode::UnsupportedMediaType,
            message: error.to_string(),
        }
    } else if rejection.is_not_found() {
        ApiError {
            service_code: ServiceErrorCode::NotFound,
            message: "Route not found".to_string(),
        }
    } else if let Some(error) = rejection.find::<serde_qs::Error>() {
        ApiError {
            service_code: ServiceErrorCode::BadRequest,
            message: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<InvalidJsonRequest>() {
        // Happens when the request body could not be deserialized correctly.
        ApiError {
            service_code: ServiceErrorCode::BadRequest,
            message: error.0.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::filters::body::BodyDeserializeError>() {
        // Happens when the request body could not be deserialized correctly.
        ApiError {
            service_code: ServiceErrorCode::BadRequest,
            message: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::UnsupportedMediaType>() {
        ApiError {
            service_code: ServiceErrorCode::UnsupportedMediaType,
            message: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::InvalidQuery>() {
        ApiError {
            service_code: ServiceErrorCode::BadRequest,
            message: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::LengthRequired>() {
        ApiError {
            service_code: ServiceErrorCode::BadRequest,
            message: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::MissingHeader>() {
        ApiError {
            service_code: ServiceErrorCode::BadRequest,
            message: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::InvalidHeader>() {
        ApiError {
            service_code: ServiceErrorCode::BadRequest,
            message: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::MethodNotAllowed>() {
        ApiError {
            service_code: ServiceErrorCode::MethodNotAllowed,
            message: error.to_string(),
        }
    } else if let Some(error) = rejection.find::<warp::reject::PayloadTooLarge>() {
        ApiError {
            service_code: ServiceErrorCode::BadRequest,
            message: error.to_string(),
        }
    } else {
        error!("REST server error: {:?}", rejection);
        ApiError {
            service_code: ServiceErrorCode::Internal,
            message: "Internal server error.".to_string(),
        }
    }
}

fn build_cors(cors_origins: &[String]) -> CorsLayer {
    let mut cors = CorsLayer::new().allow_methods([
        Method::GET,
        Method::POST,
        Method::PUT,
        Method::DELETE,
        Method::OPTIONS,
    ]);
    if !cors_origins.is_empty() {
        let allow_any = cors_origins.iter().any(|origin| origin.as_str() == "*");

        if allow_any {
            info!("CORS is enabled, all origins will be allowed");
            cors = cors.allow_origin(tower_http::cors::Any);
        } else {
            info!(origins = ?cors_origins, "CORS is enabled, the following origins will be allowed");
            let origins = cors_origins
                .iter()
                .map(|origin| origin.parse::<HeaderValue>().unwrap())
                .collect::<Vec<_>>();
            cors = cors.allow_origin(origins);
        };
    }

    cors
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use hyper::{Request, Response, StatusCode};
    use tower::Service;

    use super::*;

    #[tokio::test]
    async fn test_cors() {
        // No cors enabled
        {
            let cors = build_cors(&[]);

            let mut layer = ServiceBuilder::new().layer(cors).service(HelloWorld);

            let resp = layer.call(Request::new(())).await.unwrap();
            let headers = resp.headers();
            assert_eq!(headers.get("Access-Control-Allow-Origin"), None);
            assert_eq!(headers.get("Access-Control-Allow-Methods"), None);
            assert_eq!(headers.get("Access-Control-Allow-Headers"), None);
            assert_eq!(headers.get("Access-Control-Max-Age"), None);

            let resp = layer
                .call(cors_request("http://localhost:3000"))
                .await
                .unwrap();
            let headers = resp.headers();
            assert_eq!(headers.get("Access-Control-Allow-Origin"), None);
            assert_eq!(
                headers.get("Access-Control-Allow-Methods"),
                Some(
                    &"GET,POST,PUT,DELETE,OPTIONS"
                        .parse::<HeaderValue>()
                        .unwrap()
                )
            );
            assert_eq!(headers.get("Access-Control-Allow-Headers"), None);
            assert_eq!(headers.get("Access-Control-Max-Age"), None);
        }

        // Wildcard cors enabled
        {
            let cors = build_cors(&["*".to_string()]);

            let mut layer = ServiceBuilder::new().layer(cors).service(HelloWorld);

            let resp = layer.call(Request::new(())).await.unwrap();
            let headers = resp.headers();
            assert_eq!(
                headers.get("Access-Control-Allow-Origin"),
                Some(&"*".parse::<HeaderValue>().unwrap())
            );
            assert_eq!(headers.get("Access-Control-Allow-Methods"), None);
            assert_eq!(headers.get("Access-Control-Allow-Headers"), None);
            assert_eq!(headers.get("Access-Control-Max-Age"), None);

            let resp = layer
                .call(cors_request("http://localhost:3000"))
                .await
                .unwrap();
            let headers = resp.headers();
            assert_eq!(
                headers.get("Access-Control-Allow-Origin"),
                Some(&"*".parse::<HeaderValue>().unwrap())
            );
            assert_eq!(
                headers.get("Access-Control-Allow-Methods"),
                Some(
                    &"GET,POST,PUT,DELETE,OPTIONS"
                        .parse::<HeaderValue>()
                        .unwrap()
                )
            );
            assert_eq!(headers.get("Access-Control-Allow-Headers"), None);
            assert_eq!(headers.get("Access-Control-Max-Age"), None);
        }

        // Specific origin cors enabled
        {
            let cors = build_cors(&["https://quickwit.io".to_string()]);

            let mut layer = ServiceBuilder::new().layer(cors).service(HelloWorld);

            let resp = layer.call(Request::new(())).await.unwrap();
            let headers = resp.headers();
            assert_eq!(headers.get("Access-Control-Allow-Origin"), None);
            assert_eq!(headers.get("Access-Control-Allow-Methods"), None);
            assert_eq!(headers.get("Access-Control-Allow-Headers"), None);
            assert_eq!(headers.get("Access-Control-Max-Age"), None);

            let resp = layer
                .call(cors_request("http://localhost:3000"))
                .await
                .unwrap();
            let headers = resp.headers();
            assert_eq!(headers.get("Access-Control-Allow-Origin"), None);
            assert_eq!(
                headers.get("Access-Control-Allow-Methods"),
                Some(
                    &"GET,POST,PUT,DELETE,OPTIONS"
                        .parse::<HeaderValue>()
                        .unwrap()
                )
            );
            assert_eq!(headers.get("Access-Control-Allow-Headers"), None);
            assert_eq!(headers.get("Access-Control-Max-Age"), None);

            let resp = layer
                .call(cors_request("https://quickwit.io"))
                .await
                .unwrap();
            let headers = resp.headers();
            assert_eq!(
                headers.get("Access-Control-Allow-Origin"),
                Some(&"https://quickwit.io".parse::<HeaderValue>().unwrap())
            );
            assert_eq!(
                headers.get("Access-Control-Allow-Methods"),
                Some(
                    &"GET,POST,PUT,DELETE,OPTIONS"
                        .parse::<HeaderValue>()
                        .unwrap()
                )
            );
            assert_eq!(headers.get("Access-Control-Allow-Headers"), None);
            assert_eq!(headers.get("Access-Control-Max-Age"), None);
        }

        // Specific multiple-origin cors enabled
        {
            let cors = build_cors(&[
                "https://quickwit.io".to_string(),
                "http://localhost:3000".to_string(),
            ]);

            let mut layer = ServiceBuilder::new().layer(cors).service(HelloWorld);

            let resp = layer.call(Request::new(())).await.unwrap();
            let headers = resp.headers();
            assert_eq!(headers.get("Access-Control-Allow-Origin"), None);
            assert_eq!(headers.get("Access-Control-Allow-Methods"), None);
            assert_eq!(headers.get("Access-Control-Allow-Headers"), None);
            assert_eq!(headers.get("Access-Control-Max-Age"), None);

            let resp = layer
                .call(cors_request("http://localhost:3000"))
                .await
                .unwrap();
            let headers = resp.headers();
            assert_eq!(
                headers.get("Access-Control-Allow-Origin"),
                Some(&"http://localhost:3000".parse::<HeaderValue>().unwrap())
            );
            assert_eq!(
                headers.get("Access-Control-Allow-Methods"),
                Some(
                    &"GET,POST,PUT,DELETE,OPTIONS"
                        .parse::<HeaderValue>()
                        .unwrap()
                )
            );
            assert_eq!(headers.get("Access-Control-Allow-Headers"), None);
            assert_eq!(headers.get("Access-Control-Max-Age"), None);

            let resp = layer
                .call(cors_request("https://quickwit.io"))
                .await
                .unwrap();
            let headers = resp.headers();
            assert_eq!(
                headers.get("Access-Control-Allow-Origin"),
                Some(&"https://quickwit.io".parse::<HeaderValue>().unwrap())
            );
            assert_eq!(
                headers.get("Access-Control-Allow-Methods"),
                Some(
                    &"GET,POST,PUT,DELETE,OPTIONS"
                        .parse::<HeaderValue>()
                        .unwrap()
                )
            );
            assert_eq!(headers.get("Access-Control-Allow-Headers"), None);
            assert_eq!(headers.get("Access-Control-Max-Age"), None);
        }
    }

    fn cors_request(origin: &'static str) -> Request<()> {
        let mut request = Request::new(());
        (*request.method_mut()) = Method::OPTIONS;
        request
            .headers_mut()
            .insert("Origin", HeaderValue::from_static(origin));
        request
    }

    struct HelloWorld;

    impl Service<Request<()>> for HelloWorld {
        type Response = Response<String>;
        type Error = http::Error;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<()>) -> Self::Future {
            let body = "hello, world!\n".to_string();
            let resp = Response::builder()
                .status(StatusCode::OK)
                .body(body)
                .expect("Unable to create `http::Response`");

            let fut = async { Ok(resp) };

            Box::pin(fut)
        }
    }
}
