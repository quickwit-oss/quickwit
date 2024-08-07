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

use std::fmt::Formatter;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use http_serde::http::StatusCode;
use hyper::{http, Method};
use quickwit_common::tower::BoxFutureInfaillible;
use quickwit_search::SearchService;
use tower::make::Shared;
use tower::ServiceBuilder;
use tower_http::compression::predicate::{NotForContentType, Predicate, SizeAbove};
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tracing::{error, info};
use warp::filters::log::Info;
use warp::{redirect, Filter, Rejection, Reply};

use crate::cluster_api::cluster_handler;
use crate::decompression::{CorruptedData, UnsupportedEncoding};
use crate::delete_task_api::delete_task_api_handlers;
use crate::developer_api::developer_api_routes;
use crate::elasticsearch_api::elastic_api_handlers;
use crate::health_check_api::health_check_handlers;
use crate::index_api::index_management_handlers;
use crate::indexing_api::indexing_get_handler;
use crate::ingest_api::ingest_api_handlers;
use crate::jaeger_api::jaeger_api_handlers;
use crate::metrics_api::metrics_handler;
use crate::node_info_handler::node_info_handler;
use crate::otlp_api::otlp_ingest_api_handlers;
use crate::rest_api_response::{RestApiError, RestApiResponse};
use crate::search_api::{
    search_get_handler, search_plan_get_handler, search_plan_post_handler, search_post_handler,
    search_stream_handler,
};
use crate::template_api::index_template_api_handlers;
use crate::ui_handler::ui_handler;
use crate::{BodyFormat, BuildInfo, QuickwitServices, RuntimeInfo};

#[derive(Debug)]
pub(crate) struct InvalidJsonRequest(pub serde_json::Error);

impl warp::reject::Reject for InvalidJsonRequest {}

#[derive(Debug)]
pub(crate) struct InvalidArgument(pub String);

impl warp::reject::Reject for InvalidArgument {}

#[derive(Debug)]
pub struct TooManyRequests;

impl warp::reject::Reject for TooManyRequests {}

impl std::fmt::Display for TooManyRequests {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "too many requests")
    }
}

#[derive(Debug)]
pub struct InternalError(pub String);

impl warp::reject::Reject for InternalError {}

impl std::fmt::Display for InternalError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "internal error: {}", self.0)
    }
}

/// Env variable key to define the minimum size above which a response should be compressed.
/// If unset, no compression is applied.
const QW_MINIMUM_COMPRESSION_SIZE_KEY: &str = "QW_MINIMUM_COMPRESSION_SIZE";

#[derive(Clone, Copy)]
struct CompressionPredicate {
    size_above_opt: Option<SizeAbove>,
}

impl CompressionPredicate {
    fn from_env() -> CompressionPredicate {
        let minimum_compression_size_opt: Option<u16> = quickwit_common::get_from_env_opt::<usize>(
            QW_MINIMUM_COMPRESSION_SIZE_KEY,
        )
        .map(|minimum_compression_size: usize| {
            u16::try_from(minimum_compression_size).unwrap_or(u16::MAX)
        });
        let size_above_opt = minimum_compression_size_opt.map(SizeAbove::new);
        CompressionPredicate { size_above_opt }
    }
}

impl Predicate for CompressionPredicate {
    fn should_compress<B>(&self, response: &http::Response<B>) -> bool
    where B: hyper::body::HttpBody {
        if let Some(size_above) = self.size_above_opt {
            size_above.should_compress(response)
        } else {
            false
        }
    }
}

/// Starts REST services.
pub(crate) async fn start_rest_server(
    rest_listen_addr: SocketAddr,
    quickwit_services: Arc<QuickwitServices>,
    readiness_trigger: BoxFutureInfaillible<()>,
    shutdown_signal: BoxFutureInfaillible<()>,
) -> anyhow::Result<()> {
    let request_counter = warp::log::custom(|info: Info| {
        let elapsed = info.elapsed();
        let status = info.status();
        let label_values: [&str; 2] = [info.method().as_str(), status.as_str()];
        crate::SERVE_METRICS
            .request_duration_secs
            .with_label_values(label_values)
            .observe(elapsed.as_secs_f64());
        crate::SERVE_METRICS
            .http_requests_total
            .with_label_values(label_values)
            .inc();
    });
    // Docs routes
    let api_doc = warp::path("openapi.json")
        .and(warp::get())
        .map(|| warp::reply::json(&crate::openapi::build_docs()))
        .recover(recover_fn);

    // `/health/*` routes.
    let health_check_routes = health_check_handlers(
        quickwit_services.cluster.clone(),
        quickwit_services.indexing_service_opt.clone(),
        quickwit_services.janitor_service_opt.clone(),
    );

    // `/metrics` route.
    let metrics_routes = warp::path("metrics")
        .and(warp::get())
        .map(metrics_handler)
        .recover(recover_fn);

    // `/api/developer/*` route.
    let developer_routes = developer_api_routes(
        quickwit_services.cluster.clone(),
        quickwit_services.env_filter_reload_fn.clone(),
    );
    // `/api/v1/*` routes.
    let api_v1_root_route = api_v1_routes(quickwit_services.clone());

    let redirect_root_to_ui_route = warp::path::end()
        .and(warp::get())
        .map(|| redirect(http::Uri::from_static("/ui/search")))
        .recover(recover_fn);

    let extra_headers = warp::reply::with::headers(to_warp_header_map(
        &quickwit_services.node_config.rest_config.extra_headers,
    ));

    // Combine all the routes together.
    let rest_routes = api_v1_root_route
        .or(api_doc)
        .or(redirect_root_to_ui_route)
        .or(ui_handler())
        .or(health_check_routes)
        .or(metrics_routes)
        .or(developer_routes)
        .with(request_counter)
        .recover(recover_fn_final)
        .with(extra_headers)
        .boxed();

    let warp_service = warp::service(rest_routes);
    let compression_predicate = CompressionPredicate::from_env().and(NotForContentType::IMAGES);
    let cors = build_cors(&quickwit_services.node_config.rest_config.cors_allow_origins);

    let service = ServiceBuilder::new()
        .layer(
            CompressionLayer::new()
                .zstd(true)
                .gzip(true)
                .quality(tower_http::CompressionLevel::Fastest)
                .compress_when(compression_predicate),
        )
        .layer(cors)
        .service(warp_service);

    info!(
        rest_listen_addr=?rest_listen_addr,
        "Starting REST server listening on {rest_listen_addr}."
    );

    // `graceful_shutdown()` seems to be blocking in presence of existing connections.
    // The following approach of dropping the serve supposedly is not bullet proof, but it seems to
    // work in our unit test.
    //
    // See more of the discussion here:
    // https://github.com/hyperium/hyper/issues/2386
    let serve_fut = async move {
        tokio::select! {
             res = hyper::Server::bind(&rest_listen_addr).serve(Shared::new(service)) => { res }
             _ = shutdown_signal => { Ok(()) }
        }
    };

    let (serve_res, _trigger_res) = tokio::join!(serve_fut, readiness_trigger);
    serve_res?;
    Ok(())
}

fn search_routes(
    search_service: Arc<dyn SearchService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    search_get_handler(search_service.clone())
        .or(search_post_handler(search_service.clone()))
        .or(search_plan_get_handler(search_service.clone()))
        .or(search_plan_post_handler(search_service.clone()))
        .or(search_stream_handler(search_service))
        .recover(recover_fn)
}

fn api_v1_routes(
    quickwit_services: Arc<QuickwitServices>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    let api_v1_root_url = warp::path!("api" / "v1" / ..);
    api_v1_root_url.and(
        elastic_api_handlers(
            quickwit_services.node_config.clone(),
            quickwit_services.search_service.clone(),
            quickwit_services.ingest_service.clone(),
            quickwit_services.ingest_router_service.clone(),
            quickwit_services.metastore_client.clone(),
            quickwit_services.index_manager.clone(),
        )
        .or(cluster_handler(quickwit_services.cluster.clone()))
        .or(node_info_handler(
            BuildInfo::get(),
            RuntimeInfo::get(),
            quickwit_services.node_config.clone(),
        ))
        .or(indexing_get_handler(
            quickwit_services.indexing_service_opt.clone(),
        ))
        .or(search_routes(quickwit_services.search_service.clone()))
        .or(ingest_api_handlers(
            quickwit_services.ingest_router_service.clone(),
            quickwit_services.ingest_service.clone(),
            quickwit_services.node_config.ingest_api_config.clone(),
        ))
        .or(otlp_ingest_api_handlers(
            quickwit_services.otlp_logs_service_opt.clone(),
            quickwit_services.otlp_traces_service_opt.clone(),
        ))
        .or(index_management_handlers(
            quickwit_services.index_manager.clone(),
            quickwit_services.node_config.clone(),
        ))
        .or(delete_task_api_handlers(
            quickwit_services.metastore_client.clone(),
        ))
        .or(jaeger_api_handlers(
            quickwit_services.jaeger_service_opt.clone(),
        ))
        .or(index_template_api_handlers(
            quickwit_services.metastore_client.clone(),
        )),
    )
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
    let error = get_status_with_error(rejection)?;
    let status_code = error.status_code;
    Ok(RestApiResponse::new::<(), _>(
        &Err(error),
        status_code,
        BodyFormat::default(),
    ))
}

pub async fn recover_fn_final(rejection: Rejection) -> Result<impl Reply, Rejection> {
    let error = get_status_with_error(rejection).unwrap_or_else(|rejection: Rejection| {
        if rejection.is_not_found() {
            RestApiError {
                status_code: StatusCode::NOT_FOUND,
                message: "Route not found".to_string(),
            }
        } else {
            error!("REST server error: {:?}", rejection);
            RestApiError {
                status_code: StatusCode::INTERNAL_SERVER_ERROR,
                message: "internal server error".to_string(),
            }
        }
    });
    let status_code = error.status_code;
    Ok(RestApiResponse::new::<(), _>(
        &Err(error),
        status_code,
        BodyFormat::default(),
    ))
}

fn get_status_with_error(rejection: Rejection) -> Result<RestApiError, Rejection> {
    if let Some(error) = rejection.find::<crate::format::UnsupportedMediaType>() {
        Ok(RestApiError {
            status_code: StatusCode::UNSUPPORTED_MEDIA_TYPE,
            message: error.to_string(),
        })
    } else if let Some(error) = rejection.find::<serde_qs::Error>() {
        Ok(RestApiError {
            status_code: StatusCode::BAD_REQUEST,
            message: error.to_string(),
        })
    } else if let Some(error) = rejection.find::<InvalidJsonRequest>() {
        // Happens when the request body could not be deserialized correctly.
        Ok(RestApiError {
            status_code: StatusCode::BAD_REQUEST,
            message: error.0.to_string(),
        })
    } else if let Some(error) = rejection.find::<warp::filters::body::BodyDeserializeError>() {
        // Happens when the request body could not be deserialized correctly.
        Ok(RestApiError {
            status_code: StatusCode::BAD_REQUEST,
            message: error.to_string(),
        })
    } else if let Some(error) = rejection.find::<warp::reject::UnsupportedMediaType>() {
        Ok(RestApiError {
            status_code: StatusCode::UNSUPPORTED_MEDIA_TYPE,
            message: error.to_string(),
        })
    } else if let Some(error) = rejection.find::<UnsupportedEncoding>() {
        Ok(RestApiError {
            status_code: StatusCode::UNSUPPORTED_MEDIA_TYPE,
            message: error.to_string(),
        })
    } else if let Some(error) = rejection.find::<CorruptedData>() {
        Ok(RestApiError {
            status_code: StatusCode::BAD_REQUEST,
            message: error.to_string(),
        })
    } else if let Some(error) = rejection.find::<warp::reject::InvalidQuery>() {
        Ok(RestApiError {
            status_code: StatusCode::BAD_REQUEST,
            message: error.to_string(),
        })
    } else if let Some(error) = rejection.find::<warp::reject::LengthRequired>() {
        Ok(RestApiError {
            status_code: StatusCode::LENGTH_REQUIRED,
            message: error.to_string(),
        })
    } else if let Some(error) = rejection.find::<warp::reject::MissingHeader>() {
        Ok(RestApiError {
            status_code: StatusCode::BAD_REQUEST,
            message: error.to_string(),
        })
    } else if let Some(error) = rejection.find::<warp::reject::InvalidHeader>() {
        Ok(RestApiError {
            status_code: StatusCode::BAD_REQUEST,
            message: error.to_string(),
        })
    } else if let Some(error) = rejection.find::<warp::reject::PayloadTooLarge>() {
        Ok(RestApiError {
            status_code: StatusCode::PAYLOAD_TOO_LARGE,
            message: error.to_string(),
        })
    } else if let Some(err) = rejection.find::<TooManyRequests>() {
        Ok(RestApiError {
            status_code: StatusCode::TOO_MANY_REQUESTS,
            message: err.to_string(),
        })
    } else if let Some(error) = rejection.find::<InvalidArgument>() {
        // Happens when the url path or request body contains invalid argument(s).
        Ok(RestApiError {
            status_code: StatusCode::BAD_REQUEST,
            message: error.0.to_string(),
        })
    } else if let Some(error) = rejection.find::<warp::reject::MethodNotAllowed>() {
        Ok(RestApiError {
            status_code: StatusCode::METHOD_NOT_ALLOWED,
            message: error.to_string(),
        })
    } else {
        Err(rejection)
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
                .map(|origin| origin.parse::<warp::http::HeaderValue>().unwrap())
                .collect::<Vec<_>>();
            cors = cors.allow_origin(origins);
        };
    }

    cors
}

fn to_warp_header_map(header_map: &http_serde::http::HeaderMap) -> warp::http::HeaderMap {
    let mut warp_header_map = warp::http::HeaderMap::new();
    header_map.iter().for_each(|(key, value)| {
        warp_header_map.insert(
            warp::http::HeaderName::from_str(key.as_str()).expect("header name must be valid"),
            warp::http::HeaderValue::from_str(
                value.to_str().expect("header value must be a valid str"),
            )
            .expect("header value must be valid"),
        );
    });
    warp_header_map
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use http_serde::http::{HeaderName, HeaderValue};
    use hyper::{Request, Response};
    use quickwit_cluster::{create_cluster_for_test, ChannelTransport};
    use quickwit_config::NodeConfig;
    use quickwit_index_management::IndexService;
    use quickwit_ingest::{IngestApiService, IngestServiceClient};
    use quickwit_proto::control_plane::ControlPlaneServiceClient;
    use quickwit_proto::ingest::router::IngestRouterServiceClient;
    use quickwit_proto::metastore::MetastoreServiceClient;
    use quickwit_search::MockSearchService;
    use quickwit_storage::StorageResolver;
    use tower::Service;

    use super::*;
    use crate::rest::recover_fn_final;

    pub(crate) fn ingest_service_client() -> IngestServiceClient {
        let universe = quickwit_actors::Universe::new();
        let (ingest_service_mailbox, _) = universe.create_test_mailbox::<IngestApiService>();
        IngestServiceClient::from_mailbox(ingest_service_mailbox)
    }

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
                        .parse::<warp::http::HeaderValue>()
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
                Some(&"*".parse::<warp::http::HeaderValue>().unwrap())
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
                Some(&"*".parse::<warp::http::HeaderValue>().unwrap())
            );
            assert_eq!(
                headers.get("Access-Control-Allow-Methods"),
                Some(
                    &"GET,POST,PUT,DELETE,OPTIONS"
                        .parse::<warp::http::HeaderValue>()
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
                        .parse::<warp::http::HeaderValue>()
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
                Some(
                    &"https://quickwit.io"
                        .parse::<warp::http::HeaderValue>()
                        .unwrap()
                )
            );
            assert_eq!(
                headers.get("Access-Control-Allow-Methods"),
                Some(
                    &"GET,POST,PUT,DELETE,OPTIONS"
                        .parse::<warp::http::HeaderValue>()
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
                Some(
                    &"http://localhost:3000"
                        .parse::<warp::http::HeaderValue>()
                        .unwrap()
                )
            );
            assert_eq!(
                headers.get("Access-Control-Allow-Methods"),
                Some(
                    &"GET,POST,PUT,DELETE,OPTIONS"
                        .parse::<warp::http::HeaderValue>()
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
                Some(
                    &"https://quickwit.io"
                        .parse::<warp::http::HeaderValue>()
                        .unwrap()
                )
            );
            assert_eq!(
                headers.get("Access-Control-Allow-Methods"),
                Some(
                    &"GET,POST,PUT,DELETE,OPTIONS"
                        .parse::<warp::http::HeaderValue>()
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
            .insert("Origin", warp::http::HeaderValue::from_static(origin));
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
                .status(http::StatusCode::OK)
                .body(body)
                .expect("Unable to create `http::Response`");

            let fut = async { Ok(resp) };

            Box::pin(fut)
        }
    }

    #[tokio::test]
    async fn test_extra_headers() {
        let mut node_config = NodeConfig::for_test();
        node_config.rest_config.extra_headers.insert(
            HeaderName::from_static("x-custom-header"),
            HeaderValue::from_static("custom-value"),
        );
        node_config.rest_config.extra_headers.insert(
            HeaderName::from_static("x-custom-header-2"),
            HeaderValue::from_static("custom-value-2"),
        );
        let metastore_client = MetastoreServiceClient::mocked();
        let index_service =
            IndexService::new(metastore_client.clone(), StorageResolver::unconfigured());
        let control_plane_client = ControlPlaneServiceClient::mocked();
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &[], &transport, false)
            .await
            .unwrap();
        let quickwit_services = QuickwitServices {
            _report_splits_subscription_handle_opt: None,
            _local_shards_update_listener_handle_opt: None,
            cluster,
            control_plane_server_opt: None,
            control_plane_client,
            indexing_service_opt: None,
            index_manager: index_service,
            ingest_service: ingest_service_client(),
            ingest_router_opt: None,
            ingest_router_service: IngestRouterServiceClient::mocked(),
            ingester_opt: None,
            janitor_service_opt: None,
            otlp_logs_service_opt: None,
            otlp_traces_service_opt: None,
            metastore_client,
            metastore_server_opt: None,
            node_config: Arc::new(node_config.clone()),
            search_service: Arc::new(MockSearchService::new()),
            jaeger_service_opt: None,
            env_filter_reload_fn: crate::do_nothing_env_filter_reload_fn(),
        };

        let handler = api_v1_routes(Arc::new(quickwit_services))
            .recover(recover_fn_final)
            .with(warp::reply::with::headers(to_warp_header_map(
                &node_config.rest_config.extra_headers,
            )));

        let resp = warp::test::request()
            .path("/api/v1/version")
            .reply(&handler.clone())
            .await;

        assert_eq!(resp.status(), 200);
        assert_eq!(
            resp.headers().get("x-custom-header").unwrap(),
            "custom-value"
        );
        assert_eq!(
            resp.headers().get("x-custom-header-2").unwrap(),
            "custom-value-2"
        );

        let resp_404 = warp::test::request()
            .path("/api/v1/version404")
            .reply(&handler)
            .await;

        assert_eq!(resp_404.status(), 404);
        assert_eq!(
            resp_404.headers().get("x-custom-header").unwrap(),
            "custom-value"
        );
        assert_eq!(
            resp_404.headers().get("x-custom-header-2").unwrap(),
            "custom-value-2"
        );
    }
}
