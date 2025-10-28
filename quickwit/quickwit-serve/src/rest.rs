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

use std::fmt::Formatter;
use std::io;
use std::sync::Arc;

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use hyper_util::service::TowerToHyperService;
use quickwit_common::tower::BoxFutureInfaillible;
use quickwit_config::{disable_ingest_v1, enable_ingest_v2};
use quickwit_search::SearchService;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tokio_util::either::Either;
use tower::ServiceBuilder;
use tower_http::compression::CompressionLayer;
use tower_http::compression::predicate::{NotForContentType, Predicate, SizeAbove};
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::{error, info};
use warp::filters::log::Info;
use warp::hyper::http::HeaderValue;
use warp::hyper::{Method, StatusCode, http};
use warp::{Filter, Rejection, Reply, redirect};

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

/// Env variable key to define the minimum size above which a response should be compressed.
/// If unset, no compression is applied.
const QW_MINIMUM_COMPRESSION_SIZE_KEY: &str = "QW_MINIMUM_COMPRESSION_SIZE";

#[derive(Clone, Copy)]
struct CompressionPredicate {
    size_above_opt: Option<SizeAbove>,
}

impl CompressionPredicate {
    fn from_env() -> CompressionPredicate {
        let minimum_compression_size_opt: Option<u16> =
            quickwit_common::get_from_env_opt::<usize>(QW_MINIMUM_COMPRESSION_SIZE_KEY, false).map(
                |minimum_compression_size: usize| {
                    u16::try_from(minimum_compression_size).unwrap_or(u16::MAX)
                },
            );
        let size_above_opt = minimum_compression_size_opt.map(SizeAbove::new);
        CompressionPredicate { size_above_opt }
    }
}

impl Predicate for CompressionPredicate {
    fn should_compress<B>(&self, response: &http::Response<B>) -> bool
    where B: http_body::Body {
        if let Some(size_above) = self.size_above_opt {
            size_above.should_compress(response)
        } else {
            false
        }
    }
}

async fn apply_tls_if_necessary(
    tcp_stream: TcpStream,
    tls_acceptor_opt: &Option<TlsAcceptor>,
) -> io::Result<impl AsyncRead + AsyncWrite + Unpin + 'static> {
    let Some(tls_acceptor) = &tls_acceptor_opt else {
        return Ok(Either::Right(tcp_stream));
    };
    let tls_stream_res = tls_acceptor
        .accept(tcp_stream)
        .await
        .inspect_err(|err| error!("failed to perform tls handshake: {err:#}"))?;
    Ok(Either::Left(tls_stream_res))
}

/// Starts REST services.
pub(crate) async fn start_rest_server(
    tcp_listener: TcpListener,
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
        .recover(recover_fn)
        .boxed();

    // `/health/*` routes.
    let health_check_routes = health_check_handlers(
        quickwit_services.cluster.clone(),
        quickwit_services.indexing_service_opt.clone(),
        quickwit_services.janitor_service_opt.clone(),
    )
    .boxed();

    // `/metrics` route.
    let metrics_routes = warp::path("metrics")
        .and(warp::get())
        .map(metrics_handler)
        .recover(recover_fn)
        .boxed();

    // `/api/developer/*` route.
    let developer_routes = developer_api_routes(
        quickwit_services.cluster.clone(),
        quickwit_services.env_filter_reload_fn.clone(),
    )
    .boxed();

    // `/api/v1/*` routes.
    let api_v1_root_route = api_v1_routes(quickwit_services.clone());

    let redirect_root_to_ui_route = warp::path::end()
        .and(warp::get())
        .map(|| redirect(http::Uri::from_static("/ui/search")))
        .recover(recover_fn)
        .boxed();

    let extra_headers = warp::reply::with::headers(
        quickwit_services
            .node_config
            .rest_config
            .extra_headers
            .clone(),
    );

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

    let rest_listen_addr = tcp_listener.local_addr()?;
    info!(
        rest_listen_addr=?rest_listen_addr,
        "starting REST server listening on {rest_listen_addr}"
    );

    let service = TowerToHyperService::new(service);

    let server = Builder::new(TokioExecutor::new());
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();
    let mut shutdown_signal = std::pin::pin!(shutdown_signal);
    readiness_trigger.await;

    let tls_acceptor_opt: Option<TlsAcceptor> =
        if let Some(tls_config) = &quickwit_services.node_config.rest_config.tls {
            let rustls_config = tls::make_rustls_config(tls_config)?;
            Some(TlsAcceptor::from(rustls_config))
        } else {
            None
        };

    loop {
        tokio::select! {
            tcp_accept_res = tcp_listener.accept() => {
                let tcp_stream = match tcp_accept_res {
                    Ok((tcp_stream, _remote_addr)) => tcp_stream,
                    Err(err) => {
                        error!("failed to accept connection: {err:#}");
                        continue;
                    }
                };

                let Ok(tcp_or_tls_stream) = apply_tls_if_necessary(tcp_stream, &tls_acceptor_opt).await else {
                    continue;
                };

                let serve_fut = server.serve_connection_with_upgrades(TokioIo::new(tcp_or_tls_stream), service.clone());
                let serve_with_shutdown_fut = graceful.watch(serve_fut.into_owned());
                tokio::spawn(async move {
                    if let Err(err) = serve_with_shutdown_fut.await {
                        error!("failed to serve connection: {err:#}");
                    }
                });
            },
            _ = &mut shutdown_signal => {
                info!("REST server shutdown signal received");
                break;
            }
        }
    }

    graceful.shutdown().await;
    info!("gracefully shutdown");

    Ok(())
}

fn search_routes(
    search_service: Arc<dyn SearchService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    search_get_handler(search_service.clone())
        .or(search_post_handler(search_service.clone()))
        .or(search_plan_get_handler(search_service.clone()))
        .or(search_plan_post_handler(search_service.clone()))
        .recover(recover_fn)
        .boxed()
}

fn api_v1_routes(
    quickwit_services: Arc<QuickwitServices>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    let api_v1_root_url = warp::path!("api" / "v1" / ..);
    api_v1_root_url.and(
        elastic_api_handlers(
            quickwit_services.cluster.clone(),
            quickwit_services.node_config.clone(),
            quickwit_services.search_service.clone(),
            quickwit_services.ingest_service.clone(),
            quickwit_services.ingest_router_service.clone(),
            quickwit_services.metastore_client.clone(),
            quickwit_services.index_manager.clone(),
            !disable_ingest_v1(),
            enable_ingest_v2(),
        )
        .or(cluster_handler(quickwit_services.cluster.clone()))
        .boxed()
        .or(node_info_handler(
            BuildInfo::get(),
            RuntimeInfo::get(),
            quickwit_services.node_config.clone(),
        ))
        .boxed()
        .or(indexing_get_handler(
            quickwit_services.indexing_service_opt.clone(),
        ))
        .boxed()
        .or(search_routes(quickwit_services.search_service.clone()))
        .boxed()
        .or(ingest_api_handlers(
            quickwit_services.ingest_router_service.clone(),
            quickwit_services.ingest_service.clone(),
            quickwit_services.node_config.ingest_api_config.clone(),
            !disable_ingest_v1(),
            enable_ingest_v2(),
        ))
        .boxed()
        .or(otlp_ingest_api_handlers(
            quickwit_services.otlp_logs_service_opt.clone(),
            quickwit_services.otlp_traces_service_opt.clone(),
        ))
        .boxed()
        .or(index_management_handlers(
            quickwit_services.index_manager.clone(),
            quickwit_services.node_config.clone(),
        ))
        .boxed()
        .or(delete_task_api_handlers(
            quickwit_services.metastore_client.clone(),
        ))
        .boxed()
        .or(jaeger_api_handlers(
            quickwit_services.jaeger_service_opt.clone(),
        ))
        .boxed()
        .or(index_template_api_handlers(
            quickwit_services.metastore_client.clone(),
        ))
        .boxed(),
    )
}

/// This function returns a formatted error based on the given rejection reason.
///
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
    let debug_mode = quickwit_common::get_bool_from_env("QW_ENABLE_CORS_DEBUG", false);
    if debug_mode {
        info!("CORS debug mode is enabled, localhost and 127.0.0.1 origins will be allowed");
        return CorsLayer::new()
            .allow_methods([
                Method::GET,
                Method::POST,
                Method::PUT,
                Method::PATCH,
                Method::DELETE,
            ])
            .allow_origin(AllowOrigin::predicate(|origin, _parts| {
                [b"https://localhost:", b"https://127.0.0.1:"]
                    .iter()
                    .any(|prefix| origin.as_bytes().starts_with(*prefix))
            }))
            .allow_headers([http::header::CONTENT_TYPE]);
    }

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

mod tls {
    // most of this module is copied from hyper-tls examples, licensed under Apache 2.0, MIT or ISC

    use std::sync::Arc;
    use std::vec::Vec;
    use std::{fs, io};

    use quickwit_config::TlsConfig;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use tokio_rustls::rustls::ServerConfig;

    fn io_error(error: String) -> io::Error {
        io::Error::other(error)
    }

    // Load public certificate from file.
    fn load_certs(filename: &str) -> io::Result<Vec<CertificateDer<'static>>> {
        // Open certificate file.
        let certfile = fs::File::open(filename)
            .map_err(|error| io_error(format!("failed to open {filename}: {error}")))?;
        let mut reader = io::BufReader::new(certfile);
        // Load and return certificate.
        rustls_pemfile::certs(&mut reader).collect()
    }

    // Load private key from file.
    fn load_private_key(filename: &str) -> io::Result<PrivateKeyDer<'static>> {
        // Open keyfile.
        let keyfile = fs::File::open(filename)
            .map_err(|error| io_error(format!("failed to open {filename}: {error}")))?;
        let mut reader = io::BufReader::new(keyfile);

        // Load and return a single private key.
        rustls_pemfile::private_key(&mut reader).map(|key| key.unwrap())
    }

    pub fn make_rustls_config(config: &TlsConfig) -> anyhow::Result<Arc<ServerConfig>> {
        let certs = load_certs(&config.cert_path)?;
        let key = load_private_key(&config.key_path)?;

        // TODO we could add support for client authorization, it seems less important than on the
        // gRPC side though
        if config.validate_client {
            anyhow::bail!("mTLS isn't supported on rest api");
        }

        let mut cfg = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|error| io_error(error.to_string()))?;
        // Configure ALPN to accept HTTP/2, HTTP/1.1, and HTTP/1.0 in that order.
        cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec(), b"http/1.0".to_vec()];
        Ok(Arc::new(cfg))
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use quickwit_cluster::{ChannelTransport, create_cluster_for_test};
    use quickwit_config::NodeConfig;
    use quickwit_index_management::IndexService;
    use quickwit_ingest::{IngestApiService, IngestServiceClient};
    use quickwit_proto::control_plane::ControlPlaneServiceClient;
    use quickwit_proto::ingest::router::IngestRouterServiceClient;
    use quickwit_proto::metastore::MetastoreServiceClient;
    use quickwit_search::MockSearchService;
    use quickwit_storage::StorageResolver;
    use tower::Service;
    use warp::http::HeaderName;
    use warp::hyper::{Request, Response, StatusCode};

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
            .with(warp::reply::with::headers(
                node_config.rest_config.extra_headers.clone(),
            ));

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
