use std::sync::Arc;

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as HttpServerBuilder;
use hyper_util::service::TowerToHyperService;
use quickwit_config::JaegerConfig;
use quickwit_jaeger::JaegerService;
use quickwit_search::SearchService;
use quickwit_serve::jaeger_api::jaeger_api_handlers;
use quickwit_serve::rest::search_routes;
use tokio::net::TcpListener;
use tracing::{error, info};
use warp::{Filter, Rejection};

fn api_v1_routes(
    search_service: Arc<dyn SearchService>,
    jaeger_service: JaegerService,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    let api_v1_root_url = warp::path!("api" / "v1" / ..);
    api_v1_root_url.and(search_routes(search_service).or(jaeger_api_handlers(Some(jaeger_service))))
}

async fn shutdown_signal() {
    // Wait for the CTRL+C signal
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C signal handler");
}

pub async fn rest_server(
    search_service: Arc<dyn SearchService + 'static>,
    tcp_listener: TcpListener,
) -> anyhow::Result<()> {
    let jaeger_service = JaegerService::new(JaegerConfig::default(), search_service.clone());
    let rest_routes = api_v1_routes(search_service, jaeger_service);

    let hyper_service = TowerToHyperService::new(warp::service(rest_routes));
    let http_server_builder = HttpServerBuilder::new(TokioExecutor::new());
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();
    let mut shutdown_signal = std::pin::pin!(shutdown_signal());

    loop {
        tokio::select! {
            tcp_conn_res = tcp_listener.accept() => {
                let (tcp_stream, _remote_addr) = match tcp_conn_res {
                    Ok(conn) => conn,
                    Err(err) => {
                        error!("failed to accept connection: {err:#}");
                        continue;
                    }
                };

                let conn = http_server_builder.serve_connection_with_upgrades(TokioIo::new(tcp_stream), hyper_service.clone());
                let conn_graceful_watch = graceful.watch(conn.into_owned());
                tokio::spawn(async move {
                    if let Err(err) = conn_graceful_watch.await {
                        error!("error  while service connection: {err}");
                    }
                });
            }
            _ = &mut shutdown_signal => {
                info!("shutting down server");
                break;
            }
        }
    }
    Ok(())
}
