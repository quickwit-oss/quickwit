use std::sync::Arc;

use hyper::server::conn::AddrIncoming;
use quickwit_config::JaegerConfig;
use quickwit_jaeger::JaegerService;
use quickwit_search::SearchService;
use quickwit_serve::jaeger_api::jaeger_api_handlers;
use quickwit_serve::rest::{recover_fn, search_routes};
use quickwit_serve::ui_handler::ui_handler;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower::make::Shared;
use warp::{Filter, Rejection, redirect};

fn api_v1_routes(
    search_service: Arc<dyn SearchService>,
    jaeger_service: JaegerService,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    let api_v1_root_url = warp::path!("api" / "v1" / ..);
    api_v1_root_url.and(search_routes(search_service).or(jaeger_api_handlers(Some(jaeger_service))))
}

pub async fn rest_server(
    search_service: Arc<dyn SearchService>,
    tcp_listener: TcpListener,
) -> anyhow::Result<()> {
    let jaeger_service = JaegerService::new(JaegerConfig::default(), search_service.clone());

    let redirect_root_to_ui_route = warp::path::end()
        .and(warp::get())
        .map(|| redirect(http::Uri::from_static("/ui/search")))
        .recover(recover_fn)
        .boxed();

    let rest_routes = api_v1_routes(search_service, jaeger_service)
        .or(redirect_root_to_ui_route)
        .or(ui_handler());

    let warp_service = warp::service(rest_routes);

    let service = ServiceBuilder::new()
        /*
                .layer(
                    CompressionLayer::new()
                        .zstd(true)
                        .gzip(true)
                        .quality(tower_http::CompressionLevel::Fastest)
                        .compress_when(compression_predicate),
                )
                .layer(cors)
        */
        .service(warp_service);

    let incoming = AddrIncoming::from_listener(tcp_listener)?;

    let serve_fut = async move {
        tokio::select! {
             res = hyper::Server::builder(incoming).serve(Shared::new(service)) => { res }
             //_ = shutdown_signal => { Ok(()) }
        }
    };
    serve_fut.await?;
    Ok(())
}
