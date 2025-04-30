use std::error::Error;
use std::sync::Arc;

use quickwit_config::JaegerConfig;
use quickwit_jaeger::JaegerService;
use quickwit_proto::jaeger::storage::v1::span_reader_plugin_server::SpanReaderPluginServer;
use quickwit_search::SearchService;
use tokio::net::TcpListener;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;

pub async fn grpc_server(
    search_service: Arc<dyn SearchService>,
    tcp_listener: TcpListener,
) -> anyhow::Result<()> {
    let jaeger_service = JaegerService::new(JaegerConfig::default(), search_service.clone());
    let jaeger_grpc_service = SpanReaderPluginServer::new(jaeger_service);

    let tcp_incoming = TcpIncoming::from_listener(tcp_listener, true, None)
        .map_err(|err: Box<dyn Error + Send + Sync>| anyhow::anyhow!(err))?;

    Server::builder()
        .add_service(jaeger_grpc_service)
        .serve_with_incoming(tcp_incoming)
        .await?;

    Ok(())
}
