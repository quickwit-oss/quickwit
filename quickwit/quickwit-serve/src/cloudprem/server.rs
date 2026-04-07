use std::collections::BTreeSet;
use std::sync::{Arc, LazyLock};

use quickwit_common::tower::BoxFutureInfaillible;
use quickwit_config::NodeConfig;
use quickwit_config::service::QuickwitService;
use quickwit_proto::cloudprem::CloudPremServiceClient;
use quickwit_proto::tonic::transport::Server;
use quickwit_proto::tonic::transport::server::TcpIncoming;
use tokio::net::TcpListener;
use tracing::*;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use super::auth::MtlsHeaderInterceptorLayer;
use super::service::CloudPremServiceImpl;
use super::websocket::maintain_websocket;
use crate::QuickwitServices;
use crate::grpc::HttpHeadersCarrier;

pub(crate) static DISABLE_CERTIFICATE_VERIFICATION: LazyLock<bool> = LazyLock::new(|| {
    quickwit_common::get_bool_from_env("CP_DISABLE_CERTIFICATE_VERIFICATION", false)
});

/// Starts and binds gRPC services to `grpc_listen_addr`.
pub(crate) async fn start_cloudprem_server(
    tcp_listener: TcpListener,
    node_config: NodeConfig,
    services: Arc<QuickwitServices>,
    readiness_trigger: BoxFutureInfaillible<()>,
    shutdown_signal: BoxFutureInfaillible<()>,
) -> anyhow::Result<()> {
    let cluster_name = node_config.cluster_id.clone();
    let cloudprem_config = node_config.cloudprem_config.clone();
    let mut enabled_grpc_services = BTreeSet::new();
    let mut file_descriptor_sets = Vec::new();
    let grpc_config = cloudprem_config.grpc_config;

    let server = Server::builder().trace_fn(|request| {
        let method = request.method();
        let path = request.uri().path();
        let span = tracing::span!(tracing::Level::INFO, "grpc-request", %method, %path);

        let parent_context = opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.extract(&HttpHeadersCarrier(request.headers()))
        });
        let _ = span.set_parent(parent_context);
        span
    });

    /*
     * TODO this could be used to do standalone CloudPrem (with no reverse proxy),
     * but we need to emit the header the rest of the code except (or not put the auth
     * layer, but then we also lose auditing capabilities)
    if let Some(tls_config) = grpc_config.tls {
        let cert = std::fs::read_to_string(tls_config.cert_path)?;
        let key = std::fs::read_to_string(tls_config.key_path)?;
        let identity = Identity::from_pem(cert, key);

        let mut tls = ServerTlsConfig::new().identity(identity);

        if tls_config.validate_client {
            let ca_cert = std::fs::read_to_string(tls_config.ca_path)?;
            let ca_cert = Certificate::from_pem(ca_cert);
            tls = tls.client_ca_root(ca_cert);
        }
        // TODO using this builtin method means we have no way of hot-reloading certificates
        // (i.e. the process must be restarted every time its certificate expires)
        // to do better, we'd need to wrap the TcpListener with something that does (m)TLS
        // and that we control, however it would be somewhat painful, and more error prone
        server = server.tls_config(tls)?;
    }
    */

    let cloudprem_grpc_service = if services
        .node_config
        .is_service_enabled(QuickwitService::Searcher)
    {
        enabled_grpc_services.insert("cloudprem");
        file_descriptor_sets.push(quickwit_proto::cloudprem::CLOUDPREM_FILE_DESCRIPTOR_SET);
        file_descriptor_sets.push(quickwit_proto::cloudprem::CLOUDPREM_METRICS_FILE_DESCRIPTOR_SET);

        let search_service = services.search_service.clone();
        let cloudprem_service_impl = CloudPremServiceImpl::new(
            search_service,
            services.metastore_client.clone(),
            services.cluster.clone(),
            services.node_config.clone(),
        );
        let cloudprem_service_client =
            CloudPremServiceClient::tower().build(cloudprem_service_impl);
        if cloudprem_config.enable_reverse_connection {
            let datadog_config = cloudprem_config.datadog_config.clone();
            info!(
                datadog_config.site,
                "connecting to Datadog using reverse connection"
            );
            tokio::spawn(maintain_websocket(
                datadog_config
                    .site
                    .expect("site should be set when reverse connection is enabled"),
                datadog_config
                    .dd_api_key
                    .expect("API key should be set when reverse connection is enabled"),
                cluster_name,
                cloudprem_service_client.clone(),
                services.metastore_client.clone(),
            ));
        }
        Some(cloudprem_service_client.as_grpc_service(grpc_config.max_message_size))
    } else {
        None
    };

    let aws_mtls_interceptor_layer_opt: Option<MtlsHeaderInterceptorLayer<'static>> =
        if *DISABLE_CERTIFICATE_VERIFICATION {
            tracing::warn!("mTLS client certificate verification disabled");
            None
        } else {
            tracing::info!("mTLS client certificate verification enabled");
            Some(MtlsHeaderInterceptorLayer::for_cloudprem_port(
                cloudprem_config.mtls_header,
            ))
        };

    let server_router = server
        .layer(tower::util::option_layer(aws_mtls_interceptor_layer_opt))
        .add_optional_service(cloudprem_grpc_service);

    let grpc_listen_addr = tcp_listener.local_addr()?;
    info!(
        enabled_grpc_services=?enabled_grpc_services,
        grpc_listen_addr=?grpc_listen_addr,
        "starting gRPC server listening on {grpc_listen_addr}"
    );
    // nodelay=true and keepalive=None are the default values for Server::builder()
    let tcp_incoming = TcpIncoming::from(tcp_listener).with_nodelay(Some(true));
    let serve_fut = server_router.serve_with_incoming_shutdown(tcp_incoming, shutdown_signal);
    let (serve_res, _trigger_res) = tokio::join!(serve_fut, readiness_trigger);
    serve_res?;
    Ok(())
}
