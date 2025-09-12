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

use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::Context;
use quickwit_cluster::cluster_grpc_server;
use quickwit_common::tower::BoxFutureInfaillible;
use quickwit_config::GrpcConfig;
use quickwit_config::service::QuickwitService;
use quickwit_proto::developer::DeveloperServiceClient;
use quickwit_proto::indexing::IndexingServiceClient;
use quickwit_proto::jaeger::storage::v1::span_reader_plugin_server::SpanReaderPluginServer;
use quickwit_proto::opentelemetry::proto::collector::logs::v1::logs_service_server::LogsServiceServer;
use quickwit_proto::opentelemetry::proto::collector::trace::v1::trace_service_server::TraceServiceServer;
use quickwit_proto::search::search_service_server::SearchServiceServer;
use quickwit_proto::tonic::codegen::CompressionEncoding;
use quickwit_proto::tonic::transport::server::TcpIncoming;
use quickwit_proto::tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tokio::net::TcpListener;
use tonic_health::pb::FILE_DESCRIPTOR_SET as HEALTH_FILE_DESCRIPTOR_SET;
use tonic_health::pb::health_server::{Health, HealthServer};
use tonic_reflection::pb::v1::FILE_DESCRIPTOR_SET as REFLECTION_FILE_DESCRIPTOR_SET;
use tonic_reflection::server::v1::{ServerReflection, ServerReflectionServer};
use tracing::*;

use crate::developer_api::DeveloperApiServer;
use crate::search_api::GrpcSearchAdapter;
use crate::{INDEXING_GRPC_SERVER_METRICS_LAYER, QuickwitServices};

/// Starts and binds gRPC services to `grpc_listen_addr`.
pub(crate) async fn start_grpc_server(
    tcp_listener: TcpListener,
    grpc_config: GrpcConfig,
    services: Arc<QuickwitServices>,
    readiness_trigger: BoxFutureInfaillible<()>,
    shutdown_signal: BoxFutureInfaillible<()>,
    health_service: HealthServer<impl Health>,
) -> anyhow::Result<()> {
    let mut enabled_grpc_services = BTreeSet::new();
    let mut file_descriptor_sets = Vec::new();
    let mut server = Server::builder();

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
        // to do better, we'd need to wra the TcpListener with something that does (m)TLS
        // and that we control, however it would be somewhat painful, and more error prone
        server = server.tls_config(tls)?;
    }

    let cluster_grpc_service = cluster_grpc_server(services.cluster.clone());
    file_descriptor_sets.push(quickwit_proto::cluster::CLUSTER_PLANE_FILE_DESCRIPTOR_SET);

    // Mount gRPC metastore service if `QuickwitService::Metastore` is enabled on node.
    let metastore_grpc_service = if let Some(metastore_server) = &services.metastore_server_opt {
        enabled_grpc_services.insert("metastore");
        file_descriptor_sets.push(quickwit_proto::metastore::METASTORE_FILE_DESCRIPTOR_SET);

        Some(metastore_server.as_grpc_service(grpc_config.max_message_size))
    } else {
        None
    };
    // Mount gRPC indexing service if `QuickwitService::Indexer` is enabled on node.
    let indexing_grpc_service = if services
        .node_config
        .is_service_enabled(QuickwitService::Indexer)
    {
        if let Some(indexing_service) = services.indexing_service_opt.clone() {
            enabled_grpc_services.insert("indexing");
            file_descriptor_sets.push(quickwit_proto::indexing::INDEXING_FILE_DESCRIPTOR_SET);

            let indexing_service = IndexingServiceClient::tower()
                .stack_layer(INDEXING_GRPC_SERVER_METRICS_LAYER.clone())
                .build_from_mailbox(indexing_service);
            Some(indexing_service.as_grpc_service(grpc_config.max_message_size))
        } else {
            None
        }
    } else {
        None
    };
    // Mount gRPC ingest service if `QuickwitService::Indexer` is enabled on node.
    let ingest_api_grpc_service = if services
        .node_config
        .is_service_enabled(QuickwitService::Indexer)
    {
        enabled_grpc_services.insert("ingest-api");
        Some(
            services
                .ingest_service
                .as_grpc_service(grpc_config.max_message_size),
        )
    } else {
        None
    };
    let ingest_router_grpc_service = if services
        .node_config
        .is_service_enabled(QuickwitService::Indexer)
    {
        enabled_grpc_services.insert("ingest-router");

        let ingest_router_service = services
            .ingest_router_service
            .as_grpc_service(grpc_config.max_message_size);
        Some(ingest_router_service)
    } else {
        None
    };

    let ingester_grpc_service = if let Some(ingester_service) = services.ingester_service() {
        enabled_grpc_services.insert("ingester");
        file_descriptor_sets.push(quickwit_proto::ingest::INGEST_FILE_DESCRIPTOR_SET);
        let ingester_grpc_service = ingester_service.as_grpc_service(grpc_config.max_message_size);
        Some(ingester_grpc_service)
    } else {
        None
    };

    // Mount gRPC control plane service if `QuickwitService::ControlPlane` is enabled on node.
    let control_plane_grpc_service = if services
        .node_config
        .is_service_enabled(QuickwitService::ControlPlane)
    {
        enabled_grpc_services.insert("control-plane");
        file_descriptor_sets.push(quickwit_proto::control_plane::CONTROL_PLANE_FILE_DESCRIPTOR_SET);

        Some(
            services
                .control_plane_client
                .as_grpc_service(grpc_config.max_message_size),
        )
    } else {
        None
    };
    // Mount gRPC OpenTelemetry OTLP services if present.
    let otlp_trace_grpc_service =
        if let Some(otlp_traces_service) = services.otlp_traces_service_opt.clone() {
            enabled_grpc_services.insert("otlp-traces");
            let trace_service = TraceServiceServer::new(otlp_traces_service)
                .accept_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Zstd);
            Some(trace_service)
        } else {
            None
        };
    let otlp_log_grpc_service =
        if let Some(otlp_logs_service) = services.otlp_logs_service_opt.clone() {
            enabled_grpc_services.insert("otlp-logs");
            let logs_service = LogsServiceServer::new(otlp_logs_service)
                .accept_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Zstd);
            Some(logs_service)
        } else {
            None
        };
    // Mount gRPC search service if `QuickwitService::Searcher` is enabled on node.
    let search_grpc_service = if services
        .node_config
        .is_service_enabled(QuickwitService::Searcher)
    {
        enabled_grpc_services.insert("search");
        file_descriptor_sets.push(quickwit_proto::search::SEARCH_FILE_DESCRIPTOR_SET);

        let search_service = services.search_service.clone();
        let grpc_search_service = GrpcSearchAdapter::from(search_service);
        Some(
            SearchServiceServer::new(grpc_search_service)
                .max_decoding_message_size(grpc_config.max_message_size.0 as usize)
                .max_encoding_message_size(grpc_config.max_message_size.0 as usize),
        )
    } else {
        None
    };

    // Mount gRPC jaeger service if present.
    let jaeger_grpc_service = if let Some(jaeger_service) = services.jaeger_service_opt.clone() {
        enabled_grpc_services.insert("jaeger");
        Some(SpanReaderPluginServer::new(jaeger_service))
    } else {
        None
    };
    let developer_grpc_service = {
        enabled_grpc_services.insert("developer");
        file_descriptor_sets.push(quickwit_proto::developer::DEVELOPER_FILE_DESCRIPTOR_SET);

        let developer_service = DeveloperApiServer::from_services(&services);

        DeveloperServiceClient::new(developer_service)
            .as_grpc_service(DeveloperApiServer::MAX_GRPC_MESSAGE_SIZE)
    };
    enabled_grpc_services.insert("health");
    file_descriptor_sets.push(HEALTH_FILE_DESCRIPTOR_SET);

    enabled_grpc_services.insert("reflection");
    file_descriptor_sets.push(REFLECTION_FILE_DESCRIPTOR_SET);
    let reflection_service = build_reflection_service(&file_descriptor_sets)?;

    let server_router = server
        .add_service(cluster_grpc_service)
        .add_service(developer_grpc_service)
        .add_service(health_service)
        .add_service(reflection_service)
        .add_optional_service(control_plane_grpc_service)
        .add_optional_service(indexing_grpc_service)
        .add_optional_service(ingest_api_grpc_service)
        .add_optional_service(ingest_router_grpc_service)
        .add_optional_service(ingester_grpc_service)
        .add_optional_service(jaeger_grpc_service)
        .add_optional_service(metastore_grpc_service)
        .add_optional_service(otlp_log_grpc_service)
        .add_optional_service(otlp_trace_grpc_service)
        .add_optional_service(search_grpc_service);

    let grpc_listen_addr = tcp_listener.local_addr()?;
    info!(
        enabled_grpc_services=?enabled_grpc_services,
        grpc_listen_addr=?grpc_listen_addr,
        "starting gRPC server listening on {grpc_listen_addr}"
    );
    // nodelay=true and keepalive=None are the default values for Server::builder()
    let tcp_incoming = TcpIncoming::from(tcp_listener)
        .with_nodelay(Some(true))
        .with_keepalive(None);
    let serve_fut = server_router.serve_with_incoming_shutdown(tcp_incoming, shutdown_signal);
    let (serve_res, _trigger_res) = tokio::join!(serve_fut, readiness_trigger);
    serve_res?;
    Ok(())
}

fn build_reflection_service(
    file_descriptor_sets: &[&[u8]],
) -> anyhow::Result<ServerReflectionServer<impl ServerReflection>> {
    let mut builder = tonic_reflection::server::Builder::configure();

    for file_descriptor_set in file_descriptor_sets {
        builder = builder.register_encoded_file_descriptor_set(file_descriptor_set)
    }
    builder
        .build_v1()
        .context("failed to build reflection service")
}
