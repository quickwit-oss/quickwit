// Copyright (C) 2021 Quickwit, Inc.
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

mod args;
mod counters;
mod error;
mod grpc;
mod grpc_adapter;
mod http_handler;
mod rest;

use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;

use quickwit_cluster::cluster::{read_or_create_host_key, Cluster};
use quickwit_cluster::service::ClusterServiceImpl;
use quickwit_metastore::{do_checks, MetastoreUriResolver};
use quickwit_search::{
    http_addr_to_grpc_addr, http_addr_to_swim_addr, ClusterClient, SearchClientPool,
    SearchServiceImpl,
};
use quickwit_storage::quickwit_storage_uri_resolver;
use quickwit_telemetry::payload::{ServeEvent, TelemetryEvent};
use termcolor::{self, Color, ColorChoice, ColorSpec, StandardStream, WriteColor};
use tracing::debug;

pub use crate::args::ServeArgs;
pub use crate::counters::COUNTERS;
pub use crate::error::ApiError;
use crate::grpc::start_grpc_service;
use crate::grpc_adapter::cluster_adapter::GrpcClusterAdapter;
use crate::grpc_adapter::search_adapter::GrpcSearchAdapter;
use crate::rest::start_rest_service;

fn display_help_message(
    rest_socket_addr: SocketAddr,
    example_index_name: &str,
) -> anyhow::Result<()> {
    // No-color if we are not in a terminal.
    let mut stdout = StandardStream::stdout(ColorChoice::Auto);
    write!(&mut stdout, "Server started on ")?;
    stdout.set_color(ColorSpec::new().set_fg(Some(Color::Green)))?;
    writeln!(&mut stdout, "http://{}/", &rest_socket_addr)?;
    stdout.set_color(&ColorSpec::new())?;
    writeln!(
        &mut stdout,
        "\nYou can test it using the following command:"
    )?;
    stdout.set_color(ColorSpec::new().set_fg(Some(Color::Blue)))?;
    writeln!(
        &mut stdout,
        "curl 'http://{}/api/v1/{}/search?query=my+query'",
        rest_socket_addr, example_index_name
    )?;
    stdout.set_color(&ColorSpec::new())?;
    // TODO add link to the documentation of the query language.
    Ok(())
}

/// Start Quickwit search node.
pub async fn serve_cli(args: ServeArgs) -> anyhow::Result<()> {
    debug!(args=?args, "serve-cli");
    quickwit_telemetry::send_telemetry_event(TelemetryEvent::Serve(ServeEvent {
        has_seed: !args.peer_socket_addrs.is_empty(),
    }))
    .await;
    let storage_resolver = quickwit_storage_uri_resolver().clone();
    let metastore_resolver = MetastoreUriResolver::default();
    let example_index_name = "my_index".to_string();
    let metastore = metastore_resolver.resolve(&args.metastore_uri).await?;

    do_checks(vec![("metastore", metastore.check_connectivity())]).await?;

    let host_key = read_or_create_host_key(args.host_key_path.as_path())?;
    let swim_addr = http_addr_to_swim_addr(args.rest_socket_addr);
    let cluster = Arc::new(Cluster::new(host_key, swim_addr)?);
    for peer_socket_addr in args
        .peer_socket_addrs
        .iter()
        .filter(|peer_rest_addr| peer_rest_addr != &&args.rest_socket_addr)
    {
        // If the peer address is specified,
        // it joins the cluster in which that node participates.
        let peer_swim_addr = http_addr_to_swim_addr(*peer_socket_addr);
        debug!(peer_swim_addr=?peer_swim_addr, "Add peer node.");
        cluster.add_peer_node(peer_swim_addr).await;
    }

    let client_pool = Arc::new(SearchClientPool::new(cluster.clone()).await?);
    let cluster_client = ClusterClient::new(client_pool.clone());
    let search_service = Arc::new(SearchServiceImpl::new(
        metastore,
        storage_resolver,
        cluster_client,
        client_pool,
    ));

    let cluster_service = Arc::new(ClusterServiceImpl::new(cluster.clone()));

    let grpc_socket_addr = http_addr_to_grpc_addr(args.rest_socket_addr);
    let grpc_search_service = GrpcSearchAdapter::from(search_service.clone());
    let grpc_cluster_service = GrpcClusterAdapter::from(cluster_service.clone());
    let grpc_server =
        start_grpc_service(grpc_socket_addr, grpc_search_service, grpc_cluster_service);

    let rest_server = start_rest_service(args.rest_socket_addr, search_service, cluster_service);

    display_help_message(args.rest_socket_addr, &example_index_name)?;

    tokio::try_join!(rest_server, grpc_server)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::array::IntoIter;
    use std::collections::HashMap;
    use std::ops::Range;
    use std::sync::Arc;

    use futures::TryStreamExt;
    use quickwit_index_config::WikipediaIndexConfig;
    use quickwit_indexing::mock_split_meta;
    use quickwit_metastore::checkpoint::Checkpoint;
    use quickwit_metastore::{IndexMetadata, MockMetastore, SplitState};
    use quickwit_proto::search_service_server::SearchServiceServer;
    use quickwit_proto::OutputFormat;
    use quickwit_search::{
        create_search_service_client, root_search_stream, MockSearchService, SearchError,
        SearchService,
    };
    use tokio::sync::RwLock;
    use tokio_stream::wrappers::UnboundedReceiverStream;
    use tonic::transport::Server;

    use super::*;

    async fn start_test_server(
        address: SocketAddr,
        search_service: Arc<dyn SearchService>,
    ) -> anyhow::Result<()> {
        let search_grpc_adpater = GrpcSearchAdapter::from_mock(search_service);
        let _ = tokio::spawn(async move {
            Server::builder()
                .add_service(SearchServiceServer::new(search_grpc_adpater))
                .serve(address)
                .await?;
            Result::<_, anyhow::Error>::Ok(())
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_serve_search_stream_with_a_leaf_error_on_leaf_node() -> anyhow::Result<()> {
        // This test aims at checking the client grpc implementation.
        let request = quickwit_proto::SearchStreamRequest {
            index_id: "test-idx".to_string(),
            query: "test".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: None,
            end_timestamp: None,
            fast_field: "timestamp".to_string(),
            output_format: OutputFormat::Csv as i32,
            partition_by_field: None,
            tags: vec![],
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata {
                    index_id: "test-idx".to_string(),
                    index_uri: "file:///path/to/index/test-idx".to_string(),
                    index_config: Arc::new(WikipediaIndexConfig::new()),
                    checkpoint: Checkpoint::default(),
                })
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str,
             _split_state: SplitState,
             _time_range: Option<Range<i64>>,
             _tags: &[String]| {
                Ok(vec![mock_split_meta("split_1"), mock_split_meta("split_2")])
            },
        );
        let mut mock_search_service = MockSearchService::new();
        let (result_sender, result_receiver) = tokio::sync::mpsc::unbounded_channel();
        result_sender.send(Ok(quickwit_proto::LeafSearchStreamResponse {
            data: b"123".to_vec(),
            split_id: "split_1".to_string(),
        }))?;
        result_sender.send(Err(SearchError::InternalError(
            "Error on `split2`".to_string(),
        )))?;
        mock_search_service
            .expect_leaf_search_stream()
            .withf(|request| request.split_metadata.len() == 2) // First request.
            .return_once(
                |_leaf_search_req: quickwit_proto::LeafSearchStreamRequest| {
                    Ok(UnboundedReceiverStream::new(result_receiver))
                },
            );
        mock_search_service
            .expect_leaf_search_stream()
            .withf(|request| request.split_metadata.len() == 1) // Retry request on the failing split.
            .return_once(
                |_leaf_search_req: quickwit_proto::LeafSearchStreamRequest| {
                    Err(SearchError::InternalError(
                        "Error again on `split2`".to_string(),
                    ))
                },
            );
        // The test will hang on indefinitely if we don't drop the sender.
        drop(result_sender);

        let grpc_addr: SocketAddr = format!("127.0.0.1:{}", 10000).parse()?;
        start_test_server(grpc_addr, Arc::new(mock_search_service)).await?;
        let client = create_search_service_client(grpc_addr).await?;
        let clients: HashMap<_, _> = IntoIter::new([(grpc_addr, client)]).collect();
        let client_pool = Arc::new(SearchClientPool {
            clients: Arc::new(RwLock::new(clients)),
        });
        let cluster_client = ClusterClient::new(client_pool.clone());
        let stream = root_search_stream(request, &metastore, cluster_client, &client_pool).await?;
        let result: Result<Vec<_>, SearchError> = stream.try_collect().await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Internal error: `Internal error: `Error again on `split2``.`."
        );
        Ok(())
    }
}
