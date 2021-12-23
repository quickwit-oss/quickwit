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

use std::sync::Arc;

use quickwit_cluster::cluster::Cluster;
use quickwit_cluster::service::ClusterServiceImpl;
use quickwit_config::{QuickwitConfig, SEARCHER_CONFIG_INSTANCE};
use quickwit_metastore::Metastore;
use quickwit_search::{ClusterClient, SearchClientPool, SearchServiceImpl};
use quickwit_storage::{
    LocalFileStorageFactory, RegionProvider, S3CompatibleObjectStorageFactory, StorageUriResolver,
};
use tracing::{debug, info};

pub use crate::args::ServeArgs;
pub use crate::counters::COUNTERS;
pub use crate::error::ApiError;
use crate::grpc::start_grpc_service;
use crate::grpc_adapter::cluster_adapter::GrpcClusterAdapter;
use crate::grpc_adapter::search_adapter::GrpcSearchAdapter;
use crate::rest::start_rest_service;

/// Builds a storage uri resolver that handles
/// - s3:// uris. This storage comes with a cache that stores hotcache files.
/// - s3+localstack://
/// - file:// uris.
fn storage_uri_resolver() -> StorageUriResolver {
    let s3_storage = S3CompatibleObjectStorageFactory::default();
    StorageUriResolver::builder()
        .register(LocalFileStorageFactory::default())
        .register(s3_storage)
        .register(S3CompatibleObjectStorageFactory::new(
            RegionProvider::Localstack,
            "s3+localstack",
        ))
        .build()
}

/// Starts a search node, aka a `searcher`.
pub async fn run_searcher(
    quickwit_config: QuickwitConfig,
    metastore: Arc<dyn Metastore>,
) -> anyhow::Result<()> {
    SEARCHER_CONFIG_INSTANCE
        .set(quickwit_config.searcher_config.clone())
        .expect("could not set searcher config in global once cell");
    let searcher_config = quickwit_config.searcher_config;
    let cluster = Arc::new(Cluster::new(
        quickwit_config.node_id.clone(),
        searcher_config.gossip_socket_addr()?,
    )?);
    for peer_socket_addr in searcher_config.peer_socket_addrs()? {
        // If the peer address is specified,
        // it joins the cluster in which that node participates.
        debug!(peer_addr = %peer_socket_addr, "Add peer node.");
        cluster.add_peer_node(peer_socket_addr).await;
    }
    let storage_uri_resolver = storage_uri_resolver();
    let client_pool = Arc::new(SearchClientPool::new(cluster.clone()).await?);
    let cluster_client = ClusterClient::new(client_pool.clone());
    let search_service = Arc::new(SearchServiceImpl::new(
        metastore,
        storage_uri_resolver,
        cluster_client,
        client_pool,
    ));

    let cluster_service = Arc::new(ClusterServiceImpl::new(cluster.clone()));

    let grpc_addr = searcher_config.grpc_socket_addr()?;
    let grpc_search_service = GrpcSearchAdapter::from(search_service.clone());
    let grpc_cluster_service = GrpcClusterAdapter::from(cluster_service.clone());
    let grpc_server = start_grpc_service(grpc_addr, grpc_search_service, grpc_cluster_service);

    let rest_socket_addr = searcher_config.rest_socket_addr()?;
    let rest_server = start_rest_service(rest_socket_addr, search_service, cluster_service);
    info!(
        "Searcher ready to accept requests at http://{}/",
        rest_socket_addr
    );
    tokio::try_join!(rest_server, grpc_server)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::array::IntoIter;
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::ops::Range;
    use std::sync::Arc;

    use futures::TryStreamExt;
    use quickwit_indexing::mock_split;
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
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-idx",
                    "file:///path/to/index/test-idx",
                ))
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str, _split_state: SplitState, _time_range: Option<Range<i64>>, _tags| {
                Ok(vec![mock_split("split_1"), mock_split("split_2")])
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
