// Copyright (C) 2022 Quickwit, Inc.
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

mod grpc_adapter;
mod rest_handler;

pub use self::grpc_adapter::GrpcSearchAdapter;
pub use self::rest_handler::{search_get_handler, search_post_handler, search_stream_handler};

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::ops::Range;
    use std::sync::Arc;

    use futures::TryStreamExt;
    use quickwit_indexing::mock_split;
    use quickwit_metastore::{IndexMetadata, MockMetastore, SplitState};
    use quickwit_proto::search_service_server::SearchServiceServer;
    use quickwit_proto::{tonic, OutputFormat};
    use quickwit_search::{
        root_search_stream, ClusterClient, MockSearchService, SearchClientPool, SearchError,
        SearchService,
    };
    use tokio_stream::wrappers::UnboundedReceiverStream;
    use tonic::transport::Server;

    use crate::search_api::GrpcSearchAdapter;

    async fn start_test_server(
        address: SocketAddr,
        search_service: Arc<dyn SearchService>,
    ) -> anyhow::Result<()> {
        let search_grpc_adpater = GrpcSearchAdapter::from(search_service);
        tokio::spawn(async move {
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
        // This test aims at checking the client gRPC implementation.
        let request = quickwit_proto::SearchStreamRequest {
            index_id: "test-index".to_string(),
            query: "test".to_string(),
            search_fields: vec!["body".to_string()],
            snippet_fields: vec![],
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
                    "test-index",
                    "ram:///indexes/test-index",
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
            .withf(|request| request.split_offsets.len() == 2) // First request.
            .return_once(
                |_leaf_search_req: quickwit_proto::LeafSearchStreamRequest| {
                    Ok(UnboundedReceiverStream::new(result_receiver))
                },
            );
        mock_search_service
            .expect_leaf_search_stream()
            .withf(|request| request.split_offsets.len() == 1) // Retry request on the failing split.
            .return_once(
                |_leaf_search_req: quickwit_proto::LeafSearchStreamRequest| {
                    Err(SearchError::InternalError(
                        "Error again on `split2`".to_string(),
                    ))
                },
            );
        // The test will hang on indefinitely if we don't drop the sender.
        drop(result_sender);

        let grpc_addr: SocketAddr = "127.0.0.1:20000".parse()?;
        start_test_server(grpc_addr, Arc::new(mock_search_service)).await?;
        let client_pool = SearchClientPool::for_addrs(&[grpc_addr]).await?;
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
