// Copyright (C) 2023 Quickwit, Inc.
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
pub(crate) use self::rest_handler::extract_index_id_patterns;
pub use self::rest_handler::{
    search_get_handler, search_post_handler, search_request_from_api_request,
    search_stream_handler, SearchApi, SearchRequestQueryString, SortBy,
};

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use futures::TryStreamExt;
    use quickwit_indexing::MockSplitBuilder;
    use quickwit_metastore::{IndexMetadata, MockMetastore};
    use quickwit_proto::search::search_service_server::SearchServiceServer;
    use quickwit_proto::search::OutputFormat;
    use quickwit_proto::tonic;
    use quickwit_query::query_ast::qast_json_helper;
    use quickwit_search::{
        create_search_client_from_grpc_addr, root_search_stream, ClusterClient, MockSearchService,
        SearchError, SearchJobPlacer, SearchService, SearcherPool,
    };
    use tokio_stream::wrappers::UnboundedReceiverStream;
    use tonic::transport::Server;

    use crate::search_api::GrpcSearchAdapter;

    async fn start_test_server(
        address: SocketAddr,
        search_service: Arc<dyn SearchService>,
    ) -> anyhow::Result<()> {
        let search_grpc_adapter = GrpcSearchAdapter::from(search_service);
        tokio::spawn(async move {
            Server::builder()
                .add_service(SearchServiceServer::new(search_grpc_adapter))
                .serve(address)
                .await?;
            Result::<_, anyhow::Error>::Ok(())
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_serve_search_stream_with_a_leaf_error_on_leaf_node() -> anyhow::Result<()> {
        // This test aims at checking the client gRPC implementation.
        let request = quickwit_proto::search::SearchStreamRequest {
            index_id: "test-index".to_string(),
            query_ast: qast_json_helper("test", &["body"]),
            snippet_fields: Vec::new(),
            start_timestamp: None,
            end_timestamp: None,
            fast_field: "timestamp".to_string(),
            output_format: OutputFormat::Csv as i32,
            partition_by_field: None,
        };
        let mut metastore = MockMetastore::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///indexes/test-index");
        let index_uid = index_metadata.index_uid.clone();
        metastore
            .expect_index_metadata()
            .returning(move |_index_id: &str| Ok(index_metadata.clone()));
        metastore.expect_list_splits().returning(move |_filter| {
            Ok(vec![
                MockSplitBuilder::new("split_1")
                    .with_index_uid(&index_uid)
                    .build(),
                MockSplitBuilder::new("split_2")
                    .with_index_uid(&index_uid)
                    .build(),
            ])
        });
        let mut mock_search_service = MockSearchService::new();
        let (result_sender, result_receiver) = tokio::sync::mpsc::unbounded_channel();
        result_sender.send(Ok(quickwit_proto::search::LeafSearchStreamResponse {
            data: b"123".to_vec(),
            split_id: "split_1".to_string(),
        }))?;
        result_sender.send(Err(SearchError::Internal("Error on `split2`".to_string())))?;
        mock_search_service
            .expect_leaf_search_stream()
            .withf(|request| request.split_offsets.len() == 2) // First request.
            .return_once(
                |_leaf_search_req: quickwit_proto::search::LeafSearchStreamRequest| {
                    Ok(UnboundedReceiverStream::new(result_receiver))
                },
            );
        mock_search_service
            .expect_leaf_search_stream()
            .withf(|request| request.split_offsets.len() == 1) // Retry request on the failing split.
            .return_once(
                |_leaf_search_req: quickwit_proto::search::LeafSearchStreamRequest| {
                    Err(SearchError::Internal("error again on `split2`".to_string()))
                },
            );
        // The test will hang on indefinitely if we don't drop the sender.
        drop(result_sender);

        let grpc_addr: SocketAddr = "127.0.0.1:10001".parse()?;
        start_test_server(grpc_addr, Arc::new(mock_search_service)).await?;

        let searcher_pool = SearcherPool::default();
        searcher_pool.insert(grpc_addr, create_search_client_from_grpc_addr(grpc_addr));
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let stream = root_search_stream(request, &metastore, cluster_client).await?;
        let search_stream_result: Result<Vec<_>, SearchError> = stream.try_collect().await;
        let search_error = search_stream_result.unwrap_err();
        assert_eq!(
            search_error.to_string(),
            "internal error: `internal error: `error again on `split2```"
        );
        Ok(())
    }
}
