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

mod grpc_adapter;
mod rest_handler;

pub use self::grpc_adapter::GrpcSearchAdapter;
pub(crate) use self::rest_handler::{extract_index_id_patterns, extract_index_id_patterns_default};
pub use self::rest_handler::{
    search_get_handler, search_plan_get_handler, search_plan_post_handler, search_post_handler,
    search_request_from_api_request, search_stream_handler, SearchApi, SearchRequestQueryString,
    SortBy,
};

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use bytesize::ByteSize;
    use futures::TryStreamExt;
    use quickwit_common::ServiceStream;
    use quickwit_indexing::MockSplitBuilder;
    use quickwit_metastore::{IndexMetadata, IndexMetadataResponseExt, ListSplitsResponseExt};
    use quickwit_proto::metastore::{
        IndexMetadataResponse, ListSplitsResponse, MetastoreServiceClient, MockMetastoreService,
    };
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
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///indexes/test-index");
        let index_uid = index_metadata.index_uid.clone();
        mock_metastore.expect_index_metadata().returning(move |_| {
            Ok(IndexMetadataResponse::try_from_index_metadata(&index_metadata).unwrap())
        });
        mock_metastore.expect_list_splits().returning(move |_| {
            let splits = vec![
                MockSplitBuilder::new("split_1")
                    .with_index_uid(&index_uid)
                    .build(),
                MockSplitBuilder::new("split_2")
                    .with_index_uid(&index_uid)
                    .build(),
            ];
            let splits = ListSplitsResponse::try_from_splits(splits).unwrap();
            Ok(ServiceStream::from(vec![Ok(splits)]))
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
        searcher_pool.insert(
            grpc_addr,
            create_search_client_from_grpc_addr(grpc_addr, ByteSize::mib(1)),
        );
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let stream = root_search_stream(
            request,
            MetastoreServiceClient::from_mock(mock_metastore),
            cluster_client,
        )
        .await?;
        let search_stream_result: Result<Vec<_>, SearchError> = stream.try_collect().await;
        let search_error = search_stream_result.unwrap_err();
        assert_eq!(
            search_error.to_string(),
            "internal error: `internal error: `error again on `split2```"
        );
        Ok(())
    }
}
