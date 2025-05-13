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

use std::collections::HashSet;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use quickwit_common::uri::Uri;
use quickwit_config::build_doc_mapper;
use quickwit_doc_mapper::tag_pruning::extract_tags_from_query;
use quickwit_metastore::IndexMetadataResponseExt;
use quickwit_proto::metastore::{IndexMetadataRequest, MetastoreService, MetastoreServiceClient};
use quickwit_proto::search::{LeafSearchStreamRequest, SearchRequest, SearchStreamRequest};
use quickwit_query::query_ast::QueryAst;
use tokio_stream::StreamMap;
use tracing::*;

use crate::cluster_client::ClusterClient;
use crate::extract_timestamp_range::extract_start_end_timestamp_from_ast;
use crate::root::SearchJob;
use crate::{SearchError, list_relevant_splits};

/// Perform a distributed search stream.
#[instrument(skip(metastore, cluster_client))]
pub async fn root_search_stream(
    mut search_stream_request: SearchStreamRequest,
    mut metastore: MetastoreServiceClient,
    cluster_client: ClusterClient,
) -> crate::Result<impl futures::Stream<Item = crate::Result<Bytes>>> {
    // TODO: building a search request should not be necessary for listing splits.
    // This needs some refactoring: relevant splits, metadata_map, jobs...
    let index_metadata_request =
        IndexMetadataRequest::for_index_id(search_stream_request.index_id.clone());
    let index_metadata = metastore
        .index_metadata(index_metadata_request)
        .await?
        .deserialize_index_metadata()?;
    let index_uid = index_metadata.index_uid.clone();
    let index_config = index_metadata.into_index_config();

    let doc_mapper = build_doc_mapper(&index_config.doc_mapping, &index_config.search_settings)
        .map_err(|err| {
            SearchError::Internal(format!("failed to build doc mapper. cause: {err}"))
        })?;

    let query_ast: QueryAst = serde_json::from_str(&search_stream_request.query_ast)
        .map_err(|err| SearchError::InvalidQuery(err.to_string()))?;
    let mut query_ast_resolved = query_ast.parse_user_query(doc_mapper.default_search_fields())?;
    let tags_filter_ast = extract_tags_from_query(query_ast_resolved.clone());

    if let Some(timestamp_field) = doc_mapper.timestamp_field_name() {
        query_ast_resolved = extract_start_end_timestamp_from_ast(
            query_ast_resolved,
            timestamp_field,
            &mut search_stream_request.start_timestamp,
            &mut search_stream_request.end_timestamp,
        );
    }

    // Validates the query by effectively building it against the current schema.
    doc_mapper.query(doc_mapper.schema(), &query_ast_resolved, true)?;
    search_stream_request.query_ast = serde_json::to_string(&query_ast_resolved)?;

    let search_request = SearchRequest::try_from(search_stream_request.clone())?;
    let split_metadatas = list_relevant_splits(
        vec![index_uid],
        search_request.start_timestamp,
        search_request.end_timestamp,
        tags_filter_ast,
        &mut metastore,
    )
    .await?;

    let doc_mapper_str = serde_json::to_string(&doc_mapper).map_err(|err| {
        SearchError::Internal(format!("failed to serialize doc mapper: cause {err}"))
    })?;

    let index_uri: &Uri = &index_config.index_uri;
    let leaf_search_jobs: Vec<SearchJob> = split_metadatas.iter().map(SearchJob::from).collect();
    let assigned_leaf_search_jobs = cluster_client
        .search_job_placer
        .assign_jobs(leaf_search_jobs, &HashSet::default())
        .await?;

    let mut stream_map: StreamMap<usize, _> = StreamMap::new();
    for (leaf_ord, (client, client_jobs)) in assigned_leaf_search_jobs.enumerate() {
        let leaf_request: LeafSearchStreamRequest = jobs_to_leaf_request(
            &search_stream_request,
            &doc_mapper_str,
            index_uri.as_ref(),
            client_jobs,
        );
        let leaf_stream = cluster_client
            .leaf_search_stream(leaf_request, client)
            .await;
        stream_map.insert(leaf_ord, leaf_stream);
    }
    Ok(stream_map
        .map(|(_leaf_ord, result)| result)
        .map_ok(|leaf_response| Bytes::from(leaf_response.data)))
}

fn jobs_to_leaf_request(
    request: &SearchStreamRequest,
    doc_mapper_str: &str,
    index_uri: &str, // TODO make Uri
    jobs: Vec<SearchJob>,
) -> LeafSearchStreamRequest {
    LeafSearchStreamRequest {
        request: Some(request.clone()),
        split_offsets: jobs.into_iter().map(Into::into).collect(),
        doc_mapper: doc_mapper_str.to_string(),
        index_uri: index_uri.to_string(),
    }
}

#[cfg(test)]
mod tests {

    use quickwit_common::ServiceStream;
    use quickwit_indexing::MockSplitBuilder;
    use quickwit_metastore::{IndexMetadata, ListSplitsResponseExt};
    use quickwit_proto::metastore::{
        IndexMetadataResponse, ListSplitsResponse, MockMetastoreService,
    };
    use quickwit_proto::search::OutputFormat;
    use quickwit_query::query_ast::qast_json_helper;
    use tokio_stream::wrappers::UnboundedReceiverStream;

    use super::*;
    use crate::{MockSearchService, SearchJobPlacer, searcher_pool_for_test};

    #[tokio::test]
    async fn test_root_search_stream_single_split() -> anyhow::Result<()> {
        let request = quickwit_proto::search::SearchStreamRequest {
            index_id: "test-index".to_string(),
            query_ast: qast_json_helper("test", &["body"]),
            fast_field: "timestamp".to_string(),
            output_format: OutputFormat::Csv as i32,
            ..Default::default()
        };
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        mock_metastore.expect_index_metadata().returning(move |_| {
            Ok(IndexMetadataResponse::try_from_index_metadata(&index_metadata).unwrap())
        });
        mock_metastore.expect_list_splits().returning(move |_| {
            let splits = vec![
                MockSplitBuilder::new("split1")
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
        result_sender.send(Ok(quickwit_proto::search::LeafSearchStreamResponse {
            data: b"456".to_vec(),
            split_id: "split_1".to_string(),
        }))?;
        mock_search_service.expect_leaf_search_stream().return_once(
            |_leaf_search_req: quickwit_proto::search::LeafSearchStreamRequest| {
                Ok(UnboundedReceiverStream::new(result_receiver))
            },
        );
        // The test will hang on indefinitely if we don't drop the receiver.
        drop(result_sender);

        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", mock_search_service)]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let result: Vec<Bytes> = root_search_stream(
            request,
            MetastoreServiceClient::from_mock(mock_metastore),
            cluster_client,
        )
        .await?
        .try_collect()
        .await?;
        assert_eq!(result.len(), 2);
        assert_eq!(&result[0], &b"123"[..]);
        assert_eq!(&result[1], &b"456"[..]);
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_stream_single_split_partitioned() -> anyhow::Result<()> {
        let request = quickwit_proto::search::SearchStreamRequest {
            index_id: "test-index".to_string(),
            query_ast: qast_json_helper("test", &["body"]),
            fast_field: "timestamp".to_string(),
            output_format: OutputFormat::Csv as i32,
            partition_by_field: Some("timestamp".to_string()),
            ..Default::default()
        };
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        mock_metastore.expect_index_metadata().returning(move |_| {
            Ok(IndexMetadataResponse::try_from_index_metadata(&index_metadata).unwrap())
        });
        mock_metastore.expect_list_splits().returning(move |_| {
            let splits = vec![
                MockSplitBuilder::new("split1")
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
            split_id: "1".to_string(),
        }))?;
        result_sender.send(Ok(quickwit_proto::search::LeafSearchStreamResponse {
            data: b"456".to_vec(),
            split_id: "2".to_string(),
        }))?;
        mock_search_service.expect_leaf_search_stream().return_once(
            |_leaf_search_req: quickwit_proto::search::LeafSearchStreamRequest| {
                Ok(UnboundedReceiverStream::new(result_receiver))
            },
        );
        // The test will hang on indefinitely if we don't drop the sender.
        drop(result_sender);

        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", mock_search_service)]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let stream = root_search_stream(
            request,
            MetastoreServiceClient::from_mock(mock_metastore),
            cluster_client,
        )
        .await?;
        let result: Vec<_> = stream.try_collect().await?;
        assert_eq!(result.len(), 2);
        assert_eq!(&result[0], &b"123"[..]);
        assert_eq!(&result[1], &b"456"[..]);
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_stream_single_split_with_error() -> anyhow::Result<()> {
        let request = quickwit_proto::search::SearchStreamRequest {
            index_id: "test-index".to_string(),
            query_ast: qast_json_helper("test", &["body"]),
            fast_field: "timestamp".to_string(),
            output_format: OutputFormat::Csv as i32,
            ..Default::default()
        };
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        mock_metastore.expect_index_metadata().returning(move |_| {
            Ok(IndexMetadataResponse::try_from_index_metadata(&index_metadata).unwrap())
        });
        mock_metastore.expect_list_splits().returning(move |_| {
            let splits = vec![
                MockSplitBuilder::new("split1")
                    .with_index_uid(&index_uid)
                    .build(),
                MockSplitBuilder::new("split2")
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
            split_id: "split1".to_string(),
        }))?;
        result_sender.send(Err(SearchError::Internal("error".to_string())))?;
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
            .withf(|request| request.split_offsets.len() == 1) // Retry request on the failed split.
            .return_once(
                |_leaf_search_req: quickwit_proto::search::LeafSearchStreamRequest| {
                    Err(SearchError::Internal("error".to_string()))
                },
            );
        // The test will hang on indefinitely if we don't drop the sender.
        drop(result_sender);

        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", mock_search_service)]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let cluster_client = ClusterClient::new(search_job_placer.clone());
        let stream = root_search_stream(
            request,
            MetastoreServiceClient::from_mock(mock_metastore),
            cluster_client,
        )
        .await?;
        let result: Result<Vec<_>, SearchError> = stream.try_collect().await;
        assert_eq!(result.is_err(), true);
        assert_eq!(result.unwrap_err().to_string(), "internal error: `error`");
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_stream_with_invalid_query() -> anyhow::Result<()> {
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        mock_metastore.expect_index_metadata().returning(move |_| {
            Ok(IndexMetadataResponse::try_from_index_metadata(&index_metadata).unwrap())
        });
        mock_metastore.expect_list_splits().returning(move |_| {
            let splits = vec![
                MockSplitBuilder::new("split")
                    .with_index_uid(&index_uid)
                    .build(),
            ];
            let splits = ListSplitsResponse::try_from_splits(splits).unwrap();
            Ok(ServiceStream::from(vec![Ok(splits)]))
        });

        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", MockSearchService::new())]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let metastore = MetastoreServiceClient::from_mock(mock_metastore);
        assert!(
            root_search_stream(
                quickwit_proto::search::SearchStreamRequest {
                    index_id: "test-index".to_string(),
                    query_ast: qast_json_helper(r#"invalid_field:"test""#, &[]),
                    fast_field: "timestamp".to_string(),
                    output_format: OutputFormat::Csv as i32,
                    partition_by_field: Some("timestamp".to_string()),
                    ..Default::default()
                },
                metastore.clone(),
                ClusterClient::new(search_job_placer.clone()),
            )
            .await
            .is_err()
        );

        assert!(
            root_search_stream(
                quickwit_proto::search::SearchStreamRequest {
                    index_id: "test-index".to_string(),
                    query_ast: qast_json_helper("test", &["invalid_field"]),
                    fast_field: "timestamp".to_string(),
                    output_format: OutputFormat::Csv as i32,
                    partition_by_field: Some("timestamp".to_string()),
                    ..Default::default()
                },
                metastore,
                ClusterClient::new(search_job_placer.clone()),
            )
            .await
            .is_err()
        );

        Ok(())
    }
}
