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

use std::sync::Arc;

use quickwit_config::NodeConfig;
use quickwit_doc_mapper::{analyze_text, TokenizerConfig};
use quickwit_index_management::{IndexService, IndexServiceError};
use quickwit_query::query_ast::{query_ast_from_user_text, QueryAst};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use tracing::warn;
use warp::{Filter, Rejection};

use super::get_index_metadata_handler;
use super::index_resource::{
    __path_clear_index, __path_create_index, __path_delete_index, __path_list_indexes_metadata,
    __path_update_index, clear_index_handler, create_index_handler, delete_index_handler,
    describe_index_handler, list_indexes_metadata_handler, update_index_handler, IndexStats,
    __path_describe_index,
};
use super::source_resource::{
    __path_create_source, __path_delete_source, __path_reset_source_checkpoint,
    __path_toggle_source, __path_update_source, create_source_handler, delete_source_handler,
    get_source_handler, get_source_shards_handler, reset_source_checkpoint_handler,
    toggle_source_handler, update_source_handler, ToggleSource,
};
use super::split_resource::{
    __path_list_splits, __path_mark_splits_for_deletion, list_splits_handler,
    mark_splits_for_deletion_handler, SplitsForDeletion,
};
use crate::format::extract_format_from_qs;
use crate::rest::recover_fn;
use crate::rest_api_response::into_rest_api_response;
use crate::simple_list::from_simple_list;

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        create_index,
        update_index,
        clear_index,
        delete_index,
        list_indexes_metadata,
        list_splits,
        describe_index,
        mark_splits_for_deletion,
        create_source,
        update_source,
        reset_source_checkpoint,
        toggle_source,
        delete_source,
    ),
    components(schemas(ToggleSource, SplitsForDeletion, IndexStats))
)]
pub struct IndexApi;

pub fn log_failure<T, E: std::fmt::Display>(
    message: &'static str,
) -> impl Fn(Result<T, E>) -> Result<T, E> + Clone {
    move |result| {
        if let Err(err) = &result {
            warn!("{message}: {err}");
        };
        result
    }
}

pub fn json_body<T: DeserializeOwned + Send>(
) -> impl Filter<Extract = (T,), Error = warp::Rejection> + Clone {
    warp::body::content_length_limit(1024 * 1024).and(warp::body::json())
}

pub fn index_management_handlers(
    index_service: IndexService,
    node_config: Arc<NodeConfig>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    // Indexes handlers.
    get_index_metadata_handler(index_service.metastore())
        .or(list_indexes_metadata_handler(index_service.metastore()))
        .or(create_index_handler(index_service.clone(), node_config))
        .or(update_index_handler(index_service.metastore()))
        .or(clear_index_handler(index_service.clone()))
        .or(delete_index_handler(index_service.clone()))
        .boxed()
        // Splits handlers
        .or(list_splits_handler(index_service.metastore()))
        .or(describe_index_handler(index_service.metastore()))
        .or(mark_splits_for_deletion_handler(index_service.metastore()))
        .boxed()
        // Sources handlers.
        .or(reset_source_checkpoint_handler(index_service.metastore()))
        .or(toggle_source_handler(index_service.metastore()))
        .or(create_source_handler(index_service.clone()))
        .or(update_source_handler(index_service.clone()))
        .or(get_source_handler(index_service.metastore()))
        .or(delete_source_handler(index_service.metastore()))
        .or(get_source_shards_handler(index_service.metastore()))
        .boxed()
        // Tokenizer handlers.
        .or(analyze_request_handler())
        // Parse query into query AST handler.
        .or(parse_query_request_handler())
        .recover(recover_fn)
        .boxed()
}

#[derive(Debug, Deserialize, utoipa::IntoParams, utoipa::ToSchema)]
struct AnalyzeRequest {
    /// The tokenizer to use.
    #[serde(flatten)]
    pub tokenizer_config: TokenizerConfig,
    /// The text to analyze.
    pub text: String,
}

fn analyze_request_filter() -> impl Filter<Extract = (AnalyzeRequest,), Error = Rejection> + Clone {
    warp::path!("analyze")
        .and(warp::post())
        .and(warp::body::json())
}

fn analyze_request_handler() -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone
{
    analyze_request_filter()
        .then(analyze_request)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
        .boxed()
}

/// Analyzes text with given tokenizer config and returns the list of tokens.
#[utoipa::path(
    post,
    tag = "analyze",
    path = "/analyze",
    request_body = AnalyzeRequest,
    responses(
        (status = 200, description = "Successfully analyze text.")
    ),
)]
async fn analyze_request(request: AnalyzeRequest) -> Result<serde_json::Value, IndexServiceError> {
    let tokens = analyze_text(&request.text, &request.tokenizer_config)
        .map_err(|err| IndexServiceError::Internal(format!("{err:?}")))?;
    let json_value = serde_json::to_value(tokens)
        .map_err(|err| IndexServiceError::Internal(format!("cannot serialize tokens: {err}")))?;
    Ok(json_value)
}

#[derive(Debug, Deserialize, utoipa::IntoParams, utoipa::ToSchema)]
struct ParseQueryRequest {
    /// Query text. The query language is that of tantivy.
    pub query: String,
    // Fields to search on.
    #[param(rename = "search_field")]
    #[serde(default)]
    #[serde(rename(deserialize = "search_field"))]
    #[serde(deserialize_with = "from_simple_list")]
    pub search_fields: Option<Vec<String>>,
}

fn parse_query_request_filter(
) -> impl Filter<Extract = (ParseQueryRequest,), Error = Rejection> + Clone {
    warp::path!("parse-query")
        .and(warp::post())
        .and(warp::body::json())
}

fn parse_query_request_handler(
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    parse_query_request_filter()
        .then(parse_query_request)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
        .boxed()
}

/// Analyzes text with given tokenizer config and returns the list of tokens.
#[utoipa::path(
    post,
    tag = "parse_query",
    path = "/parse_query",
    request_body = ParseQueryRequest,
    responses(
        (status = 200, description = "Successfully parsed query into AST.")
    ),
)]
async fn parse_query_request(request: ParseQueryRequest) -> Result<QueryAst, IndexServiceError> {
    let query_ast = query_ast_from_user_text(&request.query, request.search_fields)
        .parse_user_query(&[])
        .map_err(|err| IndexServiceError::Internal(err.to_string()))?;
    Ok(query_ast)
}

#[cfg(test)]
mod tests {
    use std::ops::{Bound, RangeInclusive};

    use assert_json_diff::assert_json_include;
    use quickwit_common::uri::Uri;
    use quickwit_common::ServiceStream;
    use quickwit_config::{
        NodeConfig, SourceParams, VecSourceParams, CLI_SOURCE_ID, INGEST_API_SOURCE_ID,
    };
    use quickwit_indexing::{mock_split, MockSplitBuilder};
    use quickwit_metastore::{
        metastore_for_test, IndexMetadata, IndexMetadataResponseExt,
        ListIndexesMetadataResponseExt, ListSplitsRequestExt, ListSplitsResponseExt, SplitState,
    };
    use quickwit_proto::metastore::{
        DeleteSourceRequest, EmptyResponse, EntityKind, IndexMetadataRequest,
        IndexMetadataResponse, ListIndexesMetadataRequest, ListIndexesMetadataResponse,
        ListSplitsRequest, ListSplitsResponse, MarkSplitsForDeletionRequest, MetastoreError,
        MetastoreService, MetastoreServiceClient, MockMetastoreService,
        ResetSourceCheckpointRequest, SourceType, ToggleSourceRequest,
    };
    use quickwit_proto::types::IndexUid;
    use quickwit_storage::StorageResolver;
    use serde_json::Value as JsonValue;

    use super::*;
    use crate::recover_fn;

    #[tokio::test]
    async fn test_get_index() -> anyhow::Result<()> {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore.expect_index_metadata().return_once(|_| {
            Ok(
                IndexMetadataResponse::try_from_index_metadata(&IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
                .unwrap(),
            )
        });
        let index_service = IndexService::new(
            MetastoreServiceClient::from_mock(mock_metastore),
            StorageResolver::unconfigured(),
        );
        let index_management_handler =
            super::index_management_handlers(index_service, Arc::new(NodeConfig::for_test()))
                .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/test-index")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let actual_response_json: JsonValue = serde_json::from_slice(resp.body())?;
        let expected_response_json = serde_json::json!({
            "index_id": "test-index",
            "index_uri": "ram:///indexes/test-index",
        });
        assert_json_include!(
            actual: actual_response_json.get("index_config").unwrap(),
            expected: expected_response_json
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_get_non_existing_index() {
        let metastore = metastore_for_test();
        let index_service = IndexService::new(metastore, StorageResolver::unconfigured());
        let index_management_handler =
            super::index_management_handlers(index_service, Arc::new(NodeConfig::for_test()))
                .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/test-index")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 404);
    }

    #[tokio::test]
    async fn test_get_splits() {
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata =
            IndexMetadata::for_test("quickwit-demo-index", "ram:///indexes/quickwit-demo-index");
        let index_uid = index_metadata.index_uid.clone();
        mock_metastore
            .expect_index_metadata()
            .returning(move |_| {
                Ok(IndexMetadataResponse::try_from_index_metadata(&index_metadata).unwrap())
            })
            .times(2);
        mock_metastore
            .expect_list_splits()
            .returning(move |list_splits_request: ListSplitsRequest| {
                let list_split_query = list_splits_request.deserialize_list_splits_query().unwrap();
                if list_split_query.index_uids.unwrap().contains(&index_uid)
                    && list_split_query.split_states
                        == vec![SplitState::Published, SplitState::Staged]
                    && list_split_query.time_range.start == Bound::Included(10)
                    && list_split_query.time_range.end == Bound::Excluded(20)
                    && list_split_query.create_timestamp.end == Bound::Excluded(2)
                {
                    let splits = vec![MockSplitBuilder::new("split_1")
                        .with_index_uid(&index_uid)
                        .build()];
                    let splits = ListSplitsResponse::try_from_splits(splits).unwrap();
                    return Ok(ServiceStream::from(vec![Ok(splits)]));
                }
                Err(MetastoreError::Internal {
                    message: "".to_string(),
                    cause: "".to_string(),
                })
            })
            .times(2);
        let index_service = IndexService::new(
            MetastoreServiceClient::from_mock(mock_metastore),
            StorageResolver::unconfigured(),
        );
        let index_management_handler =
            super::index_management_handlers(index_service, Arc::new(NodeConfig::for_test()))
                .recover(recover_fn);
        {
            let resp = warp::test::request()
                .path(
                    "/indexes/quickwit-demo-index/splits?split_states=Published,Staged&\
                     start_timestamp=10&end_timestamp=20&end_create_timestamp=2",
                )
                .reply(&index_management_handler)
                .await;
            assert_eq!(resp.status(), 200);
            let actual_response_json: JsonValue = serde_json::from_slice(resp.body()).unwrap();
            let expected_response_json = serde_json::json!({
                "splits": [
                    {
                        "create_timestamp": 0,
                        "split_id": "split_1",
                    }
                ]
            });
            assert_json_include!(
                actual: actual_response_json,
                expected: expected_response_json
            );
        }
        {
            let resp = warp::test::request()
                .path(
                    "/indexes/quickwit-demo-index/splits?split_states=Published&\
                     start_timestamp=11&end_timestamp=20&end_create_timestamp=2",
                )
                .reply(&index_management_handler)
                .await;
            assert_eq!(resp.status(), 500);
        }
    }

    #[tokio::test]
    async fn test_describe_index() -> anyhow::Result<()> {
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata =
            IndexMetadata::for_test("quickwit-demo-index", "ram:///indexes/quickwit-demo-index");
        let index_uid = index_metadata.index_uid.clone();
        mock_metastore
            .expect_index_metadata()
            .return_once(move |_| {
                Ok(IndexMetadataResponse::try_from_index_metadata(&index_metadata).unwrap())
            });
        let split_1 = MockSplitBuilder::new("split_1")
            .with_index_uid(&index_uid)
            .build();
        let split_1_time_range = split_1.split_metadata.time_range.clone().unwrap();
        let mut split_2 = MockSplitBuilder::new("split_2")
            .with_index_uid(&index_uid)
            .build();
        split_2.split_metadata.time_range = Some(RangeInclusive::new(
            split_1_time_range.start() - 10,
            split_1_time_range.end() + 10,
        ));
        mock_metastore
            .expect_list_splits()
            .withf(move |list_split_request| -> bool {
                let list_split_query = list_split_request.deserialize_list_splits_query().unwrap();
                list_split_query.index_uids.unwrap().contains(&index_uid)
            })
            .return_once(move |_| {
                let splits = vec![split_1, split_2];
                let splits = ListSplitsResponse::try_from_splits(splits).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits)]))
            });

        let index_service = IndexService::new(
            MetastoreServiceClient::from_mock(mock_metastore),
            StorageResolver::unconfigured(),
        );
        let index_management_handler =
            super::index_management_handlers(index_service, Arc::new(NodeConfig::for_test()))
                .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index/describe")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);

        let actual_response_json: JsonValue = serde_json::from_slice(resp.body()).unwrap();
        let expected_response_json = serde_json::json!({
            "index_id": "quickwit-demo-index",
            "index_uri": "ram:///indexes/quickwit-demo-index",
            "num_published_splits": 2,
            "size_published_splits": 1600,
            "num_published_docs": 20,
            "size_published_docs_uncompressed": 512,
            "timestamp_field_name": "timestamp",
            "min_timestamp": split_1_time_range.start() - 10,
            "max_timestamp": split_1_time_range.end() + 10,
        });

        assert_eq!(actual_response_json, expected_response_json);
        Ok(())
    }

    #[tokio::test]
    async fn test_get_all_splits() {
        let mut mock_metastore = MockMetastoreService::new();
        let index_metadata =
            IndexMetadata::for_test("quickwit-demo-index", "ram:///indexes/quickwit-demo-index");
        let index_uid = index_metadata.index_uid.clone();
        mock_metastore
            .expect_index_metadata()
            .return_once(move |_| {
                Ok(IndexMetadataResponse::try_from_index_metadata(&index_metadata).unwrap())
            });
        mock_metastore.expect_list_splits().return_once(
            move |list_split_request: ListSplitsRequest| {
                let list_split_query = list_split_request.deserialize_list_splits_query().unwrap();
                if list_split_query.index_uids.unwrap().contains(&index_uid)
                    && list_split_query.split_states.is_empty()
                    && list_split_query.time_range.is_unbounded()
                    && list_split_query.create_timestamp.is_unbounded()
                {
                    return Ok(ServiceStream::empty());
                }
                Err(MetastoreError::Internal {
                    message: "".to_string(),
                    cause: "".to_string(),
                })
            },
        );
        let index_service = IndexService::new(
            MetastoreServiceClient::from_mock(mock_metastore),
            StorageResolver::unconfigured(),
        );
        let index_management_handler =
            super::index_management_handlers(index_service, Arc::new(NodeConfig::for_test()))
                .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index/splits")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
    }

    #[tokio::test]
    async fn test_mark_splits_for_deletion() -> anyhow::Result<()> {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_index_metadata()
            .returning(|_| {
                Ok(
                    IndexMetadataResponse::try_from_index_metadata(&IndexMetadata::for_test(
                        "quickwit-demo-index",
                        "ram:///indexes/quickwit-demo-index",
                    ))
                    .unwrap(),
                )
            })
            .times(2);
        mock_metastore
            .expect_mark_splits_for_deletion()
            .returning(
                |mark_splits_for_deletion_request: MarkSplitsForDeletionRequest| {
                    let index_uid: IndexUid = mark_splits_for_deletion_request.index_uid().clone();
                    let split_ids = mark_splits_for_deletion_request.split_ids;
                    if index_uid.index_id == "quickwit-demo-index"
                        && split_ids == ["split-1", "split-2"]
                    {
                        return Ok(EmptyResponse {});
                    }
                    Err(MetastoreError::Internal {
                        message: "".to_string(),
                        cause: "".to_string(),
                    })
                },
            )
            .times(2);
        let index_service = IndexService::new(
            MetastoreServiceClient::from_mock(mock_metastore),
            StorageResolver::unconfigured(),
        );
        let index_management_handler =
            super::index_management_handlers(index_service, Arc::new(NodeConfig::for_test()))
                .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index/splits/mark-for-deletion")
            .method("PUT")
            .json(&true)
            .body(r#"{"split_ids": ["split-1", "split-2"]}"#)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index/splits/mark-for-deletion")
            .json(&true)
            .body(r#"{"split_ids": [""]}"#)
            .method("PUT")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 500);
        Ok(())
    }

    #[tokio::test]
    async fn test_get_list_indexes() -> anyhow::Result<()> {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_list_indexes_metadata()
            .return_once(|list_indexes_request| {
                assert_eq!(
                    list_indexes_request.index_id_patterns,
                    vec!["test-index-*".to_string()]
                );
                let index_metadata =
                    IndexMetadata::for_test("test-index", "ram:///indexes/test-index");
                Ok(ListIndexesMetadataResponse::for_test(vec![index_metadata]))
            });
        let index_service = IndexService::new(
            MetastoreServiceClient::from_mock(mock_metastore),
            StorageResolver::unconfigured(),
        );
        let index_management_handler =
            super::index_management_handlers(index_service, Arc::new(NodeConfig::for_test()))
                .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes?index_id_patterns=test-index-*")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let actual_response_json: JsonValue = serde_json::from_slice(resp.body())?;
        let actual_response_arr: &Vec<JsonValue> = actual_response_json.as_array().unwrap();
        assert_eq!(actual_response_arr.len(), 1);
        let actual_index_metadata_json: &JsonValue = &actual_response_arr[0];
        let expected_response_json = serde_json::json!({
            "index_id": "test-index",
            "index_uri": "ram:///indexes/test-index",
        });
        assert_json_include!(
            actual: actual_index_metadata_json.get("index_config").unwrap(),
            expected: expected_response_json
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_clear_index() -> anyhow::Result<()> {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore.expect_index_metadata().return_once(|_| {
            Ok(
                IndexMetadataResponse::try_from_index_metadata(&IndexMetadata::for_test(
                    "quickwit-demo-index",
                    "file:///path/to/index/quickwit-demo-index",
                ))
                .unwrap(),
            )
        });
        mock_metastore.expect_list_splits().return_once(|_| {
            let splits = ListSplitsResponse::try_from_splits(vec![mock_split("split_1")]).unwrap();
            Ok(ServiceStream::from(vec![Ok(splits)]))
        });
        mock_metastore
            .expect_mark_splits_for_deletion()
            .return_once(|_| Ok(EmptyResponse {}));
        mock_metastore
            .expect_delete_splits()
            .return_once(|_| Ok(EmptyResponse {}));
        mock_metastore
            .expect_reset_source_checkpoint()
            .return_once(|_| Ok(EmptyResponse {}));
        let index_service = IndexService::new(
            MetastoreServiceClient::from_mock(mock_metastore),
            StorageResolver::unconfigured(),
        );
        let index_management_handler =
            super::index_management_handlers(index_service, Arc::new(NodeConfig::for_test()))
                .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index/clear")
            .method("PUT")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_index() {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_index_metadata()
            .returning(|_| {
                Ok(
                    IndexMetadataResponse::try_from_index_metadata(&IndexMetadata::for_test(
                        "quickwit-demo-index",
                        "file:///path/to/index/quickwit-demo-index",
                    ))
                    .unwrap(),
                )
            })
            .times(2);
        mock_metastore
            .expect_list_splits()
            .returning(|_| {
                let splits =
                    ListSplitsResponse::try_from_splits(vec![mock_split("split_1")]).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits)]))
            })
            .times(3);
        mock_metastore
            .expect_mark_splits_for_deletion()
            .return_once(|_| Ok(EmptyResponse {}));
        mock_metastore
            .expect_delete_splits()
            .return_once(|_| Ok(EmptyResponse {}));
        mock_metastore
            .expect_delete_index()
            .return_once(|_| Ok(EmptyResponse {}));
        let index_service = IndexService::new(
            MetastoreServiceClient::from_mock(mock_metastore),
            StorageResolver::unconfigured(),
        );
        let index_management_handler =
            super::index_management_handlers(index_service, Arc::new(NodeConfig::for_test()))
                .recover(recover_fn);
        {
            // Dry run
            let resp = warp::test::request()
                .path("/indexes/quickwit-demo-index?dry_run=true")
                .method("DELETE")
                .reply(&index_management_handler)
                .await;
            assert_eq!(resp.status(), 200);
            let resp_json: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
            let expected_response_json = serde_json::json!([{
                "file_name": "split_1.split",
                "file_size_bytes": "800 B",
            }]);
            assert_json_include!(actual: resp_json, expected: expected_response_json);
        }
        {
            let resp = warp::test::request()
                .path("/indexes/quickwit-demo-index")
                .method("DELETE")
                .reply(&index_management_handler)
                .await;
            assert_eq!(resp.status(), 200);
            let resp_json: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
            let expected_response_json = serde_json::json!([{
                "file_name": "split_1.split",
                "file_size_bytes": "800 B",
            }]);
            assert_json_include!(actual: resp_json, expected: expected_response_json);
        }
    }

    #[tokio::test]
    async fn test_delete_on_non_existing_index() {
        let metastore = metastore_for_test();
        let index_service = IndexService::new(metastore, StorageResolver::unconfigured());
        let index_management_handler =
            super::index_management_handlers(index_service, Arc::new(NodeConfig::for_test()))
                .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index")
            .method("DELETE")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 404);
    }

    #[tokio::test]
    async fn test_create_index_with_overwrite() {
        let metastore = metastore_for_test();
        let index_service = IndexService::new(metastore.clone(), StorageResolver::unconfigured());
        let mut node_config = NodeConfig::for_test();
        node_config.default_index_root_uri = Uri::for_test("file:///default-index-root-uri");
        let index_management_handler =
            super::index_management_handlers(index_service, Arc::new(node_config));
        {
            let resp = warp::test::request()
                .path("/indexes?overwrite=true")
                .method("POST")
                .json(&true)
                .body(r#"{"version": "0.9", "index_id": "hdfs-logs", "doc_mapping": {"field_mappings":[{"name": "timestamp", "type": "i64", "fast": true, "indexed": true}]}}"#)
                .reply(&index_management_handler)
                .await;
            assert_eq!(resp.status(), 200);
        }
        {
            let resp = warp::test::request()
                .path("/indexes?overwrite=true")
                .method("POST")
                .json(&true)
                .body(r#"{"version": "0.9", "index_id": "hdfs-logs", "doc_mapping": {"field_mappings":[{"name": "timestamp", "type": "i64", "fast": true, "indexed": true}]}}"#)
                .reply(&index_management_handler)
                .await;
            assert_eq!(resp.status(), 200);
        }
        {
            let resp = warp::test::request()
                .path("/indexes")
                .method("POST")
                .json(&true)
                .body(r#"{"version": "0.9", "index_id": "hdfs-logs", "doc_mapping": {"field_mappings":[{"name": "timestamp", "type": "i64", "fast": true, "indexed": true}]}}"#)
                .reply(&index_management_handler)
                .await;
            assert_eq!(resp.status(), 400);
        }
    }

    #[tokio::test]
    async fn test_create_delete_index_and_source() {
        let metastore = metastore_for_test();
        let index_service = IndexService::new(metastore.clone(), StorageResolver::unconfigured());
        let mut node_config = NodeConfig::for_test();
        node_config.default_index_root_uri = Uri::for_test("file:///default-index-root-uri");
        let index_management_handler =
            super::index_management_handlers(index_service, Arc::new(node_config));
        let resp = warp::test::request()
            .path("/indexes")
            .method("POST")
            .json(&true)
            .body(r#"{"version": "0.9", "index_id": "hdfs-logs", "doc_mapping": {"field_mappings":[{"name": "timestamp", "type": "i64", "fast": true, "indexed": true}]}}"#)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let resp_json: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        let expected_response_json = serde_json::json!({
            "index_config": {
                "index_id": "hdfs-logs",
                "index_uri": "file:///default-index-root-uri/hdfs-logs",
            }
        });
        assert_json_include!(actual: resp_json, expected: expected_response_json);

        // Create source.
        let source_config_body = r#"{"version": "0.9", "source_id": "vec-source", "source_type": "vec", "params": {"docs": [], "batch_num_docs": 10}}"#;
        let resp = warp::test::request()
            .path("/indexes/hdfs-logs/sources")
            .method("POST")
            .json(&true)
            .body(source_config_body)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);

        // Get source.
        let resp = warp::test::request()
            .path("/indexes/hdfs-logs/sources/vec-source")
            .method("GET")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);

        // Check that the source has been added to index metadata.
        let index_metadata = metastore
            .index_metadata(IndexMetadataRequest::for_index_id("hdfs-logs".to_string()))
            .await
            .unwrap()
            .deserialize_index_metadata()
            .unwrap();
        assert!(index_metadata.sources.contains_key("vec-source"));
        let source_config = index_metadata.sources.get("vec-source").unwrap();
        assert_eq!(source_config.source_type(), SourceType::Vec);
        assert_eq!(
            source_config.source_params,
            SourceParams::Vec(VecSourceParams {
                docs: Vec::new(),
                batch_num_docs: 10,
                partition: "".to_string(),
            })
        );

        // Check delete source.
        let resp = warp::test::request()
            .path("/indexes/hdfs-logs/sources/vec-source")
            .method("DELETE")
            .body(source_config_body)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let index_metadata = metastore
            .index_metadata(IndexMetadataRequest::for_index_id("hdfs-logs".to_string()))
            .await
            .unwrap()
            .deserialize_index_metadata()
            .unwrap();
        assert!(!index_metadata.sources.contains_key("file-source"));

        // Check cannot delete source managed by Quickwit.
        let resp = warp::test::request()
            .path(format!("/indexes/hdfs-logs/sources/{INGEST_API_SOURCE_ID}").as_str())
            .method("DELETE")
            .body(source_config_body)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 403);

        let resp = warp::test::request()
            .path(format!("/indexes/hdfs-logs/sources/{CLI_SOURCE_ID}").as_str())
            .method("DELETE")
            .body(source_config_body)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 403);

        // Check get a non existing source returns 404.
        let resp = warp::test::request()
            .path("/indexes/hdfs-logs/sources/file-source")
            .method("GET")
            .body(source_config_body)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 404);

        // Check delete index.
        let resp = warp::test::request()
            .path("/indexes/hdfs-logs")
            .method("DELETE")
            .body(source_config_body)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let indexes = metastore
            .list_indexes_metadata(ListIndexesMetadataRequest::all())
            .await
            .unwrap()
            .deserialize_indexes_metadata()
            .await
            .unwrap();
        assert!(indexes.is_empty());
    }

    #[tokio::test]
    async fn test_create_index_with_yaml() {
        let metastore = metastore_for_test();
        let index_service = IndexService::new(metastore.clone(), StorageResolver::unconfigured());
        let mut node_config = NodeConfig::for_test();
        node_config.default_index_root_uri = Uri::for_test("file:///default-index-root-uri");
        let index_management_handler =
            super::index_management_handlers(index_service, Arc::new(node_config))
                .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes")
            .method("POST")
            .header("content-type", "application/yaml")
            .body(
                r#"
            version: 0.8
            index_id: hdfs-logs
            doc_mapping:
              field_mappings:
                - name: timestamp
                  type: i64
                  fast: true
                  indexed: true
            "#,
            )
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let resp_json: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        let expected_response_json = serde_json::json!({
            "index_config": {
                "index_id": "hdfs-logs",
                "index_uri": "file:///default-index-root-uri/hdfs-logs",
            }
        });
        assert_json_include!(actual: resp_json, expected: expected_response_json);
    }

    #[tokio::test]
    async fn test_create_index_and_source_with_toml() {
        let metastore = metastore_for_test();
        let index_service = IndexService::new(metastore.clone(), StorageResolver::unconfigured());
        let mut node_config = NodeConfig::for_test();
        node_config.default_index_root_uri = Uri::for_test("file:///default-index-root-uri");
        let index_management_handler =
            super::index_management_handlers(index_service, Arc::new(node_config))
                .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes")
            .method("POST")
            .header("content-type", "application/toml")
            .body(
                r#"
            version = "0.9"
            index_id = "hdfs-logs"
            [doc_mapping]
            field_mappings = [
                { name = "timestamp", type = "i64", fast = true, indexed = true}
            ]
            "#,
            )
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let resp_json: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        let expected_response_json = serde_json::json!({
            "index_config": {
                "index_id": "hdfs-logs",
                "index_uri": "file:///default-index-root-uri/hdfs-logs",
            }
        });
        assert_json_include!(actual: resp_json, expected: expected_response_json);
    }

    #[tokio::test]
    async fn test_create_index_with_wrong_content_type() {
        let metastore = metastore_for_test();
        let index_service = IndexService::new(metastore.clone(), StorageResolver::unconfigured());
        let mut node_config = NodeConfig::for_test();
        node_config.default_index_root_uri = Uri::for_test("file:///default-index-root-uri");
        let index_management_handler =
            super::index_management_handlers(index_service, Arc::new(node_config))
                .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes")
            .method("POST")
            .header("content-type", "application/yoml")
            .body(r#""#)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 415);
        let body = std::str::from_utf8(resp.body()).unwrap();
        assert!(body.contains("content-type is not supported"));
    }

    #[tokio::test]
    async fn test_create_index_with_bad_config() -> anyhow::Result<()> {
        let index_service = IndexService::new(
            MetastoreServiceClient::mocked(),
            StorageResolver::unconfigured(),
        );
        let index_management_handler =
            super::index_management_handlers(index_service, Arc::new(NodeConfig::for_test()))
                .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes")
            .method("POST")
            .json(&true)
            .body(
                r#"{"version": "0.9", "index_id": "hdfs-log", "doc_mapping":
    {"field_mappings":[{"name": "timestamp", "type": "unknown", "fast": true, "indexed":
    true}]}}"#,
            )
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 400);
        let body = std::str::from_utf8(resp.body()).unwrap();
        assert!(body.contains("field `timestamp` has an unknown type"));
        Ok(())
    }

    #[tokio::test]
    async fn test_update_index() {
        let metastore = metastore_for_test();
        let index_service = IndexService::new(metastore.clone(), StorageResolver::unconfigured());
        let mut node_config = NodeConfig::for_test();
        node_config.default_index_root_uri = Uri::for_test("file:///default-index-root-uri");
        let index_management_handler =
            super::index_management_handlers(index_service, Arc::new(node_config));
        {
            let resp = warp::test::request()
                .path("/indexes")
                .method("POST")
                .json(&true)
                .body(r#"{"version": "0.9", "index_id": "hdfs-logs", "doc_mapping": {"field_mappings":[{"name": "timestamp", "type": "i64", "fast": true, "indexed": true}]},"search_settings":{"default_search_fields":["body"]}}"#)
                .reply(&index_management_handler)
                .await;
            assert_eq!(resp.status(), 200);
            let resp_json: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
            let expected_response_json = serde_json::json!({
                "index_config": {
                    "search_settings": {
                        "default_search_fields": ["body"]
                    }
                }
            });
            assert_json_include!(actual: resp_json, expected: expected_response_json);
        }
        {
            let resp = warp::test::request()
                .path("/indexes/hdfs-logs")
                .method("PUT")
                .json(&true)
                .body(r#"{"version": "0.9", "index_id": "hdfs-logs", "doc_mapping": {"field_mappings":[{"name": "timestamp", "type": "i64", "fast": true, "indexed": true}]},"search_settings":{"default_search_fields":["severity_text", "body"]}}"#)
                .reply(&index_management_handler)
                .await;
            assert_eq!(resp.status(), 200);
            let resp_json: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
            let expected_response_json = serde_json::json!({
                "index_config": {
                    "search_settings": {
                        "default_search_fields": ["severity_text", "body"]
                    }
                }
            });
            assert_json_include!(actual: resp_json, expected: expected_response_json);
        }
        // check that the metastore was updated
        let index_metadata = metastore
            .index_metadata(IndexMetadataRequest::for_index_id("hdfs-logs".to_string()))
            .await
            .unwrap()
            .deserialize_index_metadata()
            .unwrap();
        assert_eq!(
            index_metadata
                .index_config
                .search_settings
                .default_search_fields,
            ["severity_text", "body"]
        );
    }

    #[tokio::test]
    async fn test_create_source_with_bad_config() {
        let metastore = metastore_for_test();
        let index_service = IndexService::new(metastore, StorageResolver::unconfigured());
        let index_management_handler =
            super::index_management_handlers(index_service, Arc::new(NodeConfig::for_test()))
                .recover(recover_fn);
        {
            // Source config with bad version.
            let resp = warp::test::request()
                .path("/indexes/my-index/sources")
                .method("POST")
                .json(&true)
                .body(r#"{"version": 0.4, "source_id": "file-source"}"#)
                .reply(&index_management_handler)
                .await;
            assert_eq!(resp.status(), 400);
            let body = std::str::from_utf8(resp.body()).unwrap();
            assert!(body.contains("invalid type: floating point `0.4`"));
        }
        {
            // Invalid pulsar source config with number of pipelines > 1, not supported yet.
            let resp = warp::test::request()
                .path("/indexes/my-index/sources")
                .method("POST")
                .json(&true)
                .body(
                    r#"{"version": "0.9", "source_id": "pulsar-source",
    "num_pipelines": 2, "source_type": "pulsar", "params": {"topics": ["my-topic"],
    "address": "pulsar://localhost:6650" }}"#,
                )
                .reply(&index_management_handler)
                .await;
            assert_eq!(resp.status(), 400);
            let body = std::str::from_utf8(resp.body()).unwrap();
            assert!(body.contains(
                "Quickwit currently supports multiple pipelines only for GCP PubSub or Kafka \
                 sources"
            ));
        }
        {
            let resp = warp::test::request()
                .path("/indexes/hdfs-logs/sources")
                .method("POST")
                .body(
                    r#"{"version": "0.9", "source_id": "my-stdin-source", "source_type": "stdin"}"#,
                )
                .reply(&index_management_handler)
                .await;
            assert_eq!(resp.status(), 400);
            let response_body = std::str::from_utf8(resp.body()).unwrap();
            assert!(
                response_body.contains("stdin can only be used as source through the CLI command")
            )
        }
        {
            let resp = warp::test::request()
                .path("/indexes/hdfs-logs/sources")
                .method("POST")
                .body(
                    r#"{"version": "0.9", "source_id": "my-local-file-source", "source_type": "file", "params": {"filepath": "localfile"}}"#,
                )
                .reply(&index_management_handler)
                .await;
            assert_eq!(resp.status(), 400);
            let response_body = std::str::from_utf8(resp.body()).unwrap();
            assert!(response_body.contains("limited to a local usage"))
        }
    }

    #[cfg(feature = "sqs-for-tests")]
    #[tokio::test]
    async fn test_update_source() {
        use quickwit_indexing::source::sqs_queue::test_helpers::start_mock_sqs_get_queue_attributes_endpoint;

        let metastore = metastore_for_test();
        let (queue_url, _guard) = start_mock_sqs_get_queue_attributes_endpoint();
        let index_service = IndexService::new(metastore.clone(), StorageResolver::unconfigured());
        let mut node_config = NodeConfig::for_test();
        node_config.default_index_root_uri = Uri::for_test("file:///default-index-root-uri");
        let index_management_handler =
            super::index_management_handlers(index_service, Arc::new(node_config));
        let resp = warp::test::request()
            .path("/indexes")
            .method("POST")
            .json(&true)
            .body(r#"{"version": "0.9", "index_id": "hdfs-logs", "doc_mapping": {"field_mappings":[{"name": "timestamp", "type": "i64", "fast": true, "indexed": true}]}}"#)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let resp_json: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        let expected_response_json = serde_json::json!({
            "index_config": {
                "index_id": "hdfs-logs",
                "index_uri": "file:///default-index-root-uri/hdfs-logs",
            }
        });
        assert_json_include!(actual: resp_json, expected: expected_response_json);

        // Create source.
        let source_config_body = serde_json::json!({
            "version": "0.9",
            "source_id": "sqs-source",
            "source_type": "file",
            "params": {"notifications": [{"type": "sqs", "queue_url": queue_url, "message_type": "s3_notification"}]},
        });
        let resp = warp::test::request()
            .path("/indexes/hdfs-logs/sources")
            .method("POST")
            .json(&source_config_body)
            .reply(&index_management_handler)
            .await;
        let resp_body = std::str::from_utf8(resp.body()).unwrap();
        assert_eq!(resp.status(), 200, "{resp_body}");

        {
            // Update the source.
            let update_source_config_body = serde_json::json!({
                "version": "0.9",
                "source_id": "sqs-source",
                "source_type": "file",
                "params": {"notifications": [{"type": "sqs", "queue_url": queue_url, "message_type": "s3_notification"}]},
            });
            let resp = warp::test::request()
                .path("/indexes/hdfs-logs/sources/sqs-source")
                .method("PUT")
                .json(&update_source_config_body)
                .reply(&index_management_handler)
                .await;
            let resp_body = std::str::from_utf8(resp.body()).unwrap();
            assert_eq!(resp.status(), 200, "{resp_body}");
            // Check that the source has been updated.
            let index_metadata = metastore
                .index_metadata(IndexMetadataRequest::for_index_id("hdfs-logs".to_string()))
                .await
                .unwrap()
                .deserialize_index_metadata()
                .unwrap();
            let metastore_source_config = index_metadata.sources.get("sqs-source").unwrap();
            assert_eq!(metastore_source_config.source_type(), SourceType::File);
            assert_eq!(
                metastore_source_config,
                &serde_json::from_value(update_source_config_body).unwrap(),
            );
        }
        {
            // Update the source with a different source_id (forbidden)
            let update_source_config_body = serde_json::json!({
                "version": "0.9",
                "source_id": "new-source-id",
                "source_type": "file",
                "params": {"notifications": [{"type": "sqs", "queue_url": queue_url, "message_type": "s3_notification"}]},
            });
            let resp = warp::test::request()
                .path("/indexes/hdfs-logs/sources/sqs-source")
                .method("PUT")
                .json(&update_source_config_body)
                .reply(&index_management_handler)
                .await;
            let resp_body = std::str::from_utf8(resp.body()).unwrap();
            assert_eq!(resp.status(), 400, "{resp_body}");
            // Check that the source hasn't been updated.
            let index_metadata = metastore
                .index_metadata(IndexMetadataRequest::for_index_id("hdfs-logs".to_string()))
                .await
                .unwrap()
                .deserialize_index_metadata()
                .unwrap();
            assert!(index_metadata.sources.contains_key("sqs-source"));
            assert!(!index_metadata.sources.contains_key("other-source-id"));
        }
    }

    #[tokio::test]
    async fn test_delete_non_existing_source() {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore.expect_index_metadata().return_once(|_| {
            Ok(
                IndexMetadataResponse::try_from_index_metadata(&IndexMetadata::for_test(
                    "quickwit-demo-index",
                    "file:///path/to/index/quickwit-demo-index",
                ))
                .unwrap(),
            )
        });
        // TODO
        // metastore
        //     .expect_index_exists()
        //     .return_once(|index_id: &str| Ok(index_id == "quickwit-demo-index"));
        mock_metastore.expect_delete_source().return_once(
            |delete_source_request: DeleteSourceRequest| {
                let index_uid: IndexUid = delete_source_request.index_uid().clone();
                let source_id = delete_source_request.source_id;
                assert_eq!(index_uid.index_id, "quickwit-demo-index");
                Err(MetastoreError::NotFound(EntityKind::Source {
                    index_id: "quickwit-demo-index".to_string(),
                    source_id: source_id.to_string(),
                }))
            },
        );
        let index_service = IndexService::new(
            MetastoreServiceClient::from_mock(mock_metastore),
            StorageResolver::unconfigured(),
        );
        let index_management_handler =
            super::index_management_handlers(index_service, Arc::new(NodeConfig::for_test()))
                .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index/sources/foo-source")
            .method("DELETE")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 404);
    }

    #[tokio::test]
    async fn test_source_reset_checkpoint() -> anyhow::Result<()> {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_index_metadata()
            .returning(|_| {
                Ok(
                    IndexMetadataResponse::try_from_index_metadata(&IndexMetadata::for_test(
                        "quickwit-demo-index",
                        "file:///path/to/index/quickwit-demo-index",
                    ))
                    .unwrap(),
                )
            })
            .times(2);
        mock_metastore
            .expect_reset_source_checkpoint()
            .returning(
                |reset_source_checkpoint_request: ResetSourceCheckpointRequest| {
                    let index_uid: IndexUid = reset_source_checkpoint_request.index_uid().clone();
                    let source_id = reset_source_checkpoint_request.source_id;
                    if index_uid.index_id == "quickwit-demo-index" && source_id == "source-to-reset"
                    {
                        return Ok(EmptyResponse {});
                    }
                    Err(MetastoreError::Internal {
                        message: "".to_string(),
                        cause: "".to_string(),
                    })
                },
            )
            .times(2);
        let index_service = IndexService::new(
            MetastoreServiceClient::from_mock(mock_metastore),
            StorageResolver::unconfigured(),
        );
        let index_management_handler =
            super::index_management_handlers(index_service, Arc::new(NodeConfig::for_test()))
                .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index/sources/source-to-reset/reset-checkpoint")
            .method("PUT")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index/sources/source-to-reset-2/reset-checkpoint")
            .method("PUT")
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 500);
        Ok(())
    }

    #[tokio::test]
    async fn test_source_toggle() -> anyhow::Result<()> {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_index_metadata()
            .returning(|_| {
                Ok(
                    IndexMetadataResponse::try_from_index_metadata(&IndexMetadata::for_test(
                        "quickwit-demo-index",
                        "file:///path/to/index/quickwit-demo-index",
                    ))
                    .unwrap(),
                )
            })
            .times(3);
        mock_metastore.expect_toggle_source().return_once(
            |toggle_source_request: ToggleSourceRequest| {
                let index_uid: IndexUid = toggle_source_request.index_uid().clone();
                let source_id = toggle_source_request.source_id;
                let enable = toggle_source_request.enable;
                if index_uid.index_id == "quickwit-demo-index"
                    && source_id == "source-to-toggle"
                    && enable
                {
                    return Ok(EmptyResponse {});
                }
                Err(MetastoreError::Internal {
                    message: "".to_string(),
                    cause: "".to_string(),
                })
            },
        );
        let index_service = IndexService::new(
            MetastoreServiceClient::from_mock(mock_metastore),
            StorageResolver::unconfigured(),
        );
        let index_management_handler =
            super::index_management_handlers(index_service, Arc::new(NodeConfig::for_test()))
                .recover(recover_fn);
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index/sources/source-to-toggle/toggle")
            .method("PUT")
            .json(&true)
            .body(r#"{"enable": true}"#)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let resp = warp::test::request()
            .path("/indexes/quickwit-demo-index/sources/source-to-toggle/toggle")
            .method("PUT")
            .json(&true)
            .body(r#"{"toggle": true}"#) // unknown field, should return 400.
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 400);
        // Check cannot toggle source managed by Quickwit.
        let resp = warp::test::request()
            .path(format!("/indexes/hdfs-logs/sources/{INGEST_API_SOURCE_ID}/toggle").as_str())
            .method("PUT")
            .body(r#"{"enable": true}"#)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 403);

        let resp = warp::test::request()
            .path(format!("/indexes/hdfs-logs/sources/{CLI_SOURCE_ID}/toggle").as_str())
            .method("PUT")
            .body(r#"{"enable": true}"#)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 403);
        Ok(())
    }

    #[tokio::test]
    async fn test_analyze_request() {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore.expect_index_metadata().return_once(|_| {
            Ok(
                IndexMetadataResponse::try_from_index_metadata(&IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
                .unwrap(),
            )
        });
        let index_service = IndexService::new(
            MetastoreServiceClient::from_mock(mock_metastore),
            StorageResolver::unconfigured(),
        );
        let index_management_handler =
            super::index_management_handlers(index_service, Arc::new(NodeConfig::for_test()))
                .recover(recover_fn);
        let resp = warp::test::request()
            .path("/analyze")
            .method("POST")
            .json(&true)
            .body(
                r#"{"type": "ngram", "min_gram": 3, "max_gram": 3, "text": "Hel", "filters":
    ["lower_caser"]}"#,
            )
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let actual_response_json: JsonValue = serde_json::from_slice(resp.body()).unwrap();
        let expected_response_json = serde_json::json!([
            {
                "offset_from": 0,
                "offset_to": 3,
                "position": 0,
                "position_length": 1,
                "text": "hel"
            }
        ]);
        assert_json_include!(
            actual: actual_response_json,
            expected: expected_response_json
        );
    }

    #[tokio::test]
    async fn test_parse_query_request() {
        let index_service = IndexService::new(
            MetastoreServiceClient::mocked(),
            StorageResolver::unconfigured(),
        );
        let index_management_handler =
            super::index_management_handlers(index_service, Arc::new(NodeConfig::for_test()))
                .recover(recover_fn);
        let resp = warp::test::request()
            .path("/parse-query")
            .method("POST")
            .json(&true)
            .body(r#"{"query": "field:this AND field:that"}"#)
            .reply(&index_management_handler)
            .await;
        assert_eq!(resp.status(), 200);
    }
}
