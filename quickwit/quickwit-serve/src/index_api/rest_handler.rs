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

use axum::body::Bytes;
use axum::extract::{Path, Query};
use axum::response::{IntoResponse, Json};
use axum::routing::{get, post, put};
use axum::{Extension, Router};
use axum_extra::TypedHeader;
use axum_extra::headers::ContentType;
use quickwit_config::{ConfigFormat, NodeConfig};
use quickwit_doc_mapper::{TokenizerConfig, analyze_text};
use quickwit_index_management::{IndexService, IndexServiceError};
use quickwit_proto::metastore::MetastoreError;
use quickwit_query::query_ast::{QueryAst, query_ast_from_user_text};
use serde::Deserialize;

use crate::simple_list::from_simple_list;

// #[derive(utoipa::OpenApi)]
// #[openapi(
//     paths(
//         // create_index,
//         // update_index,
//         // clear_index,
//         // delete_index,
//         // list_indexes_metadata,
//         // list_splits,
//         // describe_index,
//         // mark_splits_for_deletion,
//         create_source,
//         update_source,
//         reset_source_checkpoint,
//         toggle_source,
//         delete_source,
//     ),
//     components(schemas(ToggleSource, SplitsForDeletion, IndexStats))
// )]
// pub struct IndexApi;

#[derive(Debug, Deserialize, utoipa::IntoParams, utoipa::ToSchema)]
struct AnalyzeRequest {
    /// The tokenizer to use.
    #[serde(flatten)]
    pub tokenizer_config: TokenizerConfig,
    /// The text to analyze.
    pub text: String,
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

/// Extract config format from Content-Type typed header
fn extract_config_format_from_content_type(content_type: Option<&ContentType>) -> ConfigFormat {
    if let Some(content_type) = content_type {
        let content_type_str = content_type.to_string();
        match content_type_str.as_str() {
            "application/json" | "text/json" => ConfigFormat::Json,
            "application/yaml" | "text/yaml" => ConfigFormat::Yaml,
            "application/toml" | "text/toml" => ConfigFormat::Toml,
            _ => ConfigFormat::Json, // Default to JSON for unknown types
        }
    } else {
        ConfigFormat::Json // Default to JSON if no content-type
    }
}

/// Check if content type is supported
fn is_content_type_supported(content_type: Option<&ContentType>) -> Result<(), String> {
    if let Some(content_type) = content_type {
        let content_type_str = content_type.to_string();
        match content_type_str.as_str() {
            "application/json" | "text/json" | "application/yaml" | "text/yaml"
            | "application/toml" | "text/toml" => Ok(()),
            _ => Err(format!(
                "content-type {} is not supported",
                content_type_str
            )),
        }
    } else {
        Ok(()) // No content-type is acceptable (defaults to JSON)
    }
}

/// Creates routes for Index API endpoints
pub fn index_management_routes(
    index_service: IndexService,
    node_config: Arc<NodeConfig>,
) -> Router {
    Router::new()
        // Index endpoints
        .route("/indexes", get(list_indexes).post(create_index))
        .route(
            "/indexes/:index_id",
            get(get_index_metadata)
                .put(update_index)
                .delete(delete_index),
        )
        .route("/indexes/:index_id/describe", get(describe_index))
        .route("/indexes/:index_id/clear", put(clear_index))
        // Split endpoints
        .route("/indexes/:index_id/splits", get(list_splits))
        .route(
            "/indexes/:index_id/splits/mark-for-deletion",
            put(mark_splits_for_deletion_axum),
        )
        // Source endpoints
        .route("/indexes/:index_id/sources", post(create_source))
        .route(
            "/indexes/:index_id/sources/:source_id",
            get(get_source).put(update_source).delete(delete_source),
        )
        .route(
            "/indexes/:index_id/sources/:source_id/reset-checkpoint",
            put(reset_source_checkpoint),
        )
        .route(
            "/indexes/:index_id/sources/:source_id/toggle",
            put(toggle_source),
        )
        .route(
            "/indexes/:index_id/sources/:source_id/shards",
            get(get_source_shards),
        )
        // Utility endpoints
        .route("/analyze", post(analyze_request_endpoint))
        .route("/parse-query", post(parse_query_request_endpoint))
        .layer(Extension(index_service))
        .layer(Extension(node_config))
}

// Index endpoints axum handlers

/// Axum handler for GET /indexes
async fn list_indexes(
    Extension(index_service): Extension<IndexService>,
    Query(params): Query<super::index_resource::ListIndexesQueryParams>,
) -> impl IntoResponse {
    let result =
        super::index_resource::list_indexes_metadata(params, index_service.metastore()).await;
    crate::rest_api_response::into_rest_api_response::<
        Vec<quickwit_metastore::IndexMetadata>,
        MetastoreError,
    >(result, crate::format::BodyFormat::default())
}

/// Axum handler for GET /indexes/{index_id}
async fn get_index_metadata(
    Extension(index_service): Extension<IndexService>,
    Path(index_id): Path<String>,
) -> impl IntoResponse {
    let result =
        super::index_resource::get_index_metadata(index_id, index_service.metastore()).await;
    crate::rest_api_response::into_rest_api_response::<
        quickwit_metastore::IndexMetadata,
        MetastoreError,
    >(result, crate::format::BodyFormat::default())
}

/// Axum handler for GET /indexes/{index_id}/describe
async fn describe_index(
    Extension(index_service): Extension<IndexService>,
    Path(index_id): Path<String>,
) -> impl IntoResponse {
    let result = super::index_resource::describe_index(index_id, index_service.metastore()).await;
    crate::rest_api_response::into_rest_api_response::<
        super::index_resource::IndexStats,
        MetastoreError,
    >(result, crate::format::BodyFormat::default())
}

/// Axum handler for POST /indexes
async fn create_index(
    Extension(index_service): Extension<IndexService>,
    Extension(node_config): Extension<Arc<NodeConfig>>,
    Query(params): Query<super::index_resource::CreateIndexQueryParams>,
    content_type: Option<TypedHeader<ContentType>>,
    body: Bytes,
) -> impl IntoResponse {
    // Extract the ContentType from the TypedHeader
    let content_type_ref = content_type.as_ref().map(|h| &h.0);

    // Check for unsupported content types
    if let Err(error_msg) = is_content_type_supported(content_type_ref) {
        return (axum::http::StatusCode::UNSUPPORTED_MEDIA_TYPE, error_msg).into_response();
    }

    // Extract config format from Content-Type header
    let config_format = extract_config_format_from_content_type(content_type_ref);

    let result = super::index_resource::create_index(
        params,
        config_format,
        body,
        index_service,
        node_config,
    )
    .await;
    crate::rest_api_response::into_rest_api_response::<
        quickwit_metastore::IndexMetadata,
        IndexServiceError,
    >(result, crate::format::BodyFormat::default())
    .into_response()
}

/// Axum handler for PUT /indexes/{index_id}
async fn update_index(
    Extension(index_service): Extension<IndexService>,
    Extension(node_config): Extension<Arc<NodeConfig>>,
    Path(index_id): Path<String>,
    Query(params): Query<super::index_resource::UpdateQueryParams>,
    content_type: Option<TypedHeader<ContentType>>,
    body: Bytes,
) -> impl IntoResponse {
    // Extract the ContentType from the TypedHeader
    let content_type_ref = content_type.as_ref().map(|h| &h.0);

    // Check for unsupported content types
    if let Err(error_msg) = is_content_type_supported(content_type_ref) {
        return (axum::http::StatusCode::UNSUPPORTED_MEDIA_TYPE, error_msg).into_response();
    }

    // Extract config format from Content-Type header
    let config_format = extract_config_format_from_content_type(content_type_ref);

    let result = super::index_resource::update_index(
        index_id,
        config_format,
        params,
        body,
        index_service,
        node_config,
    )
    .await;
    crate::rest_api_response::into_rest_api_response::<
        quickwit_metastore::IndexMetadata,
        IndexServiceError,
    >(result, crate::format::BodyFormat::default())
    .into_response()
}

/// Axum handler for PUT /indexes/{index_id}/clear
async fn clear_index(
    Extension(index_service): Extension<IndexService>,
    Path(index_id): Path<String>,
) -> impl IntoResponse {
    let result = super::index_resource::clear_index(index_id, index_service).await;
    crate::rest_api_response::into_rest_api_response::<(), IndexServiceError>(
        result,
        crate::format::BodyFormat::default(),
    )
}

/// Axum handler for DELETE /indexes/{index_id}
async fn delete_index(
    Extension(index_service): Extension<IndexService>,
    Path(index_id): Path<String>,
    Query(params): Query<super::index_resource::DeleteIndexQueryParam>,
) -> impl IntoResponse {
    let result = super::index_resource::delete_index(index_id, params, index_service).await;
    crate::rest_api_response::into_rest_api_response::<
        Vec<quickwit_metastore::SplitInfo>,
        IndexServiceError,
    >(result, crate::format::BodyFormat::default())
}

// Split endpoints axum handlers

/// Axum handler for GET /indexes/{index_id}/splits
async fn list_splits(
    Extension(index_service): Extension<IndexService>,
    Path(index_id): Path<String>,
    Query(params): Query<super::split_resource::ListSplitsQueryParams>,
) -> impl IntoResponse {
    let result =
        super::split_resource::list_splits(index_id, params, index_service.metastore()).await;
    crate::rest_api_response::into_rest_api_response::<
        super::split_resource::ListSplitsResponse,
        MetastoreError,
    >(result, crate::format::BodyFormat::default())
}

/// Axum handler for PUT /indexes/{index_id}/splits/mark-for-deletion
async fn mark_splits_for_deletion_axum(
    Extension(index_service): Extension<IndexService>,
    Path(index_id): Path<String>,
    Json(splits_for_deletion): Json<super::split_resource::SplitsForDeletion>,
) -> impl IntoResponse {
    let result = super::split_resource::mark_splits_for_deletion(
        index_id,
        splits_for_deletion,
        index_service.metastore(),
    )
    .await;
    crate::rest_api_response::into_rest_api_response::<(), MetastoreError>(
        result,
        crate::format::BodyFormat::default(),
    )
}

// Source endpoints axum handlers

/// Axum handler for POST /indexes/{index_id}/sources
async fn create_source(
    Extension(index_service): Extension<IndexService>,
    Path(index_id): Path<String>,
    content_type: Option<TypedHeader<ContentType>>,
    body: Bytes,
) -> impl IntoResponse {
    // Extract the ContentType from the TypedHeader
    let content_type_ref = content_type.as_ref().map(|h| &h.0);

    // Extract config format from Content-Type header
    let config_format = extract_config_format_from_content_type(content_type_ref);
    let result =
        super::source_resource::create_source(index_id, config_format, body, index_service).await;
    crate::rest_api_response::into_rest_api_response::<
        quickwit_config::SourceConfig,
        IndexServiceError,
    >(result, crate::format::BodyFormat::default())
}

/// Axum handler for GET /indexes/{index_id}/sources/{source_id}
async fn get_source(
    Extension(index_service): Extension<IndexService>,
    Path((index_id, source_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let result =
        super::source_resource::get_source(index_id, source_id, index_service.metastore()).await;
    crate::rest_api_response::into_rest_api_response::<quickwit_config::SourceConfig, MetastoreError>(
        result,
        crate::format::BodyFormat::default(),
    )
}

/// Axum handler for PUT /indexes/{index_id}/sources/{source_id}
async fn update_source(
    Extension(index_service): Extension<IndexService>,
    Path((index_id, source_id)): Path<(String, String)>,
    Query(params): Query<super::source_resource::UpdateQueryParams>,
    content_type: Option<TypedHeader<ContentType>>,
    body: Bytes,
) -> impl IntoResponse {
    // Extract the ContentType from the TypedHeader
    let content_type_ref = content_type.as_ref().map(|h| &h.0);

    // Extract config format from Content-Type header
    let config_format = extract_config_format_from_content_type(content_type_ref);
    let result = super::source_resource::update_source(
        index_id,
        source_id,
        config_format,
        params,
        body,
        index_service,
    )
    .await;
    crate::rest_api_response::into_rest_api_response::<
        quickwit_config::SourceConfig,
        IndexServiceError,
    >(result, crate::format::BodyFormat::default())
}

/// Axum handler for DELETE /indexes/{index_id}/sources/{source_id}
async fn delete_source(
    Extension(index_service): Extension<IndexService>,
    Path((index_id, source_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let result =
        super::source_resource::delete_source(index_id, source_id, index_service.metastore()).await;
    crate::rest_api_response::into_rest_api_response::<(), IndexServiceError>(
        result,
        crate::format::BodyFormat::default(),
    )
}

/// Axum handler for PUT /indexes/{index_id}/sources/{source_id}/reset-checkpoint
async fn reset_source_checkpoint(
    Extension(index_service): Extension<IndexService>,
    Path((index_id, source_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let result = super::source_resource::reset_source_checkpoint(
        index_id,
        source_id,
        index_service.metastore(),
    )
    .await;
    crate::rest_api_response::into_rest_api_response::<(), MetastoreError>(
        result,
        crate::format::BodyFormat::default(),
    )
}

/// Axum handler for PUT /indexes/{index_id}/sources/{source_id}/toggle
async fn toggle_source(
    Extension(index_service): Extension<IndexService>,
    Path((index_id, source_id)): Path<(String, String)>,
    Json(toggle_source): Json<super::source_resource::ToggleSource>,
) -> impl IntoResponse {
    let result = super::source_resource::toggle_source(
        index_id,
        source_id,
        toggle_source,
        index_service.metastore(),
    )
    .await;
    crate::rest_api_response::into_rest_api_response::<(), IndexServiceError>(
        result,
        crate::format::BodyFormat::default(),
    )
}

/// Axum handler for GET /indexes/{index_id}/sources/{source_id}/shards
async fn get_source_shards(
    Extension(index_service): Extension<IndexService>,
    Path((index_id, source_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let result =
        super::source_resource::get_source_shards(index_id, source_id, index_service.metastore())
            .await;
    crate::rest_api_response::into_rest_api_response::<
        Vec<quickwit_proto::ingest::Shard>,
        MetastoreError,
    >(result, crate::format::BodyFormat::default())
}

// Utility endpoints axum handlers

/// Axum handler for POST /analyze
async fn analyze_request_endpoint(Json(request): Json<AnalyzeRequest>) -> impl IntoResponse {
    let result = analyze_request(request).await;
    crate::rest_api_response::into_rest_api_response::<serde_json::Value, IndexServiceError>(
        result,
        crate::format::BodyFormat::default(),
    )
}

/// Axum handler for POST /parse-query
async fn parse_query_request_endpoint(Json(request): Json<ParseQueryRequest>) -> impl IntoResponse {
    let result = parse_query_request(request).await;
    crate::rest_api_response::into_rest_api_response::<QueryAst, IndexServiceError>(
        result,
        crate::format::BodyFormat::default(),
    )
}

#[cfg(test)]
mod tests {
    use std::ops::{Bound, RangeInclusive};

    use assert_json_diff::assert_json_include;
    use axum_test::TestServer;
    use http::StatusCode;
    use quickwit_common::ServiceStream;
    use quickwit_common::uri::Uri;
    use quickwit_config::{
        CLI_SOURCE_ID, INGEST_API_SOURCE_ID, NodeConfig, SourceParams, VecSourceParams,
    };
    use quickwit_indexing::{MockSplitBuilder, mock_split};
    use quickwit_metastore::{
        IndexMetadata, IndexMetadataResponseExt, ListIndexesMetadataResponseExt,
        ListSplitsRequestExt, ListSplitsResponseExt, SplitState, metastore_for_test,
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

    /// Helper function to create a test server with the index management routes
    fn create_test_server(index_service: IndexService, node_config: Arc<NodeConfig>) -> TestServer {
        let app = index_management_routes(index_service, node_config);
        TestServer::new(app).unwrap()
    }

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
        let server = create_test_server(index_service, Arc::new(NodeConfig::for_test()));

        let response = server.get("/indexes/test-index").await;
        response.assert_status_ok();

        let actual_response_json: JsonValue = response.json();
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
        let server = create_test_server(index_service, Arc::new(NodeConfig::for_test()));

        let response = server.get("/indexes/test-index").await;
        response.assert_status(StatusCode::NOT_FOUND);
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
                    let splits = vec![
                        MockSplitBuilder::new("split_1")
                            .with_index_uid(&index_uid)
                            .build(),
                    ];
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
        let server = create_test_server(index_service, Arc::new(NodeConfig::for_test()));

        {
            let response = server
                .get(
                    "/indexes/quickwit-demo-index/splits?split_states=Published,Staged&\
                     start_timestamp=10&end_timestamp=20&end_create_timestamp=2",
                )
                .await;
            response.assert_status_ok();

            let actual_response_json: JsonValue = response.json();
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
            let response = server
                .get(
                    "/indexes/quickwit-demo-index/splits?split_states=Published&\
                     start_timestamp=11&end_timestamp=20&end_create_timestamp=2",
                )
                .await;
            response.assert_status(StatusCode::INTERNAL_SERVER_ERROR);
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
        let server = create_test_server(index_service, Arc::new(NodeConfig::for_test()));

        let response = server.get("/indexes/quickwit-demo-index/describe").await;
        response.assert_status_ok();

        let actual_response_json: JsonValue = response.json();
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
        let server = create_test_server(index_service, Arc::new(NodeConfig::for_test()));

        let response = server.get("/indexes/quickwit-demo-index/splits").await;
        response.assert_status_ok();
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
        let server = create_test_server(index_service, Arc::new(NodeConfig::for_test()));

        let response = server
            .put("/indexes/quickwit-demo-index/splits/mark-for-deletion")
            .json(&serde_json::json!({"split_ids": ["split-1", "split-2"]}))
            .await;
        response.assert_status_ok();

        let response = server
            .put("/indexes/quickwit-demo-index/splits/mark-for-deletion")
            .json(&serde_json::json!({"split_ids": [""]}))
            .await;
        response.assert_status(StatusCode::INTERNAL_SERVER_ERROR);
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
        let server = create_test_server(index_service, Arc::new(NodeConfig::for_test()));

        let response = server.get("/indexes?index_id_patterns=test-index-*").await;
        response.assert_status_ok();
        let actual_response_json: JsonValue = response.json();
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
        let server = create_test_server(index_service, Arc::new(NodeConfig::for_test()));

        let response = server.put("/indexes/quickwit-demo-index/clear").await;
        response.assert_status_ok();
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
        let server = create_test_server(index_service, Arc::new(NodeConfig::for_test()));
        {
            // Dry run
            let response = server
                .delete("/indexes/quickwit-demo-index?dry_run=true")
                .await;
            response.assert_status_ok();
            let resp_json: serde_json::Value = response.json();
            let expected_response_json = serde_json::json!([{
                "file_name": "split_1.split",
                "file_size_bytes": "800 B",
            }]);
            assert_json_include!(actual: resp_json, expected: expected_response_json);
        }
        {
            let response = server.delete("/indexes/quickwit-demo-index").await;
            response.assert_status_ok();
            let resp_json: serde_json::Value = response.json();
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
        let server = create_test_server(index_service, Arc::new(NodeConfig::for_test()));

        let response = server.delete("/indexes/quickwit-demo-index").await;
        response.assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_create_index_with_overwrite() {
        let metastore = metastore_for_test();
        let index_service = IndexService::new(metastore.clone(), StorageResolver::unconfigured());
        let mut node_config = NodeConfig::for_test();
        node_config.default_index_root_uri = Uri::for_test("file:///default-index-root-uri");
        let server = create_test_server(index_service, Arc::new(node_config));

        {
            let response = server
                .post("/indexes")
                .json(&serde_json::json!({
                    "version": "0.7",
                    "index_id": "hdfs-logs",
                    "doc_mapping": {
                        "field_mappings": [{
                            "name": "timestamp",
                            "type": "i64",
                            "fast": true,
                            "indexed": true
                        }]
                    },
                    "search_settings": {
                        "default_search_fields": ["body"]
                    }
                }))
                .await;
            if response.status_code() != 200 {
                println!("Error response body: {}", response.text());
            }
            response.assert_status_ok();
            let resp_json: serde_json::Value = response.json();
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
            let response = server
                .post("/indexes")
                .json(&serde_json::json!({
                    "version": "0.7",
                    "index_id": "hdfs-logs",
                    "doc_mapping": {
                        "field_mappings": [{
                            "name": "timestamp",
                            "type": "i64",
                            "fast": true,
                            "indexed": true
                        }]
                    },
                    "search_settings": {
                        "default_search_fields": ["body"]
                    }
                }))
                .await;
            response.assert_status(StatusCode::BAD_REQUEST);
        }
    }

    #[tokio::test]
    async fn test_create_delete_index_and_source() {
        let metastore = metastore_for_test();
        let index_service = IndexService::new(metastore.clone(), StorageResolver::unconfigured());
        let mut node_config = NodeConfig::for_test();
        node_config.default_index_root_uri = Uri::for_test("file:///default-index-root-uri");
        let server = create_test_server(index_service, Arc::new(node_config));

        let response = server
            .post("/indexes")
            .json(&serde_json::json!({
                "version": "0.7",
                "index_id": "hdfs-logs",
                "doc_mapping": {
                    "field_mappings": [{
                        "name": "timestamp",
                        "type": "i64",
                        "fast": true,
                        "indexed": true
                    }]
                },
                "search_settings": {
                    "default_search_fields": ["body"]
                }
            }))
            .await;
        response.assert_status_ok();
        let resp_json: serde_json::Value = response.json();
        let expected_response_json = serde_json::json!({
            "index_config": {
                "index_id": "hdfs-logs",
                "index_uri": "file:///default-index-root-uri/hdfs-logs",
                "search_settings": {
                    "default_search_fields": ["body"]
                }
            }
        });
        assert_json_include!(actual: resp_json, expected: expected_response_json);

        // Create source.
        let response = server
            .post("/indexes/hdfs-logs/sources")
            .json(&serde_json::json!({
                "version": "0.7",
                "source_id": "vec-source",
                "source_type": "vec",
                "params": {
                    "docs": [],
                    "batch_num_docs": 10
                }
            }))
            .await;
        response.assert_status_ok();

        // Get source.
        let response = server.get("/indexes/hdfs-logs/sources/vec-source").await;
        response.assert_status_ok();

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
        let response = server.delete("/indexes/hdfs-logs/sources/vec-source").await;
        response.assert_status_ok();
        let index_metadata = metastore
            .index_metadata(IndexMetadataRequest::for_index_id("hdfs-logs".to_string()))
            .await
            .unwrap()
            .deserialize_index_metadata()
            .unwrap();
        assert!(!index_metadata.sources.contains_key("file-source"));

        // Check cannot delete source managed by Quickwit.
        let response = server
            .delete(&format!(
                "/indexes/hdfs-logs/sources/{INGEST_API_SOURCE_ID}"
            ))
            .await;
        response.assert_status(StatusCode::FORBIDDEN);

        let response = server
            .delete(&format!("/indexes/hdfs-logs/sources/{CLI_SOURCE_ID}"))
            .await;
        response.assert_status(StatusCode::FORBIDDEN);

        // Check get a non existing source returns 404.
        let response = server.get("/indexes/hdfs-logs/sources/file-source").await;
        response.assert_status(StatusCode::NOT_FOUND);

        // Check delete index.
        let response = server.delete("/indexes/hdfs-logs").await;
        response.assert_status_ok();
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
        let server = create_test_server(index_service, Arc::new(node_config));

        let yaml_content = r#"
            version: 0.8
            index_id: hdfs-logs
            doc_mapping:
              field_mappings:
                - name: timestamp
                  type: i64
                  fast: true
                  indexed: true
            "#;

        let response = server
            .post("/indexes")
            .add_header("content-type", "application/yaml")
            .bytes(yaml_content.as_bytes().into())
            .await;
        response.assert_status_ok();
        let resp_json: serde_json::Value = response.json();
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
        let server = create_test_server(index_service, Arc::new(node_config));

        let toml_content = r#"
            version = "0.7"
            index_id = "hdfs-logs"
            [doc_mapping]
            field_mappings = [
                { name = "timestamp", type = "i64", fast = true, indexed = true}
            ]
            "#;

        let response = server
            .post("/indexes")
            .add_header("content-type", "application/toml")
            .bytes(toml_content.as_bytes().into())
            .await;
        response.assert_status_ok();
        let resp_json: serde_json::Value = response.json();
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
        let server = create_test_server(index_service, Arc::new(node_config));

        let response = server
            .post("/indexes")
            .add_header("content-type", "application/yoml")
            .bytes("".as_bytes().into())
            .await;
        response.assert_status(StatusCode::UNSUPPORTED_MEDIA_TYPE);
        let body = response.text();
        assert!(body.contains("content-type") && body.contains("is not supported"));
    }

    #[tokio::test]
    async fn test_create_index_with_bad_config() -> anyhow::Result<()> {
        let index_service = IndexService::new(
            MetastoreServiceClient::mocked(),
            StorageResolver::unconfigured(),
        );
        let server = create_test_server(index_service, Arc::new(NodeConfig::for_test()));

        let response = server
            .post("/indexes")
            .json(&serde_json::json!({
                "version": "0.7",
                "index_id": "hdfs-log",
                "doc_mapping": {
                    "field_mappings": [{
                        "name": "timestamp",
                        "type": "unknown",
                        "fast": true,
                        "indexed": true
                    }]
                }
            }))
            .await;
        response.assert_status(StatusCode::BAD_REQUEST);
        let body = response.text();
        assert!(body.contains("field `timestamp` has an unknown type"));
        Ok(())
    }

    #[tokio::test]
    async fn test_update_index() {
        let metastore = metastore_for_test();
        let index_service = IndexService::new(metastore.clone(), StorageResolver::unconfigured());
        let mut node_config = NodeConfig::for_test();
        node_config.default_index_root_uri = Uri::for_test("file:///default-index-root-uri");
        let server = create_test_server(index_service, Arc::new(node_config));

        let response = server
            .post("/indexes")
            .json(&serde_json::json!({
                "version": "0.7",
                "index_id": "hdfs-logs",
                "doc_mapping": {
                    "field_mappings": [{
                        "name": "timestamp",
                        "type": "i64",
                        "fast": true,
                        "indexed": true
                    }]
                },
                "search_settings": {
                    "default_search_fields": ["body"]
                }
            }))
            .await;
        response.assert_status_ok();
        let resp_json: serde_json::Value = response.json();
        let expected_response_json = serde_json::json!({
            "index_config": {
                "search_settings": {
                    "default_search_fields": ["body"]
                }
            }
        });
        assert_json_include!(actual: resp_json, expected: expected_response_json);

        let response = server
            .put("/indexes/hdfs-logs")
            .json(&serde_json::json!({
                "version": "0.7",
                "index_id": "hdfs-logs",
                "doc_mapping": {
                    "field_mappings": [{
                        "name": "timestamp",
                        "type": "i64",
                        "fast": true,
                        "indexed": true
                    }]
                },
                "search_settings": {
                    "default_search_fields": ["severity_text", "body"]
                }
            }))
            .await;
        response.assert_status_ok();
        let resp_json: serde_json::Value = response.json();
        let expected_response_json = serde_json::json!({
            "index_config": {
                "search_settings": {
                    "default_search_fields": ["severity_text", "body"]
                }
            }
        });
        assert_json_include!(actual: resp_json, expected: expected_response_json);
    }

    #[tokio::test]
    async fn test_create_source_with_bad_config() {
        let metastore = metastore_for_test();
        let index_service = IndexService::new(metastore, StorageResolver::unconfigured());
        let server = create_test_server(index_service, Arc::new(NodeConfig::for_test()));

        let response = server
            .post("/indexes/my-index/sources")
            .json(&serde_json::json!({
                "version": 0.4,
                "source_id": "file-source"
            }))
            .await;
        response.assert_status(StatusCode::BAD_REQUEST);
        let body = response.text();
        assert!(body.contains("invalid type: floating point `0.4`"));

        let response = server
            .post("/indexes/my-index/sources")
            .json(&serde_json::json!({
                "version": "0.8",
                "source_id": "pulsar-source",
                "num_pipelines": 2,
                "source_type": "pulsar",
                "params": {
                    "topics": ["my-topic"],
                    "address": "pulsar://localhost:6650"
                }
            }))
            .await;
        response.assert_status(StatusCode::BAD_REQUEST);
        let body = response.text();
        assert!(body.contains(
            "Quickwit currently supports multiple pipelines only for GCP PubSub or Kafka sources"
        ));

        let response = server
            .post("/indexes/hdfs-logs/sources")
            .text(r#"{"version": "0.8", "source_id": "my-stdin-source", "source_type": "stdin"}"#)
            .await;
        response.assert_status(StatusCode::BAD_REQUEST);
        let body = response.text();
        assert!(body.contains("stdin can only be used as source through the CLI command"));

        let response = server
            .post("/indexes/hdfs-logs/sources")
            .text(
                r#"{"version": "0.8", "source_id": "my-local-file-source", "source_type": "file", "params": {"filepath": "localfile"}}"#
            )
            .await;
        response.assert_status(StatusCode::BAD_REQUEST);
        let body = response.text();
        assert!(body.contains("limited to a local usage"));
    }

    #[cfg(feature = "sqs-for-tests")]
    #[tokio::test]
    async fn test_update_source() {
        use quickwit_indexing::source::sqs_queue::test_helpers::start_mock_sqs_get_queue_attributes_endpoint;

        let metastore = metastore_for_test();
        let (queue_url, _guard) = start_mock_sqs_get_queue_attributes_endpoint();
        let index_service = IndexService::new(metastore.clone(), StorageResolver::unconfigured());
        let server = create_test_server(index_service, Arc::new(NodeConfig::for_test()));

        let response = server
            .post("/indexes")
            .json(&serde_json::json!({
                "version": "0.7",
                "index_id": "hdfs-logs",
                "doc_mapping": {
                    "field_mappings": [{
                        "name": "timestamp",
                        "type": "i64",
                        "fast": true,
                        "indexed": true
                    }]
                },
                "search_settings": {
                    "default_search_fields": ["body"]
                }
            }))
            .await;
        response.assert_status_ok();
        let resp_json: serde_json::Value = response.json();
        let expected_response_json = serde_json::json!({
            "index_config": {
                "index_id": "hdfs-logs",
                "index_uri": "file:///default-index-root-uri/hdfs-logs",
                "search_settings": {
                    "default_search_fields": ["body"]
                }
            }
        });
        assert_json_include!(actual: resp_json, expected: expected_response_json);

        // Create source.
        let response = server
            .post("/indexes/hdfs-logs/sources")
            .json(&serde_json::json!({
                "version": "0.7",
                "source_id": "sqs-source",
                "source_type": "file",
                "params": {
                    "notifications": [{"type": "sqs", "queue_url": queue_url, "message_type": "s3_notification"}]
                }
            }))
            .await;
        response.assert_status_ok();

        {
            // Update the source.
            let response = server
                .put("/indexes/hdfs-logs/sources/sqs-source")
                .json(&serde_json::json!({
                    "version": "0.7",
                    "source_id": "sqs-source",
                    "source_type": "file",
                    "params": {
                        "notifications": [{"type": "sqs", "queue_url": queue_url, "message_type": "s3_notification"}]
                    }
                }))
                .await;
            response.assert_status_ok();
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
                &serde_json::from_value(serde_json::json!({
                    "version": "0.7",
                    "source_id": "sqs-source",
                    "source_type": "file",
                    "params": {
                        "notifications": [{"type": "sqs", "queue_url": queue_url, "message_type": "s3_notification"}]
                    }
                })).unwrap(),
            );
        }
        {
            // Update the source with a different source_id (forbidden)
            let response = server
                .put("/indexes/hdfs-logs/sources/new-source-id")
                .json(&serde_json::json!({
                    "version": "0.7",
                    "source_id": "new-source-id",
                    "source_type": "file",
                    "params": {
                        "notifications": [{"type": "sqs", "queue_url": queue_url, "message_type": "s3_notification"}]
                    }
                }))
                .await;
            response.assert_status(StatusCode::BAD_REQUEST);
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
        let server = create_test_server(index_service, Arc::new(NodeConfig::for_test()));
        let response = server
            .delete("/indexes/quickwit-demo-index/sources/foo-source")
            .await;
        response.assert_status(StatusCode::NOT_FOUND);
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
        let server = create_test_server(index_service, Arc::new(NodeConfig::for_test()));
        let response = server
            .put("/indexes/quickwit-demo-index/sources/source-to-reset/reset-checkpoint")
            .await;
        response.assert_status_ok();
        let response = server
            .put("/indexes/quickwit-demo-index/sources/source-to-reset-2/reset-checkpoint")
            .await;
        response.assert_status(StatusCode::INTERNAL_SERVER_ERROR);
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
        let server = create_test_server(index_service, Arc::new(NodeConfig::for_test()));
        let response = server
            .put("/indexes/quickwit-demo-index/sources/source-to-toggle/toggle")
            .json(&serde_json::json!({"enable": true}))
            .await;
        response.assert_status_ok();
        let response = server
            .put("/indexes/quickwit-demo-index/sources/source-to-toggle/toggle")
            .json(&serde_json::json!({"toggle": true}))
            .await;
        response.assert_status(StatusCode::UNPROCESSABLE_ENTITY);
        // Check cannot toggle source managed by Quickwit.
        let response = server
            .put(format!("/indexes/hdfs-logs/sources/{INGEST_API_SOURCE_ID}/toggle").as_str())
            .json(&serde_json::json!({"enable": true}))
            .await;
        response.assert_status(StatusCode::FORBIDDEN);

        let response = server
            .put(format!("/indexes/hdfs-logs/sources/{CLI_SOURCE_ID}/toggle").as_str())
            .json(&serde_json::json!({"enable": true}))
            .await;
        response.assert_status(StatusCode::FORBIDDEN);
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
        let server = create_test_server(index_service, Arc::new(NodeConfig::for_test()));
        let response = server
            .post("/analyze")
            .json(&serde_json::json!({
                "type": "ngram",
                "min_gram": 3,
                "max_gram": 3,
                "text": "Hel",
                "filters": ["lower_caser"]
            }))
            .await;
        response.assert_status_ok();
        let actual_response_json: JsonValue = response.json();
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
        let server = create_test_server(index_service, Arc::new(NodeConfig::for_test()));
        let response = server
            .post("/parse-query")
            .json(&serde_json::json!({
                "query": "field:this AND field:that"
            }))
            .await;
        response.assert_status_ok();
    }
}
