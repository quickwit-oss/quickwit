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

use std::collections::HashMap;
use std::time::Instant;

use axum::extract::{Extension, Query};
use axum::http::StatusCode;
use bytes::Bytes;
use quickwit_ingest::{
    CommitType, DocBatchBuilder, IngestRequest, IngestService, IngestServiceClient,
};
use quickwit_proto::ingest::router::IngestRouterServiceClient;
use quickwit_proto::types::IndexId;

use super::bulk_v2::{ElasticBulkResponse, elastic_bulk_ingest_v2};
use super::model::{BulkAction, ElasticBulkOptions, ElasticsearchError, ElasticsearchResult};
use super::rest_handler::ElasticIndexPatterns;
use crate::elasticsearch_api::IngestConfig;
use crate::ingest_api::lines;

/// GET or POST _elastic/_bulk
pub async fn es_compat_bulk_handler(
    Query(bulk_options): Query<super::model::ElasticBulkOptions>,
    Extension(ingest_router): Extension<IngestRouterServiceClient>,
    Extension(ingest_service): Extension<IngestServiceClient>,
    Extension(ingest_config): Extension<IngestConfig>,
    body: Bytes,
) -> ElasticsearchResult<super::bulk_v2::ElasticBulkResponse> {
    elastic_ingest_bulk(
        None,
        body,
        bulk_options,
        ingest_service,
        ingest_router,
        ingest_config.enable_ingest_v1,
        ingest_config.enable_ingest_v2,
    )
    .await
    .into()
}

/// GET or POST _elastic/{index}/_bulk  
pub async fn es_compat_index_bulk_handler(
    ElasticIndexPatterns(index_id_patterns): ElasticIndexPatterns,
    Query(bulk_options): Query<super::model::ElasticBulkOptions>,
    Extension(ingest_router): Extension<IngestRouterServiceClient>,
    Extension(ingest_service): Extension<IngestServiceClient>,
    Extension(ingest_config): Extension<IngestConfig>,
    body: Bytes,
) -> ElasticsearchResult<super::bulk_v2::ElasticBulkResponse> {
    // For bulk operations on a specific index, use the first pattern as the default index
    let default_index = index_id_patterns.first().map(|s| s.clone());
    elastic_ingest_bulk(
        default_index,
        body,
        bulk_options,
        ingest_service,
        ingest_router,
        ingest_config.enable_ingest_v1,
        ingest_config.enable_ingest_v2,
    )
    .await
    .into()
}

pub(crate) async fn elastic_ingest_bulk(
    default_index_id: Option<IndexId>,
    body: Bytes,
    bulk_options: ElasticBulkOptions,
    ingest_service: IngestServiceClient,
    ingest_router: IngestRouterServiceClient,
    enable_ingest_v1: bool,
    enable_ingest_v2: bool,
) -> Result<ElasticBulkResponse, ElasticsearchError> {
    if enable_ingest_v2 && !bulk_options.use_legacy_ingest {
        return elastic_bulk_ingest_v2(default_index_id, body, bulk_options, ingest_router).await;
    }
    if !enable_ingest_v1 {
        return Err(ElasticsearchError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "ingest v1 is disabled: environment variable `QW_DISABLE_INGEST_V1` is set".to_string(),
            None,
        ));
    }
    let now = Instant::now();
    let mut doc_batch_builders = HashMap::new();
    let mut lines = lines(&body).enumerate();

    while let Some((line_number, line)) = lines.next() {
        let action = serde_json::from_slice::<BulkAction>(line).map_err(|error| {
            ElasticsearchError::new(
                StatusCode::BAD_REQUEST,
                format!("Malformed action/metadata line [#{line_number}]. Details: `{error}`"),
                None,
            )
        })?;
        let (_, source) = lines.next().ok_or_else(|| {
            ElasticsearchError::new(
                StatusCode::BAD_REQUEST,
                "expected source for the action".to_string(),
                None,
            )
        })?;
        // when ingesting on /my-index/_bulk, if _index: is set to something else than my-index,
        // ES honors it and create the doc in the requested index. That is, `my-index` is a default
        // value in case _index: is missing, but not a constraint on each sub-action.
        let index_id = action
            .into_index_id()
            .or_else(|| default_index_id.clone())
            .ok_or_else(|| {
                ElasticsearchError::new(
                    StatusCode::BAD_REQUEST,
                    format!("missing required field: `_index` in the line [#{line_number}]."),
                    None,
                )
            })?;
        let doc_batch_builder = doc_batch_builders
            .entry(index_id.clone())
            .or_insert(DocBatchBuilder::new(index_id));

        doc_batch_builder.ingest_doc(source);
    }
    let doc_batches = doc_batch_builders
        .into_values()
        .map(|builder| builder.build())
        .collect();
    let commit_type: CommitType = bulk_options.refresh.into();
    let ingest_request = IngestRequest {
        doc_batches,
        commit: commit_type.into(),
    };
    ingest_service.ingest(ingest_request).await?;

    let took_millis = now.elapsed().as_millis() as u64;
    let errors = false;
    let bulk_response = ElasticBulkResponse {
        took_millis,
        errors,
        actions: Vec::new(),
    };
    Ok(bulk_response)
}

#[cfg(test)]
mod tests {
    use quickwit_proto::ingest::CommitTypeV2;
    use quickwit_proto::ingest::router::{
        IngestFailure, IngestRequestV2 as IngestV2Request, IngestResponseV2, IngestSuccess,
        MockIngestRouterService,
    };
    use quickwit_search::MockSearchService;

    use crate::elasticsearch_api::tests::create_elasticsearch_test_server_with_router;

    #[tokio::test]
    async fn test_bulk_api_returns_404_if_index_id_does_not_exist() {
        let mut mock_ingest_router = MockIngestRouterService::new();
        mock_ingest_router.expect_ingest().return_once(|_| {
            Ok(IngestResponseV2 {
                successes: vec![],
                failures: vec![IngestFailure {
                    subrequest_id: 0,
                    index_id: "index-does-not-exist".to_string(),
                    source_id: "_doc".to_string(),
                    reason: quickwit_proto::ingest::router::IngestFailureReason::IndexNotFound
                        as i32,
                }],
            })
        });

        let ingest_router = quickwit_proto::ingest::router::IngestRouterServiceClient::from_mock(
            mock_ingest_router,
        );
        let server =
            create_elasticsearch_test_server_with_router(MockSearchService::new(), ingest_router)
                .await;

        let response = server
            .post("/_elastic/_bulk")
            .text(
                r#"{"index": {"_index": "index-does-not-exist"}}
{"field": "value1"}"#,
            )
            .await;

        assert_eq!(response.status_code(), 200);
        let response_json: serde_json::Value = response.json();
        assert!(response_json["errors"].as_bool().unwrap_or(false));
        assert_eq!(response_json["items"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_bulk_api_returns_200() {
        let mut mock_ingest_router = MockIngestRouterService::new();
        mock_ingest_router.expect_ingest().return_once(|_| {
            Ok(IngestResponseV2 {
                successes: vec![IngestSuccess {
                    subrequest_id: 0,
                    index_uid: Some(quickwit_proto::types::IndexUid::for_test("my-index-1", 0)),
                    source_id: "_doc".to_string(),
                    shard_id: Some("shard_01".into()),
                    replication_position_inclusive: Some(quickwit_proto::types::Position::offset(
                        0u64,
                    )),
                    num_ingested_docs: 1,
                    parse_failures: vec![],
                }],
                failures: vec![],
            })
        });

        let ingest_router = quickwit_proto::ingest::router::IngestRouterServiceClient::from_mock(
            mock_ingest_router,
        );
        let server =
            create_elasticsearch_test_server_with_router(MockSearchService::new(), ingest_router)
                .await;

        let response = server
            .post("/_elastic/_bulk")
            .text(
                r#"{"index": {"_index": "my-index-1"}}
{"field": "value1"}"#,
            )
            .await;

        assert_eq!(response.status_code(), 200);
        let response_json: serde_json::Value = response.json();
        assert!(!response_json["errors"].as_bool().unwrap_or(true));
        assert_eq!(response_json["items"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_bulk_api_returns_200_if_payload_has_blank_lines() {
        let mut mock_ingest_router = MockIngestRouterService::new();
        mock_ingest_router.expect_ingest().return_once(|_| {
            Ok(IngestResponseV2 {
                successes: vec![IngestSuccess {
                    subrequest_id: 0,
                    index_uid: Some(quickwit_proto::types::IndexUid::for_test("my-index-1", 0)),
                    source_id: "_doc".to_string(),
                    shard_id: Some("shard_01".into()),
                    replication_position_inclusive: Some(quickwit_proto::types::Position::offset(
                        0u64,
                    )),
                    num_ingested_docs: 1,
                    parse_failures: vec![],
                }],
                failures: vec![],
            })
        });

        let ingest_router = quickwit_proto::ingest::router::IngestRouterServiceClient::from_mock(
            mock_ingest_router,
        );
        let server =
            create_elasticsearch_test_server_with_router(MockSearchService::new(), ingest_router)
                .await;

        let response = server
            .post("/_elastic/_bulk")
            .text(
                r#"
{"index": {"_index": "my-index-1"}}

{"field": "value1"}

"#,
            )
            .await;

        assert_eq!(response.status_code(), 200);
        let response_json: serde_json::Value = response.json();
        assert!(!response_json["errors"].as_bool().unwrap_or(true));
        assert_eq!(response_json["items"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_bulk_index_api_returns_200() {
        let mut mock_ingest_router = MockIngestRouterService::new();
        mock_ingest_router.expect_ingest().return_once(|_| {
            Ok(IngestResponseV2 {
                successes: vec![IngestSuccess {
                    subrequest_id: 0,
                    index_uid: Some(quickwit_proto::types::IndexUid::for_test("my-index-1", 0)),
                    source_id: "_doc".to_string(),
                    shard_id: Some("shard_01".into()),
                    replication_position_inclusive: Some(quickwit_proto::types::Position::offset(
                        0u64,
                    )),
                    num_ingested_docs: 1,
                    parse_failures: vec![],
                }],
                failures: vec![],
            })
        });

        let ingest_router = quickwit_proto::ingest::router::IngestRouterServiceClient::from_mock(
            mock_ingest_router,
        );
        let server =
            create_elasticsearch_test_server_with_router(MockSearchService::new(), ingest_router)
                .await;

        let response = server
            .post("/_elastic/my-index-1/_bulk") // Index-specific bulk endpoint
            .text(
                r#"{"index": {}}
{"field": "value1"}"#,
            )
            .await;

        assert_eq!(response.status_code(), 200);
        let response_json: serde_json::Value = response.json();
        assert!(!response_json["errors"].as_bool().unwrap_or(true));
        assert_eq!(response_json["items"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_bulk_api_blocks_when_refresh_wait_for_is_specified() {
        let mut mock_ingest_router = MockIngestRouterService::new();
        mock_ingest_router
            .expect_ingest()
            .return_once(|request: IngestV2Request| {
                // Verify that the commit type is WaitFor when refresh=wait_for is specified
                assert_eq!(request.commit_type(), CommitTypeV2::WaitFor);
                Ok(IngestResponseV2 {
                    successes: vec![IngestSuccess {
                        subrequest_id: 0,
                        index_uid: Some(quickwit_proto::types::IndexUid::for_test("my-index-1", 0)),
                        source_id: "_doc".to_string(),
                        shard_id: Some("shard_01".into()),
                        replication_position_inclusive: Some(
                            quickwit_proto::types::Position::offset(0u64),
                        ),
                        num_ingested_docs: 1,
                        parse_failures: vec![],
                    }],
                    failures: vec![],
                })
            });

        let ingest_router = quickwit_proto::ingest::router::IngestRouterServiceClient::from_mock(
            mock_ingest_router,
        );
        let server =
            create_elasticsearch_test_server_with_router(MockSearchService::new(), ingest_router)
                .await;

        let response = server
            .post("/_elastic/_bulk?refresh=wait_for")
            .text(
                r#"{"index": {"_index": "my-index-1"}}
{"field": "value1"}"#,
            )
            .await;

        assert_eq!(response.status_code(), 200);
    }

    #[tokio::test]
    async fn test_bulk_api_blocks_when_refresh_true_is_specified() {
        let mut mock_ingest_router = MockIngestRouterService::new();
        mock_ingest_router
            .expect_ingest()
            .return_once(|request: IngestV2Request| {
                // Verify that the commit type is Force when refresh=true is specified
                assert_eq!(request.commit_type(), CommitTypeV2::Force);
                Ok(IngestResponseV2 {
                    successes: vec![IngestSuccess {
                        subrequest_id: 0,
                        index_uid: Some(quickwit_proto::types::IndexUid::for_test("my-index-1", 0)),
                        source_id: "_doc".to_string(),
                        shard_id: Some("shard_01".into()),
                        replication_position_inclusive: Some(
                            quickwit_proto::types::Position::offset(0u64),
                        ),
                        num_ingested_docs: 1,
                        parse_failures: vec![],
                    }],
                    failures: vec![],
                })
            });

        let ingest_router = quickwit_proto::ingest::router::IngestRouterServiceClient::from_mock(
            mock_ingest_router,
        );
        let server =
            create_elasticsearch_test_server_with_router(MockSearchService::new(), ingest_router)
                .await;

        let response = server
            .post("/_elastic/_bulk?refresh=true")
            .text(
                r#"{"index": {"_index": "my-index-1"}}
{"field": "value1"}"#,
            )
            .await;

        assert_eq!(response.status_code(), 200);
    }

    #[tokio::test]
    async fn test_bulk_ingest_request_returns_400_if_action_is_malformed() {
        let ingest_router = quickwit_proto::ingest::router::IngestRouterServiceClient::mocked();
        let server =
            create_elasticsearch_test_server_with_router(MockSearchService::new(), ingest_router)
                .await;

        let response = server
            .post("/_elastic/_bulk")
            .text(
                r#"{"invalid_action": {"_index": "my-index-1"}}
{"field": "value1"}"#,
            )
            .await;

        assert_eq!(response.status_code(), 400);
        let response_json: serde_json::Value = response.json();
        assert!(
            response_json["error"]["reason"]
                .as_str()
                .unwrap()
                .contains("invalid_action")
        );
    }
}
