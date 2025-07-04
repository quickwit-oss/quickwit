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

use axum::extract::{DefaultBodyLimit, Path, Query};
use axum::routing::{get, post};
use axum::{Extension, Router};
use bytes::{Buf, Bytes};
use quickwit_config::{INGEST_V2_SOURCE_ID, IngestApiConfig, validate_identifier};
use quickwit_ingest::{
    CommitType, DocBatchBuilder, DocBatchV2Builder, FetchResponse, IngestRequest, IngestService,
    IngestServiceClient, IngestServiceError, TailRequest,
};
use quickwit_proto::ingest::CommitTypeV2;
use quickwit_proto::ingest::router::{
    IngestRequestV2, IngestRouterService, IngestRouterServiceClient, IngestSubrequest,
};
use quickwit_proto::types::{DocUidGenerator, IndexId};
use serde::Deserialize;

use super::RestIngestResponse;
use crate::BodyFormat;
use crate::decompression::DecompressedBody;
use crate::rest_api_response::into_rest_api_response;

// Wrapper types to distinguish between the two boolean Extension values
#[derive(Clone, Copy)]
struct EnableIngestV1(bool);

#[derive(Clone, Copy)]
struct EnableIngestV2(bool);

#[derive(utoipa::OpenApi)]
#[openapi(paths(ingest, tail_endpoint,))]
pub struct IngestApi;

#[derive(utoipa::OpenApi)]
#[openapi(components(schemas(
    quickwit_ingest::DocBatch,
    quickwit_ingest::FetchResponse,
    quickwit_ingest::IngestResponse,
    quickwit_ingest::CommitType,
)))]
pub struct IngestApiSchemas;

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct IngestOptions {
    #[serde(alias = "commit", default = "IngestOptions::default_commit_type")]
    commit_type: CommitTypeV2,
    #[serde(default)]
    use_legacy_ingest: bool,
    #[serde(default)]
    detailed_response: bool,
}

impl IngestOptions {
    // This default implementation is necessary because `CommitTypeV2::default()` is
    // `CommitTypeV2::Unspecified`.
    fn default_commit_type() -> CommitTypeV2 {
        CommitTypeV2::Auto
    }

    fn commit_type_v1(&self) -> CommitType {
        match self.commit_type {
            CommitTypeV2::Unspecified | CommitTypeV2::Auto => CommitType::Auto,
            CommitTypeV2::Force => CommitType::Force,
            CommitTypeV2::WaitFor => CommitType::WaitFor,
        }
    }
}

// Axum routes
pub fn ingest_routes(
    ingest_router: IngestRouterServiceClient,
    ingest_service: IngestServiceClient,
    config: IngestApiConfig,
    enable_ingest_v1: bool,
    enable_ingest_v2: bool,
) -> Router {
    let mut router = Router::new().route("/:index_id/tail", get(tail_handler_axum));

    // Apply content length limit to ingest route
    let ingest_route = post(ingest_handler_axum).layer(DefaultBodyLimit::max(
        config.content_length_limit.as_u64() as usize,
    ));

    router = router.route("/:index_id/ingest", ingest_route);

    router
        .layer(axum::Extension(ingest_router))
        .layer(axum::Extension(ingest_service))
        .layer(axum::Extension(config))
        .layer(axum::Extension(EnableIngestV1(enable_ingest_v1)))
        .layer(axum::Extension(EnableIngestV2(enable_ingest_v2)))
}

async fn ingest_handler_axum(
    Path(index_id): Path<String>,
    Query(ingest_options): Query<IngestOptions>,
    Extension(ingest_router): Extension<IngestRouterServiceClient>,
    Extension(ingest_service): Extension<IngestServiceClient>,
    Extension(EnableIngestV1(enable_ingest_v1)): Extension<EnableIngestV1>,
    Extension(EnableIngestV2(enable_ingest_v2)): Extension<EnableIngestV2>,
    body: DecompressedBody,
) -> impl axum::response::IntoResponse {
    let result = ingest(
        index_id,
        body.0,
        ingest_options,
        ingest_router,
        ingest_service,
        enable_ingest_v1,
        enable_ingest_v2,
    )
    .await;

    into_rest_api_response(result, BodyFormat::default())
}

async fn tail_handler_axum(
    Path(index_id): Path<String>,
    Extension(ingest_service): Extension<IngestServiceClient>,
) -> impl axum::response::IntoResponse {
    let result = tail_endpoint(index_id, ingest_service).await;
    into_rest_api_response(result, BodyFormat::default())
}

#[utoipa::path(
    post,
    tag = "Ingest",
    path = "/{index_id}/ingest",
    request_body(content = String, description = "Documents to ingest in NDJSON format and limited to 10MB", content_type = "application/json"),
    responses(
        (status = 200, description = "Successfully ingested documents.", body = RestIngestResponse)
    ),
    params(
        ("index_id" = String, Path, description = "The index ID to add docs to."),
        ("commit" = Option<CommitType>, Query, description = "Force or wait for commit at the end of the indexing operation."),
    )
)]
/// Ingest documents
async fn ingest(
    index_id: IndexId,
    body: Bytes,
    ingest_options: IngestOptions,
    ingest_router: IngestRouterServiceClient,
    ingest_service: IngestServiceClient,
    enable_ingest_v1: bool,
    enable_ingest_v2: bool,
) -> Result<RestIngestResponse, IngestServiceError> {
    if enable_ingest_v2 && !ingest_options.use_legacy_ingest {
        return ingest_v2(index_id, body, ingest_options, ingest_router).await;
    }
    if !enable_ingest_v1 {
        let message = "ingest v1 is disabled: environment variable `QW_DISABLE_INGEST_V1` is set";
        return Err(IngestServiceError::Internal(message.to_string()));
    }
    ingest_v1(index_id, body, ingest_options, ingest_service).await
}

/// Ingest documents
async fn ingest_v1(
    index_id: IndexId,
    body: Bytes,
    ingest_options: IngestOptions,
    ingest_service: IngestServiceClient,
) -> Result<RestIngestResponse, IngestServiceError> {
    if ingest_options.detailed_response {
        return Err(IngestServiceError::BadRequest(
            "detailed_response is not supported in ingest v1".to_string(),
        ));
    }
    // The size of the body should be an upper bound of the size of the batch. The removal of the
    // end of line character for each doc compensates the addition of the `DocCommand` header.
    let mut doc_batch_builder = DocBatchBuilder::with_capacity(index_id, body.remaining());
    for line in lines(&body) {
        doc_batch_builder.ingest_doc(line);
    }
    let ingest_req = IngestRequest {
        doc_batches: vec![doc_batch_builder.build()],
        commit: ingest_options.commit_type_v1() as i32,
    };
    let ingest_response = ingest_service.ingest(ingest_req).await?;
    Ok(RestIngestResponse::from_ingest_v1(ingest_response))
}

async fn ingest_v2(
    index_id: IndexId,
    body: Bytes,
    ingest_options: IngestOptions,
    ingest_router: IngestRouterServiceClient,
) -> Result<RestIngestResponse, IngestServiceError> {
    let mut doc_batch_builder = DocBatchV2Builder::default();
    let mut doc_uid_generator = DocUidGenerator::default();

    for line in lines(&body) {
        if is_empty_or_blank_line(line) {
            continue;
        }
        let doc_uid = doc_uid_generator.next_doc_uid();
        doc_batch_builder.add_doc(doc_uid, line);
    }
    drop(body);
    let doc_batch_opt = doc_batch_builder.build();
    let Some(doc_batch) = doc_batch_opt else {
        return Ok(RestIngestResponse::default());
    };
    let num_docs_for_processing = doc_batch.num_docs() as u64;
    let doc_batch_clone_opt = if ingest_options.detailed_response {
        Some(doc_batch.clone())
    } else {
        None
    };

    // Validate index ID early because propagating back the right error (400)
    // from deeper ingest layers is harder
    if validate_identifier("", &index_id).is_err() {
        return Err(IngestServiceError::BadRequest(
            "invalid index ID".to_string(),
        ));
    }

    let subrequest = IngestSubrequest {
        subrequest_id: 0,
        index_id,
        source_id: INGEST_V2_SOURCE_ID.to_string(),
        doc_batch: Some(doc_batch),
    };
    let request = IngestRequestV2 {
        commit_type: ingest_options.commit_type as i32,
        subrequests: vec![subrequest],
    };
    let response = ingest_router.ingest(request).await?;
    RestIngestResponse::from_ingest_v2(
        response,
        doc_batch_clone_opt.as_ref(),
        num_docs_for_processing,
    )
}

#[utoipa::path(
    get,
    tag = "Ingest",
    path = "/{index_id}/tail",
    responses(
        (status = 200, description = "Successfully fetched documents.", body = FetchResponse)
    ),
    params(
        ("index_id" = String, Path, description = "The index ID to tail."),
    )
)]
/// Returns the last few ingested documents.
async fn tail_endpoint(
    index_id: IndexId,
    ingest_service: IngestServiceClient,
) -> Result<FetchResponse, IngestServiceError> {
    let fetch_response = ingest_service.tail(TailRequest { index_id }).await?;
    Ok(fetch_response)
}

pub(crate) fn lines(body: &Bytes) -> impl Iterator<Item = &[u8]> {
    body.split(|byte| byte == &b'\n')
        .filter(|line| !is_empty_or_blank_line(line))
}

#[inline]
fn is_empty_or_blank_line(line: &[u8]) -> bool {
    line.is_empty() || line.iter().all(|ch| ch.is_ascii_whitespace())
}

#[cfg(test)]
pub(crate) mod tests {
    use std::str;
    use std::time::Duration;

    use axum_test::TestServer;
    use bytes::Bytes;
    use http::StatusCode;
    use quickwit_actors::{Mailbox, Universe};
    use quickwit_config::IngestApiConfig;
    use quickwit_ingest::{
        CreateQueueIfNotExistsRequest, FetchRequest, FetchResponse, IngestApiService,
        IngestServiceClient, QUEUES_DIR_NAME, SuggestTruncateRequest, init_ingest_api,
    };
    use quickwit_proto::ingest::router::IngestRouterServiceClient;

    use super::{RestIngestResponse, ingest_routes};
    use crate::ingest_api::lines;

    fn create_test_server(
        ingest_router: quickwit_proto::ingest::router::IngestRouterServiceClient,
        ingest_service: IngestServiceClient,
        config: IngestApiConfig,
        enable_ingest_v1: bool,
        enable_ingest_v2: bool,
    ) -> TestServer {
        let app = ingest_routes(
            ingest_router,
            ingest_service,
            config,
            enable_ingest_v1,
            enable_ingest_v2,
        );
        TestServer::new(app).unwrap()
    }

    #[test]
    fn test_process_lines() {
        let test_cases = [
            // an empty line is inserted before the metadata action and the doc
            (&b"\n{ \"create\" : { \"_index\" : \"my-index-1\", \"_id\" : \"1\"} }\n{\"id\": 1, \"message\": \"push\"}"[..], 2),
            // a blank line is inserted before the metadata action and the doc
            (&b"       \n{ \"create\" : { \"_index\" : \"my-index-1\", \"_id\" : \"1\"} }\n{\"id\": 1, \"message\": \"push\"}"[..], 2),
            // an empty line is inserted after the metadata action and before the doc
            (&b"{ \"create\" : { \"_index\" : \"my-index-1\", \"_id\" : \"1\"} }\n\n{\"id\": 1, \"message\": \"push\"}"[..], 2),
            // a blank line is inserted after the metadata action and before the doc
            (&b"{ \"create\" : { \"_index\" : \"my-index-1\", \"_id\" : \"1\"} }\n     \n{\"id\": 1, \"message\": \"push\"}"[..], 2),
        ];

        for &(input, expected_count) in &test_cases {
            assert_eq!(lines(&Bytes::from(input)).count(), expected_count);
        }
    }

    pub(crate) async fn setup_ingest_v1_service(
        queues: &[&str],
        config: &IngestApiConfig,
    ) -> (
        Universe,
        tempfile::TempDir,
        IngestServiceClient,
        Mailbox<IngestApiService>,
    ) {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();
        let queues_dir_path = temp_dir.path().join(QUEUES_DIR_NAME);
        let ingest_service_mailbox = init_ingest_api(&universe, &queues_dir_path, config)
            .await
            .unwrap();
        for queue in queues {
            let create_queue_req = CreateQueueIfNotExistsRequest {
                queue_id: queue.to_string(),
            };
            ingest_service_mailbox
                .ask_for_res(create_queue_req)
                .await
                .unwrap();
        }
        let ingest_service = IngestServiceClient::from_mailbox(ingest_service_mailbox.clone());
        (universe, temp_dir, ingest_service, ingest_service_mailbox)
    }

    #[tokio::test]
    async fn test_ingest_api_returns_200_when_ingest_json_and_fetch() {
        let (universe, _temp_dir, ingest_service, _) =
            setup_ingest_v1_service(&["my-index"], &IngestApiConfig::default()).await;
        let ingest_router = IngestRouterServiceClient::mocked();
        let server = create_test_server(
            ingest_router,
            ingest_service,
            IngestApiConfig::default(),
            true,
            false,
        );

        let response = server
            .post("/my-index/ingest")
            .json(&serde_json::json!({"id": 1, "message": "push"}))
            .await;
        assert_eq!(response.status_code(), StatusCode::OK);
        let ingest_response: RestIngestResponse = response.json();
        assert_eq!(ingest_response.num_docs_for_processing, 1);

        let response = server.get("/my-index/tail").await;
        assert_eq!(response.status_code(), StatusCode::OK);
        let fetch_response: FetchResponse = response.json();
        let doc_batch = fetch_response.doc_batch.unwrap();
        assert_eq!(doc_batch.index_id, "my-index");
        assert_eq!(doc_batch.num_docs(), 1);
        assert_eq!(
            doc_batch.doc_lengths.iter().sum::<u32>() as usize,
            doc_batch.doc_buffer.len()
        );

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_ingest_api_returns_200_when_ingest_ndjson_and_fetch() {
        let (universe, _temp_dir, ingest_service, _) =
            setup_ingest_v1_service(&["my-index"], &IngestApiConfig::default()).await;
        let ingest_router = IngestRouterServiceClient::mocked();
        let server = create_test_server(
            ingest_router,
            ingest_service,
            IngestApiConfig::default(),
            true,
            false,
        );
        let payload = r#"
            {"id": 1, "message": "push"}
            {"id": 2, "message": "push"}
            {"id": 3, "message": "push"}"#;
        let response = server.post("/my-index/ingest").text(payload).await;
        assert_eq!(response.status_code(), StatusCode::OK);
        let ingest_response: RestIngestResponse = response.json();
        assert_eq!(ingest_response.num_docs_for_processing, 3);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_ingest_api_return_429_if_above_limits() {
        let config: IngestApiConfig =
            serde_json::from_str(r#"{ "max_queue_memory_usage": "1" }"#).unwrap();
        let (universe, _temp_dir, ingest_service, _) =
            setup_ingest_v1_service(&["my-index"], &config).await;
        let ingest_router = IngestRouterServiceClient::mocked();
        let server = create_test_server(
            ingest_router,
            ingest_service,
            IngestApiConfig::default(),
            true,
            false,
        );
        let response = server
            .post("/my-index/ingest")
            .text(r#"{"id": 1, "message": "push"}"#)
            .await;
        assert_eq!(response.status_code(), StatusCode::TOO_MANY_REQUESTS);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_ingest_api_return_413_if_above_content_limit() {
        let config: IngestApiConfig =
            serde_json::from_str(r#"{ "content_length_limit": "1" }"#).unwrap();
        let (universe, _temp_dir, ingest_service, _) =
            setup_ingest_v1_service(&["my-index"], &IngestApiConfig::default()).await;
        let ingest_router = IngestRouterServiceClient::mocked();
        let server = create_test_server(ingest_router, ingest_service, config.clone(), true, false);
        let response = server
            .post("/my-index/ingest")
            .text(r#"{"id": 1, "message": "push"}"#)
            .await;
        assert_eq!(response.status_code(), StatusCode::PAYLOAD_TOO_LARGE);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_ingest_api_blocks_when_wait_is_specified() {
        let (universe, _temp_dir, ingest_service_client, ingest_service_mailbox) =
            setup_ingest_v1_service(&["my-index"], &IngestApiConfig::default()).await;
        let ingest_router = IngestRouterServiceClient::mocked();
        let server = create_test_server(
            ingest_router,
            ingest_service_client,
            IngestApiConfig::default(),
            true,
            false,
        );

        // Start the request in the background
        let request_future = server
            .post("/my-index/ingest?commit=wait_for")
            .text(r#"{"id": 1, "message": "push"}"#);

        // Use tokio::select! to test that the request blocks
        let result = tokio::select! {
            _response = request_future => {
                panic!("Request should have blocked but completed immediately");
            }
            _ = universe.sleep(Duration::from_secs(1)) => {
                // Expected: request should still be blocking after 1 second
                "blocked"
            }
        };

        assert_eq!(result, "blocked");

        // Verify data was ingested
        assert_eq!(
            ingest_service_mailbox
                .ask_for_res(FetchRequest {
                    index_id: "my-index".to_string(),
                    start_after: None,
                    num_bytes_limit: None,
                })
                .await
                .unwrap()
                .doc_batch
                .unwrap()
                .num_docs(),
            1
        );

        // Complete the commit
        ingest_service_mailbox
            .ask_for_res(SuggestTruncateRequest {
                index_id: "my-index".to_string(),
                up_to_position_included: 0,
            })
            .await
            .unwrap();

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_ingest_api_blocks_when_force_is_specified() {
        let (universe, _temp_dir, ingest_service_client, ingest_service_mailbox) =
            setup_ingest_v1_service(&["my-index"], &IngestApiConfig::default()).await;
        let ingest_router = IngestRouterServiceClient::mocked();
        let server = create_test_server(
            ingest_router,
            ingest_service_client,
            IngestApiConfig::default(),
            true,
            false,
        );

        // Start the request in the background
        let request_future = server
            .post("/my-index/ingest?commit=force")
            .text(r#"{"id": 1, "message": "push"}"#);

        // Use tokio::select! to test that the request blocks
        let result = tokio::select! {
            _response = request_future => {
                panic!("Request should have blocked but completed immediately");
            }
            _ = universe.sleep(Duration::from_secs(1)) => {
                // Expected: request should still be blocking after 1 second
                "blocked"
            }
        };

        assert_eq!(result, "blocked");

        // Verify data was ingested (force should ingest 2 docs)
        assert_eq!(
            ingest_service_mailbox
                .ask_for_res(FetchRequest {
                    index_id: "my-index".to_string(),
                    start_after: None,
                    num_bytes_limit: None,
                })
                .await
                .unwrap()
                .doc_batch
                .unwrap()
                .num_docs(),
            2
        );

        // Complete the commit
        ingest_service_mailbox
            .ask_for_res(SuggestTruncateRequest {
                index_id: "my-index".to_string(),
                up_to_position_included: 0,
            })
            .await
            .unwrap();

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_ingest_api_unsupported_detailed_errors() {
        let (universe, _temp_dir, ingest_service, _) =
            setup_ingest_v1_service(&["my-index"], &IngestApiConfig::default()).await;
        let ingest_router = IngestRouterServiceClient::mocked();
        let server = create_test_server(
            ingest_router,
            ingest_service,
            IngestApiConfig::default(),
            true,
            false,
        );
        let response = server
            .post("/my-index/ingest?detailed_response=true")
            .text(r#"{"id": 1, "message": "push"}"#)
            .await;
        assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
        universe.assert_quit().await;
    }
}
