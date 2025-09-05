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
use warp::{Filter, Rejection};

use super::RestIngestResponse;
use crate::decompression::get_body_bytes;
use crate::format::extract_format_from_qs;
use crate::rest_api_response::into_rest_api_response;
use crate::{Body, BodyFormat, with_arg};

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

pub(crate) fn ingest_api_handlers(
    ingest_router: IngestRouterServiceClient,
    ingest_service: IngestServiceClient,
    config: IngestApiConfig,
    enable_ingest_v1: bool,
    enable_ingest_v2: bool,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    ingest_handler(
        ingest_router,
        ingest_service.clone(),
        config,
        enable_ingest_v1,
        enable_ingest_v2,
    )
    .or(tail_handler(ingest_service))
    .boxed()
}

fn ingest_filter(
    config: IngestApiConfig,
) -> impl Filter<Extract = (String, Body, IngestOptions), Error = Rejection> + Clone {
    warp::path!(String / "ingest")
        .and(warp::post())
        .and(warp::body::content_length_limit(
            config.content_length_limit.as_u64(),
        ))
        .and(get_body_bytes())
        .and(warp::query::<IngestOptions>())
}

fn ingest_handler(
    ingest_router: IngestRouterServiceClient,
    ingest_service: IngestServiceClient,
    config: IngestApiConfig,
    enable_ingest_v1: bool,
    enable_ingest_v2: bool,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    ingest_filter(config)
        .and(with_arg(ingest_router))
        .and(with_arg(ingest_service))
        .then(
            move |index_id, body, ingest_options, ingest_router, ingest_service| {
                ingest(
                    index_id,
                    body,
                    ingest_options,
                    ingest_router,
                    ingest_service,
                    enable_ingest_v1,
                    enable_ingest_v2,
                )
            },
        )
        .map(|result| into_rest_api_response(result, BodyFormat::default()))
        .boxed()
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
    body: Body,
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
    body: Body,
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
    let mut doc_batch_builder = DocBatchBuilder::with_capacity(index_id, body.content.remaining());
    for line in lines(&body.content) {
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
    body: Body,
    ingest_options: IngestOptions,
    ingest_router: IngestRouterServiceClient,
) -> Result<RestIngestResponse, IngestServiceError> {
    let mut doc_batch_builder = DocBatchV2Builder::default();
    let mut doc_uid_generator = DocUidGenerator::default();

    for doc in lines(&body.content) {
        doc_batch_builder.add_doc(doc_uid_generator.next_doc_uid(), doc);
    }
    drop(body);
    let doc_batch_opt = doc_batch_builder.build();

    let Some(doc_batch) = doc_batch_opt else {
        let response = RestIngestResponse::default();
        return Ok(response);
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

pub fn tail_handler(
    ingest_service: IngestServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    tail_filter()
        .and(with_arg(ingest_service))
        .then(tail_endpoint)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
        .boxed()
}

fn tail_filter() -> impl Filter<Extract = (String,), Error = Rejection> + Clone {
    warp::path!(String / "tail").and(warp::get())
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

    use bytes::Bytes;
    use quickwit_actors::{Mailbox, Universe};
    use quickwit_config::IngestApiConfig;
    use quickwit_ingest::{
        CreateQueueIfNotExistsRequest, FetchRequest, FetchResponse, IngestApiService,
        IngestServiceClient, QUEUES_DIR_NAME, SuggestTruncateRequest, init_ingest_api,
    };
    use quickwit_proto::ingest::router::IngestRouterServiceClient;

    use super::{RestIngestResponse, ingest_api_handlers};
    use crate::ingest_api::lines;

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
        let ingest_api_handlers = ingest_api_handlers(
            ingest_router,
            ingest_service,
            IngestApiConfig::default(),
            true,
            false,
        );
        let resp = warp::test::request()
            .path("/my-index/ingest")
            .method("POST")
            .json(&true)
            .body(r#"{"id": 1, "message": "push"}"#)
            .reply(&ingest_api_handlers)
            .await;
        assert_eq!(resp.status(), 200);
        let ingest_response: RestIngestResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(ingest_response.num_docs_for_processing, 1);

        let resp = warp::test::request()
            .path("/my-index/tail")
            .method("GET")
            .reply(&ingest_api_handlers)
            .await;
        assert_eq!(resp.status(), 200);
        let fetch_response: FetchResponse = serde_json::from_slice(resp.body()).unwrap();
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
        let ingest_api_handlers = ingest_api_handlers(
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
        let resp = warp::test::request()
            .path("/my-index/ingest")
            .method("POST")
            .body(payload)
            .reply(&ingest_api_handlers)
            .await;
        assert_eq!(resp.status(), 200);
        let ingest_response: RestIngestResponse = serde_json::from_slice(resp.body()).unwrap();
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
        let ingest_api_handlers = ingest_api_handlers(
            ingest_router,
            ingest_service,
            IngestApiConfig::default(),
            true,
            false,
        );
        let resp = warp::test::request()
            .path("/my-index/ingest")
            .method("POST")
            .json(&true)
            .body(r#"{"id": 1, "message": "push"}"#)
            .reply(&ingest_api_handlers)
            .await;
        assert_eq!(resp.status(), 429);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_ingest_api_return_413_if_above_content_limit() {
        let config: IngestApiConfig =
            serde_json::from_str(r#"{ "content_length_limit": "1" }"#).unwrap();
        let (universe, _temp_dir, ingest_service, _) =
            setup_ingest_v1_service(&["my-index"], &IngestApiConfig::default()).await;
        let ingest_router = IngestRouterServiceClient::mocked();
        let ingest_api_handlers =
            ingest_api_handlers(ingest_router, ingest_service, config.clone(), true, false);
        let resp = warp::test::request()
            .path("/my-index/ingest")
            .method("POST")
            .json(&true)
            .body(r#"{"id": 1, "message": "push"}"#)
            .reply(&ingest_api_handlers)
            .await;
        assert_eq!(resp.status(), 413);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_ingest_api_blocks_when_wait_is_specified() {
        let (universe, _temp_dir, ingest_service_client, ingest_service_mailbox) =
            setup_ingest_v1_service(&["my-index"], &IngestApiConfig::default()).await;
        let ingest_router = IngestRouterServiceClient::mocked();
        let ingest_api_handlers = ingest_api_handlers(
            ingest_router,
            ingest_service_client,
            IngestApiConfig::default(),
            true,
            false,
        );
        let handle = tokio::spawn(async move {
            let resp = warp::test::request()
                .path("/my-index/ingest?commit=wait_for")
                .method("POST")
                .json(&true)
                .body(r#"{"id": 1, "message": "push"}"#)
                .reply(&ingest_api_handlers)
                .await;
            assert_eq!(resp.status(), 200);
            let ingest_response: RestIngestResponse = serde_json::from_slice(resp.body()).unwrap();
            assert_eq!(ingest_response.num_docs_for_processing, 1);
        });
        universe.sleep(Duration::from_secs(10)).await;
        assert!(!handle.is_finished());
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
        ingest_service_mailbox
            .ask_for_res(SuggestTruncateRequest {
                index_id: "my-index".to_string(),
                up_to_position_included: 0,
            })
            .await
            .unwrap();
        handle.await.unwrap();
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_ingest_api_blocks_when_force_is_specified() {
        let (universe, _temp_dir, ingest_service_client, ingest_service_mailbox) =
            setup_ingest_v1_service(&["my-index"], &IngestApiConfig::default()).await;
        let ingest_router = IngestRouterServiceClient::mocked();
        let ingest_api_handlers = ingest_api_handlers(
            ingest_router,
            ingest_service_client,
            IngestApiConfig::default(),
            true,
            false,
        );
        let handle = tokio::spawn(async move {
            let resp = warp::test::request()
                .path("/my-index/ingest?commit=force")
                .method("POST")
                .json(&true)
                .body(r#"{"id": 1, "message": "push"}"#)
                .reply(&ingest_api_handlers)
                .await;
            assert_eq!(resp.status(), 200);
            let ingest_response: RestIngestResponse = serde_json::from_slice(resp.body()).unwrap();
            assert_eq!(ingest_response.num_docs_for_processing, 1);
        });
        universe.sleep(Duration::from_secs(10)).await;
        assert!(!handle.is_finished());
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
        ingest_service_mailbox
            .ask_for_res(SuggestTruncateRequest {
                index_id: "my-index".to_string(),
                up_to_position_included: 0,
            })
            .await
            .unwrap();
        handle.await.unwrap();
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_ingest_api_unsupported_detailed_errors() {
        let (universe, _temp_dir, ingest_service, _) =
            setup_ingest_v1_service(&["my-index"], &IngestApiConfig::default()).await;
        let ingest_router = IngestRouterServiceClient::mocked();
        let ingest_api_handlers = ingest_api_handlers(
            ingest_router,
            ingest_service,
            IngestApiConfig::default(),
            true,
            false,
        );
        let resp = warp::test::request()
            .path("/my-index/ingest?detailed_response=true")
            .method("POST")
            .json(&true)
            .body(r#"{"id": 1, "message": "push"}"#)
            .reply(&ingest_api_handlers)
            .await;
        assert_eq!(resp.status(), 400);
        universe.assert_quit().await;
    }
}
