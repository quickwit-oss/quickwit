// Copyright (C) 2024 Quickwit, Inc.
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

use bytes::{Buf, Bytes};
use quickwit_config::{disable_ingest_v1, IngestApiConfig, INGEST_V2_SOURCE_ID};
use quickwit_ingest::{
    CommitType, DocBatchBuilder, DocBatchV2Builder, FetchResponse, IngestRequest, IngestResponse,
    IngestService, IngestServiceClient, IngestServiceError, TailRequest,
};
use quickwit_proto::ingest::router::{
    IngestFailureReason, IngestRequestV2, IngestResponseV2, IngestRouterService,
    IngestRouterServiceClient, IngestSubrequest,
};
use quickwit_proto::types::{DocUidGenerator, IndexId};
use serde::Deserialize;
use warp::{Filter, Rejection};

use crate::decompression::get_body_bytes;
use crate::format::extract_format_from_qs;
use crate::rest_api_response::into_rest_api_response;
use crate::{with_arg, Body, BodyFormat};

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

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
struct IngestOptions {
    #[serde(alias = "commit")]
    #[serde(default)]
    commit_type: CommitType,
}

pub(crate) fn ingest_api_handlers(
    ingest_router: IngestRouterServiceClient,
    ingest_service: IngestServiceClient,
    config: IngestApiConfig,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    ingest_handler(ingest_service.clone(), config.clone())
        .or(tail_handler(ingest_service))
        .or(ingest_v2_handler(ingest_router, config))
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
        .and(serde_qs::warp::query::<IngestOptions>(
            serde_qs::Config::default(),
        ))
}

fn ingest_handler(
    ingest_service: IngestServiceClient,
    config: IngestApiConfig,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    ingest_filter(config)
        .and(with_arg(ingest_service))
        .then(ingest)
        .map(|result| into_rest_api_response(result, BodyFormat::default()))
}

fn ingest_v2_filter(
    config: IngestApiConfig,
) -> impl Filter<Extract = (String, Body, IngestOptions), Error = Rejection> + Clone {
    warp::path!(String / "ingest-v2")
        .and(warp::post())
        .and(warp::body::content_length_limit(
            config.content_length_limit.as_u64(),
        ))
        .and(get_body_bytes())
        .and(serde_qs::warp::query::<IngestOptions>(
            serde_qs::Config::default(),
        ))
}

fn ingest_v2_handler(
    ingest_router: IngestRouterServiceClient,
    config: IngestApiConfig,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    ingest_v2_filter(config)
        .and(with_arg(ingest_router))
        .then(ingest_v2)
        .and(with_arg(BodyFormat::default()))
        .map(into_rest_api_response)
}

async fn ingest_v2(
    index_id: IndexId,
    body: Body,
    ingest_options: IngestOptions,
    ingest_router: IngestRouterServiceClient,
) -> Result<IngestResponse, IngestServiceError> {
    let mut doc_batch_builder = DocBatchV2Builder::default();
    let mut doc_uid_generator = DocUidGenerator::default();

    for doc in lines(&body.content) {
        doc_batch_builder.add_doc(doc_uid_generator.next_doc_uid(), doc);
    }
    let doc_batch_opt = doc_batch_builder.build();

    let Some(doc_batch) = doc_batch_opt else {
        let response = IngestResponse::default();
        return Ok(response);
    };
    let num_docs = doc_batch.num_docs();

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
    convert_ingest_response_v2(response, num_docs)
}

fn convert_ingest_response_v2(
    mut response: IngestResponseV2,
    num_docs: usize,
) -> Result<IngestResponse, IngestServiceError> {
    let num_responses = response.successes.len() + response.failures.len();
    if num_responses != 1 {
        return Err(IngestServiceError::Internal(format!(
            "Expected a single failure/success, got {}.",
            num_responses
        )));
    }
    if response.successes.pop().is_some() {
        return Ok(IngestResponse {
            num_docs_for_processing: num_docs as u64,
        });
    }
    let ingest_failure = response.failures.pop().unwrap();
    Err(match ingest_failure.reason() {
        IngestFailureReason::Unspecified => {
            IngestServiceError::Internal("unknown error".to_string())
        }
        IngestFailureReason::IndexNotFound => IngestServiceError::IndexNotFound {
            index_id: ingest_failure.index_id,
        },
        IngestFailureReason::SourceNotFound => IngestServiceError::Internal(format!(
            "Ingest v2 source not found for index {}",
            ingest_failure.index_id
        )),
        IngestFailureReason::Internal => IngestServiceError::Internal("internal error".to_string()),
        IngestFailureReason::NoShardsAvailable => {
            IngestServiceError::Unavailable("no shards available".to_string())
        }
        IngestFailureReason::RateLimited => IngestServiceError::RateLimited,
        IngestFailureReason::ResourceExhausted => IngestServiceError::RateLimited,
        IngestFailureReason::Timeout => {
            IngestServiceError::Internal("request timed out".to_string())
        }
    })
}

#[utoipa::path(
    post,
    tag = "Ingest",
    path = "/{index_id}/ingest",
    request_body(content = String, description = "Documents to ingest in NDJSON format and limited to 10MB", content_type = "application/json"),
    responses(
        (status = 200, description = "Successfully ingested documents.", body = IngestResponse)
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
    ingest_service: IngestServiceClient,
) -> Result<IngestResponse, IngestServiceError> {
    if disable_ingest_v1() {
        let message = "ingest v1 is disabled: environment variable `QW_DISABLE_INGEST_V1` is set";
        return Err(IngestServiceError::Internal(message.to_string()));
    }
    // The size of the body should be an upper bound of the size of the batch. The removal of the
    // end of line character for each doc compensates the addition of the `DocCommand` header.
    let mut doc_batch_builder = DocBatchBuilder::with_capacity(index_id, body.content.remaining());
    for line in lines(&body.content) {
        doc_batch_builder.ingest_doc(line);
    }
    let ingest_req = IngestRequest {
        doc_batches: vec![doc_batch_builder.build()],
        commit: ingest_options.commit_type.into(),
    };
    let ingest_response = ingest_service.ingest(ingest_req).await?;
    Ok(ingest_response)
}

pub fn tail_handler(
    ingest_service: IngestServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    tail_filter()
        .and(with_arg(ingest_service))
        .then(tail_endpoint)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
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
        init_ingest_api, CreateQueueIfNotExistsRequest, FetchRequest, FetchResponse,
        IngestApiService, IngestResponse, IngestServiceClient, SuggestTruncateRequest,
        QUEUES_DIR_NAME,
    };
    use quickwit_proto::ingest::router::IngestRouterServiceClient;

    use super::ingest_api_handlers;
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

    pub(crate) async fn setup_ingest_service(
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
            setup_ingest_service(&["my-index"], &IngestApiConfig::default()).await;
        let ingest_router = IngestRouterServiceClient::mocked();
        let ingest_api_handlers =
            ingest_api_handlers(ingest_router, ingest_service, IngestApiConfig::default());
        let resp = warp::test::request()
            .path("/my-index/ingest")
            .method("POST")
            .json(&true)
            .body(r#"{"id": 1, "message": "push"}"#)
            .reply(&ingest_api_handlers)
            .await;
        assert_eq!(resp.status(), 200);
        let ingest_response: IngestResponse = serde_json::from_slice(resp.body()).unwrap();
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
            setup_ingest_service(&["my-index"], &IngestApiConfig::default()).await;
        let ingest_router = IngestRouterServiceClient::mocked();
        let ingest_api_handlers =
            ingest_api_handlers(ingest_router, ingest_service, IngestApiConfig::default());
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
        let ingest_response: IngestResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(ingest_response.num_docs_for_processing, 3);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_ingest_api_return_429_if_above_limits() {
        let config: IngestApiConfig =
            serde_json::from_str(r#"{ "max_queue_memory_usage": "1" }"#).unwrap();
        let (universe, _temp_dir, ingest_service, _) =
            setup_ingest_service(&["my-index"], &config).await;
        let ingest_router = IngestRouterServiceClient::mocked();
        let ingest_api_handlers =
            ingest_api_handlers(ingest_router, ingest_service, IngestApiConfig::default());
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
            setup_ingest_service(&["my-index"], &IngestApiConfig::default()).await;
        let ingest_router = IngestRouterServiceClient::mocked();
        let ingest_api_handlers =
            ingest_api_handlers(ingest_router, ingest_service, config.clone());
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
            setup_ingest_service(&["my-index"], &IngestApiConfig::default()).await;
        let ingest_router = IngestRouterServiceClient::mocked();
        let ingest_api_handlers = ingest_api_handlers(
            ingest_router,
            ingest_service_client,
            IngestApiConfig::default(),
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
            let ingest_response: IngestResponse = serde_json::from_slice(resp.body()).unwrap();
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
            setup_ingest_service(&["my-index"], &IngestApiConfig::default()).await;
        let ingest_router = IngestRouterServiceClient::mocked();
        let ingest_api_handlers = ingest_api_handlers(
            ingest_router,
            ingest_service_client,
            IngestApiConfig::default(),
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
            let ingest_response: IngestResponse = serde_json::from_slice(resp.body()).unwrap();
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
}
