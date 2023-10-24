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

use bytes::{Buf, BufMut, Bytes, BytesMut};
use quickwit_config::{IngestApiConfig, INGEST_SOURCE_ID};
use quickwit_ingest::{
    CommitType, DocBatchBuilder, FetchResponse, IngestRequest, IngestResponse, IngestService,
    IngestServiceClient, IngestServiceError, TailRequest,
};
use quickwit_proto::ingest::router::{
    IngestRequestV2, IngestResponseV2, IngestRouterService, IngestRouterServiceClient,
    IngestSubrequest,
};
use quickwit_proto::ingest::{DocBatchV2, IngestV2Error};
use quickwit_proto::types::IndexId;
use serde::Deserialize;
use thiserror::Error;
use warp::{Filter, Rejection};

use crate::format::extract_format_from_qs;
use crate::json_api_response::make_json_api_response;
use crate::{with_arg, BodyFormat};

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

#[derive(Debug, Error)]
#[error("request body contains invalid UTF-8 characters")]
struct InvalidUtf8;

impl warp::reject::Reject for InvalidUtf8 {}

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
) -> impl Filter<Extract = (String, Bytes, IngestOptions), Error = Rejection> + Clone {
    warp::path!(String / "ingest")
        .and(warp::post())
        .and(warp::body::content_length_limit(
            config.content_length_limit,
        ))
        .and(warp::body::bytes())
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
        .map(|result| make_json_api_response(result, BodyFormat::default()))
}

fn ingest_v2_filter(
    config: IngestApiConfig,
) -> impl Filter<Extract = (String, Bytes, IngestOptions), Error = Rejection> + Clone {
    warp::path!(String / "ingest-v2")
        .and(warp::post())
        .and(warp::body::content_length_limit(
            config.content_length_limit,
        ))
        .and(warp::body::bytes())
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
        .map(make_json_api_response)
}

async fn ingest_v2(
    index_id: IndexId,
    body: Bytes,
    ingest_options: IngestOptions,
    mut ingest_router: IngestRouterServiceClient,
) -> Result<IngestResponseV2, IngestV2Error> {
    let mut doc_buffer = BytesMut::new();
    let mut doc_lengths = Vec::new();

    for line in lines(&body) {
        doc_lengths.push(line.len() as u32);
        doc_buffer.put(line);
    }
    let doc_batch = DocBatchV2 {
        doc_buffer: doc_buffer.freeze(),
        doc_lengths,
    };
    let subrequest = IngestSubrequest {
        index_id,
        source_id: INGEST_SOURCE_ID.to_string(),
        doc_batch: Some(doc_batch),
    };
    let request = IngestRequestV2 {
        commit_type: ingest_options.commit_type as i32,
        subrequests: vec![subrequest],
    };
    let response = ingest_router.ingest(request).await?;
    Ok(response)
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
    index_id: String,
    body: Bytes,
    ingest_options: IngestOptions,
    mut ingest_service: IngestServiceClient,
) -> Result<IngestResponse, IngestServiceError> {
    // The size of the body should be an upper bound of the size of the batch. The removal of the
    // end of line character for each doc compensates the addition of the `DocCommand` header.
    let mut doc_batch_builder = DocBatchBuilder::with_capacity(index_id, body.remaining());
    for line in lines(&body) {
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
        .map(make_json_api_response)
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
    index_id: String,
    mut ingest_service: IngestServiceClient,
) -> Result<FetchResponse, IngestServiceError> {
    let fetch_response = ingest_service.tail(TailRequest { index_id }).await?;
    Ok(fetch_response)
}

pub(crate) fn lines(body: &Bytes) -> impl Iterator<Item = &[u8]> {
    body.split(|byte| byte == &b'\n')
        .filter(|line| !line.is_empty())
}

#[cfg(test)]
pub(crate) mod tests {
    use std::time::Duration;

    use byte_unit::Byte;
    use quickwit_actors::{Mailbox, Universe};
    use quickwit_config::IngestApiConfig;
    use quickwit_ingest::{
        init_ingest_api, CreateQueueIfNotExistsRequest, FetchRequest, FetchResponse,
        IngestApiService, IngestResponse, IngestServiceClient, SuggestTruncateRequest,
        QUEUES_DIR_NAME,
    };
    use quickwit_proto::ingest::router::IngestRouterServiceClient;

    use super::ingest_api_handlers;

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
        let ingest_router = IngestRouterServiceClient::mock().into();
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
        let ingest_router = IngestRouterServiceClient::mock().into();
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
        let config = IngestApiConfig {
            max_queue_memory_usage: Byte::from_bytes(1),
            ..Default::default()
        };
        let (universe, _temp_dir, ingest_service, _) =
            setup_ingest_service(&["my-index"], &config).await;
        let ingest_router = IngestRouterServiceClient::mock().into();
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
        let config = IngestApiConfig {
            content_length_limit: 1,
            ..Default::default()
        };
        let (universe, _temp_dir, ingest_service, _) =
            setup_ingest_service(&["my-index"], &IngestApiConfig::default()).await;
        let ingest_router = IngestRouterServiceClient::mock().into();
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
        let ingest_router = IngestRouterServiceClient::mock().into();
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
        let ingest_router = IngestRouterServiceClient::mock().into();
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
