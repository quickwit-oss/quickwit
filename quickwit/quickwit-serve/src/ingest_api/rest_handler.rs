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

use std::collections::HashMap;

use bytes::Bytes;
use quickwit_ingest_api::{
    CommitType, DocBatchBuilder, FetchResponse, IngestRequest, IngestResponse, IngestService,
    IngestServiceClient, IngestServiceError, TailRequest,
};
use quickwit_proto::{ServiceError, ServiceErrorCode};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use thiserror::Error;
use warp::{reject, Filter, Rejection};

use crate::format::{extract_format_from_qs, make_response};
use crate::{with_arg, BodyFormat};

#[derive(utoipa::OpenApi)]
#[openapi(paths(ingest, tail_endpoint, elastic_ingest,))]
pub struct IngestApi;

#[derive(utoipa::OpenApi)]
#[openapi(components(schemas(
    quickwit_ingest_api::DocBatch,
    quickwit_ingest_api::FetchResponse,
    quickwit_ingest_api::IngestResponse,
)))]
pub struct IngestApiSchemas;

#[derive(Debug, Error)]
#[error("Body is not utf-8.")]
struct InvalidUtf8;

impl warp::reject::Reject for InvalidUtf8 {}

const CONTENT_LENGTH_LIMIT: u64 = 10 * 1024 * 1024; // 10MiB

#[derive(Error, Debug)]
pub enum IngestRestApiError {
    #[error("Failed to parse action `{0}`.")]
    BulkInvalidAction(String),
    #[error("Failed to parse source `{0}`.")]
    BulkInvalidSource(String),
    #[error(transparent)]
    IngestApi(#[from] IngestServiceError),
}

impl ServiceError for IngestRestApiError {
    fn status_code(&self) -> ServiceErrorCode {
        match self {
            Self::BulkInvalidAction(_) => ServiceErrorCode::BadRequest,
            Self::BulkInvalidSource(_) => ServiceErrorCode::BadRequest,
            Self::IngestApi(ingest_api_error) => ingest_api_error.status_code(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all(deserialize = "lowercase"))]
enum BulkAction {
    Index(BulkActionMeta),
    Create(BulkActionMeta),
}

impl BulkAction {
    fn into_index(self) -> String {
        match self {
            BulkAction::Index(meta) => meta.index,
            BulkAction::Create(meta) => meta.index,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct BulkActionMeta {
    #[serde(alias = "_index")]
    index: String,
    #[serde(alias = "_id")]
    #[serde(default)]
    id: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
struct IngestOptions {
    #[serde(default)]
    commit: CommitType,
}

pub(crate) fn ingest_api_handlers(
    ingest_service: IngestServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    ingest_handler(ingest_service.clone())
        .or(tail_handler(ingest_service.clone()))
        .or(elastic_bulk_handler(ingest_service))
}

fn ingest_filter(
) -> impl Filter<Extract = (String, String, IngestOptions), Error = Rejection> + Clone {
    warp::path!(String / "ingest")
        .and(warp::post())
        .and(warp::body::content_length_limit(CONTENT_LENGTH_LIMIT))
        .and(warp::body::bytes().and_then(|body: Bytes| async move {
            if let Ok(body_str) = std::str::from_utf8(&body) {
                Ok(body_str.to_string())
            } else {
                Err(reject::custom(InvalidUtf8))
            }
        }))
        .and(serde_qs::warp::query::<IngestOptions>(
            serde_qs::Config::default(),
        ))
}

fn ingest_handler(
    ingest_service: IngestServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    ingest_filter()
        .and(with_arg(ingest_service))
        .then(ingest)
        .map(|result| BodyFormat::default().make_rest_reply(result))
}

fn lines(body: &str) -> impl Iterator<Item = &str> {
    body.lines().filter_map(|line| {
        let line_trimmed = line.trim();
        if line_trimmed.is_empty() {
            return None;
        }
        Some(line_trimmed)
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
    index_id: String,
    payload: String,
    ingest_options: IngestOptions,
    mut ingest_service: IngestServiceClient,
) -> Result<IngestResponse, IngestServiceError> {
    let mut doc_batch = DocBatchBuilder::new(index_id);
    for doc_payload in lines(&payload) {
        doc_batch.ingest_doc(doc_payload.as_bytes());
    }
    let ingest_req = IngestRequest {
        doc_batches: vec![doc_batch.build()],
        commit: ingest_options.commit as u32,
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
        .map(make_response)
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

fn elastic_bulk_filter() -> impl Filter<Extract = (String,), Error = Rejection> + Clone {
    warp::path!("_bulk")
        .and(warp::post())
        .and(warp::body::content_length_limit(CONTENT_LENGTH_LIMIT))
        .and(warp::body::bytes().and_then(|body: Bytes| async move {
            if let Ok(body_str) = std::str::from_utf8(&body) {
                Ok(body_str.to_string())
            } else {
                Err(reject::custom(InvalidUtf8))
            }
        }))
}

pub fn elastic_bulk_handler(
    ingest_service: IngestServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_bulk_filter()
        .and(with_arg(ingest_service))
        .then(elastic_ingest)
        .and(extract_format_from_qs())
        .map(make_response)
}

#[utoipa::path(
    post,
    tag = "Ingest",
    path = "/_bulk",
    request_body(content = String, description = "Elasticsearch compatible bulk request body limited to 10MB", content_type = "application/json"),
    responses(
        (status = 200, description = "Successfully ingested documents.", body = IngestResponse)
    ),
)]
/// Elasticsearch-compatible Bulk Ingest
async fn elastic_ingest(
    payload: String,
    mut ingest_service: IngestServiceClient,
) -> Result<IngestResponse, IngestRestApiError> {
    let mut batches = HashMap::new();
    let mut payload_lines = lines(&payload);

    while let Some(json_str) = payload_lines.next() {
        let action = serde_json::from_str::<BulkAction>(json_str)
            .map_err(|e| IngestRestApiError::BulkInvalidAction(e.to_string()))?;
        let source = payload_lines
            .next()
            .ok_or_else(|| {
                IngestRestApiError::BulkInvalidSource("Expected source for the action.".to_string())
            })
            .and_then(|source| {
                serde_json::from_str::<JsonValue>(source)
                    .map_err(|err| IngestRestApiError::BulkInvalidSource(err.to_string()))
            })?;

        let index_id = action.into_index();
        let doc_batch = batches
            .entry(index_id.clone())
            .or_insert(DocBatchBuilder::new(index_id));

        doc_batch.ingest_doc(source.to_string().as_bytes());
    }

    let ingest_request = IngestRequest {
        doc_batches: batches
            .into_values()
            .map(|builder| builder.build())
            .collect(),
        commit: CommitType::Auto as u32,
    };
    let ingest_response = ingest_service.ingest(ingest_request).await?;
    Ok(ingest_response)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use byte_unit::Byte;
    use quickwit_actors::{Mailbox, Universe};
    use quickwit_config::IngestApiConfig;
    use quickwit_ingest_api::{
        init_ingest_api, CreateQueueIfNotExistsRequest, FetchRequest, FetchResponse,
        IngestApiService, IngestResponse, IngestServiceClient, SuggestTruncateRequest,
        QUEUES_DIR_NAME,
    };

    use super::{ingest_api_handlers, BulkAction, BulkActionMeta};

    #[test]
    fn test_bulk_action_serde() {
        {
            let bulk_action_json = r#"{
                "create": {
                    "_index": "test",
                    "_id" : "2"
                }
            }"#;
            let bulk_action = serde_json::from_str::<BulkAction>(bulk_action_json).unwrap();
            assert_eq!(
                bulk_action,
                BulkAction::Create(BulkActionMeta {
                    index: "test".to_string(),
                    id: Some("2".to_string()),
                })
            );
        }
        {
            let bulk_action_json = r#"{
                "create": {
                    "_index": "test"
                }
            }"#;
            let bulk_action = serde_json::from_str::<BulkAction>(bulk_action_json).unwrap();
            assert_eq!(
                bulk_action,
                BulkAction::Create(BulkActionMeta {
                    index: "test".to_string(),
                    id: None,
                })
            );
        }
        {
            let bulk_action_json = r#"{
                "delete": {
                    "_index": "test",
                    "_id": "2"
                }
            }"#;
            serde_json::from_str::<BulkAction>(bulk_action_json).unwrap_err();
        }
    }

    async fn setup_ingest_service(
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
        let ingest_api_handlers = ingest_api_handlers(ingest_service);
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
        assert_eq!(doc_batch.doc_lens.len(), 1);
        assert_eq!(
            doc_batch.doc_lens.iter().sum::<u64>() as usize,
            doc_batch.concat_docs.len()
        );

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_ingest_api_returns_200_when_ingest_ndjson_and_fetch() {
        let (universe, _temp_dir, ingest_service, _) =
            setup_ingest_service(&["my-index"], &IngestApiConfig::default()).await;
        let ingest_api_handlers = ingest_api_handlers(ingest_service);
        let payload = r#"
            {"id": 1, "message": "push"}
            {"id": 2, "message": "push"}
            {"id": 3, "message": "push"}
        "#;
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
    async fn test_ingest_api_bulk_request_returns_404_if_index_id_does_not_exist() {
        let (universe, _temp_dir, ingest_service, _) =
            setup_ingest_service(&["my-index"], &IngestApiConfig::default()).await;
        let ingest_api_handlers = ingest_api_handlers(ingest_service);
        let payload = r#"
            { "create" : { "_index" : "my-index", "_id" : "1"} }
            {"id": 1, "message": "push"}
            { "create" : { "_index" : "index-2", "_id" : "1" } }
            {"id": 1, "message": "push"}
        "#;
        let resp = warp::test::request()
            .path("/_bulk")
            .method("POST")
            .body(payload)
            .reply(&ingest_api_handlers)
            .await;
        assert_eq!(resp.status(), 404);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_ingest_api_bulk_request_returns_400_if_malformed_source() {
        let (universe, _temp_dir, ingest_service, _) =
            setup_ingest_service(&["my-index"], &IngestApiConfig::default()).await;
        let ingest_api_handlers = ingest_api_handlers(ingest_service);
        let payload = r#"
            { "create" : { "_index" : "my-index", "_id" : "1" } }
            {"id": 1, "message": "bad json}
        "#;
        let resp = warp::test::request()
            .path("/_bulk")
            .method("POST")
            .body(payload)
            .reply(&ingest_api_handlers)
            .await;
        assert_eq!(resp.status(), 400);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_ingest_api_bulk_returns_200() {
        let (universe, _temp_dir, ingest_service, _) =
            setup_ingest_service(&["my-index-1", "my-index-2"], &IngestApiConfig::default()).await;
        let ingest_api_handlers = ingest_api_handlers(ingest_service);
        let payload = r#"
            { "create" : { "_index" : "my-index-1", "_id" : "1"} }
            {"id": 1, "message": "push"}
            { "create" : { "_index" : "my-index-2", "_id" : "1"} }
            {"id": 1, "message": "push"}
            { "create" : { "_index" : "my-index-1" } }
            {"id": 2, "message": "push"}
        "#;
        let resp = warp::test::request()
            .path("/_bulk")
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
        let ingest_api_handlers = ingest_api_handlers(ingest_service);
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
    async fn test_ingest_api_blocks_when_wait_is_specified() {
        let (universe, _temp_dir, ingest_service_client, ingest_service_mailbox) =
            setup_ingest_service(&["my-index"], &IngestApiConfig::default()).await;
        let ingest_api_handlers = ingest_api_handlers(ingest_service_client);
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
        let ingest_api_handlers = ingest_api_handlers(ingest_service_client);
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
