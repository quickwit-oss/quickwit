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
use quickwit_actors::{AskError, Mailbox};
use quickwit_ingest_api::{add_doc, IngestApiError, IngestApiService};
use quickwit_proto::ingest_api::{
    DocBatch, FetchResponse, IngestRequest, IngestResponse, TailRequest,
};
use quickwit_proto::{ServiceError, ServiceErrorCode};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use thiserror::Error;
use warp::{reject, Filter, Rejection};

use crate::{require, Format};

#[derive(utoipa::OpenApi)]
#[openapi(paths(ingest, tail_endpoint, elastic_ingest,))]
pub struct IngestApi;

#[derive(utoipa::OpenApi)]
#[openapi(components(schemas(
    quickwit_proto::ingest_api::DocBatch,
    quickwit_proto::ingest_api::FetchResponse,
    quickwit_proto::ingest_api::IngestResponse,
)))]
pub struct IngestApiSchemas;

#[derive(Debug, Error)]
#[error("Body is not utf-8.")]
struct InvalidUtf8;

impl warp::reject::Reject for InvalidUtf8 {}

const CONTENT_LENGTH_LIMIT: u64 = 10_000_000; // 10M

#[derive(Error, Debug)]
pub enum IngestRestApiError {
    #[error("Could not parse action `{0}`.")]
    BulkInvalidAction(String),
    #[error("Could not parse the source `{0}`.")]
    BulkInvalidSource(String),
    #[error(transparent)]
    IngestApi(#[from] AskError<IngestApiError>),
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
    id: String,
}

pub fn ingest_api_handlers(
    ingest_api_mailbox_opt: Option<Mailbox<IngestApiService>>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    // Indexes handlers.
    ingest_handler(ingest_api_mailbox_opt.clone())
        .or(tail_handler(ingest_api_mailbox_opt.clone()))
        .or(elastic_bulk_handler(ingest_api_mailbox_opt))
}

pub fn ingest_handler(
    ingest_api_mailbox_opt: Option<Mailbox<IngestApiService>>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    ingest_filter()
        .and(require(ingest_api_mailbox_opt))
        .then(ingest)
        .map(|result| Format::default().make_rest_reply_non_serializable_error(result))
}

fn ingest_filter() -> impl Filter<Extract = (String, String), Error = Rejection> + Clone {
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
    path = "{index_id}/ingest",
    request_body(content = String, description = "Documents to ingest in NDJSON format and limited to 10MB", content_type = "application/json"),
    responses(
        (status = 200, description = "Successfully ingested documents.", body = IngestResponse)
    ),
    params(
        ("index_id" = String, Path, description = "The index ID to add docs to."),
    )
)]
/// Ingest documents
async fn ingest(
    index_id: String,
    payload: String,
    ingest_api_mailbox: Mailbox<IngestApiService>,
) -> Result<IngestResponse, AskError<IngestApiError>> {
    let mut doc_batch = DocBatch {
        index_id,
        ..Default::default()
    };
    for doc_payload in lines(&payload) {
        add_doc(doc_payload.as_bytes(), &mut doc_batch);
    }
    let ingest_req = IngestRequest {
        doc_batches: vec![doc_batch],
    };
    let ingest_response = ingest_api_mailbox.ask_for_res(ingest_req).await?;
    Ok(ingest_response)
}

pub fn tail_handler(
    ingest_api_mailbox_opt: Option<Mailbox<IngestApiService>>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    tail_filter()
        .and(require(ingest_api_mailbox_opt))
        .then(tail_endpoint)
        .map(|result| Format::default().make_rest_reply_non_serializable_error(result))
}

fn tail_filter() -> impl Filter<Extract = (String,), Error = Rejection> + Clone {
    warp::path!(String / "tail").and(warp::get())
}

#[utoipa::path(
    post,
    tag = "Ingest",
    path = "{index_id}/tail",
    request_body = String,
    responses(
        (status = 200, description = "Successfully fetched documents.", body = FetchResponse)
    ),
    params(
        ("index_id" = String, Path, description = "The index ID to tail."),
    )
)]
/// Tail
async fn tail_endpoint(
    index_id: String,
    ingest_api_service: Mailbox<IngestApiService>,
) -> Result<FetchResponse, AskError<IngestApiError>> {
    let fetch_response = ingest_api_service
        .ask_for_res(TailRequest { index_id })
        .await?;
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
    ingest_api_mailbox_opt: Option<Mailbox<IngestApiService>>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_bulk_filter()
        .and(require(ingest_api_mailbox_opt))
        .then(elastic_ingest)
        .map(|result| Format::default().make_rest_reply_non_serializable_error(result))
}

#[utoipa::path(
    post,
    tag = "Ingest",
    path = "/_bulk",
    request_body(content = String, description = "Elasticsearch compatible bulk resquest body limited to 10MB", content_type = "application/json"),
    responses(
        (status = 200, description = "Successfully ingested documents.", body = IngestResponse)
    ),
)]
/// Elasticsearch Bulk Ingest
async fn elastic_ingest(
    payload: String,
    ingest_api_mailbox: Mailbox<IngestApiService>,
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
        let doc_batch = batches.entry(index_id.clone()).or_insert(DocBatch {
            index_id,
            ..Default::default()
        });

        add_doc(source.to_string().as_bytes(), doc_batch);
    }

    let ingest_request = IngestRequest {
        doc_batches: batches.into_values().collect(),
    };
    let ingest_response = ingest_api_mailbox.ask_for_res(ingest_request).await?;
    Ok(ingest_response)
}

#[cfg(test)]
mod tests {
    use byte_unit::Byte;
    use quickwit_actors::Universe;
    use quickwit_config::IngestApiConfig;
    use quickwit_ingest_api::{init_ingest_api, QUEUES_DIR_NAME};
    use quickwit_proto::ingest_api::{
        CreateQueueIfNotExistsRequest, FetchResponse, IngestResponse,
    };

    use super::{ingest_api_handlers, BulkAction, BulkActionMeta};

    #[test]
    fn test_deserialize() {
        let json_str = r#"{ "create" : { "_index" : "test", "_id" : "2" } }"#;
        let bulk_object = serde_json::from_str::<BulkAction>(json_str).unwrap();
        assert_eq!(
            bulk_object,
            BulkAction::Create(BulkActionMeta {
                index: "test".to_string(),
                id: "2".to_string()
            })
        );

        let json_str = r#"{ "delete" : { "_index" : "test", "_id" : "2" } }"#;
        assert!(serde_json::from_str::<BulkAction>(json_str).is_err());
    }

    #[tokio::test]
    async fn test_ingest_api_returns_404_when_no_ingest_api_service() {
        let resp = warp::test::request()
            .path("/quickwit-demo-index/ingest")
            .method("POST")
            .json(&true)
            .body(r#"{"query": "*", "bad_param":10, "aggs": {"range":[]} }"#)
            .reply(&ingest_api_handlers(None))
            .await;
        assert_eq!(resp.status(), 404);
    }

    #[tokio::test]
    async fn test_ingest_api_returns_200_when_ingest_json_and_fetch() {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();
        let queues_dir_path = temp_dir.path().join(QUEUES_DIR_NAME);
        let ingest_api_service =
            init_ingest_api(&universe, &queues_dir_path, &IngestApiConfig::default())
                .await
                .unwrap();
        let create_queue_req = CreateQueueIfNotExistsRequest {
            queue_id: "quickwit-demo-index".to_string(),
        };
        ingest_api_service
            .ask_for_res(create_queue_req)
            .await
            .unwrap();
        let ingest_api_handlers = ingest_api_handlers(Some(ingest_api_service));
        let resp = warp::test::request()
            .path("/quickwit-demo-index/ingest")
            .method("POST")
            .json(&true)
            .body(r#"{"id": 1, "message": "push"}"#)
            .reply(&ingest_api_handlers)
            .await;
        assert_eq!(resp.status(), 200);
        let ingest_response: IngestResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(ingest_response.num_docs_for_processing, 1);

        let resp = warp::test::request()
            .path("/quickwit-demo-index/tail")
            .method("GET")
            .reply(&ingest_api_handlers)
            .await;
        assert_eq!(resp.status(), 200);
        let fetch_response: FetchResponse = serde_json::from_slice(resp.body()).unwrap();
        let doc_batch = fetch_response.doc_batch.unwrap();
        assert_eq!(doc_batch.index_id, "quickwit-demo-index");
        assert!(!doc_batch.concat_docs.is_empty());
        assert_eq!(doc_batch.doc_lens.len(), 1);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_ingest_api_returns_200_when_ingest_ndjson_and_fetch() {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();
        let queues_dir_path = temp_dir.path().join(QUEUES_DIR_NAME);
        let ingest_api_service =
            init_ingest_api(&universe, &queues_dir_path, &IngestApiConfig::default())
                .await
                .unwrap();
        let create_queue_req = CreateQueueIfNotExistsRequest {
            queue_id: "quickwit-demo-index".to_string(),
        };
        ingest_api_service
            .ask_for_res(create_queue_req)
            .await
            .unwrap();
        let ingest_api_handlers = ingest_api_handlers(Some(ingest_api_service));
        let body_to_post = r#"
        {"id": 1, "message": "push"}
        {"id": 2, "message": "push"}
        {"id": 3, "message": "push"}
        "#;
        let resp = warp::test::request()
            .path("/quickwit-demo-index/ingest")
            .method("POST")
            .body(body_to_post)
            .reply(&ingest_api_handlers)
            .await;
        assert_eq!(resp.status(), 200);
        let ingest_response: IngestResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(ingest_response.num_docs_for_processing, 3);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_ingest_api_bulk_request_returns_404_if_index_id_does_not_exist() {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();
        let queues_dir_path = temp_dir.path().join(QUEUES_DIR_NAME);
        let ingest_api_service =
            init_ingest_api(&universe, &queues_dir_path, &IngestApiConfig::default())
                .await
                .unwrap();
        let create_queue_req = CreateQueueIfNotExistsRequest {
            queue_id: "quickwit-demo-index".to_string(),
        };
        ingest_api_service
            .ask_for_res(create_queue_req)
            .await
            .unwrap();
        let ingest_api_handlers = ingest_api_handlers(Some(ingest_api_service));
        let body_to_post = r#"
        { "create" : { "_index" : "quickwit-demo-index", "_id" : "1"} }
        {"id": 1, "message": "push"}
        { "create" : { "_index" : "index-2", "_id" : "1" } }
        {"id": 1, "message": "push"}
        "#;
        let resp = warp::test::request()
            .path("/_bulk")
            .method("POST")
            .body(body_to_post)
            .reply(&ingest_api_handlers)
            .await;
        assert_eq!(resp.status(), 404);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_ingest_api_bulk_request_returns_400_if_malformed_source() {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();
        let queues_dir_path = temp_dir.path().join(QUEUES_DIR_NAME);
        let ingest_api_service =
            init_ingest_api(&universe, &queues_dir_path, &IngestApiConfig::default())
                .await
                .unwrap();
        let create_queue_req = CreateQueueIfNotExistsRequest {
            queue_id: "quickwit-demo-index".to_string(),
        };
        ingest_api_service
            .ask_for_res(create_queue_req)
            .await
            .unwrap();
        let ingest_api_handlers = ingest_api_handlers(Some(ingest_api_service));
        let body_to_post = r#"
        { "create" : { "_index" : "quickwit-demo-index", "_id" : "1" } }
        {"id": 1, "message": "bad json}
        "#;
        let resp = warp::test::request()
            .path("/_bulk")
            .method("POST")
            .body(body_to_post)
            .reply(&ingest_api_handlers)
            .await;
        assert_eq!(resp.status(), 400);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_ingest_api_bulk_returns_200() {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();
        let queues_dir_path = temp_dir.path().join(QUEUES_DIR_NAME);
        let ingest_api_service =
            init_ingest_api(&universe, &queues_dir_path, &IngestApiConfig::default())
                .await
                .unwrap();
        let create_queue_req_1 = CreateQueueIfNotExistsRequest {
            queue_id: "quickwit-demo-index-1".to_string(),
        };
        ingest_api_service
            .ask_for_res(create_queue_req_1)
            .await
            .unwrap();
        let create_queue_req_2 = CreateQueueIfNotExistsRequest {
            queue_id: "quickwit-demo-index-2".to_string(),
        };
        ingest_api_service
            .ask_for_res(create_queue_req_2)
            .await
            .unwrap();
        let ingest_api_handlers = ingest_api_handlers(Some(ingest_api_service));
        let body_to_post = r#"
        { "create" : { "_index" : "quickwit-demo-index-1", "_id" : "1"} }
        {"id": 1, "message": "push"}
        { "create" : { "_index" : "quickwit-demo-index-2", "_id" : "1"} }
        {"id": 1, "message": "push"}
        "#;
        let resp = warp::test::request()
            .path("/_bulk")
            .method("POST")
            .body(body_to_post)
            .reply(&ingest_api_handlers)
            .await;
        assert_eq!(resp.status(), 200);
        let ingest_response: IngestResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(ingest_response.num_docs_for_processing, 2);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_ingest_api_return_429_if_above_limits() {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();
        let queues_dir_path = temp_dir.path().join(QUEUES_DIR_NAME);
        let ingest_api_config = IngestApiConfig {
            max_queue_disk_usage: Byte::from_bytes(0),
            max_queue_memory_usage: Byte::from_bytes(0),
        };
        let ingest_api_service = init_ingest_api(&universe, &queues_dir_path, &ingest_api_config)
            .await
            .unwrap();
        let create_queue_req_1 = CreateQueueIfNotExistsRequest {
            queue_id: "quickwit-demo-index".to_string(),
        };
        ingest_api_service
            .ask_for_res(create_queue_req_1)
            .await
            .unwrap();
        let ingest_api_handlers = ingest_api_handlers(Some(ingest_api_service));
        let resp = warp::test::request()
            .path("/quickwit-demo-index/ingest")
            .method("POST")
            .json(&true)
            .body(r#"{"id": 1, "message": "push"}"#)
            .reply(&ingest_api_handlers)
            .await;
        assert_eq!(resp.status(), 429);
        universe.assert_quit().await;
    }
}
