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

use bytes::{Buf, Bytes};
use quickwit_ingest::{
    CommitType, DocBatchBuilder, FetchResponse, IngestRequest, IngestResponse, IngestService,
    IngestServiceClient, IngestServiceError, TailRequest,
};
use quickwit_proto::{ServiceError, ServiceErrorCode};
use serde::Deserialize;
use thiserror::Error;
use warp::{Filter, Rejection};

use crate::format::extract_format_from_qs;
use crate::json_api_response::make_json_api_response;
use crate::{with_arg, BodyFormat};

#[derive(utoipa::OpenApi)]
#[openapi(paths(ingest, tail_endpoint, elastic_ingest,))]
pub struct IngestApi;

#[derive(utoipa::OpenApi)]
#[openapi(components(schemas(
    quickwit_ingest::DocBatch,
    quickwit_ingest::FetchResponse,
    quickwit_ingest::IngestResponse,
    quickwit_ingest::CommitType,
    ElasticRefresh,
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
            BulkAction::Index(meta) => meta.index_id,
            BulkAction::Create(meta) => meta.index_id,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct BulkActionMeta {
    #[serde(alias = "_index")]
    index_id: String,
    #[serde(alias = "_id")]
    #[serde(default)]
    doc_id: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
struct IngestOptions {
    #[serde(alias = "commit")]
    #[serde(default)]
    commit_type: CommitType,
}

pub(crate) fn ingest_api_handlers(
    ingest_service: IngestServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    ingest_handler(ingest_service.clone())
        .or(tail_handler(ingest_service.clone()))
        .or(elastic_bulk_handler(ingest_service))
}

fn ingest_filter(
) -> impl Filter<Extract = (String, Bytes, IngestOptions), Error = Rejection> + Clone {
    warp::path!(String / "ingest")
        .and(warp::post())
        .and(warp::body::content_length_limit(CONTENT_LENGTH_LIMIT))
        .and(warp::body::bytes())
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
        .map(|result| make_json_api_response(result, BodyFormat::default()))
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
        commit: ingest_options.commit_type as u32,
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

/// ?refresh parameter for elasticsearch bulk request
///
/// The syntax for this parameter is a bit confusing for backward compatibility reasons.
/// - Absence of ?refresh parameter or ?refresh=false means no refresh
/// - Presence of ?refresh parameter without any values or ?refresh=true means force refresh
/// - ?refresh=wait_for means wait for refresh
#[derive(Clone, Debug, Deserialize, PartialEq, utoipa::ToSchema)]
#[serde(rename_all(deserialize = "snake_case"))]
#[derive(Default)]
pub enum ElasticRefresh {
    // if the refresh parameter is not present it is false
    #[default]
    /// The request doesn't wait for commit
    False,
    // but if it is present without a value like this: ?refresh, it should be the same as
    // ?refresh=true
    #[serde(alias = "")]
    /// The request forces an immediate commit after the last document in the batch and waits for
    /// it to finish.
    True,
    /// The request will wait for the next scheduled commit to finish.
    WaitFor,
}

impl From<ElasticRefresh> for CommitType {
    fn from(val: ElasticRefresh) -> Self {
        match val {
            ElasticRefresh::False => CommitType::Auto,
            ElasticRefresh::True => CommitType::Force,
            ElasticRefresh::WaitFor => CommitType::WaitFor,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
struct ElasticIngestOptions {
    #[serde(default)]
    refresh: ElasticRefresh,
}

fn elastic_bulk_filter(
) -> impl Filter<Extract = (Bytes, ElasticIngestOptions), Error = Rejection> + Clone {
    warp::path!("_bulk")
        .and(warp::post())
        .and(warp::body::content_length_limit(CONTENT_LENGTH_LIMIT))
        .and(warp::body::bytes())
        .and(serde_qs::warp::query::<ElasticIngestOptions>(
            serde_qs::Config::default(),
        ))
}

pub fn elastic_bulk_handler(
    ingest_service: IngestServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_bulk_filter()
        .and(with_arg(ingest_service))
        .then(elastic_ingest)
        .and(extract_format_from_qs())
        .map(make_json_api_response)
}

#[utoipa::path(
    post,
    tag = "Ingest",
    path = "/_bulk",
    request_body(content = String, description = "Elasticsearch compatible bulk request body limited to 10MB", content_type = "application/json"),
    responses(
        (status = 200, description = "Successfully ingested documents.", body = IngestResponse)
    ),
    params(
        ("refresh" = Option<ElasticRefresh>, Query, description = "Force or wait for commit at the end of the indexing operation."),
    )
)]
/// Elasticsearch-compatible Bulk Ingest
async fn elastic_ingest(
    body: Bytes,
    ingest_options: ElasticIngestOptions,
    mut ingest_service: IngestServiceClient,
) -> Result<IngestResponse, IngestRestApiError> {
    let mut doc_batch_builders = HashMap::new();
    let mut lines = lines(&body);

    while let Some(line) = lines.next() {
        let action = serde_json::from_slice::<BulkAction>(line)
            .map_err(|error| IngestRestApiError::BulkInvalidAction(error.to_string()))?;
        let source = lines.next().ok_or_else(|| {
            IngestRestApiError::BulkInvalidSource("Expected source for the action.".to_string())
        })?;
        let index_id = action.into_index();
        let doc_batch_builder = doc_batch_builders
            .entry(index_id.clone())
            .or_insert(DocBatchBuilder::new(index_id));

        doc_batch_builder.ingest_doc(source);
    }
    let doc_batches = doc_batch_builders
        .into_values()
        .map(|builder| builder.build())
        .collect();
    let commit_type: CommitType = ingest_options.refresh.into();
    let ingest_request = IngestRequest {
        doc_batches,
        commit: commit_type as u32,
    };
    let ingest_response = ingest_service.ingest(ingest_request).await?;
    Ok(ingest_response)
}

fn lines(body: &Bytes) -> impl Iterator<Item = &[u8]> {
    body.split(|byte| byte == &b'\n')
        .filter(|line| !line.is_empty())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use byte_unit::Byte;
    use quickwit_actors::{Mailbox, Universe};
    use quickwit_config::IngestApiConfig;
    use quickwit_ingest::{
        init_ingest_api, CreateQueueIfNotExistsRequest, FetchRequest, FetchResponse,
        IngestApiService, IngestResponse, IngestServiceClient, SuggestTruncateRequest,
        QUEUES_DIR_NAME,
    };

    use super::{ingest_api_handlers, BulkAction, BulkActionMeta};
    use crate::ingest_api::rest_handler::{ElasticIngestOptions, ElasticRefresh};

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
                    index_id: "test".to_string(),
                    doc_id: Some("2".to_string()),
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
                    index_id: "test".to_string(),
                    doc_id: None,
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
        let ingest_api_handlers = ingest_api_handlers(ingest_service);
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
    async fn test_ingest_api_bulk_request_returns_404_if_index_id_does_not_exist() {
        let (universe, _temp_dir, ingest_service, _) =
            setup_ingest_service(&["my-index"], &IngestApiConfig::default()).await;
        let ingest_api_handlers = ingest_api_handlers(ingest_service);
        let payload = r#"
            { "create" : { "_index" : "my-index", "_id" : "1"} }
            {"id": 1, "message": "push"}
            { "create" : { "_index" : "index-2", "_id" : "1" } }
            {"id": 1, "message": "push"}"#;
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
            {"id": 2, "message": "push"}"#;
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

    #[test]
    fn test_elastic_refresh_parsing() {
        assert_eq!(
            serde_qs::from_str::<ElasticIngestOptions>("")
                .unwrap()
                .refresh,
            ElasticRefresh::False
        );
        assert_eq!(
            serde_qs::from_str::<ElasticIngestOptions>("refresh=true")
                .unwrap()
                .refresh,
            ElasticRefresh::True
        );
        assert_eq!(
            serde_qs::from_str::<ElasticIngestOptions>("refresh=false")
                .unwrap()
                .refresh,
            ElasticRefresh::False
        );
        assert_eq!(
            serde_qs::from_str::<ElasticIngestOptions>("refresh=wait_for")
                .unwrap()
                .refresh,
            ElasticRefresh::WaitFor
        );
        assert_eq!(
            serde_qs::from_str::<ElasticIngestOptions>("refresh")
                .unwrap()
                .refresh,
            ElasticRefresh::True
        );
        assert_eq!(
            serde_qs::from_str::<ElasticIngestOptions>("refresh=wait")
                .unwrap_err()
                .to_string(),
            "unknown variant `wait`, expected one of `false`, `true`, `wait_for`"
        );
    }

    #[tokio::test]
    async fn test_bulk_api_blocks_when_refresh_wait_for_is_specified() {
        let (universe, _temp_dir, ingest_service, ingest_service_mailbox) =
            setup_ingest_service(&["my-index-1", "my-index-2"], &IngestApiConfig::default()).await;
        let ingest_api_handlers = ingest_api_handlers(ingest_service);
        let payload = r#"
            { "create" : { "_index" : "my-index-1", "_id" : "1"} }
            {"id": 1, "message": "push"}
            { "create" : { "_index" : "my-index-2", "_id" : "1"} }
            {"id": 1, "message": "push"}
            { "create" : { "_index" : "my-index-1" } }
            {"id": 2, "message": "push"}"#;
        let handle = tokio::spawn(async move {
            let resp = warp::test::request()
                .path("/_bulk?refresh=wait_for")
                .method("POST")
                .body(payload)
                .reply(&ingest_api_handlers)
                .await;

            assert_eq!(resp.status(), 200);
            let ingest_response: IngestResponse = serde_json::from_slice(resp.body()).unwrap();
            assert_eq!(ingest_response.num_docs_for_processing, 3);
        });
        universe.sleep(Duration::from_secs(10)).await;
        assert!(!handle.is_finished());
        assert_eq!(
            ingest_service_mailbox
                .ask_for_res(FetchRequest {
                    index_id: "my-index-1".to_string(),
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
        assert!(!handle.is_finished());
        assert_eq!(
            ingest_service_mailbox
                .ask_for_res(FetchRequest {
                    index_id: "my-index-2".to_string(),
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
                index_id: "my-index-1".to_string(),
                up_to_position_included: 1,
            })
            .await
            .unwrap();
        universe.sleep(Duration::from_secs(10)).await;
        assert!(!handle.is_finished());
        ingest_service_mailbox
            .ask_for_res(SuggestTruncateRequest {
                index_id: "my-index-2".to_string(),
                up_to_position_included: 0,
            })
            .await
            .unwrap();
        handle.await.unwrap();
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_bulk_api_blocks_when_refresh_true_is_specified() {
        let (universe, _temp_dir, ingest_service, ingest_service_mailbox) =
            setup_ingest_service(&["my-index-1", "my-index-2"], &IngestApiConfig::default()).await;
        let ingest_api_handlers = ingest_api_handlers(ingest_service);
        let payload = r#"
            { "create" : { "_index" : "my-index-1", "_id" : "1"} }
            {"id": 1, "message": "push"}
            { "create" : { "_index" : "my-index-2", "_id" : "1"} }
            {"id": 1, "message": "push"}
            { "create" : { "_index" : "my-index-1" } }
            {"id": 2, "message": "push"}"#;
        let handle = tokio::spawn(async move {
            let resp = warp::test::request()
                .path("/_bulk?refresh")
                .method("POST")
                .body(payload)
                .reply(&ingest_api_handlers)
                .await;

            assert_eq!(resp.status(), 200);
            let ingest_response: IngestResponse = serde_json::from_slice(resp.body()).unwrap();
            assert_eq!(ingest_response.num_docs_for_processing, 3);
        });
        universe.sleep(Duration::from_secs(10)).await;
        assert!(!handle.is_finished());
        assert_eq!(
            ingest_service_mailbox
                .ask_for_res(FetchRequest {
                    index_id: "my-index-1".to_string(),
                    start_after: None,
                    num_bytes_limit: None,
                })
                .await
                .unwrap()
                .doc_batch
                .unwrap()
                .num_docs(),
            3
        );
        assert_eq!(
            ingest_service_mailbox
                .ask_for_res(FetchRequest {
                    index_id: "my-index-2".to_string(),
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
                index_id: "my-index-1".to_string(),
                up_to_position_included: 1,
            })
            .await
            .unwrap();
        universe.sleep(Duration::from_secs(10)).await;
        assert!(!handle.is_finished());
        ingest_service_mailbox
            .ask_for_res(SuggestTruncateRequest {
                index_id: "my-index-2".to_string(),
                up_to_position_included: 0,
            })
            .await
            .unwrap();
        handle.await.unwrap();
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_bulk_ingest_request_returns_400_if_action_is_malformed() {
        let ingest_service = IngestServiceClient::new(IngestServiceClient::mock());
        let ingest_api_handlers = ingest_api_handlers(ingest_service);
        let payload = r#"
            {"create": {"_index": "my-index", "_id": "1"},}
            {"id": 1, "message": "my-doc"}"#;
        let resp = warp::test::request()
            .path("/_bulk")
            .method("POST")
            .body(payload)
            .reply(&ingest_api_handlers)
            .await;
        assert_eq!(resp.status(), 400);
    }
}
