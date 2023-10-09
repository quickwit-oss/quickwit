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
use quickwit_ingest::{
    CommitType, DocBatchBuilder, IngestRequest, IngestResponse, IngestService, IngestServiceClient,
    IngestServiceError,
};
use quickwit_proto::{ServiceError, ServiceErrorCode};
use thiserror::Error;
use warp::{Filter, Rejection};

use crate::elastic_search_api::filter::{elastic_bulk_filter, elastic_index_bulk_filter};
use crate::elastic_search_api::model::{BulkAction, ElasticIngestOptions};
use crate::format::extract_format_from_qs;
use crate::ingest_api::lines;
use crate::json_api_response::make_json_api_response;
use crate::with_arg;

#[derive(Error, Debug)]
pub enum IngestRestApiError {
    #[error("failed to parse action `{0}`")]
    BulkInvalidAction(String),
    #[error("failed to parse source `{0}`")]
    BulkInvalidSource(String),
    #[error(transparent)]
    IngestApi(#[from] IngestServiceError),
}

impl ServiceError for IngestRestApiError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::BulkInvalidAction(_) => ServiceErrorCode::BadRequest,
            Self::BulkInvalidSource(_) => ServiceErrorCode::BadRequest,
            Self::IngestApi(ingest_api_error) => ingest_api_error.error_code(),
        }
    }
}

/// POST `_elastic/_bulk`
pub fn es_compat_bulk_handler(
    ingest_service: IngestServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_bulk_filter()
        .and(with_arg(ingest_service))
        .then(|body, ingest_option, ingest_service| {
            elastic_ingest_bulk(None, body, ingest_option, ingest_service)
        })
        .and(extract_format_from_qs())
        .map(make_json_api_response)
}

/// POST `_elastic/<index>/_bulk`
pub fn es_compat_index_bulk_handler(
    ingest_service: IngestServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_index_bulk_filter()
        .and(with_arg(ingest_service))
        .then(|index, body, ingest_option, ingest_service| {
            elastic_ingest_bulk(Some(index), body, ingest_option, ingest_service)
        })
        .and(extract_format_from_qs())
        .map(make_json_api_response)
}

async fn elastic_ingest_bulk(
    index: Option<String>,
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
            IngestRestApiError::BulkInvalidSource("expected source for the action".to_string())
        })?;
        // when ingesting on /my-index/_bulk, if _index: is set to something else than my-index,
        // ES honors it and create the doc in the requested index. That is, `my-index` is a default
        // value in case _index: is missing, but not a constraint on each sub-action.
        let index_id = action
            .into_index()
            .or_else(|| index.clone())
            .ok_or_else(|| {
                IngestRestApiError::BulkInvalidAction(
                    "missing required field: `_index`".to_string(),
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
    let commit_type: CommitType = ingest_options.refresh.into();
    let ingest_request = IngestRequest {
        doc_batches,
        commit: commit_type.into(),
    };
    let ingest_response = ingest_service.ingest(ingest_request).await?;
    Ok(ingest_response)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use quickwit_config::{IngestApiConfig, NodeConfig};
    use quickwit_ingest::{
        FetchRequest, IngestResponse, IngestServiceClient, SuggestTruncateRequest,
    };
    use quickwit_search::MockSearchService;

    use crate::elastic_search_api::elastic_api_handlers;
    use crate::ingest_api::setup_ingest_service;

    #[tokio::test]
    async fn test_bulk_api_returns_404_if_index_id_does_not_exist() {
        let config = Arc::new(NodeConfig::for_test());
        let search_service = Arc::new(MockSearchService::new());
        let (universe, _temp_dir, ingest_service, _) =
            setup_ingest_service(&["my-index"], &IngestApiConfig::default()).await;
        let elastic_api_handlers = elastic_api_handlers(config, search_service, ingest_service);
        let payload = r#"
            { "create" : { "_index" : "my-index", "_id" : "1"} }
            {"id": 1, "message": "push"}
            { "create" : { "_index" : "index-2", "_id" : "1" } }
            {"id": 1, "message": "push"}"#;
        let resp = warp::test::request()
            .path("/_elastic/_bulk")
            .method("POST")
            .body(payload)
            .reply(&elastic_api_handlers)
            .await;
        assert_eq!(resp.status(), 404);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_bulk_api_returns_200() {
        let config = Arc::new(NodeConfig::for_test());
        let search_service = Arc::new(MockSearchService::new());
        let (universe, _temp_dir, ingest_service, _) =
            setup_ingest_service(&["my-index-1", "my-index-2"], &IngestApiConfig::default()).await;
        let elastic_api_handlers = elastic_api_handlers(config, search_service, ingest_service);
        let payload = r#"
            { "create" : { "_index" : "my-index-1", "_id" : "1"} }
            {"id": 1, "message": "push"}
            { "create" : { "_index" : "my-index-2", "_id" : "1"} }
            {"id": 1, "message": "push"}
            { "create" : { "_index" : "my-index-1" } }
            {"id": 2, "message": "push"}"#;
        let resp = warp::test::request()
            .path("/_elastic/_bulk")
            .method("POST")
            .body(payload)
            .reply(&elastic_api_handlers)
            .await;
        assert_eq!(resp.status(), 200);
        let ingest_response: IngestResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(ingest_response.num_docs_for_processing, 3);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_bulk_index_api_returns_200() {
        let config = Arc::new(NodeConfig::for_test());
        let search_service = Arc::new(MockSearchService::new());
        let (universe, _temp_dir, ingest_service, _) =
            setup_ingest_service(&["my-index-1", "my-index-2"], &IngestApiConfig::default()).await;
        let elastic_api_handlers = elastic_api_handlers(config, search_service, ingest_service);
        let payload = r#"
            { "create" : { "_index" : "my-index-1", "_id" : "1"} }
            {"id": 1, "message": "push"}
            { "create" : { "_index" : "my-index-2", "_id" : "1"} }
            {"id": 1, "message": "push"}
            { "create" : {} }
            {"id": 2, "message": "push"}"#;
        let resp = warp::test::request()
            .path("/_elastic/my-index-1/_bulk")
            .method("POST")
            .body(payload)
            .reply(&elastic_api_handlers)
            .await;
        assert_eq!(resp.status(), 200);
        let ingest_response: IngestResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(ingest_response.num_docs_for_processing, 3);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_bulk_api_blocks_when_refresh_wait_for_is_specified() {
        let config = Arc::new(NodeConfig::for_test());
        let search_service = Arc::new(MockSearchService::new());
        let (universe, _temp_dir, ingest_service, ingest_service_mailbox) =
            setup_ingest_service(&["my-index-1", "my-index-2"], &IngestApiConfig::default()).await;
        let elastic_api_handlers = elastic_api_handlers(config, search_service, ingest_service);
        let payload = r#"
            { "create" : { "_index" : "my-index-1", "_id" : "1"} }
            {"id": 1, "message": "push"}
            { "create" : { "_index" : "my-index-2", "_id" : "1"} }
            {"id": 1, "message": "push"}
            { "create" : { "_index" : "my-index-1" } }
            {"id": 2, "message": "push"}"#;
        let handle = tokio::spawn(async move {
            let resp = warp::test::request()
                .path("/_elastic/_bulk?refresh=wait_for")
                .method("POST")
                .body(payload)
                .reply(&elastic_api_handlers)
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
        let config = Arc::new(NodeConfig::for_test());
        let search_service = Arc::new(MockSearchService::new());
        let (universe, _temp_dir, ingest_service, ingest_service_mailbox) =
            setup_ingest_service(&["my-index-1", "my-index-2"], &IngestApiConfig::default()).await;
        let elastic_api_handlers = elastic_api_handlers(config, search_service, ingest_service);
        let payload = r#"
            { "create" : { "_index" : "my-index-1", "_id" : "1"} }
            {"id": 1, "message": "push"}
            { "create" : { "_index" : "my-index-2", "_id" : "1"} }
            {"id": 1, "message": "push"}
            { "create" : { "_index" : "my-index-1" } }
            {"id": 2, "message": "push"}"#;
        let handle = tokio::spawn(async move {
            let resp = warp::test::request()
                .path("/_elastic/_bulk?refresh")
                .method("POST")
                .body(payload)
                .reply(&elastic_api_handlers)
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
        let config = Arc::new(NodeConfig::for_test());
        let search_service = Arc::new(MockSearchService::new());
        let ingest_service = IngestServiceClient::new(IngestServiceClient::mock());
        let elastic_api_handlers = elastic_api_handlers(config, search_service, ingest_service);
        let payload = r#"
            {"create": {"_index": "my-index", "_id": "1"},}
            {"id": 1, "message": "my-doc"}"#;
        let resp = warp::test::request()
            .path("/_elastic/_bulk")
            .method("POST")
            .body(payload)
            .reply(&elastic_api_handlers)
            .await;
        assert_eq!(resp.status(), 400);
    }
}
