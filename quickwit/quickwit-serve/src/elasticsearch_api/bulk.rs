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

use std::collections::HashMap;
use std::time::Instant;

use http_serde::http::StatusCode;
use quickwit_config::{disable_ingest_v1, enable_ingest_v2};
use quickwit_ingest::{
    CommitType, DocBatchBuilder, IngestRequest, IngestService, IngestServiceClient,
};
use quickwit_proto::ingest::router::IngestRouterServiceClient;
use quickwit_proto::types::IndexId;
use warp::{Filter, Rejection};

use super::bulk_v2::{elastic_bulk_ingest_v2, ElasticBulkResponse};
use crate::elasticsearch_api::filter::{elastic_bulk_filter, elastic_index_bulk_filter};
use crate::elasticsearch_api::make_elastic_api_response;
use crate::elasticsearch_api::model::{BulkAction, ElasticBulkOptions, ElasticsearchError};
use crate::format::extract_format_from_qs;
use crate::ingest_api::lines;
use crate::rest::recover_fn;
use crate::{with_arg, Body};

/// POST `_elastic/_bulk`
pub fn es_compat_bulk_handler(
    ingest_service: IngestServiceClient,
    ingest_router: IngestRouterServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_bulk_filter()
        .and(with_arg(ingest_service))
        .and(with_arg(ingest_router))
        .then(|body, bulk_options, ingest_service, ingest_router| {
            elastic_ingest_bulk(None, body, bulk_options, ingest_service, ingest_router)
        })
        .and(extract_format_from_qs())
        .map(make_elastic_api_response)
        .recover(recover_fn)
}

/// POST `_elastic/<index>/_bulk`
pub fn es_compat_index_bulk_handler(
    ingest_service: IngestServiceClient,
    ingest_router: IngestRouterServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_index_bulk_filter()
        .and(with_arg(ingest_service))
        .and(with_arg(ingest_router))
        .then(
            |index_id, body, bulk_options, ingest_service, ingest_router| {
                elastic_ingest_bulk(
                    Some(index_id),
                    body,
                    bulk_options,
                    ingest_service,
                    ingest_router,
                )
            },
        )
        .and(extract_format_from_qs())
        .map(make_elastic_api_response)
        .recover(recover_fn)
}

async fn elastic_ingest_bulk(
    default_index_id: Option<IndexId>,
    body: Body,
    bulk_options: ElasticBulkOptions,
    ingest_service: IngestServiceClient,
    ingest_router: IngestRouterServiceClient,
) -> Result<ElasticBulkResponse, ElasticsearchError> {
    if enable_ingest_v2() || bulk_options.enable_ingest_v2 {
        return elastic_bulk_ingest_v2(default_index_id, body, bulk_options, ingest_router).await;
    }
    if disable_ingest_v1() {
        return Err(ElasticsearchError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "ingest v1 is disabled: environment variable `QW_DISABLE_INGEST_V1` is set".to_string(),
            None,
        ));
    }
    let now = Instant::now();
    let mut doc_batch_builders = HashMap::new();
    let mut lines = lines(&body.content).enumerate();

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
    use std::sync::Arc;
    use std::time::Duration;

    use http_serde::http::StatusCode;
    use quickwit_config::{IngestApiConfig, NodeConfig};
    use quickwit_index_management::IndexService;
    use quickwit_ingest::{FetchRequest, IngestServiceClient, SuggestTruncateRequest};
    use quickwit_metastore::metastore_for_test;
    use quickwit_proto::ingest::router::IngestRouterServiceClient;
    use quickwit_proto::metastore::MetastoreServiceClient;
    use quickwit_search::MockSearchService;
    use quickwit_storage::StorageResolver;

    use crate::elasticsearch_api::bulk_v2::ElasticBulkResponse;
    use crate::elasticsearch_api::elastic_api_handlers;
    use crate::elasticsearch_api::model::ElasticsearchError;
    use crate::ingest_api::setup_ingest_service;

    #[tokio::test]
    async fn test_bulk_api_returns_404_if_index_id_does_not_exist() {
        let config = Arc::new(NodeConfig::for_test());
        let search_service = Arc::new(MockSearchService::new());
        let (universe, _temp_dir, ingest_service, _) =
            setup_ingest_service(&["my-index"], &IngestApiConfig::default()).await;
        let ingest_router = IngestRouterServiceClient::mocked();
        let index_service =
            IndexService::new(metastore_for_test(), StorageResolver::unconfigured());
        let elastic_api_handlers = elastic_api_handlers(
            config,
            search_service,
            ingest_service,
            ingest_router,
            MetastoreServiceClient::mocked(),
            index_service,
        );
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
        let ingest_router = IngestRouterServiceClient::mocked();
        let index_service =
            IndexService::new(metastore_for_test(), StorageResolver::unconfigured());
        let elastic_api_handlers = elastic_api_handlers(
            config,
            search_service,
            ingest_service,
            ingest_router,
            MetastoreServiceClient::mocked(),
            index_service,
        );
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
        let bulk_response: ElasticBulkResponse = serde_json::from_slice(resp.body()).unwrap();
        assert!(!bulk_response.errors);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_bulk_api_returns_200_if_payload_has_blank_lines() {
        let config = Arc::new(NodeConfig::for_test());
        let search_service = Arc::new(MockSearchService::new());
        let (universe, _temp_dir, ingest_service, _) =
            setup_ingest_service(&["my-index-1"], &IngestApiConfig::default()).await;
        let ingest_router = IngestRouterServiceClient::mocked();
        let index_service =
            IndexService::new(metastore_for_test(), StorageResolver::unconfigured());
        let elastic_api_handlers = elastic_api_handlers(
            config,
            search_service,
            ingest_service,
            ingest_router,
            MetastoreServiceClient::mocked(),
            index_service,
        );
        let payload = "
            {\"create\": {\"_index\": \"my-index-1\", \"_id\": \"1674834324802805760\"}}
            \u{20}\u{20}\u{20}\u{20}\n
            {\"_line\": {\"message\": \"hello-world\"}}";
        let resp = warp::test::request()
            .path("/_elastic/_bulk")
            .method("POST")
            .body(payload)
            .reply(&elastic_api_handlers)
            .await;
        assert_eq!(resp.status(), 200);
        let bulk_response: ElasticBulkResponse = serde_json::from_slice(resp.body()).unwrap();
        assert!(!bulk_response.errors);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_bulk_index_api_returns_200() {
        let config = Arc::new(NodeConfig::for_test());
        let search_service = Arc::new(MockSearchService::new());
        let (universe, _temp_dir, ingest_service, _) =
            setup_ingest_service(&["my-index-1", "my-index-2"], &IngestApiConfig::default()).await;
        let ingest_router = IngestRouterServiceClient::mocked();
        let index_service =
            IndexService::new(metastore_for_test(), StorageResolver::unconfigured());
        let elastic_api_handlers = elastic_api_handlers(
            config,
            search_service,
            ingest_service,
            ingest_router,
            MetastoreServiceClient::mocked(),
            index_service,
        );
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
        let bulk_response: ElasticBulkResponse = serde_json::from_slice(resp.body()).unwrap();
        assert!(!bulk_response.errors);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_bulk_api_blocks_when_refresh_wait_for_is_specified() {
        let config = Arc::new(NodeConfig::for_test());
        let search_service = Arc::new(MockSearchService::new());
        let (universe, _temp_dir, ingest_service, ingest_service_mailbox) =
            setup_ingest_service(&["my-index-1", "my-index-2"], &IngestApiConfig::default()).await;
        let ingest_router = IngestRouterServiceClient::mocked();
        let index_service =
            IndexService::new(metastore_for_test(), StorageResolver::unconfigured());
        let elastic_api_handlers = elastic_api_handlers(
            config,
            search_service,
            ingest_service,
            ingest_router,
            MetastoreServiceClient::mocked(),
            index_service,
        );
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
            let bulk_response: ElasticBulkResponse = serde_json::from_slice(resp.body()).unwrap();
            assert!(!bulk_response.errors);
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
        let ingest_router = IngestRouterServiceClient::mocked();
        let index_service =
            IndexService::new(metastore_for_test(), StorageResolver::unconfigured());
        let elastic_api_handlers = elastic_api_handlers(
            config,
            search_service,
            ingest_service,
            ingest_router,
            MetastoreServiceClient::mocked(),
            index_service,
        );
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
            let bulk_response: ElasticBulkResponse = serde_json::from_slice(resp.body()).unwrap();
            assert!(!bulk_response.errors);
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
        let ingest_service = IngestServiceClient::mocked();
        let ingest_router = IngestRouterServiceClient::mocked();
        let index_service =
            IndexService::new(metastore_for_test(), StorageResolver::unconfigured());
        let elastic_api_handlers = elastic_api_handlers(
            config,
            search_service,
            ingest_service,
            ingest_router,
            MetastoreServiceClient::mocked(),
            index_service,
        );
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
        let es_error: ElasticsearchError = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(es_error.status, StatusCode::BAD_REQUEST);
        assert_eq!(
            es_error.error.reason.unwrap(),
            "Malformed action/metadata line [#0]. Details: `expected value at line 1 column 57`"
        );
    }
}
