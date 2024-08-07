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

mod bulk;
mod bulk_v2;
mod filter;
mod model;
mod rest_handler;

use std::sync::Arc;

use bulk::{es_compat_bulk_handler, es_compat_index_bulk_handler};
pub use filter::ElasticCompatibleApi;
use http_serde::http::StatusCode;
use quickwit_config::NodeConfig;
use quickwit_index_management::IndexService;
use quickwit_ingest::IngestServiceClient;
use quickwit_proto::ingest::router::IngestRouterServiceClient;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_search::SearchService;
pub use rest_handler::{
    es_compat_cat_indices_handler, es_compat_cluster_info_handler, es_compat_delete_index_handler,
    es_compat_index_cat_indices_handler, es_compat_index_count_handler,
    es_compat_index_field_capabilities_handler, es_compat_index_multi_search_handler,
    es_compat_index_search_handler, es_compat_index_stats_handler, es_compat_resolve_index_handler,
    es_compat_scroll_handler, es_compat_search_handler, es_compat_stats_handler,
};
use serde::{Deserialize, Serialize};
use warp::{Filter, Rejection};

use crate::elasticsearch_api::model::ElasticsearchError;
use crate::rest::recover_fn;
use crate::rest_api_response::RestApiResponse;
use crate::{BodyFormat, BuildInfo};

/// Setup Elasticsearch API handlers
///
/// This is where all newly supported Elasticsearch handlers
/// should be registered.
pub fn elastic_api_handlers(
    node_config: Arc<NodeConfig>,
    search_service: Arc<dyn SearchService>,
    ingest_service: IngestServiceClient,
    ingest_router: IngestRouterServiceClient,
    metastore: MetastoreServiceClient,
    index_service: IndexService,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    es_compat_cluster_info_handler(node_config, BuildInfo::get())
        .or(es_compat_search_handler(search_service.clone()))
        .or(es_compat_bulk_handler(
            ingest_service.clone(),
            ingest_router.clone(),
        ))
        .or(es_compat_index_bulk_handler(ingest_service, ingest_router))
        .or(es_compat_index_search_handler(search_service.clone()))
        .or(es_compat_index_count_handler(search_service.clone()))
        .or(es_compat_scroll_handler(search_service.clone()))
        .or(es_compat_index_multi_search_handler(search_service.clone()))
        .or(es_compat_index_field_capabilities_handler(
            search_service.clone(),
        ))
        .or(es_compat_index_stats_handler(metastore.clone()))
        .or(es_compat_delete_index_handler(index_service))
        .or(es_compat_stats_handler(metastore.clone()))
        .or(es_compat_index_cat_indices_handler(metastore.clone()))
        .or(es_compat_cat_indices_handler(metastore.clone()))
        .or(es_compat_resolve_index_handler(metastore.clone()))
        .recover(recover_fn)
    // Register newly created handlers here.
}

/// Helper type needed by the Elasticsearch endpoints.
/// Control how the total number of hits should be tracked.
///
/// When set to `Track` with a value `true`, the response will always track the number of hits that
/// match the query accurately.
///
/// When set to `Count` with an integer value `n`, the response accurately tracks the total
/// hit count that match the query up to `n` documents.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TrackTotalHits {
    /// Track the number of hits that match the query accurately.
    Track(bool),
    /// Track the number of hits up to the specified value.
    Count(i64),
}

impl From<bool> for TrackTotalHits {
    fn from(b: bool) -> Self {
        TrackTotalHits::Track(b)
    }
}

impl From<i64> for TrackTotalHits {
    fn from(i: i64) -> Self {
        TrackTotalHits::Count(i)
    }
}

fn make_elastic_api_response<T: serde::Serialize>(
    elasticsearch_result: Result<T, ElasticsearchError>,
    body_format: BodyFormat,
) -> RestApiResponse {
    let status_code = match &elasticsearch_result {
        Ok(_) => StatusCode::OK,
        Err(error) => error.status,
    };
    RestApiResponse::new(&elasticsearch_result, status_code, body_format)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_json_diff::assert_json_include;
    use mockall::predicate;
    use quickwit_config::NodeConfig;
    use quickwit_index_management::IndexService;
    use quickwit_ingest::{IngestApiService, IngestServiceClient};
    use quickwit_metastore::metastore_for_test;
    use quickwit_proto::ingest::router::IngestRouterServiceClient;
    use quickwit_proto::metastore::MetastoreServiceClient;
    use quickwit_search::MockSearchService;
    use quickwit_storage::StorageResolver;
    use serde_json::Value as JsonValue;
    use warp::Filter;

    use super::elastic_api_handlers;
    use super::model::ElasticsearchError;
    use crate::elasticsearch_api::model::MultiSearchResponse;
    use crate::elasticsearch_api::rest_handler::es_compat_cluster_info_handler;
    use crate::rest::recover_fn;
    use crate::BuildInfo;

    fn ingest_service_client() -> IngestServiceClient {
        let universe = quickwit_actors::Universe::new();
        let (ingest_service_mailbox, _) = universe.create_test_mailbox::<IngestApiService>();
        IngestServiceClient::from_mailbox(ingest_service_mailbox)
    }

    #[tokio::test]
    async fn test_msearch_api_return_200_responses() {
        let config = Arc::new(NodeConfig::for_test());
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_root_search()
            .with(predicate::function(
                |search_request: &quickwit_proto::search::SearchRequest| {
                    (search_request.index_id_patterns == vec!["index-1".to_string()]
                        && search_request.start_offset == 5
                        && search_request.max_hits == 20)
                        || (search_request.index_id_patterns == vec!["index-2".to_string()]
                            && search_request.start_offset == 0
                            && search_request.max_hits == 10)
                },
            ))
            .returning(|_| Ok(Default::default()));
        let ingest_router = IngestRouterServiceClient::mocked();
        let index_service =
            IndexService::new(metastore_for_test(), StorageResolver::unconfigured());
        let es_search_api_handler = super::elastic_api_handlers(
            config,
            Arc::new(mock_search_service),
            ingest_service_client(),
            ingest_router,
            MetastoreServiceClient::mocked(),
            index_service,
        );
        let msearch_payload = r#"
            {"index":"index-1"}
            {"query":{"query_string":{"query":"test"}}, "from": 5, "size": 20}
            {"index":"index-2"}
            {"query":{"query_string":{"query":"test"}}}
            "#;
        let resp = warp::test::request()
            .path("/_elastic/_msearch")
            .method("POST")
            .body(msearch_payload)
            .reply(&es_search_api_handler)
            .await;
        assert_eq!(resp.status(), 200);
        assert!(resp.headers().get("x-elastic-product").is_none(),);
        let string_body = String::from_utf8(resp.body().to_vec()).unwrap();
        let es_msearch_response: MultiSearchResponse = serde_json::from_str(&string_body).unwrap();
        assert_eq!(es_msearch_response.responses.len(), 2);
        for response in es_msearch_response.responses {
            assert_eq!(response.status, 200);
            assert_eq!(response.error, None);
            assert!(response.response.is_some())
        }
    }

    #[tokio::test]
    async fn test_msearch_api_return_one_500_and_one_200_responses() {
        let config = Arc::new(NodeConfig::for_test());
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_root_search()
            .returning(|search_request| {
                if search_request
                    .index_id_patterns
                    .contains(&"index-1".to_string())
                {
                    Ok(Default::default())
                } else {
                    Err(quickwit_search::SearchError::Internal(
                        "something bad happened".to_string(),
                    ))
                }
            });

        let ingest_router = IngestRouterServiceClient::mocked();
        let index_service =
            IndexService::new(metastore_for_test(), StorageResolver::unconfigured());
        let es_search_api_handler = super::elastic_api_handlers(
            config,
            Arc::new(mock_search_service),
            ingest_service_client(),
            ingest_router,
            MetastoreServiceClient::mocked(),
            index_service,
        );
        let msearch_payload = r#"
            {"index":"index-1"}
            {"query":{"query_string":{"query":"test"}}, "from": 5, "size": 10}
            {"index":"index-2"}
            {"query":{"query_string":{"query":"test"}}}
            "#;
        let resp = warp::test::request()
            .path("/_elastic/_msearch")
            .method("POST")
            .body(msearch_payload)
            .reply(&es_search_api_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let es_msearch_response: MultiSearchResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(es_msearch_response.responses.len(), 2);
        assert_eq!(es_msearch_response.responses[0].status, 200);
        assert!(es_msearch_response.responses[0].error.is_none());
        assert_eq!(es_msearch_response.responses[1].status, 500);
        assert!(es_msearch_response.responses[1].response.is_none());
        let error_cause = es_msearch_response.responses[1].error.as_ref().unwrap();
        assert_eq!(
            error_cause.reason.as_ref().unwrap(),
            "internal error: `something bad happened`"
        );
    }

    #[tokio::test]
    async fn test_msearch_api_return_400_with_malformed_request_header() {
        let config = Arc::new(NodeConfig::for_test());
        let mock_search_service = MockSearchService::new();

        let ingest_router = IngestRouterServiceClient::mocked();
        let index_service =
            IndexService::new(metastore_for_test(), StorageResolver::unconfigured());
        let es_search_api_handler = super::elastic_api_handlers(
            config,
            Arc::new(mock_search_service),
            ingest_service_client(),
            ingest_router,
            MetastoreServiceClient::mocked(),
            index_service,
        );
        let msearch_payload = r#"
            {"index":"index-1"
            {"query":{"query_string":{"query":"test"}}}
            "#;
        let resp = warp::test::request()
            .path("/_elastic/_msearch")
            .method("POST")
            .body(msearch_payload)
            .reply(&es_search_api_handler)
            .await;
        assert_eq!(resp.status(), 400);
        let es_error: ElasticsearchError = serde_json::from_slice(resp.body()).unwrap();
        assert!(es_error
            .error
            .reason
            .unwrap()
            .starts_with("Invalid argument: failed to parse request header"));
    }

    #[tokio::test]
    async fn test_msearch_api_return_400_with_malformed_request_body() {
        let config = Arc::new(NodeConfig::for_test());
        let mock_search_service = MockSearchService::new();

        let ingest_router = IngestRouterServiceClient::mocked();
        let index_service =
            IndexService::new(metastore_for_test(), StorageResolver::unconfigured());
        let es_search_api_handler = elastic_api_handlers(
            config,
            Arc::new(mock_search_service),
            ingest_service_client(),
            ingest_router,
            MetastoreServiceClient::mocked(),
            index_service,
        );
        let msearch_payload = r#"
            {"index":"index-1"}
            {"query":{"query_string":{"bad":"test"}}}
            "#;
        let resp = warp::test::request()
            .path("/_elastic/_msearch")
            .method("POST")
            .body(msearch_payload)
            .reply(&es_search_api_handler)
            .await;
        assert_eq!(resp.status(), 400);
        let es_error: ElasticsearchError = serde_json::from_slice(resp.body()).unwrap();
        assert!(es_error
            .error
            .reason
            .unwrap()
            .starts_with("Invalid argument: failed to parse request body"));
    }

    #[tokio::test]
    async fn test_msearch_api_return_400_with_only_a_header_request() {
        let config = Arc::new(NodeConfig::for_test());
        let mock_search_service = MockSearchService::new();

        let ingest_router = IngestRouterServiceClient::mocked();
        let index_service =
            IndexService::new(metastore_for_test(), StorageResolver::unconfigured());
        let es_search_api_handler = super::elastic_api_handlers(
            config,
            Arc::new(mock_search_service),
            ingest_service_client(),
            ingest_router,
            MetastoreServiceClient::mocked(),
            index_service,
        );
        let msearch_payload = r#"
            {"index":"index-1"}
            "#;
        let resp = warp::test::request()
            .path("/_elastic/_msearch")
            .method("POST")
            .body(msearch_payload)
            .reply(&es_search_api_handler)
            .await;
        assert_eq!(resp.status(), 400);
        let es_error: ElasticsearchError = serde_json::from_slice(resp.body()).unwrap();
        assert!(es_error
            .error
            .reason
            .unwrap()
            .starts_with("Invalid argument: expect request body after request header"));
    }

    #[tokio::test]
    async fn test_msearch_api_return_400_with_no_index() {
        let config = Arc::new(NodeConfig::for_test());
        let mock_search_service = MockSearchService::new();

        let ingest_router = IngestRouterServiceClient::mocked();
        let index_service =
            IndexService::new(metastore_for_test(), StorageResolver::unconfigured());
        let es_search_api_handler = super::elastic_api_handlers(
            config,
            Arc::new(mock_search_service),
            ingest_service_client(),
            ingest_router,
            MetastoreServiceClient::mocked(),
            index_service,
        );
        let msearch_payload = r#"
            {}
            {"query":{"query_string":{"bad":"test"}}}
            "#;
        let resp = warp::test::request()
            .path("/_elastic/_msearch")
            .method("POST")
            .body(msearch_payload)
            .reply(&es_search_api_handler)
            .await;
        assert_eq!(resp.status(), 400);
        let es_error: ElasticsearchError = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(
            es_error.error.reason.unwrap(),
            "Invalid argument: `_msearch` request header must define at least one index"
        );
    }

    #[tokio::test]
    async fn test_msearch_api_return_400_with_multiple_indexes() {
        let config = Arc::new(NodeConfig::for_test());
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_root_search()
            .returning(|search_request| {
                if search_request.index_id_patterns
                    == vec!["index-1".to_string(), "index-2".to_string()]
                {
                    Ok(Default::default())
                } else {
                    Err(quickwit_search::SearchError::Internal(
                        "something bad happened".to_string(),
                    ))
                }
            });
        let ingest_router = IngestRouterServiceClient::mocked();
        let index_service =
            IndexService::new(metastore_for_test(), StorageResolver::unconfigured());
        let es_search_api_handler = super::elastic_api_handlers(
            config,
            Arc::new(mock_search_service),
            ingest_service_client(),
            ingest_router,
            MetastoreServiceClient::mocked(),
            index_service,
        );
        let msearch_payload = r#"
            {"index": ["index-1", "index-2"]}
            {"query":{"query_string":{"query":"test"}}}
            "#;
        let resp = warp::test::request()
            .path("/_elastic/_msearch")
            .method("POST")
            .body(msearch_payload)
            .reply(&es_search_api_handler)
            .await;
        assert_eq!(resp.status(), 200);
    }

    #[tokio::test]
    async fn test_es_compat_cluster_info_handler() {
        let build_info = BuildInfo::get();
        let config = Arc::new(NodeConfig::for_test());
        let handler =
            es_compat_cluster_info_handler(config.clone(), build_info).recover(recover_fn);
        let resp = warp::test::request()
            .path("/_elastic")
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), 200);
        let resp_json: JsonValue = serde_json::from_slice(resp.body()).unwrap();
        let expected_response_json = serde_json::json!({
            "name" : config.node_id,
            "cluster_name" : config.cluster_id,
            "version" : {
                "distribution" : "quickwit",
                "number" : build_info.version,
                "build_hash" : build_info.commit_hash,
                "build_date" : build_info.build_date,
            }
        });
        assert_json_include!(actual: resp_json, expected: expected_response_json);
    }

    #[tokio::test]
    async fn test_head_request_on_root_endpoint() {
        let build_info = BuildInfo::get();
        let config = Arc::new(NodeConfig::for_test());
        let handler =
            es_compat_cluster_info_handler(config.clone(), build_info).recover(recover_fn);
        let resp = warp::test::request()
            .path("/_elastic")
            .method("HEAD")
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), 200);
    }
}
