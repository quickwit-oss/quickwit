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

mod bulk;
mod bulk_v2;
mod filter;
mod model;
mod rest_handler;

use std::sync::Arc;

use bulk::{es_compat_bulk_handler, es_compat_index_bulk_handler};
pub use filter::ElasticCompatibleApi;
use quickwit_cluster::Cluster;
use quickwit_config::NodeConfig;
use quickwit_index_management::IndexService;
use quickwit_ingest::IngestServiceClient;
use quickwit_proto::ingest::router::IngestRouterServiceClient;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_search::SearchService;
use rest_handler::es_compat_cluster_health_handler;
pub use rest_handler::{
    es_compat_cat_indices_handler, es_compat_cluster_info_handler, es_compat_delete_index_handler,
    es_compat_index_cat_indices_handler, es_compat_index_count_handler,
    es_compat_index_field_capabilities_handler, es_compat_index_multi_search_handler,
    es_compat_index_search_handler, es_compat_index_stats_handler, es_compat_resolve_index_handler,
    es_compat_scroll_handler, es_compat_search_handler, es_compat_stats_handler,
};
use serde::{Deserialize, Serialize};
use warp::hyper::StatusCode;
use warp::{Filter, Rejection};

use crate::elasticsearch_api::model::ElasticsearchError;
use crate::rest::recover_fn;
use crate::rest_api_response::RestApiResponse;
use crate::{BodyFormat, BuildInfo};

/// Setup Elasticsearch API handlers
///
/// This is where all newly supported Elasticsearch handlers
/// should be registered.
#[allow(clippy::too_many_arguments)] // Will go away when we remove ingest v1.
pub fn elastic_api_handlers(
    cluster: Cluster,
    node_config: Arc<NodeConfig>,
    search_service: Arc<dyn SearchService>,
    ingest_service: IngestServiceClient,
    ingest_router: IngestRouterServiceClient,
    metastore: MetastoreServiceClient,
    index_service: IndexService,
    enable_ingest_v1: bool,
    enable_ingest_v2: bool,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    let ingest_content_length_limit = node_config.ingest_api_config.content_length_limit;
    es_compat_cluster_info_handler(node_config, BuildInfo::get())
        .or(es_compat_search_handler(search_service.clone()))
        .or(es_compat_bulk_handler(
            ingest_service.clone(),
            ingest_router.clone(),
            ingest_content_length_limit,
            enable_ingest_v1,
            enable_ingest_v2,
        ))
        .boxed()
        .or(es_compat_index_bulk_handler(
            ingest_service,
            ingest_router,
            ingest_content_length_limit,
            enable_ingest_v1,
            enable_ingest_v2,
        ))
        .or(es_compat_index_search_handler(search_service.clone()))
        .or(es_compat_index_count_handler(search_service.clone()))
        .or(es_compat_scroll_handler(search_service.clone()))
        .or(es_compat_index_multi_search_handler(search_service.clone()))
        .or(es_compat_index_field_capabilities_handler(
            search_service.clone(),
        ))
        .boxed()
        .or(es_compat_index_stats_handler(metastore.clone()))
        .or(es_compat_delete_index_handler(index_service))
        .or(es_compat_stats_handler(metastore.clone()))
        .or(es_compat_cluster_health_handler(cluster))
        .or(es_compat_index_cat_indices_handler(metastore.clone()))
        .or(es_compat_cat_indices_handler(metastore.clone()))
        .or(es_compat_resolve_index_handler(metastore.clone()))
        .recover(recover_fn)
        .boxed()
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
    use quickwit_cluster::{ChannelTransport, Cluster, create_cluster_for_test};
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
    use crate::BuildInfo;
    use crate::elasticsearch_api::rest_handler::es_compat_cluster_info_handler;
    use crate::rest::recover_fn;

    fn ingest_service_client() -> IngestServiceClient {
        let universe = quickwit_actors::Universe::new();
        let (ingest_service_mailbox, _) = universe.create_test_mailbox::<IngestApiService>();
        IngestServiceClient::from_mailbox(ingest_service_mailbox)
    }

    pub async fn mock_cluster() -> Cluster {
        let transport = ChannelTransport::default();
        create_cluster_for_test(Vec::new(), &[], &transport, false)
            .await
            .unwrap()
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
            mock_cluster().await,
            config,
            Arc::new(mock_search_service),
            ingest_service_client(),
            ingest_router,
            MetastoreServiceClient::mocked(),
            index_service,
            true,
            false,
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
        let es_msearch_response: serde_json::Value = serde_json::from_str(&string_body).unwrap();
        let responses = es_msearch_response
            .get("responses")
            .unwrap()
            .as_array()
            .unwrap();
        assert_eq!(responses.len(), 2);
        for response in responses {
            assert_eq!(response.get("status").unwrap().as_u64().unwrap(), 200);
            assert_eq!(response.get("error"), None);
            response.get("hits").unwrap();
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
            mock_cluster().await,
            config,
            Arc::new(mock_search_service),
            ingest_service_client(),
            ingest_router,
            MetastoreServiceClient::mocked(),
            index_service,
            true,
            false,
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
        let es_msearch_response: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
        let responses = es_msearch_response
            .get("responses")
            .unwrap()
            .as_array()
            .unwrap();
        assert_eq!(responses.len(), 2);
        assert_eq!(responses[0].get("status").unwrap().as_u64().unwrap(), 200);
        assert_eq!(responses[0].get("error"), None);
        assert_eq!(responses[1].get("status").unwrap().as_u64().unwrap(), 500);
        assert_eq!(responses[1].get("hits"), None);
        let error_cause = responses[1].get("error").unwrap();
        assert_eq!(
            error_cause.get("reason").unwrap().as_str().unwrap(),
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
            mock_cluster().await,
            config,
            Arc::new(mock_search_service),
            ingest_service_client(),
            ingest_router,
            MetastoreServiceClient::mocked(),
            index_service,
            true,
            false,
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
        assert!(
            es_error
                .error
                .reason
                .unwrap()
                .starts_with("Invalid argument: failed to parse request header")
        );
    }

    #[tokio::test]
    async fn test_msearch_api_return_400_with_malformed_request_body() {
        let config = Arc::new(NodeConfig::for_test());
        let mock_search_service = MockSearchService::new();

        let ingest_router = IngestRouterServiceClient::mocked();
        let index_service =
            IndexService::new(metastore_for_test(), StorageResolver::unconfigured());
        let es_search_api_handler = elastic_api_handlers(
            mock_cluster().await,
            config,
            Arc::new(mock_search_service),
            ingest_service_client(),
            ingest_router,
            MetastoreServiceClient::mocked(),
            index_service,
            true,
            false,
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
        assert!(
            es_error
                .error
                .reason
                .unwrap()
                .starts_with("Invalid argument: failed to parse request body")
        );
    }

    #[tokio::test]
    async fn test_msearch_api_return_400_with_only_a_header_request() {
        let config = Arc::new(NodeConfig::for_test());
        let mock_search_service = MockSearchService::new();

        let ingest_router = IngestRouterServiceClient::mocked();
        let index_service =
            IndexService::new(metastore_for_test(), StorageResolver::unconfigured());
        let es_search_api_handler = super::elastic_api_handlers(
            mock_cluster().await,
            config,
            Arc::new(mock_search_service),
            ingest_service_client(),
            ingest_router,
            MetastoreServiceClient::mocked(),
            index_service,
            true,
            false,
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
        assert!(
            es_error
                .error
                .reason
                .unwrap()
                .starts_with("Invalid argument: expect request body after request header")
        );
    }

    #[tokio::test]
    async fn test_msearch_api_return_400_with_no_index() {
        let config = Arc::new(NodeConfig::for_test());
        let mock_search_service = MockSearchService::new();

        let ingest_router = IngestRouterServiceClient::mocked();
        let index_service =
            IndexService::new(metastore_for_test(), StorageResolver::unconfigured());
        let es_search_api_handler = super::elastic_api_handlers(
            mock_cluster().await,
            config,
            Arc::new(mock_search_service),
            ingest_service_client(),
            ingest_router,
            MetastoreServiceClient::mocked(),
            index_service,
            true,
            false,
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
            mock_cluster().await,
            config,
            Arc::new(mock_search_service),
            ingest_service_client(),
            ingest_router,
            MetastoreServiceClient::mocked(),
            index_service,
            true,
            false,
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
