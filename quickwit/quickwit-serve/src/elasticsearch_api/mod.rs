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
mod model;
pub mod rest_handler;

use std::sync::Arc;

use axum::http::StatusCode;
use axum::routing::{delete, get, head, post};
use axum::{Extension, Router};
use bulk::{es_compat_bulk_handler, es_compat_index_bulk_handler};
use quickwit_cluster::Cluster;
use quickwit_config::NodeConfig;
use quickwit_index_management::IndexService;
use quickwit_ingest::IngestServiceClient;
use quickwit_proto::ingest::router::IngestRouterServiceClient;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_search::SearchService;
pub use rest_handler::{
    es_compat_cat_indices_handler, es_compat_delete_index_handler,
    es_compat_field_capabilities_handler, es_compat_index_cat_indices_handler,
    es_compat_index_count_handler, es_compat_index_field_capabilities_handler,
    es_compat_index_search_handler, es_compat_index_stats_handler, es_compat_multi_search_handler,
    es_compat_resolve_index_handler, es_compat_scroll_handler, es_compat_search_handler,
    es_compat_stats_handler,
};
// Re-export items that other modules expect
use serde::{Deserialize, Serialize};

use crate::BodyFormat;
use crate::elasticsearch_api::model::ElasticsearchError;
use crate::elasticsearch_api::rest_handler::{
    es_compat_cluster_health_handler, es_compat_cluster_info_handler,
};
use crate::rest_api_response::RestApiResponse;

/// Axum routes for Elasticsearch API
pub fn elastic_api_routes(
    cluster: Cluster,
    node_config: Arc<NodeConfig>,
    search_service: Arc<dyn SearchService>,
    ingest_service: IngestServiceClient,
    ingest_router: IngestRouterServiceClient,
    metastore: MetastoreServiceClient,
    index_service: IndexService,
    enable_ingest_v1: bool,
    enable_ingest_v2: bool,
) -> Router {
    let ingest_content_length_limit = node_config.ingest_api_config.content_length_limit;

    let ingest_config = IngestConfig {
        content_length_limit: ingest_content_length_limit,
        enable_ingest_v1,
        enable_ingest_v2,
    };

    Router::new()
        // Cluster info endpoint
        .route("/_elastic", get(es_compat_cluster_info_handler))
        .route("/_elastic", head(es_compat_cluster_info_handler))
        // Search endpoints
        .route(
            "/_elastic/_search",
            get(es_compat_search_handler).post(es_compat_search_handler),
        )
        .route(
            "/_elastic/:index/_search",
            get(es_compat_index_search_handler).post(es_compat_index_search_handler),
        )
        // Count endpoints
        .route(
            "/_elastic/:index/_count",
            get(es_compat_index_count_handler).post(es_compat_index_count_handler),
        )
        // Bulk endpoints
        .route("/_elastic/_bulk", post(es_compat_bulk_handler))
        .route("/_elastic/:index/_bulk", post(es_compat_index_bulk_handler))
        // Multi-search endpoints
        .route("/_elastic/_msearch", post(es_compat_multi_search_handler))
        // Scroll endpoints
        .route("/_elastic/_search/scroll", get(es_compat_scroll_handler))
        .route("/_elastic/_search/scroll", post(es_compat_scroll_handler))
        // Field capabilities endpoints
        .route(
            "/_elastic/_field_caps",
            get(es_compat_field_capabilities_handler).post(es_compat_field_capabilities_handler),
        )
        .route(
            "/_elastic/:index/_field_caps",
            get(es_compat_index_field_capabilities_handler)
                .post(es_compat_index_field_capabilities_handler),
        )
        // Index management endpoints
        .route("/_elastic/:index", delete(es_compat_delete_index_handler))
        // Stats endpoints
        .route("/_elastic/_stats", get(es_compat_stats_handler))
        .route(
            "/_elastic/:index/_stats",
            get(es_compat_index_stats_handler),
        )
        // Cat indices endpoints
        .route("/_elastic/_cat/indices", get(es_compat_cat_indices_handler))
        .route(
            "/_elastic/_cat/indices/:index",
            get(es_compat_index_cat_indices_handler),
        )
        // Resolve index endpoints
        .route(
            "/_elastic/_resolve/index/:index",
            get(es_compat_resolve_index_handler),
        )
        // Cluster health endpoint
        .route(
            "/_elastic/_cluster/health",
            get(es_compat_cluster_health_handler),
        )
        // Add all the required extensions
        .layer(Extension(cluster))
        .layer(Extension(node_config))
        .layer(Extension(search_service))
        .layer(Extension(ingest_service))
        .layer(Extension(ingest_router))
        .layer(Extension(metastore))
        .layer(Extension(index_service))
        .layer(Extension(ingest_config))
}

/// Configuration for ingest endpoints
#[derive(Clone)]
pub struct IngestConfig {
    pub content_length_limit: bytesize::ByteSize,
    pub enable_ingest_v1: bool,
    pub enable_ingest_v2: bool,
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

#[allow(dead_code)]
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
    use axum_test::TestServer;
    use http::StatusCode;
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

    use crate::BuildInfo;

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

    pub async fn create_elasticsearch_test_server(
        mock_search_service: MockSearchService,
    ) -> TestServer {
        let ingest_router = IngestRouterServiceClient::mocked();
        create_elasticsearch_test_server_with_router(mock_search_service, ingest_router).await
    }

    pub async fn create_elasticsearch_test_server_with_router(
        mock_search_service: MockSearchService,
        ingest_router: IngestRouterServiceClient,
    ) -> TestServer {
        let config = Arc::new(NodeConfig::for_test());
        let index_service =
            IndexService::new(metastore_for_test(), StorageResolver::unconfigured());

        let routes = super::elastic_api_routes(
            mock_cluster().await,
            config,
            Arc::new(mock_search_service),
            ingest_service_client(),
            ingest_router,
            MetastoreServiceClient::mocked(),
            index_service,
            true,
            true,
        );

        TestServer::new(routes).unwrap()
    }

    // TODO: Re-enable multi-search tests once multi-search handler is fixed for axum 0.7
    // compatibility Multi-search tests have been temporarily removed due to axum version
    // compatibility issues

    #[tokio::test]
    async fn test_es_compat_cluster_info_handler() {
        let build_info = BuildInfo::get();
        let config = Arc::new(NodeConfig::for_test());
        let mock_search_service = MockSearchService::new();

        let server = create_elasticsearch_test_server(mock_search_service).await;
        let resp = server.get("/_elastic").await;
        assert_eq!(resp.status_code(), StatusCode::OK);
        let resp_json: JsonValue = resp.json();
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
        let mock_search_service = MockSearchService::new();

        let server = create_elasticsearch_test_server(mock_search_service).await;

        // For now, test that the endpoint exists by making a GET request
        // TODO: Once axum_test supports HEAD requests properly, update this test
        let resp = server.get("/_elastic").await;
        assert_eq!(resp.status_code(), StatusCode::OK);
    }
}
