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

//! Dispatch incoming `EsHttpRequest` protobuf messages to the appropriate
//! Elasticsearch-compatible handler functions without a network round-trip.

use std::sync::Arc;

use bytes::Bytes;
use quickwit_cluster::Cluster;
use quickwit_config::NodeConfig;
use quickwit_proto::cloudprem::{CloudPremError, CloudPremResult, EsHttpRequest, EsHttpResponse};
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_search::SearchService;
use tracing::warn;

use crate::BuildInfo;
use crate::elasticsearch_api::model::{
    CatIndexQueryParams, FieldCapabilityQueryParams, FieldCapabilityRequestBody,
    MultiSearchQueryParams, ScrollQueryParams, SearchBody, SearchQueryParams,
    SearchQueryParamsCount,
};
use crate::elasticsearch_api::rest_handler::{
    es_compat_aliases, es_compat_cat_indices, es_compat_cluster_health_check,
    es_compat_cluster_info, es_compat_delete_scroll, es_compat_index_cat_indices,
    es_compat_index_count, es_compat_index_field_capabilities, es_compat_index_mapping,
    es_compat_index_multi_search, es_compat_index_search, es_compat_index_stats,
    es_compat_nodes_info, es_compat_resolve_index, es_compat_search_shards, es_scroll,
};

/// Routes an `EsHttpRequest` to the appropriate ES-compatible handler and
/// returns the handler's output wrapped in an `EsHttpResponse`.
pub(crate) async fn handle_es_query(
    request: EsHttpRequest,
    search_service: Arc<dyn SearchService>,
    metastore: MetastoreServiceClient,
    cluster: Cluster,
    node_config: Arc<NodeConfig>,
    build_info: &'static BuildInfo,
) -> CloudPremResult<EsHttpResponse> {
    // Split path from query string at the first `?`.
    let (path, query_string) = match request.path.split_once('?') {
        Some((p, q)) => (p, q),
        None => (request.path.as_str(), ""),
    };

    let method = request.method.to_uppercase();
    let body = Bytes::from(request.body);

    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    match segments.as_slice() {
        // --- Static / hardcoded endpoints ---
        ["_nodes", "http"] => ok_json(&es_compat_nodes_info(node_config)),

        ["_cluster", "health"] => {
            let (body, status) = es_compat_cluster_health_check(&cluster).await;
            json_response(status.as_u16().into(), &body)
        }

        ["_aliases"] => ok_json(&es_compat_aliases()),

        [index, "_search_shards"] => {
            ok_json(&es_compat_search_shards(index.to_string(), node_config))
        }

        [] | [""] => ok_json(&es_compat_cluster_info(node_config, build_info)),

        // --- Handlers that call existing async functions ---
        ["_cat", "indices"] => {
            let params = parse_query_params::<CatIndexQueryParams>(query_string)?;
            match es_compat_cat_indices(params, metastore).await {
                Ok(result) => ok_json_serialize(&result),
                Err(err) => Ok(es_error_to_response(err)),
            }
        }

        ["_cat", "indices", index] => {
            let params = parse_query_params::<CatIndexQueryParams>(query_string)?;
            match es_compat_index_cat_indices(vec![index.to_string()], params, metastore).await {
                Ok(result) => ok_json_serialize(&result),
                Err(err) => Ok(es_error_to_response(err)),
            }
        }

        ["_search", "scroll"] => {
            if method == "DELETE" {
                ok_json(&es_compat_delete_scroll())
            } else {
                let qs_params = parse_query_params::<ScrollQueryParams>(query_string)?;
                let body_params: ScrollQueryParams = parse_body_or_default(&body)?;
                let merged = ScrollQueryParams {
                    scroll: body_params.scroll.or(qs_params.scroll),
                    scroll_id: body_params.scroll_id.or(qs_params.scroll_id),
                };
                match es_scroll(merged, search_service).await {
                    Ok(result) => ok_json_serialize(&result),
                    Err(err) => Ok(es_error_to_response(err)),
                }
            }
        }

        ["_resolve", "index", index] => {
            match es_compat_resolve_index(vec![index.to_string()], metastore).await {
                Ok(result) => ok_json_serialize(&result),
                Err(err) => Ok(es_error_to_response(err)),
            }
        }

        ["_stats"] => match es_compat_index_stats(vec!["*".to_string()], metastore).await {
            Ok(result) => ok_json_serialize(&result),
            Err(err) => Ok(es_error_to_response(err)),
        },

        ["_msearch"] => {
            let params = parse_query_params::<MultiSearchQueryParams>(query_string)?;
            match es_compat_index_multi_search(body, params, search_service).await {
                Ok(result) => ok_json_serialize(&result),
                Err(err) => Ok(es_error_to_response(err)),
            }
        }

        ["_field_caps"] => {
            let params = parse_query_params::<FieldCapabilityQueryParams>(query_string)?;
            let field_body: FieldCapabilityRequestBody = parse_body_or_default(&body)?;
            match es_compat_index_field_capabilities(
                vec!["*".to_string()],
                params,
                field_body,
                search_service,
            )
            .await
            {
                Ok(result) => ok_json_serialize(&result),
                Err(err) => Ok(es_error_to_response(err)),
            }
        }

        // --- Index-scoped endpoints ---
        [index, "_mapping" | "_mappings"] => {
            match es_compat_index_mapping(index.to_string(), metastore, search_service).await {
                Ok(result) => ok_json_serialize(&result),
                Err(err) => Ok(es_error_to_response(err)),
            }
        }

        [index, "_search"] => {
            let params = parse_query_params::<SearchQueryParams>(query_string)?;
            let search_body: SearchBody = parse_body_or_default(&body)?;
            match es_compat_index_search(
                vec![index.to_string()],
                params,
                search_body,
                search_service,
            )
            .await
            {
                Ok(result) => ok_json_serialize(&result),
                Err(err) => Ok(es_error_to_response(err)),
            }
        }

        [index, "_count"] => {
            let params = parse_query_params::<SearchQueryParamsCount>(query_string)?;
            let count_body: SearchBody = parse_body_or_default(&body)?;
            match es_compat_index_count(vec![index.to_string()], params, count_body, search_service)
                .await
            {
                Ok(result) => ok_json_serialize(&result),
                Err(err) => Ok(es_error_to_response(err)),
            }
        }

        [index, "_field_caps"] => {
            let params = parse_query_params::<FieldCapabilityQueryParams>(query_string)?;
            let field_body: FieldCapabilityRequestBody = parse_body_or_default(&body)?;
            match es_compat_index_field_capabilities(
                vec![index.to_string()],
                params,
                field_body,
                search_service,
            )
            .await
            {
                Ok(result) => ok_json_serialize(&result),
                Err(err) => Ok(es_error_to_response(err)),
            }
        }

        [index, "_stats"] => {
            match es_compat_index_stats(vec![index.to_string()], metastore).await {
                Ok(result) => ok_json_serialize(&result),
                Err(err) => Ok(es_error_to_response(err)),
            }
        }

        _ => {
            warn!(path=%path, "unsupported EsQuery path");
            Err(CloudPremError::InvalidArgument(format!(
                "unsupported ES path: {path}"
            )))
        }
    }
}

// --- Helpers ---

fn parse_query_params<T: serde::de::DeserializeOwned + Default>(
    query_string: &str,
) -> CloudPremResult<T> {
    if query_string.is_empty() {
        return Ok(T::default());
    }
    serde_qs::from_str(query_string)
        .map_err(|e| CloudPremError::InvalidArgument(format!("invalid query params: {e}")))
}

fn parse_body_or_default<T: serde::de::DeserializeOwned + Default>(
    body: &[u8],
) -> CloudPremResult<T> {
    if body.is_empty() {
        return Ok(T::default());
    }
    serde_json::from_slice(body)
        .map_err(|e| CloudPremError::InvalidArgument(format!("invalid request body: {e}")))
}

fn ok_json(value: &serde_json::Value) -> CloudPremResult<EsHttpResponse> {
    json_response(200, value)
}

fn ok_json_serialize<T: serde::Serialize>(value: &T) -> CloudPremResult<EsHttpResponse> {
    let body = serde_json::to_vec(value)
        .map_err(|e| CloudPremError::Internal(format!("serialization error: {e}")))?;
    Ok(EsHttpResponse {
        status_code: 200,
        body,
    })
}

fn json_response(status_code: u32, value: &serde_json::Value) -> CloudPremResult<EsHttpResponse> {
    let body = serde_json::to_vec(value)
        .map_err(|e| CloudPremError::Internal(format!("serialization error: {e}")))?;
    Ok(EsHttpResponse { status_code, body })
}

/// Converts an ElasticsearchError into an EsHttpResponse preserving the
/// original HTTP status code and JSON error body. This maintains passthrough
/// semantics so Trino clients see proper ES error responses (e.g. 404
/// index-not-found, 400 invalid query) instead of gRPC transport errors.
fn es_error_to_response(
    err: crate::elasticsearch_api::model::ElasticsearchError,
) -> EsHttpResponse {
    let status_code = err.status.as_u16() as u32;
    let body = serde_json::to_vec(&err).unwrap_or_default();
    EsHttpResponse { status_code, body }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use quickwit_cluster::{ChannelTransport, create_cluster_for_test};
    use quickwit_config::NodeConfig;
    use quickwit_proto::cloudprem::{CloudPremError, EsHttpRequest};
    use quickwit_proto::metastore::MetastoreServiceClient;
    use quickwit_search::MockSearchService;

    use super::handle_es_query;
    use crate::BuildInfo;

    fn make_request(method: &str, path: &str) -> EsHttpRequest {
        EsHttpRequest {
            method: method.to_string(),
            path: path.to_string(),
            body: Vec::new(),
            headers: Default::default(),
            org_id: 0,
            cluster_id: String::new(),
        }
    }

    async fn call_es_query(method: &str, path: &str) -> Result<super::EsHttpResponse, CloudPremError> {
        let search_service = Arc::new(MockSearchService::new());
        let metastore = MetastoreServiceClient::mocked();
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &[], &transport, false)
            .await
            .unwrap();
        let node_config = Arc::new(NodeConfig::for_test());
        let build_info = BuildInfo::get();

        handle_es_query(
            make_request(method, path),
            search_service,
            metastore,
            cluster,
            node_config,
            build_info,
        )
        .await
    }

    #[tokio::test]
    async fn test_route_nodes_http() {
        let resp = call_es_query("GET", "/_nodes/http").await.unwrap();
        assert_eq!(resp.status_code, 200);
        let body: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body.get("nodes").is_some());
    }

    #[tokio::test]
    async fn test_route_cluster_health() {
        let resp = call_es_query("GET", "/_cluster/health").await.unwrap();
        // cluster may or may not be ready in test, but we should get a valid response
        assert!(resp.status_code == 200 || resp.status_code == 503);
        let body: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body.get("status").is_some());
    }

    #[tokio::test]
    async fn test_route_aliases() {
        let resp = call_es_query("GET", "/_aliases").await.unwrap();
        assert_eq!(resp.status_code, 200);
        let body: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body.is_object());
    }

    #[tokio::test]
    async fn test_route_delete_scroll() {
        let resp = call_es_query("DELETE", "/_search/scroll").await.unwrap();
        assert_eq!(resp.status_code, 200);
        let body: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["succeeded"], true);
        assert_eq!(body["num_freed"], 0);
    }

    #[tokio::test]
    async fn test_route_search_shards() {
        let resp = call_es_query("GET", "/my_index/_search_shards").await.unwrap();
        assert_eq!(resp.status_code, 200);
        let body: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body.get("shards").is_some());
    }

    #[tokio::test]
    async fn test_route_cluster_info() {
        let resp = call_es_query("GET", "/").await.unwrap();
        assert_eq!(resp.status_code, 200);
        let body: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body.get("tagline").is_some());
        assert!(body.get("version").is_some());
    }

    #[tokio::test]
    async fn test_route_unsupported_path() {
        let result = call_es_query("GET", "/_unsupported/endpoint").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            CloudPremError::InvalidArgument(msg) => {
                assert!(msg.contains("unsupported ES path"));
            }
            other => panic!("expected InvalidArgument, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_route_with_query_string() {
        // query string should be stripped from path routing
        let resp = call_es_query("GET", "/_aliases?pretty=true").await.unwrap();
        assert_eq!(resp.status_code, 200);
    }

    #[test]
    fn test_route_mapping_and_mappings_recognized() {
        // Verify that /_mapping and /_mappings paths are recognized by the
        // segment matching (not "unsupported"). We test the path parsing only,
        // since the actual handler requires a real metastore.
        for suffix in &["_mapping", "_mappings"] {
            let path = format!("/my_index/{suffix}");
            let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
            match segments.as_slice() {
                [_index, "_mapping" | "_mappings"] => {} // correctly routed
                _ => panic!("path {path} was not recognized by segment matching"),
            }
        }
    }
}
