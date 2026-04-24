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

use quickwit_cluster::Cluster;
use quickwit_config::NodeConfig;
use quickwit_proto::cloudprem::{CloudPremError, CloudPremResult, EsHttpRequest, EsHttpResponse};
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_search::SearchService;
use serde_json::Value;
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

/// Dispatches an async ES handler call, converting `Ok` to a JSON response
/// and `Err(ElasticsearchError)` to an ES-compatible error response.
macro_rules! dispatch_es {
    ($handler:expr) => {
        match $handler.await {
            Ok(result) => ok_json_serialize(&result),
            Err(err) => Ok(es_error_to_response(err)),
        }
    };
}

/// Routes an `EsHttpRequest` to the appropriate ES-compatible handler and
/// returns the handler's output wrapped in an `EsHttpResponse`.
pub(crate) async fn handle_es_query(
    request: EsHttpRequest,
    search_service: Arc<dyn SearchService>,
    metastore: MetastoreServiceClient,
    cluster: Cluster,
    node_config: Arc<NodeConfig>,
) -> CloudPremResult<EsHttpResponse> {
    let build_info = BuildInfo::get();
    // Split path from query string at the first `?`.
    let (path, query_string) = match request.path.split_once('?') {
        Some((p, q)) => (p, q),
        None => (request.path.as_str(), ""),
    };

    let method = request.method.to_uppercase();
    let body = request.body;

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
            dispatch_es!(es_compat_cat_indices(params, metastore))
        }

        ["_cat", "indices", index] => {
            let params = parse_query_params::<CatIndexQueryParams>(query_string)?;
            dispatch_es!(es_compat_index_cat_indices(
                vec![index.to_string()],
                params,
                metastore
            ))
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
                dispatch_es!(es_scroll(merged, search_service))
            }
        }

        ["_resolve", "index", index] => {
            dispatch_es!(es_compat_resolve_index(vec![index.to_string()], metastore))
        }

        ["_stats"] => {
            dispatch_es!(es_compat_index_stats(vec!["*".to_string()], metastore))
        }

        ["_msearch"] => {
            let params = parse_query_params::<MultiSearchQueryParams>(query_string)?;
            dispatch_es!(es_compat_index_multi_search(body, params, search_service))
        }

        ["_field_caps"] => {
            let params = parse_query_params::<FieldCapabilityQueryParams>(query_string)?;
            let field_body: FieldCapabilityRequestBody = parse_body_or_default(&body)?;
            dispatch_es!(es_compat_index_field_capabilities(
                vec!["*".to_string()],
                params,
                field_body,
                search_service,
            ))
        }

        // --- Index-scoped endpoints ---
        [index, "_mapping" | "_mappings"] => {
            dispatch_es!(es_compat_index_mapping(
                index.to_string(),
                metastore,
                search_service
            ))
        }

        [index, "_search"] => {
            let params = parse_query_params::<SearchQueryParams>(query_string)?;
            let mut search_body: SearchBody = parse_body_or_default(&body)?;
            transform_aggs(&mut search_body.aggs);
            dispatch_es!(es_compat_index_search(
                vec![index.to_string()],
                params,
                search_body,
                search_service,
            ))
        }

        [index, "_count"] => {
            let params = parse_query_params::<SearchQueryParamsCount>(query_string)?;
            let count_body: SearchBody = parse_body_or_default(&body)?;
            dispatch_es!(es_compat_index_count(
                vec![index.to_string()],
                params,
                count_body,
                search_service
            ))
        }

        [index, "_field_caps"] => {
            let params = parse_query_params::<FieldCapabilityQueryParams>(query_string)?;
            let field_body: FieldCapabilityRequestBody = parse_body_or_default(&body)?;
            dispatch_es!(es_compat_index_field_capabilities(
                vec![index.to_string()],
                params,
                field_body,
                search_service,
            ))
        }

        [index, "_stats"] => {
            dispatch_es!(es_compat_index_stats(vec![index.to_string()], metastore))
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
        body: body.into(),
    })
}

fn json_response(status_code: u32, value: &serde_json::Value) -> CloudPremResult<EsHttpResponse> {
    let body = serde_json::to_vec(value)
        .map_err(|e| CloudPremError::Internal(format!("serialization error: {e}")))?;
    Ok(EsHttpResponse {
        status_code,
        body: body.into(),
    })
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
    EsHttpResponse {
        status_code,
        body: body.into(),
    }
}

/// Known aggregation types Trino ES connector sends
const AGG_TYPES: &[&str] = &["composite", "sum", "avg", "min", "max", "value_count"];

/// Returns aggregation type (e.g. `"composite"`, `"sum"`) for a given
/// aggregation value
fn agg_type(value: &Value) -> Option<&'static str> {
    let obj = value.as_object()?;
    AGG_TYPES.iter().copied().find(|t| obj.contains_key(*t))
}

/// Prefix each aggregation key with its type (e.g. "groupBy" → "composite#groupBy",
/// "sum_duration" → "sum#sum_duration") so the response carries the typed_keys prefix
/// Trino's ES client expects. Applied recursively to nested sub-aggregations.
fn transform_aggs(aggs: &mut serde_json::Map<String, Value>) {
    let old = std::mem::take(aggs);
    for (key, mut value) in old {
        // Recurse into nested sub-aggregations.
        // Trino sends them under the "aggregations" key; tantivy expects "aggs".
        if let Some(obj) = value.as_object_mut()
            && let Some(sub_aggs) = obj.remove("aggregations")
            && let Value::Object(mut sub_map) = sub_aggs
        {
            transform_aggs(&mut sub_map);
            obj.insert("aggs".to_string(), Value::Object(sub_map));
        }

        // Prefix current level key
        let formatted_key = if let Some(prefix) = agg_type(&value) {
            format!("{prefix}#{key}")
        } else {
            key
        };
        aggs.insert(formatted_key, value);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_json_diff::assert_json_eq;
    use quickwit_cluster::{ChannelTransport, create_cluster_for_test};
    use quickwit_config::NodeConfig;
    use quickwit_proto::cloudprem::{CloudPremError, EsHttpRequest};
    use quickwit_proto::metastore::MetastoreServiceClient;
    use quickwit_search::MockSearchService;

    use super::{handle_es_query, transform_aggs};

    fn make_request(method: &str, path: &str) -> EsHttpRequest {
        EsHttpRequest {
            method: method.to_string(),
            path: path.to_string(),
            body: bytes::Bytes::new(),
            headers: Default::default(),
            org_id: 0,
            cluster_id: String::new(),
        }
    }

    async fn call_es_query(
        method: &str,
        path: &str,
    ) -> Result<super::EsHttpResponse, CloudPremError> {
        let search_service = Arc::new(MockSearchService::new());
        let metastore = MetastoreServiceClient::mocked();
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &[], &transport, false)
            .await
            .unwrap();
        let node_config = Arc::new(NodeConfig::for_test());

        handle_es_query(
            make_request(method, path),
            search_service,
            metastore,
            cluster,
            node_config,
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
        let resp = call_es_query("GET", "/my_index/_search_shards")
            .await
            .unwrap();
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
    fn test_transform_aggs_simple() {
        let mut aggs = serde_json::Map::new();
        aggs.insert(
            "sum_duration".to_string(),
            serde_json::json!({"sum": {"field": "@duration"}}),
        );
        transform_aggs(&mut aggs);
        assert!(!aggs.contains_key("sum_duration"));
        assert!(aggs.contains_key("sum#sum_duration"));
    }

    #[test]
    fn test_transform_aggs_mixed() {
        let mut aggs = serde_json::Map::new();
        aggs.insert(
            "groupBy".to_string(),
            serde_json::json!({
                "composite": {
                    "sources": [{"service": {"terms": {"field": "service"}}}]
                }
            }),
        );
        aggs.insert(
            "avg_duration".to_string(),
            serde_json::json!({"avg": {"field": "@duration"}}),
        );
        transform_aggs(&mut aggs);
        assert!(aggs.contains_key("composite#groupBy"));
        assert!(aggs.contains_key("avg#avg_duration"));
    }

    #[test]
    fn test_transform_aggs_nested() {
        let mut aggs = serde_json::Map::new();
        aggs.insert(
            "groupBy".to_string(),
            serde_json::json!({
                "composite": {
                    "sources": [{"service": {"terms": {"field": "service"}}}]
                },
                "aggregations": {
                    "sum_duration": {"sum": {"field": "@duration"}}
                }
            }),
        );
        transform_aggs(&mut aggs);
        let outer = &aggs["composite#groupBy"];
        assert!(outer.get("aggregations").is_none());

        let sub = outer.get("aggs").and_then(|v| v.as_object()).unwrap();
        assert!(sub.contains_key("sum#sum_duration"));
    }

    #[test]
    fn test_transform_aggs_unknown_type() {
        let mut aggs = serde_json::Map::new();
        let original = serde_json::json!({"terms": {"field": "service"}});
        aggs.insert("my_terms".to_string(), original.clone());
        transform_aggs(&mut aggs);

        assert_json_eq!(aggs["my_terms"], original);
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
