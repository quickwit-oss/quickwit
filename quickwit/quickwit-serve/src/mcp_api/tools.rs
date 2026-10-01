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

use std::collections::BTreeMap;
use std::sync::Arc;

use http::StatusCode;
use quickwit_config::validate_index_id_pattern;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_search::SearchService;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::elasticsearch_api::model::{
    CatIndexQueryParams, ElasticsearchError, FieldCapabilityQueryParams,
    FieldCapabilityRequestBody, SearchBody, SearchQueryParams, SearchQueryParamsCount,
};
use crate::elasticsearch_api::rest_handler::{
    es_compat_index_cat_indices, es_compat_index_count, es_compat_index_field_capabilities,
    es_compat_index_search,
};

/// The transport-independent tool backend. All operations use the ES-compatible handlers rather
/// than duplicating query translation or response conversion in the MCP layer.
#[derive(Clone)]
pub(super) struct Tools {
    pub search_service: Arc<dyn SearchService>,
    pub metastore: MetastoreServiceClient,
}

pub(super) const TOOL_NAMES: [&str; 4] =
    ["list_indices", "get_field_capabilities", "search", "count"];

pub(super) fn list() -> Value {
    let descriptions = [
        "List indices and their document counts and sizes using Elasticsearch _cat/indices. Start \
         here to discover index names. Defaults to all indices; params.format defaults to json.",
        "Inspect searchable and aggregatable fields using Elasticsearch _field_caps. Use \
         params.fields (comma-separated patterns) to narrow the result before writing a query.",
        "Search documents or run aggregations using Elasticsearch _search and its JSON Query DSL. \
         Use a time-range filter and a small body.size. Use body.size=0 for aggregations only. \
         Supported pagination: from/size and search_after, not scroll.",
        "Count matching documents using Elasticsearch _count and its JSON Query DSL.",
    ];
    let tools: Vec<Value> = TOOL_NAMES
        .iter()
        .zip(descriptions)
        .map(|(name, description)| {
            let mut properties = json!({
                "index": {
                    "type": "string", "minLength": 1,
                    "description": "Index name or comma-separated index patterns, e.g. logs-*,traces."
                },
                "params": {
                    "type": "object", "additionalProperties": {"type": "string"},
                    "description": "URL query parameters supported by this Elasticsearch operation. All values, including booleans and numbers, must be strings. Lists are comma-separated."
                }
            });
            if *name != "list_indices" {
                let body_description = if *name == "get_field_capabilities" {
                    "Elasticsearch field capabilities body, e.g. {\"index_filter\":{\"match_all\":{}}}. Defaults to {}. Put field patterns in params.fields."
                } else {
                    "Elasticsearch JSON request body, e.g. {\"query\":{\"match_all\":{}}}. Defaults to an empty body."
                };
                properties["body"] = json!({"type": "object", "description": body_description});
            }
            json!({
                "name": name,
                "description": description,
                "inputSchema": {
                    "type": "object", "properties": properties,
                    "required": if *name == "list_indices" { vec![] } else { vec!["index"] },
                    "additionalProperties": false
                },
                "annotations": {
                    "readOnlyHint": true, "destructiveHint": false,
                    "idempotentHint": true, "openWorldHint": false
                }
            })
        })
        .collect();
    json!({"tools": tools})
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FieldArguments {
    index: String,
    #[serde(default)]
    params: BTreeMap<String, String>,
    #[serde(default)]
    body: FieldCapabilityRequestBody,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListArguments {
    #[serde(default = "all_indices")]
    index: String,
    #[serde(default)]
    params: BTreeMap<String, String>,
}

fn all_indices() -> String {
    "*".to_string()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchArguments {
    index: String,
    #[serde(default)]
    params: BTreeMap<String, String>,
    #[serde(default)]
    body: SearchBody,
}

type ToolResult<T> = Result<T, Box<ElasticsearchError>>;

fn invalid_argument(error: impl std::fmt::Display) -> Box<ElasticsearchError> {
    Box::new(ElasticsearchError::new(
        StatusCode::BAD_REQUEST,
        error.to_string(),
        None,
    ))
}

fn arguments<T: DeserializeOwned>(value: Value) -> ToolResult<T> {
    serde_json::from_value(value).map_err(invalid_argument)
}

fn index_patterns(index: &str) -> ToolResult<Vec<String>> {
    // MCP arguments are already decoded JSON strings, not URL path segments. Apply the same
    // validation as the ES route without percent-decoding the input a second time.
    let mut patterns = Vec::new();
    for pattern in index.split(',') {
        validate_index_id_pattern(pattern, true).map_err(invalid_argument)?;
        patterns.push(pattern.to_string());
    }
    Ok(patterns)
}

fn query_params<T: DeserializeOwned>(params: &BTreeMap<String, String>) -> ToolResult<T> {
    // Preserve URL parameter semantics (not JSON scalar coercion), including the ES models'
    // comma-separated list deserializers. serde_qs also rejects unknown model fields.
    let query_string = serde_qs::to_string(params).map_err(invalid_argument)?;
    serde_qs::from_str(&query_string).map_err(invalid_argument)
}

fn serialize<T: serde::Serialize>(value: T) -> ToolResult<Value> {
    serde_json::to_value(value).map_err(|error| {
        Box::new(ElasticsearchError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            error.to_string(),
            None,
        ))
    })
}

impl Tools {
    pub async fn call(&self, name: &str, input: Value) -> ToolResult<Value> {
        match name {
            "list_indices" => {
                let mut args: ListArguments = arguments(input)?;
                args.params
                    .entry("format".to_string())
                    .or_insert_with(|| "json".to_string());
                let params: CatIndexQueryParams = query_params(&args.params)?;
                serialize(
                    es_compat_index_cat_indices(
                        index_patterns(&args.index)?,
                        params,
                        self.metastore.clone(),
                    )
                    .await?,
                )
            }
            "get_field_capabilities" => {
                let args: FieldArguments = arguments(input)?;
                let params: FieldCapabilityQueryParams = query_params(&args.params)?;
                serialize(
                    es_compat_index_field_capabilities(
                        index_patterns(&args.index)?,
                        params,
                        args.body,
                        self.search_service.clone(),
                    )
                    .await?,
                )
            }
            "search" => {
                let args: SearchArguments = arguments(input)?;
                let params: SearchQueryParams = query_params(&args.params)?;
                if params.scroll.is_some() {
                    return Err(invalid_argument(
                        "scroll is not exposed over MCP; use from/size or search_after",
                    ));
                }
                serialize(
                    es_compat_index_search(
                        index_patterns(&args.index)?,
                        params,
                        args.body,
                        self.search_service.clone(),
                    )
                    .await?,
                )
            }
            "count" => {
                let args: SearchArguments = arguments(input)?;
                let params: SearchQueryParamsCount = query_params(&args.params)?;
                serialize(
                    es_compat_index_count(
                        index_patterns(&args.index)?,
                        params,
                        args.body,
                        self.search_service.clone(),
                    )
                    .await?,
                )
            }
            _ => Err(invalid_argument(format!("unknown tool: {name}"))),
        }
    }
}
