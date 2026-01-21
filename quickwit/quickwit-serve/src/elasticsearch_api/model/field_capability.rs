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

use std::collections::HashMap;

use quickwit_proto::search::{ListFieldType, ListFieldsEntryResponse, ListFieldsResponse};
use quickwit_query::ElasticQueryDsl;
use quickwit_query::query_ast::QueryAst;
use serde::{Deserialize, Serialize};
use warp::hyper::StatusCode;

use super::ElasticsearchError;
use super::search_query_params::*;
use crate::simple_list::{from_simple_list, to_simple_list};

#[serde_with::skip_serializing_none]
#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldCapabilityQueryParams {
    #[serde(default)]
    pub allow_no_indices: Option<bool>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    pub expand_wildcards: Option<Vec<ExpandWildcards>>,
    #[serde(serialize_with = "to_simple_list")]
    #[serde(deserialize_with = "from_simple_list")]
    #[serde(default)]
    pub fields: Option<Vec<String>>,
    #[serde(default)]
    pub ignore_unavailable: Option<bool>,
    /// Non-ES Parameter. If set, restricts splits to documents with a `time_range.start >=
    /// start_timestamp`.
    pub start_timestamp: Option<i64>,
    /// Non-ES Parameter. If set, restricts splits to documents with a `time_range.end <
    /// end_timestamp``.
    pub end_timestamp: Option<i64>,
}

#[derive(Debug, Default, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FieldCapabilityRequestBody {
    #[serde(default)]
    // unsupported currently
    pub index_filter: serde_json::Value,
    #[serde(default)]
    // unsupported currently
    pub runtime_mappings: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FieldCapabilityResponse {
    indices: Vec<String>,
    fields: HashMap<String, FieldCapabilityFieldTypesResponse>,
}

type FieldCapabilityFieldTypesResponse =
    HashMap<FieldCapabilityEntryType, FieldCapabilityEntryResponse>;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Hash)]
enum FieldCapabilityEntryType {
    #[serde(rename = "long")]
    Long,
    #[serde(rename = "keyword")]
    Keyword,
    #[serde(rename = "text")]
    Text,
    #[serde(rename = "date_nanos")]
    DateNanos,
    #[serde(rename = "binary")]
    Binary,
    #[serde(rename = "double")]
    Double,
    #[serde(rename = "boolean")]
    Boolean,
    #[serde(rename = "ip")]
    Ip,
    // Unmapped currently
    #[serde(rename = "nested")]
    Nested,
    // Unmapped currently
    #[serde(rename = "object")]
    Object,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct FieldCapabilityEntryResponse {
    metadata_field: bool, // Always false
    searchable: bool,
    aggregatable: bool,
    // Option since it is filled later
    #[serde(rename = "type")]
    typ: Option<FieldCapabilityEntryType>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    indices: Vec<String>, // [ "index1", "index2" ],
    #[serde(skip_serializing_if = "Vec::is_empty")]
    non_aggregatable_indices: Vec<String>, // [ "index1" ]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    non_searchable_indices: Vec<String>, // [ "index1" ]
}
impl FieldCapabilityEntryResponse {
    fn from_list_field_entry_response(entry: ListFieldsEntryResponse) -> Self {
        Self {
            metadata_field: false,
            searchable: entry.searchable,
            aggregatable: entry.aggregatable,
            typ: None,
            indices: entry.index_ids.clone(),
            non_aggregatable_indices: entry.non_aggregatable_index_ids,
            non_searchable_indices: entry.non_searchable_index_ids,
        }
    }
}

pub fn convert_to_es_field_capabilities_response(
    resp: ListFieldsResponse,
) -> FieldCapabilityResponse {
    let mut indices = resp
        .fields
        .iter()
        .flat_map(|entry| entry.index_ids.iter().cloned())
        .collect::<Vec<_>>();
    indices.sort();
    indices.dedup();

    let mut fields: HashMap<String, FieldCapabilityFieldTypesResponse> = HashMap::new();
    for list_field_resp in resp.fields {
        let entry = fields
            .entry(list_field_resp.field_name.to_string())
            .or_default();

        let field_type = ListFieldType::try_from(list_field_resp.field_type).unwrap();
        let add_entry =
            FieldCapabilityEntryResponse::from_list_field_entry_response(list_field_resp);
        let types = match field_type {
            ListFieldType::Str => {
                vec![
                    FieldCapabilityEntryType::Keyword,
                    FieldCapabilityEntryType::Text,
                ]
            }
            ListFieldType::U64 => vec![FieldCapabilityEntryType::Long],
            ListFieldType::I64 => vec![FieldCapabilityEntryType::Long],
            ListFieldType::F64 => vec![FieldCapabilityEntryType::Double],
            ListFieldType::Bool => vec![FieldCapabilityEntryType::Boolean],
            ListFieldType::Date => vec![FieldCapabilityEntryType::DateNanos],
            ListFieldType::Facet => continue,
            ListFieldType::Json => continue,
            ListFieldType::Bytes => vec![FieldCapabilityEntryType::Binary],
            ListFieldType::IpAddr => vec![FieldCapabilityEntryType::Ip],
        };
        for field_type in types {
            let mut add_entry = add_entry.clone();
            add_entry.typ = Some(field_type.clone());

            // If the field exists in all indices, we omit field.indices in the response.
            let exists_in_all_indices = add_entry.indices.len() == indices.len();
            if exists_in_all_indices {
                add_entry.indices = Vec::new();
            }

            entry.insert(field_type, add_entry);
        }
    }
    FieldCapabilityResponse { indices, fields }
}

/// Parses an Elasticsearch index_filter JSON value into a Quickwit QueryAst.
///
/// Returns `Ok(None)` if the index_filter is null or empty.
/// Returns `Ok(Some(QueryAst))` if the index_filter is valid.
/// Returns `Err` if the index_filter is invalid or cannot be converted.
pub fn parse_index_filter_to_query_ast(
    index_filter: serde_json::Value,
) -> Result<Option<QueryAst>, ElasticsearchError> {
    if index_filter.is_null() || index_filter == serde_json::Value::Object(Default::default()) {
        return Ok(None);
    }

    // Parse ES Query DSL to internal QueryAst
    let elastic_query_dsl: ElasticQueryDsl =
        serde_json::from_value(index_filter).map_err(|err| {
            ElasticsearchError::new(
                StatusCode::BAD_REQUEST,
                format!("Invalid index_filter: {err}"),
                None,
            )
        })?;

    let query_ast: QueryAst = elastic_query_dsl.try_into().map_err(|err: anyhow::Error| {
        ElasticsearchError::new(
            StatusCode::BAD_REQUEST,
            format!("Failed to convert index_filter: {err}"),
            None,
        )
    })?;

    Ok(Some(query_ast))
}

#[allow(clippy::result_large_err)]
pub fn build_list_field_request_for_es_api(
    index_id_patterns: Vec<String>,
    search_params: FieldCapabilityQueryParams,
    search_body: FieldCapabilityRequestBody,
) -> Result<quickwit_proto::search::ListFieldsRequest, ElasticsearchError> {
    let query_ast = parse_index_filter_to_query_ast(search_body.index_filter)?;
    let query_ast_json = query_ast
        .map(|ast| serde_json::to_string(&ast).expect("QueryAst should be JSON serializable"));

    Ok(quickwit_proto::search::ListFieldsRequest {
        index_id_patterns,
        fields: search_params.fields.unwrap_or_default(),
        start_timestamp: search_params.start_timestamp,
        end_timestamp: search_params.end_timestamp,
        query_ast: query_ast_json,
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_build_list_field_request_empty_index_filter() {
        let result = build_list_field_request_for_es_api(
            vec!["test_index".to_string()],
            FieldCapabilityQueryParams::default(),
            FieldCapabilityRequestBody::default(),
        )
        .unwrap();

        assert_eq!(result.index_id_patterns, vec!["test_index".to_string()]);
        assert!(result.query_ast.is_none());
    }

    #[test]
    fn test_build_list_field_request_with_term_index_filter() {
        let search_body = FieldCapabilityRequestBody {
            index_filter: json!({
                "term": {
                    "status": "active"
                }
            }),
            runtime_mappings: serde_json::Value::Null,
        };

        let result = build_list_field_request_for_es_api(
            vec!["test_index".to_string()],
            FieldCapabilityQueryParams::default(),
            search_body,
        )
        .unwrap();

        assert_eq!(result.index_id_patterns, vec!["test_index".to_string()]);
        assert!(result.query_ast.is_some());

        // Verify the query_ast is valid JSON
        let query_ast: serde_json::Value =
            serde_json::from_str(&result.query_ast.unwrap()).unwrap();
        assert!(query_ast.is_object());
    }

    #[test]
    fn test_build_list_field_request_with_bool_index_filter() {
        let search_body = FieldCapabilityRequestBody {
            index_filter: json!({
                "bool": {
                    "must": [
                        { "term": { "status": "active" } }
                    ],
                    "filter": [
                        { "range": { "age": { "gte": 18 } } }
                    ]
                }
            }),
            runtime_mappings: serde_json::Value::Null,
        };

        let result = build_list_field_request_for_es_api(
            vec!["test_index".to_string()],
            FieldCapabilityQueryParams::default(),
            search_body,
        )
        .unwrap();

        assert!(result.query_ast.is_some());
    }

    #[test]
    fn test_build_list_field_request_with_invalid_index_filter() {
        let search_body = FieldCapabilityRequestBody {
            index_filter: json!({
                "invalid_query_type": {
                    "field": "value"
                }
            }),
            runtime_mappings: serde_json::Value::Null,
        };

        let result = build_list_field_request_for_es_api(
            vec!["test_index".to_string()],
            FieldCapabilityQueryParams::default(),
            search_body,
        );

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_build_list_field_request_with_null_index_filter() {
        let search_body = FieldCapabilityRequestBody {
            index_filter: serde_json::Value::Null,
            runtime_mappings: serde_json::Value::Null,
        };

        let result = build_list_field_request_for_es_api(
            vec!["test_index".to_string()],
            FieldCapabilityQueryParams::default(),
            search_body,
        )
        .unwrap();

        assert!(result.query_ast.is_none());
    }

    #[test]
    fn test_build_list_field_request_preserves_other_params() {
        let search_params = FieldCapabilityQueryParams {
            fields: Some(vec!["field1".to_string(), "field2".to_string()]),
            start_timestamp: Some(1000),
            end_timestamp: Some(2000),
            ..Default::default()
        };

        let search_body = FieldCapabilityRequestBody {
            index_filter: json!({ "match_all": {} }),
            runtime_mappings: serde_json::Value::Null,
        };

        let result = build_list_field_request_for_es_api(
            vec!["test_index".to_string()],
            search_params,
            search_body,
        )
        .unwrap();

        assert_eq!(
            result.fields,
            vec!["field1".to_string(), "field2".to_string()]
        );
        assert_eq!(result.start_timestamp, Some(1000));
        assert_eq!(result.end_timestamp, Some(2000));
        assert!(result.query_ast.is_some());
    }

    #[test]
    fn test_parse_index_filter_to_query_ast_null() {
        let result = parse_index_filter_to_query_ast(serde_json::Value::Null).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_index_filter_to_query_ast_empty_object() {
        let result = parse_index_filter_to_query_ast(json!({})).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_index_filter_to_query_ast_valid_term() {
        let result = parse_index_filter_to_query_ast(json!({
            "term": { "status": "active" }
        }))
        .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_parse_index_filter_to_query_ast_invalid() {
        let result = parse_index_filter_to_query_ast(json!({
            "invalid_query_type": { "field": "value" }
        }));
        assert!(result.is_err());
    }
}
