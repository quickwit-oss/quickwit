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
use serde::{Deserialize, Serialize};

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

#[allow(clippy::result_large_err)]
pub fn build_list_field_request_for_es_api(
    index_id_patterns: Vec<String>,
    search_params: FieldCapabilityQueryParams,
    _search_body: FieldCapabilityRequestBody,
) -> Result<quickwit_proto::search::ListFieldsRequest, ElasticsearchError> {
    Ok(quickwit_proto::search::ListFieldsRequest {
        index_id_patterns,
        fields: search_params.fields.unwrap_or_default(),
        start_timestamp: search_params.start_timestamp,
        end_timestamp: search_params.end_timestamp,
    })
}
