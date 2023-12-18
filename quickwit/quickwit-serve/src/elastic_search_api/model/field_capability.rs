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

use quickwit_proto::search::{ListFieldType, ListFieldsEntryResponse, ListFieldsResponse};
use serde::{Deserialize, Serialize};

use super::search_query_params::*;
use super::ElasticSearchError;
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
#[derive(Serialize, Deserialize, Debug, Default)]
struct FieldCapabilityFieldTypesResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    long: Option<FieldCapabilityEntryResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    keyword: Option<FieldCapabilityEntryResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<FieldCapabilityEntryResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    date_nanos: Option<FieldCapabilityEntryResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    binary: Option<FieldCapabilityEntryResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    double: Option<FieldCapabilityEntryResponse>, // Do we need float ?
    #[serde(skip_serializing_if = "Option::is_none")]
    boolean: Option<FieldCapabilityEntryResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ip: Option<FieldCapabilityEntryResponse>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct FieldCapabilityEntryResponse {
    metadata_field: bool, // Always false
    searchable: bool,
    aggregatable: bool,
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
            indices: entry.index_ids.clone(),
            non_aggregatable_indices: entry.non_aggregatable_index_ids,
            non_searchable_indices: entry.non_searchable_index_ids,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct FieldCapabilityEntry {
    searchable: bool,
    aggregatable: bool,
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

        let field_type = ListFieldType::from_i32(list_field_resp.field_type).unwrap();
        let add_entry =
            FieldCapabilityEntryResponse::from_list_field_entry_response(list_field_resp);
        match field_type {
            ListFieldType::Str => {
                if add_entry.aggregatable {
                    entry.keyword = Some(add_entry.clone());
                }
                if add_entry.searchable {
                    entry.text = Some(add_entry);
                }
            }
            ListFieldType::U64 => entry.long = Some(add_entry),
            ListFieldType::I64 => entry.long = Some(add_entry),
            ListFieldType::F64 => entry.double = Some(add_entry),
            ListFieldType::Bool => entry.boolean = Some(add_entry),
            ListFieldType::Date => entry.date_nanos = Some(add_entry),
            ListFieldType::Facet => continue,
            ListFieldType::Json => continue,
            ListFieldType::Bytes => entry.binary = Some(add_entry), // Is this mapping correct?
            ListFieldType::IpAddr => entry.ip = Some(add_entry),
        }
    }
    FieldCapabilityResponse { indices, fields }
}

pub fn build_list_field_request_for_es_api(
    index_id_patterns: Vec<String>,
    search_params: FieldCapabilityQueryParams,
    _search_body: FieldCapabilityRequestBody,
) -> Result<quickwit_proto::search::ListFieldsRequest, ElasticSearchError> {
    Ok(quickwit_proto::search::ListFieldsRequest {
        index_ids: index_id_patterns,
        fields: search_params.fields.unwrap_or_default(),
    })
}
