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

use quickwit_proto::search::{ListFieldType, ListFieldsEntryResponse, ListFieldsResponse};
use serde::{Deserialize, Serialize};
use quickwit_metastore::IndexMetadata;

use super::search_query_params::*;
use super::ElasticsearchError;
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

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct FieldCapabilityEntryResponse {
    metadata_field: bool, // Always false
    searchable: bool,
    aggregatable: bool,
    // Option since for non-time-series indices this field is not present.
    time_series_dimension: Option<bool>,
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
            time_series_dimension: None,
            typ: None,
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
    index_metadata: Vec<IndexMetadata>,
) -> FieldCapabilityResponse {
    let mut indices = resp
        .fields
        .iter()
        .flat_map(|entry| entry.index_ids.iter().cloned())
        .collect::<Vec<_>>();
    indices.sort();
    indices.dedup();

    let timestamp_fields: Vec<String> = index_metadata
        .iter()
        .filter_map(|index| index.index_config.doc_mapping.timestamp_field.clone())
        .collect();

    let mut fields: HashMap<String, FieldCapabilityFieldTypesResponse> = HashMap::new();
    for list_field_resp in resp.fields {
        let entry = fields
            .entry(list_field_resp.field_name.to_string())
            .or_default();

        let field_type = ListFieldType::from_i32(list_field_resp.field_type).unwrap();
        let field_name = list_field_resp.field_name.to_string();

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

            // Check if the field_name is in timestamp_fields and set time_series_dimension
            if timestamp_fields.contains(&field_name) {
                add_entry.time_series_dimension = Some(true);
            }

            entry.insert(field_type, add_entry);
        }
    }
    FieldCapabilityResponse { indices, fields }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeSet};
    use std::num::NonZeroU32;
    use quickwit_common::uri::Uri;
    use quickwit_config::{DocMapping, IndexConfig, IndexingSettings, SearchSettings};
    use quickwit_doc_mapper::Mode;

    #[test]
    fn test_convert_to_es_field_capabilities_response() {
        // Define test input data
        let list_fields_response = ListFieldsResponse {
            fields: vec![
                ListFieldsEntryResponse {
                    field_name: "timestamp_field_1".to_string(),
                    field_type: ListFieldType::Date as i32,
                    index_ids: vec!["index1".to_string()],
                    searchable: true,
                    aggregatable: true,
                    non_aggregatable_index_ids: vec![],
                    non_searchable_index_ids: vec![],
                },
                ListFieldsEntryResponse {
                    field_name: "non_timestamp_field".to_string(),
                    field_type: ListFieldType::Str as i32,
                    index_ids: vec!["index1".to_string()],
                    searchable: true,
                    aggregatable: true,
                    non_aggregatable_index_ids: vec![],
                    non_searchable_index_ids: vec![],
                },
            ],
        };

        let index_metadata = vec![
            IndexMetadata::new(IndexConfig {
                index_id: "index1".parse().unwrap(),
                index_uri: Uri::for_test("s3://config.json"),
                doc_mapping: DocMapping {
                    field_mappings: vec![],
                    tag_fields: BTreeSet::new(),
                    store_source: false,
                    index_field_presence: false,
                    timestamp_field: Some("timestamp_field_1".to_string()),
                    mode: Mode::default(),
                    partition_key: None,
                    max_num_partitions: NonZeroU32::new(1).unwrap(),
                    tokenizers: vec![],
                    document_length: false,
                },
                indexing_settings: IndexingSettings::default(),
                search_settings: SearchSettings::default(),
                retention_policy_opt: None,
            })
        ];

        // Expected output data
        let mut expected_fields: HashMap<String, FieldCapabilityFieldTypesResponse> = HashMap::new();
        let mut timestamp_field_1_entry: FieldCapabilityFieldTypesResponse = HashMap::new();
        timestamp_field_1_entry.insert(
            FieldCapabilityEntryType::DateNanos,
            FieldCapabilityEntryResponse {
                metadata_field: false,
                searchable: true,
                aggregatable: true,
                time_series_dimension: Some(true),
                typ: Some(FieldCapabilityEntryType::DateNanos),
                indices: vec![],
                non_aggregatable_indices: vec![],
                non_searchable_indices: vec![],
            },
        );
        expected_fields.insert("timestamp_field_1".to_string(), timestamp_field_1_entry);

        let mut non_timestamp_field_entry: FieldCapabilityFieldTypesResponse = HashMap::new();
        non_timestamp_field_entry.insert(
            FieldCapabilityEntryType::Text,
            FieldCapabilityEntryResponse {
                metadata_field: false,
                searchable: true,
                aggregatable: true,
                time_series_dimension: None,
                typ: Some(FieldCapabilityEntryType::Text),
                indices: vec![],
                non_aggregatable_indices: vec![],
                non_searchable_indices: vec![],
            },
        );
        non_timestamp_field_entry.insert(
            FieldCapabilityEntryType::Keyword,
            FieldCapabilityEntryResponse {
                metadata_field: false,
                searchable: true,
                aggregatable: true,
                time_series_dimension: None,
                typ: Some(FieldCapabilityEntryType::Keyword),
                indices: vec![],
                non_aggregatable_indices: vec![],
                non_searchable_indices: vec![],
            },
        );
        expected_fields.insert("non_timestamp_field".to_string(), non_timestamp_field_entry);

        let expected_response = FieldCapabilityResponse {
            indices: vec!["index1".to_string()],
            fields: expected_fields,
        };

        // Call the function with test data
        let result = convert_to_es_field_capabilities_response(list_fields_response, index_metadata);

        // Verify the output
        assert_eq!(result, expected_response);
    }
}
