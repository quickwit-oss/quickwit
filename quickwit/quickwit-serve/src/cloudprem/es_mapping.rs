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

//! CloudPrem `_mapping(s)` handler. Skips `list_fields` when `?fields=` lists
//! only flat declared top-level names; otherwise defers to the shared ES
//! handler.

use std::collections::HashSet;
use std::sync::Arc;

use quickwit_metastore::IndexMetadata;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_search::{SearchService, resolve_index_patterns};

use crate::elasticsearch_api::model::{
    ElasticsearchError, ElasticsearchMappingsResponse, IndexMappingQueryParams,
};
use crate::elasticsearch_api::rest_handler::es_compat_index_mapping;

pub(crate) async fn cloudprem_index_mapping(
    index_id: String,
    params: IndexMappingQueryParams,
    mut metastore: MetastoreServiceClient,
    search_service: Arc<dyn SearchService>,
) -> Result<ElasticsearchMappingsResponse, ElasticsearchError> {
    let requested_fields = params.field_patterns();

    // Skip the metastore round-trip when the hint can't satisfy the fast path.
    if requested_fields.is_empty() || !all_flat_field_names(&requested_fields) {
        return es_compat_index_mapping(index_id, params, metastore, search_service).await;
    }

    let patterns: Vec<String> = index_id.split(',').map(|s| s.trim().to_string()).collect();
    let indexes_metadata = resolve_index_patterns(&patterns, &mut metastore).await?;
    if all_requested_declared(&requested_fields, &indexes_metadata) {
        return Ok(ElasticsearchMappingsResponse::from_doc_mapping(
            indexes_metadata,
            None,
        ));
    }

    // Some names aren't declared — they may exist dynamically, so defer to the
    // leaf fan-out.
    es_compat_index_mapping(index_id, params, metastore, search_service).await
}

fn all_flat_field_names(names: &[String]) -> bool {
    names
        .iter()
        .all(|name| !name.contains('*') && !name.contains('?') && !name.contains('.'))
}

fn all_requested_declared(requested: &[String], indexes_metadata: &[IndexMetadata]) -> bool {
    let declared: HashSet<&str> = indexes_metadata
        .iter()
        .flat_map(|m| m.index_config.doc_mapping.field_mappings.iter())
        .map(|entry| entry.name.as_str())
        .collect();
    requested
        .iter()
        .all(|name| declared.contains(name.as_str()))
}

#[cfg(test)]
mod tests {
    use quickwit_doc_mapper::FieldMappingEntry;
    use serde_json::json;

    use super::*;

    fn make_index_metadata(name: &str, field_mappings: serde_json::Value) -> IndexMetadata {
        let entries: Vec<FieldMappingEntry> = serde_json::from_value(field_mappings).unwrap();
        let mut metadata = IndexMetadata::for_test(name, &format!("ram:///indexes/{name}"));
        metadata.index_config.doc_mapping.field_mappings = entries;
        metadata
    }

    #[test]
    fn all_flat_field_names_accepts_plain_identifiers() {
        let names = vec!["host".to_string(), "message".to_string()];
        assert!(all_flat_field_names(&names));
    }

    #[test]
    fn all_flat_field_names_rejects_wildcard() {
        assert!(!all_flat_field_names(&["host*".to_string()]));
        assert!(!all_flat_field_names(&["ho?t".to_string()]));
    }

    #[test]
    fn all_flat_field_names_rejects_dotted_path() {
        assert!(!all_flat_field_names(&["host.region".to_string()]));
    }

    #[test]
    fn all_requested_declared_true_when_all_present() {
        let metadata = make_index_metadata(
            "test",
            json!([
                { "name": "host", "type": "text" },
                { "name": "message", "type": "text" },
            ]),
        );
        let requested = vec!["host".to_string(), "message".to_string()];
        assert!(all_requested_declared(&requested, &[metadata]));
    }

    #[test]
    fn all_requested_declared_false_when_any_missing() {
        let metadata = make_index_metadata("test", json!([{ "name": "host", "type": "text" }]));
        let requested = vec!["host".to_string(), "trace_id".to_string()];
        assert!(!all_requested_declared(&requested, &[metadata]));
    }

    #[test]
    fn all_requested_declared_unions_across_indexes() {
        let m1 = make_index_metadata("a", json!([{ "name": "host", "type": "text" }]));
        let m2 = make_index_metadata("b", json!([{ "name": "message", "type": "text" }]));
        let requested = vec!["host".to_string(), "message".to_string()];
        assert!(all_requested_declared(&requested, &[m1, m2]));
    }
}
