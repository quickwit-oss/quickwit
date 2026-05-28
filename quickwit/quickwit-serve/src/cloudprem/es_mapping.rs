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

//! CloudPrem-specific `_mapping(s)` handler. Adds a fast path on top of the
//! shared ES handler that bypasses `list_fields` when the caller's `?fields=`
//! hint lists only flat, declared top-level fields — useful for downstream
//! connectors (e.g. Trino's ES connector) that only need schema for a small
//! known set of columns and otherwise pay for a full leaf fan-out.

use std::collections::HashSet;
use std::sync::Arc;

use quickwit_metastore::{IndexMetadata, IndexMetadataResponseExt};
use quickwit_proto::metastore::{IndexMetadataRequest, MetastoreService, MetastoreServiceClient};
use quickwit_search::{SearchError, SearchService, resolve_index_patterns};

use crate::elasticsearch_api::model::{
    ElasticsearchError, ElasticsearchMappingsResponse, IndexMappingQueryParams,
};
use crate::elasticsearch_api::rest_handler::es_compat_index_mapping;

/// CloudPrem-specific handler for `_mapping(s)`. Tries the fast path; falls
/// through to the shared ES handler for any case where dynamic fields might
/// be needed.
pub(crate) async fn cloudprem_index_mapping(
    index_id: String,
    params: IndexMappingQueryParams,
    mut metastore: MetastoreServiceClient,
    search_service: Arc<dyn SearchService>,
) -> Result<ElasticsearchMappingsResponse, ElasticsearchError> {
    let requested_fields = params.field_patterns();

    // Cheap syntactic check first — skip the metastore round-trip when the
    // hint can't possibly satisfy the fast path.
    if requested_fields.is_empty() || !all_flat_field_names(&requested_fields) {
        return es_compat_index_mapping(index_id, params, metastore, search_service).await;
    }

    let indexes_metadata = resolve_indexes_for_mapping(&index_id, &mut metastore).await?;
    if all_requested_declared(&requested_fields, &indexes_metadata) {
        // Trim each index's `field_mappings` to the requested set before
        // handing the metadata to `from_doc_mapping`. The shared builder
        // iterates that vector verbatim, so trimming upstream yields a
        // response containing only the requested top-level fields (with
        // their full object subtrees intact). Keeps the filter logic
        // CloudPrem-local without touching the OSS response builder.
        let filtered = filter_indexes_to_requested(indexes_metadata, &requested_fields);
        return Ok(ElasticsearchMappingsResponse::from_doc_mapping(
            filtered, None,
        ));
    }

    // Fast-path syntax matched but one or more names aren't declared — they
    // might be dynamic, so defer to the slow path that consults the leaves.
    es_compat_index_mapping(index_id, params, metastore, search_service).await
}

/// Trims each index's `field_mappings` to entries whose top-level name is in
/// `requested`. Caller must ensure `requested` is non-empty — an empty slice
/// would erase every field.
fn filter_indexes_to_requested(
    mut indexes_metadata: Vec<IndexMetadata>,
    requested: &[String],
) -> Vec<IndexMetadata> {
    let filter: HashSet<&str> = requested.iter().map(String::as_str).collect();
    for metadata in &mut indexes_metadata {
        metadata
            .index_config
            .doc_mapping
            .field_mappings
            .retain(|entry| filter.contains(entry.name.as_str()));
    }
    indexes_metadata
}

/// Resolves a single id, comma list, or pattern to `IndexMetadata` records.
/// Mirrors the resolution logic embedded in `es_compat_index_mapping` so the
/// fast path observes the same set of indexes the slow path would.
async fn resolve_indexes_for_mapping(
    index_id: &str,
    metastore: &mut MetastoreServiceClient,
) -> Result<Vec<IndexMetadata>, SearchError> {
    if index_id.contains('*') || index_id.contains(',') {
        let patterns: Vec<String> = index_id.split(',').map(|s| s.trim().to_string()).collect();
        resolve_index_patterns(&patterns, metastore).await
    } else {
        let request = IndexMetadataRequest::for_index_id(index_id.to_string());
        let metadata = metastore
            .index_metadata(request)
            .await?
            .deserialize_index_metadata()?;
        Ok(vec![metadata])
    }
}

/// A name is "flat" if it can be looked up directly in `doc_mapping`'s
/// top-level field map: no wildcards, no dotted subpaths.
fn all_flat_field_names(names: &[String]) -> bool {
    names
        .iter()
        .all(|name| !name.contains('*') && !name.contains('?') && !name.contains('.'))
}

/// Every requested name appears as a top-level declared field in at least one
/// resolved index's `doc_mapping`.
fn all_requested_declared(requested: &[String], indexes_metadata: &[IndexMetadata]) -> bool {
    let total_declared: usize = indexes_metadata
        .iter()
        .map(|m| m.index_config.doc_mapping.field_mappings.len())
        .sum();
    let mut declared: HashSet<&str> = HashSet::with_capacity(total_declared);
    for metadata in indexes_metadata {
        for entry in &metadata.index_config.doc_mapping.field_mappings {
            declared.insert(entry.name.as_str());
        }
    }
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
        let names = vec!["host*".to_string()];
        assert!(!all_flat_field_names(&names));
        let names = vec!["ho?t".to_string()];
        assert!(!all_flat_field_names(&names));
    }

    #[test]
    fn all_flat_field_names_rejects_dotted_path() {
        let names = vec!["host.region".to_string()];
        assert!(!all_flat_field_names(&names));
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

    #[test]
    fn filter_keeps_only_requested_field_mappings() {
        let metadata = make_index_metadata(
            "test",
            json!([
                { "name": "host", "type": "text" },
                { "name": "message", "type": "text" },
                { "name": "status", "type": "i64" },
                { "name": "service", "type": "text" },
                { "name": "trace_id", "type": "text" },
            ]),
        );
        let requested = vec!["host".to_string(), "message".to_string()];
        let filtered = filter_indexes_to_requested(vec![metadata], &requested);
        let names: Vec<&str> = filtered[0]
            .index_config
            .doc_mapping
            .field_mappings
            .iter()
            .map(|entry| entry.name.as_str())
            .collect();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"host"));
        assert!(names.contains(&"message"));
    }

    #[test]
    fn filtered_response_preserves_object_subtree() {
        let metadata = make_index_metadata(
            "test",
            json!([
                {
                    "name": "host",
                    "type": "object",
                    "field_mappings": [
                        { "name": "region", "type": "text" },
                        { "name": "name", "type": "text" }
                    ]
                },
                { "name": "message", "type": "text" }
            ]),
        );
        let requested = vec!["host".to_string()];
        let filtered = filter_indexes_to_requested(vec![metadata], &requested);
        let response = ElasticsearchMappingsResponse::from_doc_mapping(filtered, None);
        let serialized = serde_json::to_value(&response).unwrap();
        let host_props = &serialized["test"]["mappings"]["properties"]["host"]["properties"];
        assert_eq!(host_props["region"]["type"], "keyword");
        assert_eq!(host_props["name"]["type"], "keyword");
        assert!(
            serialized["test"]["mappings"]["properties"]
                .get("message")
                .is_none()
        );
    }
}
