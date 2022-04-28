// Copyright (C) 2021 Quickwit, Inc.
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

//! This projects implements quickwit's search API.
#![warn(missing_docs)]
#![allow(clippy::bool_assert_comparison)]

mod client;
mod cluster_client;
mod collector;
mod error;
mod fetch_docs;
mod filters;
mod leaf;
mod rendezvous_hasher;
mod retry;
mod root;
mod search_client_pool;
mod search_response_rest;
mod search_stream;
mod service;
mod thread_pool;

/// Refer to this as `crate::Result<T>`.
pub type Result<T> = std::result::Result<T, SearchError>;

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;

use anyhow::Context;
use itertools::Itertools;
use quickwit_cluster::Cluster;
use quickwit_config::{build_doc_mapper, QuickwitConfig, SEARCHER_CONFIG_INSTANCE};
use quickwit_doc_mapper::tag_pruning::extract_tags_from_query;
use quickwit_doc_mapper::DocMapper;
use quickwit_metastore::{Metastore, SplitMetadata, SplitState};
use quickwit_proto::{PartialHit, SearchRequest, SearchResponse, SplitIdAndFooterOffsets};
use quickwit_storage::StorageUriResolver;
use serde_json::Value as JsonValue;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::agg_result::AggregationResults;
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use tantivy::DocAddress;

pub use crate::client::SearchServiceClient;
pub use crate::cluster_client::ClusterClient;
pub use crate::error::{parse_grpc_error, SearchError};
use crate::fetch_docs::fetch_docs;
use crate::leaf::leaf_search;
pub use crate::root::root_search;
pub use crate::search_client_pool::SearchClientPool;
pub use crate::search_response_rest::SearchResponseRest;
pub use crate::search_stream::root_search_stream;
pub use crate::service::{MockSearchService, SearchService, SearchServiceImpl};
use crate::thread_pool::run_cpu_intensive;

/// GlobalDocAddress serves as a hit address.
#[derive(Clone, Copy, Eq, Debug, PartialEq, Hash, Ord, PartialOrd)]
pub(crate) struct GlobalDocAddress<'a> {
    pub split: &'a str,
    pub doc_addr: DocAddress,
}

impl<'a> GlobalDocAddress<'a> {
    fn from_partial_hit(partial_hit: &'a PartialHit) -> Self {
        Self {
            split: &partial_hit.split_id,
            doc_addr: DocAddress {
                segment_ord: partial_hit.segment_ord,
                doc_id: partial_hit.doc_id,
            },
        }
    }
}

fn partial_hit_sorting_key(partial_hit: &PartialHit) -> (Reverse<u64>, GlobalDocAddress) {
    (
        Reverse(partial_hit.sorting_field_value),
        GlobalDocAddress::from_partial_hit(partial_hit),
    )
}

fn extract_time_range(
    start_timestamp_opt: Option<i64>,
    end_timestamp_opt: Option<i64>,
) -> Option<Range<i64>> {
    match (start_timestamp_opt, end_timestamp_opt) {
        (Some(start_timestamp), Some(end_timestamp)) => Some(Range {
            start: start_timestamp,
            end: end_timestamp,
        }),
        (_, Some(end_timestamp)) => Some(Range {
            start: i64::MIN,
            end: end_timestamp,
        }),
        (Some(start_timestamp), _) => Some(Range {
            start: start_timestamp,
            end: i64::MAX,
        }),
        _ => None,
    }
}

fn extract_split_and_footer_offsets(split_metadata: &SplitMetadata) -> SplitIdAndFooterOffsets {
    SplitIdAndFooterOffsets {
        split_id: split_metadata.split_id.clone(),
        split_footer_start: split_metadata.footer_offsets.start as u64,
        split_footer_end: split_metadata.footer_offsets.end as u64,
    }
}

/// Extract the list of relevant splits for a given search request.
async fn list_relevant_splits(
    search_request: &SearchRequest,
    metastore: &dyn Metastore,
) -> crate::Result<Vec<SplitMetadata>> {
    let time_range_opt =
        extract_time_range(search_request.start_timestamp, search_request.end_timestamp);
    let tags_filter = extract_tags_from_query(&search_request.query)?;
    let split_metas = metastore
        .list_splits(
            &search_request.index_id,
            SplitState::Published,
            time_range_opt,
            tags_filter,
        )
        .await?;
    Ok(split_metas
        .into_iter()
        .map(|metadata| metadata.split_metadata)
        .collect::<Vec<_>>())
}

/// Converts a `LeafHit` into a `Hit`.
///
/// Splits may have been created with different DocMappers.
/// For this reason, leaves are returning a document that is -as much
/// as possible-, `DocMapper` agnostic.
///
/// As a result, all documents will have the actual same schema,
/// hence facilitating the implementation on the consumer side.
///
/// For instance, if the cardinality of a field changed from single-valued
/// to multivalued, we do want the documents emitted from old splits to
/// also serialize the fields values as a JsonArray.
///
/// The `convert_leaf_hit` is critical and needs to be tested against
/// allowed DocMapper changes.
fn convert_leaf_hit(
    leaf_hit: quickwit_proto::LeafHit,
    doc_mapper: &dyn DocMapper,
) -> crate::Result<quickwit_proto::Hit> {
    let hit_json: BTreeMap<String, Vec<JsonValue>> = serde_json::from_str(&leaf_hit.leaf_json)
        .map_err(|_| SearchError::InternalError("Invalid leaf json.".to_string()))?;
    let doc = doc_mapper.doc_to_json(hit_json)?;
    let json = serde_json::to_string(&doc).expect("Json serialization should never fail.");
    Ok(quickwit_proto::Hit {
        json,
        partial_hit: leaf_hit.partial_hit,
    })
}

/// Performs a search on the current node.
/// See also `[distributed_search]`.
pub async fn single_node_search(
    search_request: &SearchRequest,
    metastore: &dyn Metastore,
    storage_resolver: StorageUriResolver,
) -> crate::Result<SearchResponse> {
    let start_instant = tokio::time::Instant::now();
    let index_metadata = metastore.index_metadata(&search_request.index_id).await?;
    let index_storage = storage_resolver.resolve(&index_metadata.index_uri)?;
    let metas = list_relevant_splits(search_request, metastore).await?;
    let split_metadata: Vec<SplitIdAndFooterOffsets> =
        metas.iter().map(extract_split_and_footer_offsets).collect();
    let doc_mapper = build_doc_mapper(
        &index_metadata.doc_mapping,
        &index_metadata.search_settings,
        &index_metadata.indexing_settings,
    )
    .map_err(|err| {
        SearchError::InternalError(format!("Failed to build doc mapper. Cause: {}", err))
    })?;
    let leaf_search_response = leaf_search(
        search_request,
        index_storage.clone(),
        &split_metadata[..],
        doc_mapper.clone(),
    )
    .await
    .context("Failed to perform leaf search.")?;
    let fetch_docs_response = fetch_docs(
        leaf_search_response.partial_hits,
        index_storage,
        &split_metadata,
    )
    .await
    .context("Failed to perform fetch docs.")?;
    let hits: Vec<quickwit_proto::Hit> = fetch_docs_response
        .hits
        .into_iter()
        .map(|leaf_hit| crate::convert_leaf_hit(leaf_hit, &*doc_mapper))
        .collect::<crate::Result<_>>()?;
    let elapsed = start_instant.elapsed();
    let aggregation = if let Some(intermediate_aggregation_result) =
        leaf_search_response.intermediate_aggregation_result
    {
        let res: IntermediateAggregationResults =
            serde_json::from_str(&intermediate_aggregation_result)?;
        let req: Aggregations = serde_json::from_str(search_request.aggregation_request())?;
        let res: AggregationResults = AggregationResults::from_intermediate_and_req(res, req)?;
        Some(serde_json::to_string(&res)?)
    } else {
        None
    };
    Ok(SearchResponse {
        aggregation,
        num_hits: leaf_search_response.num_hits,
        hits,
        elapsed_time_micros: elapsed.as_micros() as u64,
        errors: leaf_search_response
            .failed_splits
            .iter()
            .map(|error| format!("{:?}", error))
            .collect_vec(),
    })
}

/// Starts a search node, aka a `searcher`.
pub async fn start_searcher_service(
    quickwit_config: &QuickwitConfig,
    metastore: Arc<dyn Metastore>,
    storage_uri_resolver: StorageUriResolver,
    cluster: Arc<Cluster>,
) -> anyhow::Result<Arc<dyn SearchService>> {
    SEARCHER_CONFIG_INSTANCE
        .set(quickwit_config.searcher_config.clone())
        .expect("could not set searcher config in global once cell");
    let client_pool = SearchClientPool::create_and_keep_updated(cluster).await?;
    let cluster_client = ClusterClient::new(client_pool.clone());
    let search_service = Arc::new(SearchServiceImpl::new(
        metastore,
        storage_uri_resolver,
        cluster_client,
        client_pool,
    ));
    Ok(search_service)
}

#[cfg(test)]
mod tests {

    use std::collections::BTreeSet;

    use assert_json_diff::assert_json_include;
    use quickwit_doc_mapper::DefaultDocMapper;
    use quickwit_indexing::TestSandbox;
    use quickwit_proto::LeafHit;
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn test_single_node_simple() -> anyhow::Result<()> {
        let index_id = "single-node-simple-1";
        let doc_mapping_yaml = r#"
            field_mappings:
              - name: title
                type: text
              - name: body
                type: text
              - name: url
                type: text
        "#;
        let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "{}", &["body"]).await?;
        let docs = vec![
            json!({"title": "snoopy", "body": "Snoopy is an anthropomorphic beagle[5] in the comic strip...", "url": "http://snoopy"}),
            json!({"title": "beagle", "body": "The beagle is a breed of small scent hound, similar in appearance to the much larger foxhound.", "url": "http://beagle"}),
        ];
        test_sandbox.add_documents(docs.clone()).await?;
        let search_request = SearchRequest {
            index_id: index_id.to_string(),
            query: "anthropomorphic".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 2,
            start_offset: 0,
            ..Default::default()
        };
        let single_node_result = single_node_search(
            &search_request,
            &*test_sandbox.metastore(),
            test_sandbox.storage_uri_resolver(),
        )
        .await?;
        assert_eq!(single_node_result.num_hits, 1);
        assert_eq!(single_node_result.hits.len(), 1);
        let hit_json: serde_json::Value = serde_json::from_str(&single_node_result.hits[0].json)?;
        let expected_json: serde_json::Value = json!({"title": "snoopy", "body": "Snoopy is an anthropomorphic beagle[5] in the comic strip...", "url": "http://snoopy"});
        assert_json_include!(actual: hit_json, expected: expected_json);
        assert!(single_node_result.elapsed_time_micros > 10);
        assert!(single_node_result.elapsed_time_micros < 1_000_000);
        Ok(())
    }

    // TODO remove me once `Iterator::is_sorted_by_key` is stabilized.
    fn is_sorted<E, I: Iterator<Item = E>>(mut it: I) -> bool
    where E: Ord {
        let mut previous_el = if let Some(first_el) = it.next() {
            first_el
        } else {
            // The empty list is sorted!
            return true;
        };
        for next_el in it {
            if next_el < previous_el {
                return false;
            }
            previous_el = next_el;
        }
        true
    }

    #[tokio::test]
    async fn test_single_node_several_splits() -> anyhow::Result<()> {
        let index_id = "single-node-several-splits";
        let doc_mapping_yaml = r#"
            tag_fields:
              - "owner"
            field_mappings:
              - name: title
                type: text
              - name: body
                type: text
              - name: url
                type: text
              - name: owner
                type: text
                tokenizer: 'raw'
        "#;
        let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "{}", &["body"]).await?;
        for _ in 0..10u32 {
            test_sandbox.add_documents(vec![
                json!({"title": "snoopy", "body": "Snoopy is an anthropomorphic beagle[5] in the comic strip...", "url": "http://snoopy"}),
                json!({"title": "beagle", "body": "The beagle is a breed of small scent hound, similar in appearance to the much larger foxhound.", "url": "http://beagle"}),
            ]).await?;
        }
        let search_request = SearchRequest {
            index_id: index_id.to_string(),
            query: "beagle".to_string(),
            search_fields: vec![],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 6,
            start_offset: 0,
            ..Default::default()
        };
        let single_node_result = single_node_search(
            &search_request,
            &*test_sandbox.metastore(),
            test_sandbox.storage_uri_resolver(),
        )
        .await?;
        assert_eq!(single_node_result.num_hits, 20);
        assert_eq!(single_node_result.hits.len(), 6);
        assert!(&single_node_result.hits[0].json.contains("Snoopy"));
        assert!(&single_node_result.hits[1].json.contains("breed"));
        assert!(is_sorted(single_node_result.hits.iter().flat_map(|hit| {
            hit.partial_hit.as_ref().map(partial_hit_sorting_key)
        })));
        assert!(single_node_result.elapsed_time_micros > 10);
        assert!(single_node_result.elapsed_time_micros < 1_000_000);
        Ok(())
    }

    #[tokio::test]
    async fn test_single_node_filtering() -> anyhow::Result<()> {
        let index_id = "single-node-filtering";
        let doc_mapping_yaml = r#"
            tag_fields:
              - owner
            field_mappings:
              - name: body
                type: text
              - name: ts
                type: i64
                fast: true
              - name: owner
                type: text
                tokenizer: raw
        "#;
        let indexing_settings_json = r#"{
            "timestamp_field": "ts",
            "sort_field": "ts",
            "sort_order": "desc"
        }"#;
        let test_sandbox = TestSandbox::create(
            index_id,
            doc_mapping_yaml,
            indexing_settings_json,
            &["body"],
        )
        .await?;

        let mut docs = vec![];
        for i in 0..30 {
            let body = format!("info @ t:{}", i + 1);
            docs.push(json!({"body": body, "ts": i+1}));
        }
        test_sandbox.add_documents(docs).await?;

        let search_request = SearchRequest {
            index_id: index_id.to_string(),
            query: "info".to_string(),
            search_fields: vec![],
            start_timestamp: Some(10),
            end_timestamp: Some(20),
            max_hits: 15,
            start_offset: 0,
            ..Default::default()
        };
        let single_node_response = single_node_search(
            &search_request,
            &*test_sandbox.metastore(),
            test_sandbox.storage_uri_resolver(),
        )
        .await?;
        assert_eq!(single_node_response.num_hits, 10);
        assert_eq!(single_node_response.hits.len(), 10);
        assert!(&single_node_response.hits[0].json.contains("t:19"));
        assert!(&single_node_response.hits[9].json.contains("t:10"));

        // filter on time range [i64::MIN 20[ should only hit first 19 docs because of filtering
        let search_request = SearchRequest {
            index_id: index_id.to_string(),
            query: "info".to_string(),
            search_fields: vec![],
            start_timestamp: None,
            end_timestamp: Some(20),
            max_hits: 25,
            start_offset: 0,
            ..Default::default()
        };
        let single_node_response = single_node_search(
            &search_request,
            &*test_sandbox.metastore(),
            test_sandbox.storage_uri_resolver(),
        )
        .await?;
        assert_eq!(single_node_response.num_hits, 19);
        assert_eq!(single_node_response.hits.len(), 19);
        assert!(&single_node_response.hits[0].json.contains("t:19"));
        assert!(&single_node_response.hits[18].json.contains("t:1"));

        // filter on tag, should not return any hit since no split is tagged
        let search_request = SearchRequest {
            index_id: index_id.to_string(),
            query: "tag:foo AND info".to_string(),
            search_fields: vec![],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 25,
            start_offset: 0,
            ..Default::default()
        };
        let single_node_response = single_node_search(
            &search_request,
            &*test_sandbox.metastore(),
            test_sandbox.storage_uri_resolver(),
        )
        .await?;
        assert_eq!(single_node_response.num_hits, 0);
        assert_eq!(single_node_response.hits.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_single_node_split_pruning_by_tags() -> anyhow::Result<()> {
        let doc_mapping_yaml = r#"
            tag_fields:
              - owner
            field_mappings:
              - name: owner
                type: text
                tokenizer: raw
        "#;
        let index_id = "single-node-pruning-by-tags";
        let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "{}", &[]).await?;
        let owners = ["paul", "adrien"];
        for owner in owners {
            let mut docs = vec![];
            for i in 0..10 {
                docs.push(json!({"body": format!("content num #{}", i + 1), "owner": owner}));
            }
            test_sandbox.add_documents(docs).await?;
        }

        let selected_splits = list_relevant_splits(
            &SearchRequest {
                index_id: index_id.to_string(),
                query: "owner:francois".to_string(),
                ..Default::default()
            },
            &*test_sandbox.metastore(),
        )
        .await?;
        assert!(selected_splits.is_empty());

        let selected_splits = list_relevant_splits(
            &SearchRequest {
                index_id: index_id.to_string(),
                query: "".to_string(),
                ..Default::default()
            },
            &*test_sandbox.metastore(),
        )
        .await?;
        assert_eq!(selected_splits.len(), 2);

        let selected_splits = list_relevant_splits(
            &SearchRequest {
                index_id: index_id.to_string(),
                query: "owner:francois OR owner:paul OR owner:adrien".to_string(),
                ..Default::default()
            },
            &*test_sandbox.metastore(),
        )
        .await?;
        assert_eq!(selected_splits.len(), 2);

        let split_tags: BTreeSet<String> = selected_splits
            .iter()
            .flat_map(|split| split.tags.clone())
            .collect();
        assert_eq!(
            split_tags
                .iter()
                .map(|tag| tag.as_str())
                .collect::<Vec<&str>>(),
            vec!["owner!", "owner:adrien", "owner:paul"]
        );

        Ok(())
    }

    const DYNAMIC_TEST_INDEX_ID: &str = "search_dynamic_mode";

    async fn test_search_dynamic_util(test_sandbox: &TestSandbox, query: &str) -> Vec<u32> {
        let splits = test_sandbox
            .metastore()
            .list_all_splits(DYNAMIC_TEST_INDEX_ID)
            .await
            .unwrap();
        let splits_offsets: Vec<_> = splits
            .into_iter()
            .map(|split_meta| SplitIdAndFooterOffsets {
                split_id: split_meta.split_id().to_string(),
                split_footer_start: split_meta.split_metadata.footer_offsets.start,
                split_footer_end: split_meta.split_metadata.footer_offsets.end,
            })
            .collect();
        let request = quickwit_proto::SearchRequest {
            index_id: DYNAMIC_TEST_INDEX_ID.to_string(),
            query: query.to_string(),
            max_hits: 100,
            ..Default::default()
        };
        let search_response = leaf_search(
            &request,
            test_sandbox.storage(),
            &splits_offsets,
            test_sandbox.doc_mapper(),
        )
        .await
        .unwrap();
        search_response
            .partial_hits
            .into_iter()
            .map(|partial_hit| partial_hit.doc_id)
            .collect::<Vec<u32>>()
    }

    #[tokio::test]
    async fn test_search_dynamic_mode() -> anyhow::Result<()> {
        let doc_mapping_yaml = r#"
            field_mappings:
              - name: body
                type: text
                tokenizer: default
                indexed: true
            mode: dynamic
            dynamic_mapping:
                tokenizer: raw
        "#;
        let test_sandbox = TestSandbox::create(DYNAMIC_TEST_INDEX_ID, doc_mapping_yaml, "{}", &[])
            .await
            .unwrap();
        let docs = vec![
            json!({"body": "hello happy tax payer"}),
            json!({"body": "hello"}),
            json!({"body_dynamic": "hello happy tax payer"}),
            json!({"body_dynamic": "hello"}),
        ];
        test_sandbox.add_documents(docs).await.unwrap();
        {
            let docs = test_search_dynamic_util(&test_sandbox, "body:hello").await;
            assert_eq!(&docs[..], &[0u32, 1u32]);
        }
        {
            let docs = test_search_dynamic_util(&test_sandbox, "body_dynamic:hello").await;
            assert_eq!(&docs[..], &[3u32]); // 1 is not matched due to the raw tokenizer
        }
        Ok(())
    }

    #[track_caller]
    fn test_convert_leaf_hit_aux(
        default_doc_mapper_json: serde_json::Value,
        leaf_hit_json: serde_json::Value,
        expected_hit_json: serde_json::Value,
    ) {
        let default_doc_mapper: DefaultDocMapper =
            serde_json::from_value(default_doc_mapper_json).unwrap();
        let hit = convert_leaf_hit(
            LeafHit {
                leaf_json: serde_json::to_string(&leaf_hit_json).unwrap(),
                partial_hit: Default::default(),
            },
            &default_doc_mapper,
        )
        .unwrap();
        let hit_json: serde_json::Value = serde_json::from_str(&hit.json).unwrap();
        assert_eq!(hit_json, expected_hit_json);
    }

    #[test]
    fn test_convert_leaf_hit_multiple_cardinality() {
        test_convert_leaf_hit_aux(
            json!({
                "field_mappings": [
                    { "name": "body", "type": "array<text>" }
                ],
                "mode": "lenient"
            }),
            json!({ "body": ["hello", "happy"] }),
            json!({ "body": ["hello", "happy"] }),
        );
    }

    #[test]
    fn test_convert_leaf_hit_simple_cardinality() {
        test_convert_leaf_hit_aux(
            json!({
                "field_mappings": [
                    { "name": "body", "type": "text" }
                ],
                "mode": "lenient"
            }),
            json!({ "body": ["hello", "happy"] }),
            json!({ "body": "hello" }),
        );
    }

    #[test]
    fn test_convert_dynamic() {
        test_convert_leaf_hit_aux(
            json!({
                "field_mappings": [
                    { "name": "body", "type": "text" }
                ],
                "mode": "dynamic"
            }),
            json!({ "body": ["hello", "happy"], "_dynamic": [{"title": "hello"}] }),
            json!({ "body": "hello", "title": "hello" }),
        );
    }

    #[test]
    fn test_convert_leaf_object() {
        test_convert_leaf_hit_aux(
            json!({
                "field_mappings": [
                    {
                        "name": "user",
                        "type": "object",
                        "field_mappings": [
                            {"name": "username", "type": "text"},
                            {"name": "email", "type": "text"}
                        ]
                    }
                ],
                "mode": "lenient"
            }),
            json!({ "user.username": ["fulmicoton"], "user.email": ["werwe33@quickwit.io"]}),
            json!({ "user": {"username": "fulmicoton", "email": "werwe33@quickwit.io"}}),
        );
    }

    #[test]
    fn test_convert_leaf_object_used_to_be_dynamic() {
        test_convert_leaf_hit_aux(
            json!({
                "field_mappings": [
                    {
                        "name": "user",
                        "type": "object",
                        "field_mappings": [
                            {"name": "username", "type": "text"},
                        ]
                    }
                ],
                "mode": "dynamic"
            }),
            json!({ "_dynamic": [{ "user": {"username": "fulmicoton", "email": "werwe33@quickwit.io"}}]}),
            json!({ "user": {"username": "fulmicoton", "email": "werwe33@quickwit.io"}}),
        );
        test_convert_leaf_hit_aux(
            json!({
                "field_mappings": [
                    {
                        "name": "user",
                        "type": "object",
                        "field_mappings": [
                            {"name": "username", "type": "text"},
                        ]
                    }
                ],
                "mode": "dynamic"
            }),
            json!({ "_dynamic": [{ "user": {"email": "werwe33@quickwit.io"}}], "user.username": ["fulmicoton"] }),
            json!({ "user": {"username": "fulmicoton", "email": "werwe33@quickwit.io"}}),
        );
    }

    // This spec might change in the future. THe mode has no impact on the
    // output of convert_leaf_doc. In particular, it does not ignore the previously gathered
    // dynamic field.
    #[test]
    fn test_convert_leaf_object_arguable_mode_does_not_affect_format() {
        test_convert_leaf_hit_aux(
            json!({ "mode": "strict" }),
            json!({ "_dynamic": [{ "user": {"username": "fulmicoton", "email": "werwe33@quickwit.io"}}]}),
            json!({ "user": {"username": "fulmicoton", "email": "werwe33@quickwit.io"}}),
        );
    }

    #[test]
    fn test_convert_leaf_hit_with_source() {
        test_convert_leaf_hit_aux(
            json!({
                "field_mappings": [ {"name": "username", "type": "text"} ],
                "mode": "strict"
            }),
            json!({ "_source": [{"username": "fulmicoton"}], "username": ["fulmicoton"] }),
            json!({ "username": "fulmicoton", "_source": {"username": "fulmicoton"}}),
        );
    }
}
