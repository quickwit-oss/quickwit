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
mod client_pool;
mod cluster_client;
mod collector;
mod error;
mod fetch_docs;
mod filters;
mod leaf;
mod rendezvous_hasher;
mod retry;
mod root;
mod search_response_rest;
mod search_stream;
mod service;

/// Refer to this as `crate::Result<T>`.
pub type Result<T> = std::result::Result<T, SearchError>;

use std::cmp::Reverse;
use std::net::SocketAddr;
use std::ops::Range;

use anyhow::Context;
use quickwit_metastore::{Metastore, MetastoreResult, SplitMetadataAndFooterOffsets, SplitState};
use quickwit_proto::{PartialHit, SearchRequest, SearchResponse, SplitIdAndFooterOffsets};
use quickwit_storage::StorageUriResolver;
use tantivy::DocAddress;

pub use crate::client::{create_search_service_client, SearchServiceClient};
pub use crate::client_pool::search_client_pool::SearchClientPool;
pub use crate::client_pool::ClientPool;
pub use crate::cluster_client::ClusterClient;
pub use crate::error::{parse_grpc_error, SearchError};
use crate::fetch_docs::fetch_docs;
use crate::leaf::leaf_search;
pub use crate::root::root_search;
pub use crate::search_response_rest::SearchResponseRest;
pub use crate::search_stream::root_search_stream;
pub use crate::service::{MockSearchService, SearchService, SearchServiceImpl};

/// Compute the SWIM port from the HTTP port.
/// Add 1 to the HTTP port to get the SWIM port.
pub fn http_addr_to_swim_addr(http_addr: SocketAddr) -> SocketAddr {
    SocketAddr::new(http_addr.ip(), http_addr.port() + 1)
}

/// Compute the gRPC port from the HTTP port.
/// Add 2 to the HTTP port to get the gRPC port.
pub fn http_addr_to_grpc_addr(http_addr: SocketAddr) -> SocketAddr {
    SocketAddr::new(http_addr.ip(), http_addr.port() + 2)
}

/// Compute the gRPC port from the SWIM port.
/// Add 1 to the SWIM port to get the gRPC port.
pub fn swim_addr_to_grpc_addr(swim_addr: SocketAddr) -> SocketAddr {
    SocketAddr::new(swim_addr.ip(), swim_addr.port() + 1)
}

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

fn extract_time_range(search_request: &SearchRequest) -> Option<Range<i64>> {
    match (search_request.start_timestamp, search_request.end_timestamp) {
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

fn extract_split_and_footer_offsets(
    split_metadata_and_footer_offsets: &SplitMetadataAndFooterOffsets,
) -> SplitIdAndFooterOffsets {
    SplitIdAndFooterOffsets {
        split_id: split_metadata_and_footer_offsets
            .split_metadata
            .split_id
            .clone(),
        split_footer_start: split_metadata_and_footer_offsets.footer_offsets.start as u64,
        split_footer_end: split_metadata_and_footer_offsets.footer_offsets.end as u64,
    }
}

/// Extract the list of relevant splits for a given search request.
async fn list_relevant_splits(
    search_request: &SearchRequest,
    metastore: &dyn Metastore,
) -> MetastoreResult<Vec<SplitMetadataAndFooterOffsets>> {
    let time_range_opt = extract_time_range(search_request);
    let split_metas = metastore
        .list_splits(
            &search_request.index_id,
            SplitState::Published,
            time_range_opt,
            &search_request.tags,
        )
        .await?;
    Ok(split_metas)
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
    let index_config = index_metadata.index_config;
    let leaf_search_result = leaf_search(
        search_request,
        index_storage.clone(),
        &split_metadata[..],
        index_config,
    )
    .await
    .context("Failed to perform leaf search.")?;
    let fetch_docs_result = fetch_docs(
        leaf_search_result.partial_hits,
        index_storage,
        &split_metadata,
    )
    .await
    .context("Failed to perform fetch docs.")?;
    let elapsed = start_instant.elapsed();
    Ok(SearchResponse {
        num_hits: leaf_search_result.num_hits,
        hits: fetch_docs_result.hits,
        elapsed_time_micros: elapsed.as_micros() as u64,
        errors: vec![],
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_json_diff::assert_json_include;
    use quickwit_index_config::{DefaultIndexConfigBuilder, WikipediaIndexConfig};
    use quickwit_indexing::TestSandbox;
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn test_single_node_simple() -> anyhow::Result<()> {
        let index_id = "single-node-simple-1";
        let test_sandbox =
            TestSandbox::create(index_id, Arc::new(WikipediaIndexConfig::new())).await?;
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
            tags: vec![],
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
        let expected_json: serde_json::Value = json!({"title": ["snoopy"], "body": ["Snoopy is an anthropomorphic beagle[5] in the comic strip..."], "url": ["http://snoopy"]});
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
        let test_sandbox =
            TestSandbox::create(index_id, Arc::new(WikipediaIndexConfig::new())).await?;
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
            tags: vec![],
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
        let index_config = r#"{
            "default_search_fields": ["body"],
            "timestamp_field": "ts",
            "tag_fields": [],
            "field_mappings": [
                {
                    "name": "body",
                    "type": "text"
                },
                {
                    "name": "ts",
                    "type": "i64",
                    "fast": true
                }
            ]
        }"#;
        let index_config =
            serde_json::from_str::<DefaultIndexConfigBuilder>(index_config)?.build()?;
        let index_id = "single-node-filtering";
        let test_sandbox = TestSandbox::create(index_id, Arc::new(index_config)).await?;

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
            tags: vec![],
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
            tags: vec![],
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
            query: "info".to_string(),
            search_fields: vec![],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 25,
            start_offset: 0,
            tags: vec!["foo".to_string()],
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
}
