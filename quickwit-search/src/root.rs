/*
 * Copyright (C) 2021 Quickwit Inc.
 *
 * Quickwit is offered under the AGPL v3.0 and as commercial software.
 * For commercial licensing, contact us at hello@quickwit.io.
 *
 * AGPL:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

use std::collections::HashMap;
use std::sync::Arc;

use tantivy::TantivyError;
use tokio::task::JoinHandle;
use tracing::*;

use quickwit_metastore::{Metastore, SplitMetadata};
use quickwit_proto::{
    FetchDocsRequest, FetchDocsResult, Hit, LeafSearchRequest, LeafSearchResult, PartialHit,
    SearchRequest, SearchResult,
};
use tantivy::collector::Collector;
use tokio::task::spawn_blocking;

use crate::client_pool::Job;
use crate::error::parse_grpc_error;
use crate::list_relevant_splits;
use crate::make_collector;
use crate::ClientPool;
use crate::SearchClientPool;
use crate::SearchError;

// Measure the cost associated to searching in a given split metadata.
fn compute_split_cost(_split_metadata: &SplitMetadata) -> u32 {
    //TODO: Have a smarter cost, by smoothing the number of docs.
    1
}

/// Perform a distributed search.
/// It sends a search request over gRPC to multiple leaf nodes and merges the search results.
pub async fn root_search(
    search_request: &SearchRequest,
    metastore: &dyn Metastore,
    client_pool: &Arc<SearchClientPool>,
) -> Result<SearchResult, SearchError> {
    let start_instant = tokio::time::Instant::now();

    // Create a job for leaf node search and assign the splits that the node is responsible for based on the job.
    let split_metadata_list = list_relevant_splits(search_request, metastore).await?;

    // Create a hash map of SplitMetadata with split id as a key.
    let split_metadata_map: HashMap<String, SplitMetadata> = split_metadata_list
        .into_iter()
        .map(|split_metadata| (split_metadata.split_id.clone(), split_metadata))
        .collect();

    // Create a job for fetching docs and assign the splits that the node is responsible for based on the job.
    let leaf_search_jobs: Vec<Job> = split_metadata_map
        .keys()
        .map(|split_id| {
            // TODO: Change to a better way that does not use unwrap().
            let split_metadata = split_metadata_map.get(split_id).unwrap();
            Job {
                split: split_id.clone(),
                cost: compute_split_cost(split_metadata),
            }
        })
        .collect();

    let assigned_leaf_search_jobs = client_pool.assign_jobs(leaf_search_jobs).await?;
    debug!(assigned_leaf_search_jobs=?assigned_leaf_search_jobs, "Assigned leaf search jobs.");

    // Create search request with start offset is 0 that used by leaf node search.
    let mut search_request_with_offset_0 = search_request.clone();
    search_request_with_offset_0.start_offset = 0;
    search_request_with_offset_0.max_hits += search_request.start_offset;

    // Perform the query phase.
    let mut leaf_search_handles: Vec<JoinHandle<Result<LeafSearchResult, tonic::Status>>> =
        Vec::new();
    for (search_client, jobs) in assigned_leaf_search_jobs.iter() {
        let leaf_search_request = LeafSearchRequest {
            search_request: Some(search_request_with_offset_0.clone()),
            split_ids: jobs.iter().map(|job| job.split.clone()).collect(),
        };

        // TODO wrap search clients with some info like their socketaddr to be able to log useful debug information
        debug!(leaf_search_request=?leaf_search_request, "Leaf node search.");
        let mut search_client_clone = search_client.clone();
        let handle = tokio::spawn(async move {
            search_client_clone
                .leaf_search(leaf_search_request)
                .await
                .map(|resp| resp.into_inner())
        });
        leaf_search_handles.push(handle);
    }
    let leaf_search_responses = futures::future::try_join_all(leaf_search_handles).await?; //< An error here means that the tokio task panicked... Not that the grpc erorred.
                                                                                           //< The latter is handled later.
    let index_metadata = metastore.index_metadata(&search_request.index_id).await?;
    let index_config = index_metadata.index_config;
    let collector = make_collector(index_config.as_ref(), search_request);

    // Find the sum of the number of hits and merge multiple partial hits into a single partial hits.
    let mut leaf_search_results = Vec::new();
    for leaf_search_response in leaf_search_responses {
        match leaf_search_response {
            Ok(leaf_search_result) => {
                debug!(leaf_search_result=?leaf_search_result, "Leaf search result.");
                leaf_search_results.push(leaf_search_result)
            }
            Err(grpc_error) => {
                let leaf_search_error = parse_grpc_error(&grpc_error);
                error!(error=?grpc_error, "Leaf request failed");
                // TODO list failed leaf nodes and retry.
                return Err(leaf_search_error);
            }
        }
    }
    let leaf_search_result = spawn_blocking(move || collector.merge_fruits(leaf_search_results))
        .await?
        .map_err(|merge_error: TantivyError| {
            crate::SearchError::InternalError(format!("{}", merge_error))
        })?;
    debug!(leaf_search_result=?leaf_search_result, "Merged leaf search result.");

    // Create a hash map of PartialHit with split as a key.
    let mut partial_hits_map: HashMap<String, Vec<PartialHit>> = HashMap::new();
    for partial_hit in leaf_search_result.partial_hits.iter() {
        partial_hits_map
            .entry(partial_hit.split_id.clone())
            .or_insert_with(Vec::new)
            .push(partial_hit.clone());
    }

    // Perform the fetch docs phese.
    let mut fetch_docs_handles: Vec<JoinHandle<anyhow::Result<FetchDocsResult>>> = Vec::new();
    for (search_client, jobs) in assigned_leaf_search_jobs.iter() {
        for job in jobs {
            // TODO group fetch doc requests.
            if let Some(partial_hits) = partial_hits_map.get(&job.split) {
                let fetch_docs_request = FetchDocsRequest {
                    partial_hits: partial_hits.clone(),
                    index_id: search_request.index_id.clone(),
                };
                let mut search_client_clone = search_client.clone();
                let handle = tokio::spawn(async move {
                    match search_client_clone.fetch_docs(fetch_docs_request).await {
                        Ok(resp) => Ok(resp.into_inner()),
                        Err(err) => Err(anyhow::anyhow!("Failed to fetch docs due to {:?}", err)),
                    }
                });
                fetch_docs_handles.push(handle);
            }
        }
    }
    let fetch_docs_responses = futures::future::try_join_all(fetch_docs_handles).await?;

    // Merge the fetched docs.
    let mut hits: Vec<Hit> = Vec::new();
    for response in fetch_docs_responses {
        match response {
            Ok(fetch_docs_result) => {
                hits.extend(fetch_docs_result.hits);
            }
            // TODO handle failure.
            Err(err) => error!(err=?err),
        }
    }
    hits.sort_by(|hit1, hit2| {
        let value1 = if let Some(partial_hit) = &hit1.partial_hit {
            partial_hit.sorting_field_value
        } else {
            0
        };
        let value2 = if let Some(partial_hit) = &hit2.partial_hit {
            partial_hit.sorting_field_value
        } else {
            0
        };
        // Sort by descending order.
        value2.cmp(&value1)
    });

    let elapsed = start_instant.elapsed();

    Ok(SearchResult {
        num_hits: leaf_search_result.num_hits,
        hits,
        elapsed_time_micros: elapsed.as_micros() as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::TcpListener;

    use serde_json::{json, Value};

    use quickwit_cluster::cluster::{read_host_key, Cluster};
    use quickwit_common::to_socket_addr;
    use quickwit_core::TestSandbox;
    use quickwit_index_config::WikipediaIndexConfig;

    use crate::http_addr_to_swim_addr;
    use crate::MockSearchService;

    #[tokio::test]
    async fn test_root_search_single_node_single_split() -> anyhow::Result<()> {
        let index_id = "wikipedia-idx";

        let test_sandbox =
            TestSandbox::create(index_id, Box::new(WikipediaIndexConfig::new())).await?;

        let docs1: Vec<Value> = vec![
            json!({"title": "snoopy", "body": "Snoopy is an anthropomorphic beagle[5] in the comic strip...", "url": "http://snoopy"}),
            json!({"title": "beagle", "body": "The beagle is a breed of small scent hound, similar in appearance to the much larger foxhound.", "url": "http://beagle"}),
        ];

        test_sandbox.add_documents(docs1.clone()).await?;

        let search_request = quickwit_proto::SearchRequest {
            index_id: index_id.to_string(),
            query: "snoopy".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 10,
            start_offset: 0,
        };

        let metastore = test_sandbox.metastore();

        let tmp_dir = tempfile::tempdir().unwrap();
        let host_key_path = tmp_dir.path().join("host_key");
        let host_key = read_host_key(&host_key_path)?;

        let rest_addr_str = format!("0.0.0.0:{}", available_port()?);
        let rest_addr = to_socket_addr(&rest_addr_str)?;
        let swim_addr = http_addr_to_swim_addr(rest_addr);

        let cluster = Arc::new(Cluster::new(host_key, swim_addr)?);

        let client_pool = Arc::new(SearchClientPool::new(cluster.clone()).await?);

        let mut mock_search_service = MockSearchService::new();
        mock_search_service.expect_leaf_search().returning(
            |_leaf_tonic_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResult {
                    partial_hits: Vec::new(),
                    num_hits: 0,
                })
            },
        );

        let search_result = root_search(&search_request, &*metastore, &client_pool).await?;
        println!("search_result={:?}", search_result);

        cluster.leave();

        tmp_dir.close().unwrap();

        Ok(())
    }

    fn available_port() -> anyhow::Result<u16> {
        match TcpListener::bind("127.0.0.1:0") {
            Ok(listener) => Ok(listener.local_addr().unwrap().port()),
            Err(e) => anyhow::bail!(e),
        }
    }
}
