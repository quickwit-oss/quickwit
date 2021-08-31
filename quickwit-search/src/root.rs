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
use std::collections::HashSet;

use std::net::SocketAddr;
use std::sync::Arc;

use itertools::Itertools;
use quickwit_metastore::IndexMetadata;
use quickwit_metastore::SplitMetadataAndFooterOffsets;
use quickwit_proto::SplitSearchError;
use tantivy::collector::Collector;
use tantivy::TantivyError;
use tokio::task::spawn_blocking;
use tracing::*;

use quickwit_metastore::{Metastore, SplitMetadata};
use quickwit_proto::{
    FetchDocsRequest, FetchDocsResult, Hit, LeafSearchRequest, LeafSearchResult, PartialHit,
    SearchRequest, SearchResult,
};

use crate::client_pool::Job;
use crate::collector::make_merge_collector;
use crate::error::NodeSearchError;
use crate::extract_split_and_footer_offsets;
use crate::list_relevant_splits;
use crate::ClientPool;
use crate::SearchClientPool;
use crate::SearchError;
use crate::SearchServiceClient;

type SearchResultsByAddr = HashMap<SocketAddr, Result<LeafSearchResult, NodeSearchError>>;
type FetchDocsResultsByAddr = HashMap<SocketAddr, Result<FetchDocsResult, NodeSearchError>>;

/// Performs a distributed search.
/// It sends a search request over gRPC to multiple leaf nodes and merges the search results.
///
/// Retry Logic:
/// After a first round of leaf_search requests, we identify the list of failed but retryable splits,
/// and retry them. Complete failure against nodes are also considered retryable splits.
/// The leaf nodes which did not return any result are suspected to be unhealthy and are excluded
/// from this retry round. If all nodes are unhealthy the retry will not exclude any nodes.
///
pub async fn root_search(
    search_request: &SearchRequest,
    metastore: &dyn Metastore,
    client_pool: &Arc<SearchClientPool>,
) -> Result<SearchResult, SearchError> {
    let start_instant = tokio::time::Instant::now();

    let index_metadata = metastore.index_metadata(&search_request.index_id).await?;
    let split_metadata_list = list_relevant_splits(search_request, metastore).await?;

    // Phase 1: fetch partial hits from leaves.
    let split_metadata_map: HashMap<String, SplitMetadataAndFooterOffsets> = split_metadata_list
        .into_iter()
        .map(|metadata| (metadata.split_metadata.split_id.clone(), metadata))
        .collect();
    let split_ids = split_metadata_map.keys().collect();
    let result_per_node_addr = execute_search_with_retry(
        search_request,
        client_pool,
        &index_metadata,
        &split_ids,
        &split_metadata_map,
    )
    .await?;
    let leaf_search_results = result_per_node_addr
        .into_iter()
        .map(|(_, result)| result)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|node_search_error| {
            error!(error=?node_search_error, "Leaf request failed");
            node_search_error.search_error
        })?;

    let merge_collector = make_merge_collector(search_request);
    let leaf_search_result =
        spawn_blocking(move || merge_collector.merge_fruits(leaf_search_results))
            .await?
            .map_err(|merge_error: TantivyError| {
                crate::SearchError::InternalError(format!("{}", merge_error))
            })?;
    debug!(leaf_search_result=?leaf_search_result, "Merged leaf search result.");
    if !leaf_search_result.failed_splits.is_empty() {
        error!(error=?leaf_search_result.failed_splits, "Leaf request contains failed splits");
        return Err(SearchError::InternalError(format!(
            "{:?}",
            leaf_search_result.failed_splits
        )));
    }

    // Phase 2: fetch full docs to build hits.
    let fetch_docs_results = execute_fetch_docs_with_retry(
        client_pool,
        &search_request.index_id,
        &index_metadata.index_uri,
        &leaf_search_result.partial_hits,
        &split_metadata_map,
    )
    .await?;
    let docs_results = fetch_docs_results
        .into_iter()
        .map(|(_, result)| result)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|node_search_error| {
            error!(error=?node_search_error, "Fetch doc request failed");
            node_search_error.search_error
        })?;
    let mut hits: Vec<Hit> = docs_results
        .into_iter()
        .flat_map(|result| result.hits)
        .collect();
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
        errors: vec![],
    })
}

#[derive(Debug)]
pub struct ErrorRetries<'a> {
    #[allow(dead_code)]
    retry_split_ids: HashSet<&'a String>,
    #[allow(dead_code)]
    nodes_to_avoid: HashSet<SocketAddr>,
}

fn is_complete_failure(leaf_search_result: &Result<LeafSearchResult, NodeSearchError>) -> bool {
    if let Ok(ok_res) = leaf_search_result {
        ok_res.failed_splits.len() as u64 == ok_res.num_attempted_splits
    } else {
        true
    }
}

// Spawns all leaf search requests and return leaf search results by node address.
async fn execute_search(
    search_request: &SearchRequest,
    client_pool: &Arc<SearchClientPool>,
    index_metadata: &IndexMetadata,
    split_ids: &HashSet<&String>,
    split_metadata_map: &HashMap<String, SplitMetadataAndFooterOffsets>,
    excluded_addresses: &HashSet<SocketAddr>,
) -> anyhow::Result<SearchResultsByAddr> {
    let leaf_search_jobs = job_for_splits(split_ids, split_metadata_map);
    let assigned_leaf_search_jobs = client_pool
        .assign_jobs(leaf_search_jobs, excluded_addresses)
        .await?;
    let index_config_str = serde_json::to_string(&index_metadata.index_config)?;
    // Create search request with start offset is 0 that used by leaf node search.
    let mut search_request_with_offset_0 = search_request.clone();
    search_request_with_offset_0.start_offset = 0;
    search_request_with_offset_0.max_hits += search_request.start_offset;

    let mut result_per_node_addr_futures = HashMap::new();
    for (search_client, jobs) in assigned_leaf_search_jobs.iter() {
        let leaf_search_request = LeafSearchRequest {
            search_request: Some(search_request_with_offset_0.clone()),
            split_metadata: jobs
                .iter()
                .map(|job| extract_split_and_footer_offsets(&job.metadata))
                .collect(),
            index_config: index_config_str.to_string(),
            index_uri: index_metadata.index_uri.to_string(),
        };
        debug!(leaf_search_request=?leaf_search_request, grpc_addr=?search_client.grpc_addr(), "Leaf node search.");
        let mut search_client_clone: SearchServiceClient = search_client.clone();
        let handle = tokio::spawn(async move {
            let split_ids = leaf_search_request
                .split_metadata
                .iter()
                .map(|metadata| metadata.split_id.to_string())
                .collect_vec();
            search_client_clone
                .leaf_search(leaf_search_request)
                .await
                .map_err(|search_error| NodeSearchError {
                    search_error,
                    split_ids,
                })
        });
        result_per_node_addr_futures.insert(search_client.grpc_addr(), handle);
    }
    let mut result_per_node_addr = HashMap::new();
    for (addr, search_result) in result_per_node_addr_futures {
        ////< An error here means that the tokio task panicked... Not that the grpc erorred.
        //< The latter is handled later.
        result_per_node_addr.insert(addr, search_result.await?);
    }

    Ok(result_per_node_addr)
}

// Spawns all leaf search requests and return leaf search results by node address with a retry
// if there are some errors on split result.
async fn execute_search_with_retry(
    search_request: &SearchRequest,
    client_pool: &Arc<SearchClientPool>,
    index_metadata: &IndexMetadata,
    split_ids: &HashSet<&String>,
    split_metadata_map: &HashMap<String, SplitMetadataAndFooterOffsets>,
) -> anyhow::Result<SearchResultsByAddr> {
    let mut result_per_node_addr = execute_search(
        search_request,
        client_pool,
        index_metadata,
        split_ids,
        split_metadata_map,
        &HashSet::new(),
    )
    .await?;
    let retry_action_opt = analyze_errors(&result_per_node_addr);

    if retry_action_opt.is_none() {
        return Ok(result_per_node_addr);
    }

    let retry_action = retry_action_opt.unwrap();
    let new_results = execute_search(
        search_request,
        client_pool,
        index_metadata,
        &retry_action.retry_split_ids,
        split_metadata_map,
        &retry_action.nodes_to_avoid,
    )
    .await?;
    merge_search_results(&mut result_per_node_addr, new_results);

    Ok(result_per_node_addr)
}

/// There are different information which could be useful
/// Scenario: Multiple requests on a node
/// - Are all requests of one node failing? -> Don't consider this node as alternative of failing
/// requests
/// - Is the node ok, but just the split failing?
/// - Did all requests fail? (Should we retry in that case?)
fn analyze_errors(search_result: &SearchResultsByAddr) -> Option<ErrorRetries> {
    // Here we collect the failed requests on the node. It does not yet include failed requests
    // against that node
    let mut retry_split_ids = search_result
        .values()
        .filter_map(|result| result.as_ref().ok())
        .flat_map(|res| {
            res.failed_splits
                .iter()
                .filter(move |failed_splits| failed_splits.retryable_error) // only retry splits marked as retryable
                .map(|failed_splits| &failed_splits.split_id)
        })
        .collect::<HashSet<&String>>();

    // Include failed requests against that node
    let failed_splits = search_result
        .values()
        .filter_map(|result| result.as_ref().err())
        .flat_map(|err| err.split_ids.iter())
        .collect::<HashSet<&String>>();
    retry_split_ids.extend(&failed_splits);

    let contains_retryable_error = retry_split_ids.is_empty();
    if contains_retryable_error {
        return None;
    }

    let (complete_failure_nodes, partial_or_no_failure_nodes): (Vec<_>, Vec<_>) = search_result
        .iter()
        .partition(|(_addr, leaf_search_result)| is_complete_failure(leaf_search_result));
    let complete_failure_nodes_addr = complete_failure_nodes
        .into_iter()
        .map(|(addr, _)| *addr)
        .collect_vec();
    let partial_or_no_failure_nodes_addr = partial_or_no_failure_nodes
        .into_iter()
        .map(|(addr, _)| *addr)
        .collect_vec();

    info!("complete_failure_nodes: {:?}", &complete_failure_nodes_addr);
    info!(
        "partial_or_no_failure_nodes: {:?}",
        &partial_or_no_failure_nodes_addr
    );

    let nodes_to_avoid = complete_failure_nodes_addr;

    Some(ErrorRetries {
        retry_split_ids,
        nodes_to_avoid: nodes_to_avoid.into_iter().collect(),
    })
}

pub(crate) fn job_for_splits(
    split_ids: &HashSet<&String>,
    split_metadata_map: &HashMap<String, SplitMetadataAndFooterOffsets>,
) -> Vec<Job> {
    // Create a job for fetching docs and assign the splits that the node is responsible for based on the job.
    let leaf_search_jobs: Vec<Job> = split_metadata_map
        .iter()
        .filter(|(split_id, _)| split_ids.contains(split_id))
        .map(|(_split_id, metadata)| Job {
            metadata: metadata.clone(),
            cost: compute_split_cost(&metadata.split_metadata),
        })
        .collect();
    leaf_search_jobs
}

// Measure the cost associated to searching in a given split metadata.
fn compute_split_cost(_split_metadata: &SplitMetadata) -> u32 {
    //TODO: Have a smarter cost, by smoothing the number of docs.
    1
}

// Merges retry search results (second run) into the first search results which contains some errors.
fn merge_search_results(first_run: &mut SearchResultsByAddr, second_run: SearchResultsByAddr) {
    // Clean out old errors
    //
    // When we retry requests, we delete the old error.
    // We have complete errors against that node that we remove here, because complete errors
    // are considered retryable.
    //
    // Below we remove partial errors which are retryable.
    let complete_errors = first_run
        .iter()
        .filter(|(_addr, res)| res.is_err())
        .map(|(addr, _err)| *addr)
        .collect_vec();
    for err_addr in complete_errors {
        first_run.remove(&err_addr);
    }
    // Remove partial, retryable errors
    // In this step we have only retryable errors, since it aborts with non-retryable errors, so we can just replace them.
    for result in first_run.values_mut().flatten() {
        let contains_non_retryable_errors =
            result.failed_splits.iter().any(|err| !err.retryable_error);
        assert!(!contains_non_retryable_errors, "Result still contains non-retryable errors, but logic expects to have aborted with non-retryable errors. (this may change if we add partial results) ");

        result.failed_splits = vec![];
    }

    for (addr, new_result) in second_run {
        match (first_run.get_mut(&addr), new_result) {
            (Some(Ok(orig_res)), Ok(result)) => {
                orig_res.num_hits += result.num_hits;
                orig_res.num_attempted_splits += result.num_attempted_splits;
                orig_res
                    .failed_splits
                    .extend(result.failed_splits.into_iter());
                orig_res
                    .partial_hits
                    .extend(result.partial_hits.into_iter());
            }
            (Some(Ok(orig_res)), Err(err)) => {
                orig_res
                    .failed_splits
                    .extend(err.split_ids.iter().map(|split_id| SplitSearchError {
                        error: err.search_error.to_string(),
                        split_id: split_id.to_string(),
                        retryable_error: true,
                    }));
            }
            (Some(Err(err)), _) => {
                panic!("unexpected error left over: {:?}", err);
            }
            (None, new_result) => {
                first_run.insert(addr, new_result);
            }
        }
    }
}

// Spawns all leaf fetch docs requests and return leaf search results by node address.
async fn execute_fetch_docs(
    client_pool: &Arc<SearchClientPool>,
    index_id: &str,
    index_uri: &str,
    partial_hits: &[PartialHit],
    split_metadata_map: &HashMap<String, SplitMetadataAndFooterOffsets>,
    excluded_addresses: &HashSet<SocketAddr>,
) -> Result<FetchDocsResultsByAddr, SearchError> {
    let mut partial_hits_map: HashMap<String, Vec<PartialHit>> = HashMap::new();
    for partial_hit in partial_hits.iter() {
        partial_hits_map
            .entry(partial_hit.split_id.clone())
            .or_insert_with(Vec::new)
            .push(partial_hit.clone());
    }
    let jobs = partial_hits_map
        .keys()
        .map(|split_id| Job {
            metadata: split_metadata_map.get(split_id).unwrap().clone(),
            cost: 1,
        })
        .collect_vec();
    let doc_fetch_jobs = client_pool.assign_jobs(jobs, excluded_addresses).await?;
    let mut result_per_node_addr_futures = HashMap::new();
    for (search_client, jobs) in doc_fetch_jobs.iter() {
        let split_ids: Vec<_> = jobs
            .iter()
            .map(|job| job.metadata.split_metadata.split_id.clone())
            .collect();
        let partial_hits: Vec<_> = split_ids
            .iter()
            .map(|split_id| partial_hits_map.remove(split_id).expect("cannot happen"))
            .flatten()
            .collect();
        let split_metadata: Vec<_> = split_ids
            .iter()
            .map(|split_id| {
                extract_split_and_footer_offsets(
                    split_metadata_map.get(split_id).expect("cannot happend"),
                )
            })
            .collect();
        let fetch_docs_request = FetchDocsRequest {
            index_uri: index_uri.to_string(),
            partial_hits,
            index_id: index_id.to_string(),
            split_metadata,
        };
        let mut search_client_clone = search_client.clone();
        let handle = tokio::spawn(async move {
            search_client_clone
                .fetch_docs(fetch_docs_request)
                .await
                .map_err(|search_error| NodeSearchError {
                    search_error,
                    split_ids,
                })
        });
        result_per_node_addr_futures.insert(search_client.grpc_addr(), handle);
    }

    let mut result_per_node_addr = HashMap::new();
    for (addr, search_result) in result_per_node_addr_futures {
        result_per_node_addr.insert(addr, search_result.await?);
    }
    Ok(result_per_node_addr)
}

// Spawns all leaf fetch docs requests and return leaf search results by node address with retry
// if there are some errors on split result.
async fn execute_fetch_docs_with_retry(
    client_pool: &Arc<SearchClientPool>,
    index_id: &str,
    index_uri: &str,
    partial_hits: &[PartialHit],
    split_metadata_map: &HashMap<String, SplitMetadataAndFooterOffsets>,
) -> Result<FetchDocsResultsByAddr, SearchError> {
    let mut results = execute_fetch_docs(
        client_pool,
        index_id,
        index_uri,
        partial_hits,
        split_metadata_map,
        &HashSet::new(),
    )
    .await?;
    let retry_opt = analyze_fetch_docs_errors(&results);
    if retry_opt.is_none() {
        return Ok(results);
    }
    let retry_action = retry_opt.unwrap();
    let retry_results = execute_fetch_docs(
        client_pool,
        index_id,
        index_uri,
        partial_hits,
        split_metadata_map,
        &retry_action.nodes_to_avoid,
    )
    .await?;
    merge_fetch_docs_results(&mut results, retry_results);
    Ok(results)
}

// Dumb analysis: just identify nodes with some errors.
fn analyze_fetch_docs_errors(results: &FetchDocsResultsByAddr) -> Option<ErrorRetries> {
    let mut nodes_to_avoid = HashSet::new();
    let mut retry_split_ids = HashSet::new();
    for (addr, result) in results.iter() {
        if result.is_err() {
            nodes_to_avoid.insert(*addr);
            retry_split_ids.extend(result.as_ref().err().unwrap().split_ids.iter());
        }
    }
    if nodes_to_avoid.is_empty() {
        return None;
    }
    Some(ErrorRetries {
        nodes_to_avoid,
        retry_split_ids,
    })
}

// Merges fetch docs results (second run) into the first fetch docs results which contains some errors.
fn merge_fetch_docs_results(
    first_run: &mut FetchDocsResultsByAddr,
    second_run: FetchDocsResultsByAddr,
) {
    // Remove errors from first_run.
    first_run.retain(|_, result| result.is_ok());
    // Add hits from second_run into first_run.
    for (addr, second_run_result) in second_run.into_iter() {
        match (first_run.get_mut(&addr), second_run_result) {
            (Some(Ok(first_result)), Ok(mut second_result)) => {
                first_result.hits.append(&mut second_result.hits);
            }
            (None, Ok(second_result)) => {
                first_run.insert(addr, Ok(second_result));
            }
            (None, Err(second_run_error)) => {
                first_run.insert(addr, Err(second_run_error));
            }
            _ => (),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::ops::Range;

    use crate::MockSearchService;
    use quickwit_index_config::WikipediaIndexConfig;
    use quickwit_metastore::SplitMetadataAndFooterOffsets;
    use quickwit_metastore::{checkpoint::Checkpoint, IndexMetadata, MockMetastore, SplitState};
    use quickwit_proto::SplitSearchError;

    fn mock_split_meta(split_id: &str) -> SplitMetadataAndFooterOffsets {
        SplitMetadataAndFooterOffsets {
            footer_offsets: Default::default(),
            split_metadata: SplitMetadata {
                split_id: split_id.to_string(),
                split_state: SplitState::Published,
                num_records: 10,
                size_in_bytes: 256,
                time_range: None,
                generation: 1,
                update_timestamp: 0,
                tags: vec!["foo".to_string()],
            },
        }
    }

    fn mock_partial_hit(
        split_id: &str,
        sorting_field_value: u64,
        doc_id: u32,
    ) -> quickwit_proto::PartialHit {
        quickwit_proto::PartialHit {
            sorting_field_value,
            split_id: split_id.to_string(),
            segment_ord: 1,
            doc_id,
        }
    }

    fn get_doc_for_fetch_req(
        fetch_docs_req: quickwit_proto::FetchDocsRequest,
    ) -> Vec<quickwit_proto::Hit> {
        fetch_docs_req
            .partial_hits
            .into_iter()
            .map(|req| quickwit_proto::Hit {
                json: r#"{"title" : ""#.to_string()
                    + &req.doc_id.to_string()
                    + r#"", "body" : "test 1", "url" : "http://127.0.0.1/1"}"#,
                partial_hit: Some(req),
            })
            .collect_vec()
    }

    #[tokio::test]
    async fn test_root_search_single_split() -> anyhow::Result<()> {
        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-idx".to_string(),
            query: "test".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 10,
            start_offset: 0,
            tags: vec![],
        };

        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata {
                    index_id: "test-idx".to_string(),
                    index_uri: "file:///path/to/index/test-idx".to_string(),
                    index_config: Arc::new(WikipediaIndexConfig::new()),
                    checkpoint: Checkpoint::default(),
                })
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str,
             _split_state: SplitState,
             _time_range: Option<Range<i64>>,
             _tags: &[String]| { Ok(vec![mock_split_meta("split1")]) },
        );

        let mut mock_search_service = MockSearchService::new();
        mock_search_service.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResult {
                    num_hits: 3,
                    partial_hits: vec![
                        mock_partial_hit("split1", 3, 1),
                        mock_partial_hit("split1", 2, 2),
                        mock_partial_hit("split1", 1, 3),
                    ],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                })
            },
        );
        mock_search_service.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResult {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );

        let client_pool =
            Arc::new(SearchClientPool::from_mocks(vec![Arc::new(mock_search_service)]).await?);

        let search_result = root_search(&search_request, &metastore, &client_pool).await?;

        assert_eq!(search_result.num_hits, 3);
        assert_eq!(search_result.hits.len(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_multiple_splits() -> anyhow::Result<()> {
        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-idx".to_string(),
            query: "test".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 10,
            start_offset: 0,
            tags: vec![],
        };

        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata {
                    index_id: "test-idx".to_string(),
                    index_uri: "file:///path/to/index/test-idx".to_string(),
                    index_config: Arc::new(WikipediaIndexConfig::new()),
                    checkpoint: Checkpoint::default(),
                })
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str,
             _split_state: SplitState,
             _time_range: Option<Range<i64>>,
             _tags: &[String]| {
                Ok(vec![mock_split_meta("split1"), mock_split_meta("split2")])
            },
        );

        let mut mock_search_service1 = MockSearchService::new();
        mock_search_service1.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResult {
                    num_hits: 2,
                    partial_hits: vec![
                        mock_partial_hit("split1", 3, 1),
                        mock_partial_hit("split1", 1, 3),
                    ],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                })
            },
        );
        mock_search_service1.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResult {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );

        let mut mock_search_service2 = MockSearchService::new();
        mock_search_service2.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResult {
                    num_hits: 1,
                    partial_hits: vec![mock_partial_hit("split2", 2, 2)],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                })
            },
        );
        mock_search_service2.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResult {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );

        let client_pool = Arc::new(
            SearchClientPool::from_mocks(vec![
                Arc::new(mock_search_service1),
                Arc::new(mock_search_service2),
            ])
            .await?,
        );

        let search_result = root_search(&search_request, &metastore, &client_pool).await?;

        assert_eq!(search_result.num_hits, 3);
        assert_eq!(search_result.hits.len(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_multiple_splits_retry_on_other_node() -> anyhow::Result<()> {
        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-idx".to_string(),
            query: "test".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 10,
            start_offset: 0,
            tags: vec![],
        };

        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata {
                    index_id: "test-idx".to_string(),
                    index_uri: "file:///path/to/index/test-idx".to_string(),
                    index_config: Arc::new(WikipediaIndexConfig::new()),
                    checkpoint: Checkpoint::default(),
                })
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str,
             _split_state: SplitState,
             _time_range: Option<Range<i64>>,
             _tags: &[String]| {
                Ok(vec![mock_split_meta("split1"), mock_split_meta("split2")])
            },
        );

        let mut mock_search_service1 = MockSearchService::new();
        mock_search_service1
            .expect_leaf_search()
            .times(1)
            .returning(|_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResult {
                    // requests from split 2 arrive here - simulate failure
                    num_hits: 0,
                    partial_hits: vec![],
                    failed_splits: vec![SplitSearchError {
                        error: "mock_error".to_string(),
                        split_id: "split2".to_string(),
                        retryable_error: true,
                    }],
                    num_attempted_splits: 1,
                })
            });

        mock_search_service1.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResult {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );

        let mut mock_search_service2 = MockSearchService::new();
        mock_search_service2
            .expect_leaf_search()
            .times(2)
            .returning(|leaf_search_req: quickwit_proto::LeafSearchRequest| {
                let split_ids = leaf_search_req
                    .split_metadata
                    .iter()
                    .map(|metadata| metadata.split_id.to_string())
                    .collect_vec();
                if split_ids == vec!["split1".to_string()] {
                    Ok(quickwit_proto::LeafSearchResult {
                        num_hits: 2,
                        partial_hits: vec![
                            mock_partial_hit("split1", 3, 1),
                            mock_partial_hit("split1", 1, 3),
                        ],
                        failed_splits: Vec::new(),
                        num_attempted_splits: 1,
                    })
                } else if split_ids == vec!["split2".to_string()] {
                    // RETRY REQUEST!
                    Ok(quickwit_proto::LeafSearchResult {
                        num_hits: 1,
                        partial_hits: vec![mock_partial_hit("split2", 2, 2)],
                        failed_splits: Vec::new(),
                        num_attempted_splits: 1,
                    })
                } else {
                    panic!("unexpected request in test {:?}", split_ids);
                }
            });
        mock_search_service2.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResult {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );

        let client_pool = Arc::new(
            SearchClientPool::from_mocks(vec![
                Arc::new(mock_search_service1),
                Arc::new(mock_search_service2),
            ])
            .await?,
        );

        let search_result = root_search(&search_request, &metastore, &client_pool).await?;

        assert_eq!(search_result.num_hits, 3);
        assert_eq!(search_result.hits.len(), 3);

        Ok(())
    }
    #[tokio::test]
    async fn test_root_search_multiple_splits_retry_on_all_nodes() -> anyhow::Result<()> {
        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-idx".to_string(),
            query: "test".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 10,
            start_offset: 0,
            tags: vec![],
        };

        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata {
                    index_id: "test-idx".to_string(),
                    index_uri: "file:///path/to/index/test-idx".to_string(),
                    index_config: Arc::new(WikipediaIndexConfig::new()),
                    checkpoint: Checkpoint::default(),
                })
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str,
             _split_state: SplitState,
             _time_range: Option<Range<i64>>,
             _tags: &[String]| {
                Ok(vec![mock_split_meta("split1"), mock_split_meta("split2")])
            },
        );

        let mut first_call = true;
        let mut mock_search_service1 = MockSearchService::new();
        mock_search_service1
            .expect_leaf_search()
            .times(2)
            .returning(move |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                // requests from split 2 arrive here - simulate failure, then success
                if first_call {
                    first_call = false;
                    Ok(quickwit_proto::LeafSearchResult {
                        num_hits: 0,
                        partial_hits: vec![],
                        failed_splits: vec![SplitSearchError {
                            error: "mock_error".to_string(),
                            split_id: "split2".to_string(),
                            retryable_error: true,
                        }],
                        num_attempted_splits: 1,
                    })
                } else {
                    Ok(quickwit_proto::LeafSearchResult {
                        num_hits: 1,
                        partial_hits: vec![mock_partial_hit("split2", 2, 2)],
                        failed_splits: Vec::new(),
                        num_attempted_splits: 1,
                    })
                }
            });

        mock_search_service1.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResult {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );

        let mut first_call = true;
        let mut mock_search_service2 = MockSearchService::new();
        mock_search_service2
            .expect_leaf_search()
            .times(2)
            .returning(move |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                // requests from split 1 arrive here - simulate failure, then success
                if first_call {
                    first_call = false;
                    Ok(quickwit_proto::LeafSearchResult {
                        // requests from split 2 arrive here - simulate failure
                        num_hits: 0,
                        partial_hits: vec![],
                        failed_splits: vec![SplitSearchError {
                            error: "mock_error".to_string(),
                            split_id: "split1".to_string(),
                            retryable_error: true,
                        }],
                        num_attempted_splits: 1,
                    })
                } else {
                    // RETRY REQUEST!
                    Ok(quickwit_proto::LeafSearchResult {
                        num_hits: 2,
                        partial_hits: vec![
                            mock_partial_hit("split1", 3, 1),
                            mock_partial_hit("split1", 1, 3),
                        ],
                        failed_splits: Vec::new(),
                        num_attempted_splits: 1,
                    })
                }
            });
        mock_search_service2.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResult {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );

        let client_pool = Arc::new(
            SearchClientPool::from_mocks(vec![
                Arc::new(mock_search_service1),
                Arc::new(mock_search_service2),
            ])
            .await?,
        );

        let search_result = root_search(&search_request, &metastore, &client_pool).await?;

        assert_eq!(search_result.num_hits, 3);
        assert_eq!(search_result.hits.len(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_single_split_retry_single_node() -> anyhow::Result<()> {
        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-idx".to_string(),
            query: "test".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 10,
            start_offset: 0,
            tags: vec![],
        };

        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata {
                    index_id: "test-idx".to_string(),
                    index_uri: "file:///path/to/index/test-idx".to_string(),
                    index_config: Arc::new(WikipediaIndexConfig::new()),
                    checkpoint: Checkpoint::default(),
                })
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str,
             _split_state: SplitState,
             _time_range: Option<Range<i64>>,
             _tags: &[String]| { Ok(vec![mock_split_meta("split1")]) },
        );

        let mut first_call = true;
        let mut mock_search_service1 = MockSearchService::new();
        mock_search_service1
            .expect_leaf_search()
            .times(2)
            .returning(move |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                // requests from split 2 arrive here - simulate failure, then success
                if first_call {
                    first_call = false;
                    Ok(quickwit_proto::LeafSearchResult {
                        num_hits: 0,
                        partial_hits: vec![],
                        failed_splits: vec![SplitSearchError {
                            error: "mock_error".to_string(),
                            split_id: "split1".to_string(),
                            retryable_error: true,
                        }],
                        num_attempted_splits: 1,
                    })
                } else {
                    Ok(quickwit_proto::LeafSearchResult {
                        num_hits: 1,
                        partial_hits: vec![mock_partial_hit("split1", 2, 2)],
                        failed_splits: Vec::new(),
                        num_attempted_splits: 1,
                    })
                }
            });

        mock_search_service1.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResult {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );

        let client_pool =
            Arc::new(SearchClientPool::from_mocks(vec![Arc::new(mock_search_service1)]).await?);

        let search_result = root_search(&search_request, &metastore, &client_pool).await?;

        assert_eq!(search_result.num_hits, 1);
        assert_eq!(search_result.hits.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_single_split_retry_single_node_fails_non_retryable(
    ) -> anyhow::Result<()> {
        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-idx".to_string(),
            query: "test".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 10,
            start_offset: 0,
            tags: vec![],
        };

        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata {
                    index_id: "test-idx".to_string(),
                    index_uri: "file:///path/to/index/test-idx".to_string(),
                    index_config: Arc::new(WikipediaIndexConfig::new()),
                    checkpoint: Checkpoint::default(),
                })
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str,
             _split_state: SplitState,
             _time_range: Option<Range<i64>>,
             _tags: &[String]| { Ok(vec![mock_split_meta("split1")]) },
        );

        let mut mock_search_service1 = MockSearchService::new();
        mock_search_service1
            .expect_leaf_search()
            .times(1)
            .returning(move |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResult {
                    num_hits: 0,
                    partial_hits: vec![],
                    failed_splits: vec![SplitSearchError {
                        error: "mock_error".to_string(),
                        split_id: "split1".to_string(),
                        retryable_error: false,
                    }],
                    num_attempted_splits: 1,
                })
            });

        mock_search_service1.expect_fetch_docs().returning(
            |_fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Err(SearchError::InternalError("mockerr docs".to_string()))
            },
        );

        let client_pool =
            Arc::new(SearchClientPool::from_mocks(vec![Arc::new(mock_search_service1)]).await?);

        let search_result = root_search(&search_request, &metastore, &client_pool).await;
        assert!(search_result.is_err());

        Ok(())
    }
    #[tokio::test]
    async fn test_root_search_single_split_retry_single_node_fails() -> anyhow::Result<()> {
        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-idx".to_string(),
            query: "test".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 10,
            start_offset: 0,
            tags: vec![],
        };

        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata {
                    index_id: "test-idx".to_string(),
                    index_uri: "file:///path/to/index/test-idx".to_string(),
                    index_config: Arc::new(WikipediaIndexConfig::new()),
                    checkpoint: Checkpoint::default(),
                })
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str,
             _split_state: SplitState,
             _time_range: Option<Range<i64>>,
             _tags: &[String]| { Ok(vec![mock_split_meta("split1")]) },
        );

        let mut mock_search_service1 = MockSearchService::new();
        mock_search_service1
            .expect_leaf_search()
            .times(2)
            .returning(move |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResult {
                    num_hits: 0,
                    partial_hits: vec![],
                    failed_splits: vec![SplitSearchError {
                        error: "mock_error".to_string(),
                        split_id: "split1".to_string(),
                        retryable_error: true,
                    }],
                    num_attempted_splits: 1,
                })
            });

        mock_search_service1.expect_fetch_docs().returning(
            |_fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Err(SearchError::InternalError("mockerr docs".to_string()))
            },
        );

        let client_pool =
            Arc::new(SearchClientPool::from_mocks(vec![Arc::new(mock_search_service1)]).await?);

        let search_result = root_search(&search_request, &metastore, &client_pool).await;
        assert!(search_result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_one_splits_two_nodes_but_one_is_failing_for_split(
    ) -> anyhow::Result<()> {
        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-idx".to_string(),
            query: "test".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 10,
            start_offset: 0,
            tags: vec![],
        };

        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata {
                    index_id: "test-idx".to_string(),
                    index_uri: "file:///path/to/index/test-idx".to_string(),
                    index_config: Arc::new(WikipediaIndexConfig::new()),
                    checkpoint: Checkpoint::default(),
                })
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str,
             _split_state: SplitState,
             _time_range: Option<Range<i64>>,
             _tags: &[String]| { Ok(vec![mock_split_meta("split1")]) },
        );

        // service1 - broken node
        let mut mock_search_service1 = MockSearchService::new();
        mock_search_service1.expect_leaf_search().returning(
            move |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                // retry requests from split 1 arrive here
                Ok(quickwit_proto::LeafSearchResult {
                    num_hits: 1,
                    partial_hits: vec![mock_partial_hit("split1", 2, 2)],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                })
            },
        );

        mock_search_service1.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResult {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );

        // service2 - working node
        let mut mock_search_service2 = MockSearchService::new();
        mock_search_service2.expect_leaf_search().returning(
            move |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResult {
                    num_hits: 0,
                    partial_hits: vec![],
                    failed_splits: vec![SplitSearchError {
                        error: "mock_error".to_string(),
                        split_id: "split1".to_string(),
                        retryable_error: true,
                    }],
                    num_attempted_splits: 1,
                })
            },
        );
        mock_search_service2.expect_fetch_docs().returning(
            |_fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Err(SearchError::InternalError("mockerr docs".to_string()))
            },
        );

        let client_pool = Arc::new(
            SearchClientPool::from_mocks(vec![
                Arc::new(mock_search_service1),
                Arc::new(mock_search_service2),
            ])
            .await?,
        );

        let search_result = root_search(&search_request, &metastore, &client_pool).await?;

        assert_eq!(search_result.num_hits, 1);
        assert_eq!(search_result.hits.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_one_splits_two_nodes_but_one_is_failing_completely(
    ) -> anyhow::Result<()> {
        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-idx".to_string(),
            query: "test".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 10,
            start_offset: 0,
            tags: vec![],
        };

        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata {
                    index_id: "test-idx".to_string(),
                    index_uri: "file:///path/to/index/test-idx".to_string(),
                    index_config: Arc::new(WikipediaIndexConfig::new()),
                    checkpoint: Checkpoint::default(),
                })
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str,
             _split_state: SplitState,
             _time_range: Option<Range<i64>>,
             _tags: &[String]| { Ok(vec![mock_split_meta("split1")]) },
        );

        // service1 - working node
        let mut mock_search_service1 = MockSearchService::new();
        mock_search_service1.expect_leaf_search().returning(
            move |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResult {
                    num_hits: 1,
                    partial_hits: vec![mock_partial_hit("split1", 2, 2)],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                })
            },
        );

        mock_search_service1.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResult {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );

        // service2 - broken node
        let mut mock_search_service2 = MockSearchService::new();
        mock_search_service2.expect_leaf_search().returning(
            move |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Err(SearchError::InternalError("mockerr search".to_string()))
            },
        );
        mock_search_service2.expect_fetch_docs().returning(
            |_fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Err(SearchError::InternalError("mockerr docs".to_string()))
            },
        );

        let client_pool = Arc::new(
            SearchClientPool::from_mocks(vec![
                Arc::new(mock_search_service1),
                Arc::new(mock_search_service2),
            ])
            .await?,
        );

        let search_result = root_search(&search_request, &metastore, &client_pool).await?;

        assert_eq!(search_result.num_hits, 1);
        assert_eq!(search_result.hits.len(), 1);

        Ok(())
    }
}
