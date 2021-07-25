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
use quickwit_proto::SplitSearchError;
use tantivy::collector::Collector;
use tantivy::TantivyError;
use tokio::task::spawn_blocking;
use tokio::task::JoinHandle;
use tracing::*;

use quickwit_metastore::{Metastore, SplitMetadata};
use quickwit_proto::{
    FetchDocsRequest, FetchDocsResult, Hit, LeafSearchRequest, LeafSearchResult, PartialHit,
    SearchRequest, SearchResult,
};

use crate::client_pool::Job;
use crate::list_relevant_splits;
use crate::make_collector;
use crate::ClientPool;
use crate::SearchClientPool;
use crate::SearchError;
use crate::SearchServiceClient;

// Measure the cost associated to searching in a given split metadata.
fn compute_split_cost(_split_metadata: &SplitMetadata) -> u32 {
    //TODO: Have a smarter cost, by smoothing the number of docs.
    1
}

#[derive(Debug)]
pub struct NodeSearchError {
    search_error: SearchError,
    split_ids: Vec<String>,
}

type SearchResultsByAddr = HashMap<SocketAddr, Result<LeafSearchResult, NodeSearchError>>;

async fn execute_search(
    assigned_leaf_search_jobs: &[(SearchServiceClient, Vec<Job>)],
    search_request_with_offset_0: SearchRequest,
) -> anyhow::Result<SearchResultsByAddr> {
    // Perform the query phase.
    let mut result_per_node_addr_futures = HashMap::new();

    // Perform the query phase.
    for (search_client, jobs) in assigned_leaf_search_jobs.iter() {
        let leaf_search_request = LeafSearchRequest {
            search_request: Some(search_request_with_offset_0.clone()),
            split_ids: jobs.iter().map(|job| job.split.clone()).collect(),
        };

        debug!(leaf_search_request=?leaf_search_request, grpc_addr=?search_client.grpc_addr(), "Leaf node search.");
        let mut search_client_clone: SearchServiceClient = search_client.clone();
        let handle = tokio::spawn(async move {
            let split_ids = leaf_search_request.split_ids.to_vec();
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

#[derive(Debug)]
pub struct ErrorRetries {
    #[allow(dead_code)]
    retry_split_ids: Vec<String>,
    #[allow(dead_code)]
    nodes_to_avoid: HashSet<SocketAddr>,
}

/// There are different information which could be useful
/// Scenario: Multiple requests on a node
/// - Are all requests of one node failing? -> Don't consider this node as alternative of failing
/// requests
/// - Is the node ok, but just the split failing?
/// - Did all requests fail? (Should we retry in that case?)
fn analyze_errors(search_result: &SearchResultsByAddr) -> Option<ErrorRetries> {
    let contains_retryable_errors = search_result.values().any(|res| {
        res.as_ref().map_or(true, |ok_res| {
            ok_res
                .failed_requests
                .iter()
                .filter(|err| err.retryable_error)
                .count()
                > 0
        })
    });
    if !contains_retryable_errors {
        return None;
    }
    let complete_failure_nodes: Vec<_> = search_result
        .iter()
        .filter(|(_addr, result)| {
            if let Ok(ok_res) = result {
                ok_res.failed_requests.len() as u64 == ok_res.num_attempted_splits
            } else {
                true
            }
        })
        .map(|(addr, _)| *addr)
        .collect();
    let partial_or_no_failure_nodes: Vec<_> = search_result
        .iter()
        .filter(|(_addr, result)| {
            result
                .as_ref()
                .map(|ok_res| ok_res.failed_requests.len() as u64 != ok_res.num_attempted_splits)
                .unwrap_or(false)
        })
        .map(|(addr, _)| *addr)
        .collect();
    debug!("{:?}", &complete_failure_nodes);
    debug!("{:?}", &partial_or_no_failure_nodes);

    // Here we collect the failed requests on the node. It does not yet include failed requests
    // against that node
    let mut retry_split_ids = search_result
        .values()
        .filter_map(|result| result.as_ref().ok())
        .flat_map(|res| {
            let results = res
                .failed_requests
                .iter()
                .filter(move |failed_req| failed_req.retryable_error) // only retry splits marked as retryable
                .map(move |failed_req| failed_req.split_id.to_string())
                .collect_vec();
            results.into_iter()
        })
        .collect_vec();

    let failed_splits = search_result
        .values()
        .filter_map(|result| result.as_ref().err())
        .flat_map(|err| err.split_ids.iter().cloned());
    retry_split_ids.extend(failed_splits);

    let nodes_to_avoid = complete_failure_nodes;

    Some(ErrorRetries {
        retry_split_ids,
        nodes_to_avoid: nodes_to_avoid.into_iter().collect(),
    })
}

fn job_for_split(
    split_ids: &HashSet<&String>,
    split_metadata_map: &HashMap<String, SplitMetadata>,
) -> Vec<Job> {
    // Create a job for fetching docs and assign the splits that the node is responsible for based on the job.
    let leaf_search_jobs: Vec<Job> = split_metadata_map
        .iter()
        .filter(|(split_id, _)| split_ids.contains(split_id))
        .map(|(split_id, split_metadata)| Job {
            split: split_id.clone(),
            cost: compute_split_cost(split_metadata),
        })
        .collect();
    leaf_search_jobs
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
    //
    let leaf_search_jobs: Vec<Job> =
        job_for_split(&split_metadata_map.keys().collect(), &split_metadata_map);

    let assigned_leaf_search_jobs = client_pool.assign_jobs(leaf_search_jobs, &None).await?;
    debug!(assigned_leaf_search_jobs=?assigned_leaf_search_jobs, "Assigned leaf search jobs.");

    // Create search request with start offset is 0 that used by leaf node search.
    let mut search_request_with_offset_0 = search_request.clone();
    search_request_with_offset_0.start_offset = 0;
    search_request_with_offset_0.max_hits += search_request.start_offset;

    // Perform the query phase.
    let mut result_per_node_addr = execute_search(
        &assigned_leaf_search_jobs,
        search_request_with_offset_0.clone(),
    )
    .await?;

    let analyze_result = analyze_errors(&result_per_node_addr);
    if let Some(retry_action) = analyze_result.as_ref() {
        // Create a job for fetching docs and assign the splits that the node is responsible for based on the job.
        //
        let leaf_search_jobs: Vec<Job> = job_for_split(
            &retry_action.retry_split_ids.iter().collect(),
            &split_metadata_map,
        );

        let retry_assigned_leaf_search_jobs = client_pool
            .assign_jobs(leaf_search_jobs, &Some(retry_action.nodes_to_avoid.clone()))
            .await?;
        // Perform the query phase.
        let result_per_node_addr_new = execute_search(
            &retry_assigned_leaf_search_jobs,
            search_request_with_offset_0.clone(),
        )
        .await?;

        // clean out old errors
        let complete_errors = result_per_node_addr
            .iter()
            .filter(|(_addr, res)| res.is_err())
            .map(|(addr, _err)| *addr)
            .collect_vec();
        for err_addr in complete_errors {
            result_per_node_addr.remove(&err_addr);
        }
        // remove partial, retryable errors
        for result in result_per_node_addr.values_mut().flatten() {
            let failed_requests: Vec<SplitSearchError> = std::mem::take(&mut vec![]);
            result.failed_requests = failed_requests
                .into_iter()
                .filter(|err| !err.retryable_error)
                .collect_vec();
        }

        for (addr, new_result) in result_per_node_addr_new {
            if let Some(Ok(orig_res)) = result_per_node_addr.get_mut(&addr) {
                // old one exists and is_ok and new one is_ok
                if let Ok(result) = new_result {
                    orig_res.num_hits += result.num_hits;
                    orig_res.num_attempted_splits += result.num_attempted_splits;
                    orig_res
                        .failed_requests
                        .extend(result.failed_requests.into_iter());
                    orig_res
                        .partial_hits
                        .extend(result.partial_hits.into_iter());
                } else if let Err(err) = new_result {
                    // old one exists and is_ok and new one is not ok
                    orig_res
                        .failed_requests
                        .extend(err.split_ids.iter().map(|split_id| SplitSearchError {
                            error: err.search_error.to_string(),
                            split_id: split_id.to_string(),
                            retryable_error: true,
                        }));
                }
            } else if let Some(Err(err)) = result_per_node_addr.get_mut(&addr) {
                panic!("unexpected error leftover: {:?}", err);
            } else {
                // empty slot
                result_per_node_addr.insert(addr, new_result);
            }
        }
    }

    let index_metadata = metastore.index_metadata(&search_request.index_id).await?;
    let index_config = index_metadata.index_config;
    let collector = make_collector(index_config.as_ref(), search_request);

    // Find the sum of the number of hits and merge multiple partial hits into a single partial hits.
    let mut leaf_search_results = Vec::new();
    for (_addr, leaf_search_response) in result_per_node_addr.into_iter() {
        match leaf_search_response {
            Ok(leaf_search_result) => {
                debug!(leaf_search_result=?leaf_search_result, "Leaf search result.");
                leaf_search_results.push(leaf_search_result)
            }
            Err(node_search_error) => {
                error!(error=?node_search_error, "Leaf request failed");
                // TODO list failed leaf nodes and retry.
                return Err(node_search_error.search_error);
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

    let fetch_docs_req_jobs = partial_hits_map
        .keys()
        .map(|split_id| Job {
            split: split_id.to_string(),
            cost: 1,
        })
        .collect_vec();
    let doc_fetch_jobs = client_pool
        .assign_jobs(
            fetch_docs_req_jobs,
            &analyze_result.map(|retry_action| retry_action.nodes_to_avoid),
        )
        .await?;

    // Perform the fetch docs phase.
    let mut fetch_docs_handles: Vec<JoinHandle<Result<FetchDocsResult, SearchError>>> = Vec::new();
    for (search_client, jobs) in doc_fetch_jobs.iter() {
        for job in jobs {
            // TODO group fetch doc requests.
            if let Some(partial_hits) = partial_hits_map.get(&job.split) {
                let fetch_docs_request = FetchDocsRequest {
                    partial_hits: partial_hits.clone(),
                    index_id: search_request.index_id.clone(),
                };
                let mut search_client_clone = search_client.clone();
                let handle = tokio::spawn(async move {
                    search_client_clone.fetch_docs(fetch_docs_request).await
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
        errors: vec![],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::ops::Range;

    use quickwit_index_config::WikipediaIndexConfig;
    use quickwit_metastore::{IndexMetadata, MockMetastore, SplitState};
    use quickwit_proto::SplitSearchError;

    use crate::{MockSearchService, SearchResultJson};

    fn mock_split_meta(split_id: &str) -> SplitMetadata {
        SplitMetadata {
            split_id: split_id.to_string(),
            split_state: SplitState::Published,
            num_records: 10,
            size_in_bytes: 256,
            time_range: None,
            generation: 1,
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
        };
        println!("search_request={:?}", search_request);

        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata {
                    index_id: "test-idx".to_string(),
                    index_uri: "file:///path/to/index/test-idx".to_string(),
                    index_config: Box::new(WikipediaIndexConfig::new()),
                })
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str, _split_state: SplitState, _time_range: Option<Range<i64>>| {
                Ok(vec![mock_split_meta("split1")])
            },
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
                    failed_requests: Vec::new(),
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
        println!("search_result={:?}", search_result);

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
        };
        println!("search_request={:?}", search_request);

        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata {
                    index_id: "test-idx".to_string(),
                    index_uri: "file:///path/to/index/test-idx".to_string(),
                    index_config: Box::new(WikipediaIndexConfig::new()),
                })
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str, _split_state: SplitState, _time_range: Option<Range<i64>>| {
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
                    failed_requests: Vec::new(),
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
                    failed_requests: Vec::new(),
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
        println!("search_result={:?}", search_result);

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
        };
        println!("search_request={:?}", search_request);

        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata {
                    index_id: "test-idx".to_string(),
                    index_uri: "file:///path/to/index/test-idx".to_string(),
                    index_config: Box::new(WikipediaIndexConfig::new()),
                })
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str, _split_state: SplitState, _time_range: Option<Range<i64>>| {
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
                    failed_requests: vec![SplitSearchError {
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
                if leaf_search_req.split_ids == vec!["split1".to_string()] {
                    Ok(quickwit_proto::LeafSearchResult {
                        num_hits: 2,
                        partial_hits: vec![
                            mock_partial_hit("split1", 3, 1),
                            mock_partial_hit("split1", 1, 3),
                        ],
                        failed_requests: Vec::new(),
                        num_attempted_splits: 1,
                    })
                } else if leaf_search_req.split_ids == vec!["split2".to_string()] {
                    // RETRY REQUEST!
                    Ok(quickwit_proto::LeafSearchResult {
                        num_hits: 1,
                        partial_hits: vec![mock_partial_hit("split2", 2, 2)],
                        failed_requests: Vec::new(),
                        num_attempted_splits: 1,
                    })
                } else {
                    panic!("unexpected request in test {:?}", leaf_search_req.split_ids);
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
        //println!("search_result={:?}", search_result);

        let search_result_json = SearchResultJson::from(search_result.clone());
        let search_result_json = serde_json::to_string_pretty(&search_result_json)?;
        println!("{}", search_result_json);

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
        };
        println!("search_request={:?}", search_request);

        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata {
                    index_id: "test-idx".to_string(),
                    index_uri: "file:///path/to/index/test-idx".to_string(),
                    index_config: Box::new(WikipediaIndexConfig::new()),
                })
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str, _split_state: SplitState, _time_range: Option<Range<i64>>| {
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
                        failed_requests: vec![SplitSearchError {
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
                        failed_requests: Vec::new(),
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
                        failed_requests: vec![SplitSearchError {
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
                        failed_requests: Vec::new(),
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
        //println!("search_result={:?}", search_result);

        let search_result_json = SearchResultJson::from(search_result.clone());
        let search_result_json = serde_json::to_string_pretty(&search_result_json)?;
        println!("{}", search_result_json);

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
        };
        println!("search_request={:?}", search_request);

        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata {
                    index_id: "test-idx".to_string(),
                    index_uri: "file:///path/to/index/test-idx".to_string(),
                    index_config: Box::new(WikipediaIndexConfig::new()),
                })
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str, _split_state: SplitState, _time_range: Option<Range<i64>>| {
                Ok(vec![mock_split_meta("split1")])
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
                        failed_requests: vec![SplitSearchError {
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
                        failed_requests: Vec::new(),
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
        //println!("search_result={:?}", search_result);

        let search_result_json = SearchResultJson::from(search_result.clone());
        let search_result_json = serde_json::to_string_pretty(&search_result_json)?;
        println!("{}", search_result_json);

        assert_eq!(search_result.num_hits, 1);
        assert_eq!(search_result.hits.len(), 1);

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
        };
        println!("search_request={:?}", search_request);

        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata {
                    index_id: "test-idx".to_string(),
                    index_uri: "file:///path/to/index/test-idx".to_string(),
                    index_config: Box::new(WikipediaIndexConfig::new()),
                })
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str, _split_state: SplitState, _time_range: Option<Range<i64>>| {
                Ok(vec![mock_split_meta("split1")])
            },
        );

        // service1 - broken node
        let mut mock_search_service1 = MockSearchService::new();
        mock_search_service1.expect_leaf_search().returning(
            move |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                // retry requests from split 1 arrive here
                Ok(quickwit_proto::LeafSearchResult {
                    num_hits: 1,
                    partial_hits: vec![mock_partial_hit("split1", 2, 2)],
                    failed_requests: Vec::new(),
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
                    failed_requests: vec![SplitSearchError {
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
        //println!("search_result={:?}", search_result);

        let search_result_json = SearchResultJson::from(search_result.clone());
        let search_result_json = serde_json::to_string_pretty(&search_result_json)?;
        println!("{}", search_result_json);

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
        };
        println!("search_request={:?}", search_request);

        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata {
                    index_id: "test-idx".to_string(),
                    index_uri: "file:///path/to/index/test-idx".to_string(),
                    index_config: Box::new(WikipediaIndexConfig::new()),
                })
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str, _split_state: SplitState, _time_range: Option<Range<i64>>| {
                Ok(vec![mock_split_meta("split1")])
            },
        );

        // service1 - working node
        let mut mock_search_service1 = MockSearchService::new();
        mock_search_service1.expect_leaf_search().returning(
            move |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResult {
                    num_hits: 1,
                    partial_hits: vec![mock_partial_hit("split1", 2, 2)],
                    failed_requests: Vec::new(),
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
        //println!("search_result={:?}", search_result);

        let search_result_json = SearchResultJson::from(search_result.clone());
        let search_result_json = serde_json::to_string_pretty(&search_result_json)?;
        println!("{}", search_result_json);

        assert_eq!(search_result.num_hits, 1);
        assert_eq!(search_result.hits.len(), 1);

        Ok(())
    }
}
