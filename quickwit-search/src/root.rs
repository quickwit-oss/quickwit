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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use quickwit_metastore::{Metastore, SplitMetadata};
use quickwit_proto::{
    FetchDocsRequest, FetchDocsResponse, LeafSearchRequest, LeafSearchResponse, PartialHit,
    SearchRequest, SearchResponse,
};
use tantivy::collector::Collector;
use tantivy::TantivyError;
use tokio::task::spawn_blocking;
use tracing::{debug, error, instrument};

use crate::client_pool::Job;
use crate::cluster_client::ClusterClient;
use crate::collector::make_merge_collector;
use crate::{
    extract_split_and_footer_offsets, list_relevant_splits, ClientPool, SearchClientPool,
    SearchError,
};

pub const MAX_CONCURRENT_LEAF_TASKS: usize = if cfg!(test) { 2 } else { 10 };

/// Performs a distributed search.
/// 1. Sends leaf request over gRPC to multiple leaf nodes.
/// 2. Merges the search results.
/// 3. Sends fetch docs requests to multiple leaf nodes.
/// 4. Builds the response with docs and returns.
#[instrument(skip(search_request, cluster_client, client_pool, metastore))]
pub async fn root_search(
    search_request: &SearchRequest,
    metastore: &dyn Metastore,
    cluster_client: &ClusterClient,
    client_pool: &Arc<SearchClientPool>,
) -> crate::Result<SearchResponse> {
    let start_instant = tokio::time::Instant::now();
    let index_metadata = metastore.index_metadata(&search_request.index_id).await?;
    let doc_mapper = index_metadata.build_doc_mapper().map_err(|err| {
        SearchError::InternalError(format!("Failed to build doc mapper. Cause: {}", err))
    })?;
    let doc_mapper_str = serde_json::to_string(&doc_mapper).map_err(|err| {
        SearchError::InternalError(format!("Failed to serialize doc mapper: Cause {}", err))
    })?;
    let split_metadata_list = list_relevant_splits(search_request, metastore).await?;
    let split_metadata_map: HashMap<String, SplitMetadata> = split_metadata_list
        .into_iter()
        .map(|metadata| (metadata.split_id().to_string(), metadata))
        .collect();
    let jobs: Vec<Job> = job_for_splits(&split_metadata_map.keys().collect(), &split_metadata_map);
    let assigned_leaf_search_jobs = client_pool.assign_jobs(jobs, &HashSet::default()).await?;
    debug!(assigned_leaf_search_jobs=?assigned_leaf_search_jobs, "Assigned leaf search jobs.");
    let leaf_search_responses: Vec<LeafSearchResponse> =
        futures::stream::iter(assigned_leaf_search_jobs.into_iter())
            .map(|(client, client_jobs)| {
                let leaf_request = jobs_to_leaf_request(
                    search_request,
                    &doc_mapper_str,
                    &index_metadata.index_uri,
                    &split_metadata_map,
                    &client_jobs,
                );
                cluster_client.leaf_search((leaf_request, client))
            })
            .buffer_unordered(MAX_CONCURRENT_LEAF_TASKS)
            .try_collect()
            .await?;

    let merge_collector = make_merge_collector(search_request);
    let leaf_search_response =
        spawn_blocking(move || merge_collector.merge_fruits(leaf_search_responses))
            .await?
            .map_err(|merge_error: TantivyError| {
                crate::SearchError::InternalError(format!("{}", merge_error))
            })?;
    debug!(leaf_search_response = ?leaf_search_response, "Merged leaf search response.");

    if !leaf_search_response.failed_splits.is_empty() {
        error!(failed_splits = ?leaf_search_response.failed_splits, "Leaf search response contains at least one failed split.");
        return Err(SearchError::InternalError(format!(
            "{:?}",
            leaf_search_response.failed_splits
        )));
    }

    // Create a hash map of PartialHit with split as a key.
    let mut partial_hits_map: HashMap<String, Vec<PartialHit>> = HashMap::new();
    for partial_hit in leaf_search_response.partial_hits.iter() {
        partial_hits_map
            .entry(partial_hit.split_id.clone())
            .or_insert_with(Vec::new)
            .push(partial_hit.clone());
    }

    let fetch_docs_req_jobs = partial_hits_map
        .keys()
        .map(|split_id| Job {
            split_id: split_id.clone(),
            cost: 1,
        })
        .collect_vec();
    let assigned_doc_fetch_jobs = client_pool
        .assign_jobs(fetch_docs_req_jobs, &HashSet::new())
        .await?;
    let fetch_docs_responses: Vec<FetchDocsResponse> =
        futures::stream::iter(assigned_doc_fetch_jobs.into_iter())
            .map(|(client, client_jobs)| {
                let doc_request = jobs_to_fetch_docs_request(
                    &search_request.index_id,
                    &index_metadata.index_uri,
                    &split_metadata_map,
                    &mut partial_hits_map,
                    &client_jobs,
                );
                cluster_client.fetch_docs((doc_request, client))
            })
            .buffer_unordered(MAX_CONCURRENT_LEAF_TASKS)
            .try_collect()
            .await?;

    // Merge the fetched docs.
    let hits = fetch_docs_responses
        .iter()
        .map(|response| response.hits.clone())
        .flatten()
        .sorted_by(|hit1, hit2| {
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
        })
        .collect_vec();

    let elapsed = start_instant.elapsed();

    Ok(SearchResponse {
        num_hits: leaf_search_response.num_hits,
        hits,
        elapsed_time_micros: elapsed.as_micros() as u64,
        errors: vec![],
    })
}

// Measure the cost associated to searching in a given split metadata.
fn compute_split_cost(_split_metadata: &SplitMetadata) -> u32 {
    // TODO: Have a smarter cost, by smoothing the number of docs.
    1
}

pub(crate) fn job_for_splits(
    split_ids: &HashSet<&String>,
    split_metadata_map: &HashMap<String, SplitMetadata>,
) -> Vec<Job> {
    // Create a job for fetching docs and assign the splits that the node is responsible for based
    // on the job.
    let leaf_search_jobs: Vec<Job> = split_metadata_map
        .iter()
        .filter(|(split_id, _)| split_ids.contains(split_id))
        .map(|(split_id, metadata)| Job {
            split_id: split_id.to_string(),
            cost: compute_split_cost(metadata),
        })
        .collect();
    leaf_search_jobs
}

fn jobs_to_leaf_request(
    request: &SearchRequest,
    index_config_str: &str,
    index_uri: &str,
    split_metadata_map: &HashMap<String, SplitMetadata>,
    jobs: &[Job],
) -> LeafSearchRequest {
    let mut request_with_offset_0 = request.clone();
    request_with_offset_0.start_offset = 0;
    request_with_offset_0.max_hits += request.start_offset;

    LeafSearchRequest {
        search_request: Some(request_with_offset_0),
        split_metadata: jobs
            .iter()
            .map(|job| {
                extract_split_and_footer_offsets(split_metadata_map.get(&job.split_id).unwrap())
            })
            .collect(),
        index_config: index_config_str.to_string(),
        index_uri: index_uri.to_string(),
    }
}

fn jobs_to_fetch_docs_request(
    index_id: &str,
    index_uri: &str,
    split_metadata_map: &HashMap<String, SplitMetadata>,
    partial_hits_map: &mut HashMap<String, Vec<PartialHit>>,
    jobs: &[Job],
) -> FetchDocsRequest {
    let partial_hits = jobs
        .iter()
        .map(|job| partial_hits_map.remove(&job.split_id).unwrap())
        .flatten()
        .collect_vec();
    let splits_footer_and_offsets = jobs
        .iter()
        .map(|job| split_metadata_map.get(&job.split_id).unwrap())
        .map(extract_split_and_footer_offsets)
        .collect_vec();

    FetchDocsRequest {
        partial_hits,
        index_id: index_id.to_string(),
        split_metadata: splits_footer_and_offsets,
        index_uri: index_uri.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use quickwit_indexing::mock_split;
    use quickwit_metastore::{IndexMetadata, MockMetastore, SplitState};
    use quickwit_proto::SplitSearchError;

    use super::*;
    use crate::MockSearchService;

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
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-idx",
                    "file:///path/to/index/test-idx",
                ))
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str, _split_state: SplitState, _time_range: Option<Range<i64>>, _tags| {
                Ok(vec![mock_split("split1")])
            },
        );
        let mut mock_search_service = MockSearchService::new();
        mock_search_service.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResponse {
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
                Ok(quickwit_proto::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let client_pool =
            Arc::new(SearchClientPool::from_mocks(vec![Arc::new(mock_search_service)]).await?);
        let cluster_client = ClusterClient::new(client_pool.clone());
        let search_response =
            root_search(&search_request, &metastore, &cluster_client, &client_pool).await?;
        assert_eq!(search_response.num_hits, 3);
        assert_eq!(search_response.hits.len(), 3);
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
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-idx",
                    "file:///path/to/index/test-idx",
                ))
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str, _split_state: SplitState, _time_range: Option<Range<i64>>, _tags| {
                Ok(vec![mock_split("split1"), mock_split("split2")])
            },
        );
        let mut mock_search_service1 = MockSearchService::new();
        mock_search_service1.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResponse {
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
                Ok(quickwit_proto::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let mut mock_search_service2 = MockSearchService::new();
        mock_search_service2.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResponse {
                    num_hits: 1,
                    partial_hits: vec![mock_partial_hit("split2", 2, 2)],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                })
            },
        );
        mock_search_service2.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResponse {
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
        let cluster_client = ClusterClient::new(client_pool.clone());
        let search_response =
            root_search(&search_request, &metastore, &cluster_client, &client_pool).await?;
        assert_eq!(search_response.num_hits, 3);
        assert_eq!(search_response.hits.len(), 3);
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
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-idx",
                    "file:///path/to/index/test-idx",
                ))
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str, _split_state: SplitState, _time_range: Option<Range<i64>>, _tags| {
                Ok(vec![mock_split("split1"), mock_split("split2")])
            },
        );

        let mut mock_search_service1 = MockSearchService::new();
        mock_search_service1
            .expect_leaf_search()
            .times(1)
            .returning(|_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResponse {
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
                Ok(quickwit_proto::FetchDocsResponse {
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
                    Ok(quickwit_proto::LeafSearchResponse {
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
                    Ok(quickwit_proto::LeafSearchResponse {
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
                Ok(quickwit_proto::FetchDocsResponse {
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
        let cluster_client = ClusterClient::new(client_pool.clone());
        let search_response =
            root_search(&search_request, &metastore, &cluster_client, &client_pool).await?;
        assert_eq!(search_response.num_hits, 3);
        assert_eq!(search_response.hits.len(), 3);
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
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-idx",
                    "file:///path/to/index/test-idx",
                ))
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str, _split_state: SplitState, _time_range: Option<Range<i64>>, _tags| {
                Ok(vec![mock_split("split1"), mock_split("split2")])
            },
        );
        let mut mock_search_service1 = MockSearchService::new();
        mock_search_service1
            .expect_leaf_search()
            .withf(|leaf_search_req| leaf_search_req.split_metadata[0].split_id == "split2")
            .return_once(|_| {
                println!("request from service1 split2?");
                // requests from split 2 arrive here - simulate failure.
                // a retry will be made on the second service.
                Ok(quickwit_proto::LeafSearchResponse {
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
        mock_search_service1
            .expect_leaf_search()
            .withf(|leaf_search_req| leaf_search_req.split_metadata[0].split_id == "split1")
            .return_once(|_| {
                println!("request from service1 split1?");
                // RETRY REQUEST from split1
                Ok(quickwit_proto::LeafSearchResponse {
                    num_hits: 2,
                    partial_hits: vec![
                        mock_partial_hit("split1", 3, 1),
                        mock_partial_hit("split1", 1, 3),
                    ],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                })
            });
        mock_search_service1.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let mut mock_search_service2 = MockSearchService::new();
        mock_search_service2
            .expect_leaf_search()
            .withf(|leaf_search_req| leaf_search_req.split_metadata[0].split_id == "split2")
            .return_once(|_| {
                println!("request from service2 split2?");
                // retry for split 2 arrive here, simulate success.
                Ok(quickwit_proto::LeafSearchResponse {
                    num_hits: 1,
                    partial_hits: vec![mock_partial_hit("split2", 2, 2)],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                })
            });
        mock_search_service2
            .expect_leaf_search()
            .withf(|leaf_search_req| leaf_search_req.split_metadata[0].split_id == "split1")
            .return_once(|_| {
                println!("request from service2 split1?");
                // requests from split 1 arrive here - simulate failure, then success.
                Ok(quickwit_proto::LeafSearchResponse {
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
            });
        mock_search_service2.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResponse {
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
        let cluster_client = ClusterClient::new(client_pool.clone());
        let search_response =
            root_search(&search_request, &metastore, &cluster_client, &client_pool).await?;
        assert_eq!(search_response.num_hits, 3);
        assert_eq!(search_response.hits.len(), 3);
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
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-idx",
                    "file:///path/to/index/test-idx",
                ))
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str, _split_state: SplitState, _time_range: Option<Range<i64>>, _tags| {
                Ok(vec![mock_split("split1")])
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
                    Ok(quickwit_proto::LeafSearchResponse {
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
                    Ok(quickwit_proto::LeafSearchResponse {
                        num_hits: 1,
                        partial_hits: vec![mock_partial_hit("split1", 2, 2)],
                        failed_splits: Vec::new(),
                        num_attempted_splits: 1,
                    })
                }
            });
        mock_search_service1.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let client_pool =
            Arc::new(SearchClientPool::from_mocks(vec![Arc::new(mock_search_service1)]).await?);
        let cluster_client = ClusterClient::new(client_pool.clone());
        let search_response =
            root_search(&search_request, &metastore, &cluster_client, &client_pool).await?;
        assert_eq!(search_response.num_hits, 1);
        assert_eq!(search_response.hits.len(), 1);
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
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-idx",
                    "file:///path/to/index/test-idx",
                ))
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str, _split_state: SplitState, _time_range: Option<Range<i64>>, _tags| {
                Ok(vec![mock_split("split1")])
            },
        );

        let mut mock_search_service1 = MockSearchService::new();
        mock_search_service1
            .expect_leaf_search()
            .times(2)
            .returning(move |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResponse {
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
        let cluster_client = ClusterClient::new(client_pool.clone());
        let search_response =
            root_search(&search_request, &metastore, &cluster_client, &client_pool).await;
        assert!(search_response.is_err());
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
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-idx",
                    "file:///path/to/index/test-idx",
                ))
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str, _split_state: SplitState, _time_range: Option<Range<i64>>, _tags| {
                Ok(vec![mock_split("split1")])
            },
        );
        // Service1 - broken node.
        let mut mock_search_service1 = MockSearchService::new();
        mock_search_service1.expect_leaf_search().returning(
            move |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                // retry requests from split 1 arrive here
                Ok(quickwit_proto::LeafSearchResponse {
                    num_hits: 1,
                    partial_hits: vec![mock_partial_hit("split1", 2, 2)],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                })
            },
        );
        mock_search_service1.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        // Service2 - working node.
        let mut mock_search_service2 = MockSearchService::new();
        mock_search_service2.expect_leaf_search().returning(
            move |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResponse {
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
        let cluster_client = ClusterClient::new(client_pool.clone());
        let search_response =
            root_search(&search_request, &metastore, &cluster_client, &client_pool).await?;
        assert_eq!(search_response.num_hits, 1);
        assert_eq!(search_response.hits.len(), 1);
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
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-idx",
                    "file:///path/to/index/test-idx",
                ))
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str, _split_state: SplitState, _time_range: Option<Range<i64>>, _tags| {
                Ok(vec![mock_split("split1")])
            },
        );

        // Service1 - working node.
        let mut mock_search_service1 = MockSearchService::new();
        mock_search_service1.expect_leaf_search().returning(
            move |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResponse {
                    num_hits: 1,
                    partial_hits: vec![mock_partial_hit("split1", 2, 2)],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                })
            },
        );
        mock_search_service1.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        // Service2 - broken node.
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
        let cluster_client = ClusterClient::new(client_pool.clone());
        let search_response =
            root_search(&search_request, &metastore, &cluster_client, &client_pool).await?;
        assert_eq!(search_response.num_hits, 1);
        assert_eq!(search_response.hits.len(), 1);
        Ok(())
    }
}
