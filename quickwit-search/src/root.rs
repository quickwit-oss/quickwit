// Copyright (C) 2022 Quickwit, Inc.
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

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};

use futures::future::try_join_all;
use itertools::Itertools;
use quickwit_config::build_doc_mapper;
use quickwit_metastore::{Metastore, SplitMetadata};
use quickwit_proto::{
    FetchDocsRequest, FetchDocsResponse, LeafSearchRequest, LeafSearchResponse, PartialHit,
    SearchRequest, SearchResponse, SplitIdAndFooterOffsets,
};
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::agg_result::AggregationResults;
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use tantivy::collector::Collector;
use tantivy::TantivyError;
use tokio::task::spawn_blocking;
use tracing::{debug, error, instrument};

use crate::cluster_client::ClusterClient;
use crate::collector::{make_merge_collector, HitScore};
use crate::search_client_pool::Job;
use crate::{
    extract_split_and_footer_offsets, list_relevant_splits, SearchClientPool, SearchError,
    SearchServiceClient,
};

#[derive(Debug, PartialEq)]
pub(crate) struct SearchJob {
    cost: u32,
    offsets: SplitIdAndFooterOffsets,
}

impl SearchJob {
    #[cfg(test)]
    pub fn for_test(split_id: &str, cost: u32) -> SearchJob {
        SearchJob {
            cost,
            offsets: SplitIdAndFooterOffsets {
                split_id: split_id.to_string(),
                ..Default::default()
            },
        }
    }
}

impl From<SearchJob> for SplitIdAndFooterOffsets {
    fn from(search_job: SearchJob) -> Self {
        search_job.offsets
    }
}

impl<'a> From<&'a SplitMetadata> for SearchJob {
    fn from(split_metadata: &'a SplitMetadata) -> Self {
        SearchJob {
            cost: compute_split_cost(split_metadata),
            offsets: extract_split_and_footer_offsets(split_metadata),
        }
    }
}

impl Job for SearchJob {
    fn split_id(&self) -> &str {
        &self.offsets.split_id
    }

    fn cost(&self) -> u32 {
        self.cost
    }
}

pub(crate) struct FetchDocsJob {
    offsets: SplitIdAndFooterOffsets,
    pub partial_hits: Vec<PartialHit>,
}

impl Job for FetchDocsJob {
    fn split_id(&self) -> &str {
        &self.offsets.split_id
    }

    fn cost(&self) -> u32 {
        self.partial_hits.len() as u32
    }
}

impl From<FetchDocsJob> for SplitIdAndFooterOffsets {
    fn from(fetch_docs_job: FetchDocsJob) -> SplitIdAndFooterOffsets {
        fetch_docs_job.offsets
    }
}

fn validate_request(search_request: &SearchRequest) -> crate::Result<()> {
    if let Some(agg) = search_request.aggregation_request.as_ref() {
        let _agg: Aggregations = serde_json::from_str(agg)
            .map_err(|err| SearchError::InvalidAggregationRequest(err.to_string()))?;
    };

    if search_request.start_offset > 10_000 {
        return Err(SearchError::InvalidArgument(format!(
            "max value for start_offset is 10_000, but got {}",
            search_request.start_offset
        )));
    }

    if search_request.max_hits > 10_000 {
        return Err(SearchError::InvalidArgument(format!(
            "max value for max_hits is 10_000, but got {}",
            search_request.max_hits
        )));
    }

    Ok(())
}

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
    client_pool: &SearchClientPool,
) -> crate::Result<SearchResponse> {
    let start_instant = tokio::time::Instant::now();

    let index_metadata = metastore.index_metadata(&search_request.index_id).await?;

    let doc_mapper = build_doc_mapper(
        &index_metadata.doc_mapping,
        &index_metadata.search_settings,
        &index_metadata.indexing_settings,
    )
    .map_err(|err| {
        SearchError::InternalError(format!("Failed to build doc mapper. Cause: {}", err))
    })?;

    validate_request(search_request)?;

    // try to build query against current schema
    let _query = doc_mapper.query(doc_mapper.schema(), search_request)?;

    let doc_mapper_str = serde_json::to_string(&doc_mapper).map_err(|err| {
        SearchError::InternalError(format!("Failed to serialize doc mapper: Cause {}", err))
    })?;

    let split_metadatas: Vec<SplitMetadata> =
        list_relevant_splits(search_request, metastore).await?;

    let split_offsets_map: HashMap<String, SplitIdAndFooterOffsets> = split_metadatas
        .iter()
        .map(|metadata| {
            (
                metadata.split_id().to_string(),
                extract_split_and_footer_offsets(metadata),
            )
        })
        .collect();

    let jobs: Vec<SearchJob> = split_metadatas.iter().map(SearchJob::from).collect();
    let assigned_leaf_search_jobs = client_pool.assign_jobs(jobs, &HashSet::default())?;
    debug!(assigned_leaf_search_jobs=?assigned_leaf_search_jobs, "Assigned leaf search jobs.");
    let leaf_search_responses: Vec<LeafSearchResponse> = try_join_all(
        assigned_leaf_search_jobs
            .into_iter()
            .map(|(client, client_jobs)| {
                let leaf_request = jobs_to_leaf_request(
                    search_request,
                    &doc_mapper_str,
                    index_metadata.index_uri.as_ref(),
                    client_jobs,
                );
                cluster_client.leaf_search(leaf_request, client)
            }),
    )
    .await?;

    // Creates a collector which merges responses into one
    let merge_collector = make_merge_collector(search_request)?;

    // Merging is a cpu-bound task.
    // It should be executed by Tokio's blocking threads.

    // Wrap into result for merge_fruits
    let leaf_search_responses: Vec<tantivy::Result<LeafSearchResponse>> =
        leaf_search_responses.into_iter().map(Ok).collect_vec();
    let leaf_search_response =
        spawn_blocking(move || merge_collector.merge_fruits(leaf_search_responses))
            .await?
            .map_err(|merge_error: TantivyError| {
                crate::SearchError::InternalError(format!("{}", merge_error))
            })?;
    debug!(leaf_search_response = ?leaf_search_response, "Merged leaf search response.");

    if !leaf_search_response.failed_splits.is_empty() {
        error!(failed_splits = ?leaf_search_response.failed_splits, "Leaf search response contains at least one failed split.");
        let errors: String = leaf_search_response
            .failed_splits
            .iter()
            .map(|splits| format!("{}", splits))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(SearchError::InternalError(errors));
    }

    let client_fetch_docs_task: Vec<(SearchServiceClient, Vec<FetchDocsJob>)> =
        assign_client_fetch_doc_tasks(
            &leaf_search_response.partial_hits,
            &split_offsets_map,
            client_pool,
        )?;

    let fetch_docs_resp_futures =
        client_fetch_docs_task
            .into_iter()
            .map(|(client, fetch_docs_jobs)| {
                let partial_hits: Vec<PartialHit> = fetch_docs_jobs
                    .iter()
                    .flat_map(|fetch_doc_job| fetch_doc_job.partial_hits.iter().cloned())
                    .collect();
                let split_offsets: Vec<SplitIdAndFooterOffsets> = fetch_docs_jobs
                    .into_iter()
                    .map(|fetch_doc_job| fetch_doc_job.into())
                    .collect();
                let fetch_docs_req = FetchDocsRequest {
                    partial_hits,
                    index_id: search_request.index_id.to_string(),
                    split_offsets,
                    index_uri: index_metadata.index_uri.to_string(),
                };
                cluster_client.fetch_docs(fetch_docs_req, client)
            });

    let fetch_docs_resps: Vec<FetchDocsResponse> = try_join_all(fetch_docs_resp_futures).await?;

    // Merge the fetched docs.
    let leaf_hits = fetch_docs_resps
        .into_iter()
        .flat_map(|response| response.hits.into_iter());

    let mut hits: Vec<quickwit_proto::Hit> = leaf_hits
        .map(|leaf_hit: quickwit_proto::LeafHit| crate::convert_leaf_hit(leaf_hit, &*doc_mapper, search_request))
        .collect::<crate::Result<_>>()?;

    hits.sort_unstable_by_key(|hit| {
        Reverse(
            hit.partial_hit
                .as_ref()
                .map(|hit| HitScore::from(hit.sorting_field_value))
                .unwrap_or(0f32.into()),
        )
    });

    let elapsed = start_instant.elapsed();

    let aggregation = if let Some(intermediate_aggregation_result) =
        leaf_search_response.intermediate_aggregation_result
    {
        let res: IntermediateAggregationResults =
            serde_json::from_str(&intermediate_aggregation_result)?;
        let req: Aggregations = serde_json::from_str(search_request.aggregation_request())?;
        let res: AggregationResults = res.into_final_bucket_result(req)?;
        Some(serde_json::to_string(&res)?)
    } else {
        None
    };

    Ok(SearchResponse {
        aggregation,
        num_hits: leaf_search_response.num_hits,
        hits,
        elapsed_time_micros: elapsed.as_micros() as u64,
        errors: vec![],
    })
}

fn assign_client_fetch_doc_tasks(
    partial_hits: &[PartialHit],
    split_offsets_map: &HashMap<String, SplitIdAndFooterOffsets>,
    client_pool: &SearchClientPool,
) -> crate::Result<Vec<(SearchServiceClient, Vec<FetchDocsJob>)>> {
    // Group the partial hits per split
    let mut partial_hits_map: HashMap<String, Vec<PartialHit>> = HashMap::new();
    for partial_hit in partial_hits.iter() {
        partial_hits_map
            .entry(partial_hit.split_id.clone())
            .or_insert_with(Vec::new)
            .push(partial_hit.clone());
    }

    let mut fetch_docs_req_jobs: Vec<FetchDocsJob> = Vec::new();
    for (split_id, partial_hits) in partial_hits_map {
        let offsets = split_offsets_map
            .get(&split_id)
            .ok_or_else(|| {
                crate::SearchError::InternalError(format!(
                    "Received partial hit from an Unknown split {split_id}"
                ))
            })?
            .clone();
        let fetch_docs_job = FetchDocsJob {
            offsets,
            partial_hits,
        };
        fetch_docs_req_jobs.push(fetch_docs_job);
    }

    let assigned_jobs: Vec<(SearchServiceClient, Vec<FetchDocsJob>)> =
        client_pool.assign_jobs(fetch_docs_req_jobs, &HashSet::new())?;
    Ok(assigned_jobs)
}

// Measure the cost associated to searching in a given split metadata.
fn compute_split_cost(_split_metadata: &SplitMetadata) -> u32 {
    // TODO: Have a smarter cost, by smoothing the number of docs.
    1
}

fn jobs_to_leaf_request(
    request: &SearchRequest,
    doc_mapper_str: &str,
    index_uri: &str,
    jobs: Vec<SearchJob>,
) -> LeafSearchRequest {
    let mut request_with_offset_0 = request.clone();
    request_with_offset_0.start_offset = 0;
    request_with_offset_0.max_hits += request.start_offset;
    LeafSearchRequest {
        search_request: Some(request_with_offset_0),
        split_offsets: jobs.into_iter().map(|job| job.offsets).collect(),
        doc_mapper: doc_mapper_str.to_string(),
        index_uri: index_uri.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::sync::Arc;

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
            sorting_field_value: sorting_field_value as f32,
            split_id: split_id.to_string(),
            segment_ord: 1,
            doc_id,
        }
    }

    fn get_doc_for_fetch_req(
        fetch_docs_req: quickwit_proto::FetchDocsRequest,
    ) -> Vec<quickwit_proto::LeafHit> {
        fetch_docs_req
            .partial_hits
            .into_iter()
            .map(|req| quickwit_proto::LeafHit {
                leaf_json: serde_json::to_string_pretty(&serde_json::json!({
                    "title": [req.doc_id.to_string()],
                    "body": ["test 1"],
                    "url": ["http://127.0.0.1/1"]
                }))
                .expect("Json serialization should not fail"),
                partial_hit: Some(req),
            })
            .collect()
    }

    #[tokio::test]
    async fn test_root_search_offset_out_of_bounds_1085() -> anyhow::Result<()> {
        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-index".to_string(),
            query: "test".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 10,
            start_offset: 10,
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str, _split_state: SplitState, _time_range: Option<Range<i64>>, _tags| {
                Ok(vec![mock_split("split1"), mock_split("split2")])
            },
        );
        let mut mock_search_service2 = MockSearchService::new();
        mock_search_service2.expect_leaf_search().returning(
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
                    ..Default::default()
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
        let mut mock_search_service1 = MockSearchService::new();
        mock_search_service1.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResponse {
                    num_hits: 2,
                    partial_hits: vec![
                        mock_partial_hit("split2", 3, 1),
                        mock_partial_hit("split2", 1, 3),
                    ],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                    ..Default::default()
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
        let client_pool = SearchClientPool::from_mocks(vec![
            Arc::new(mock_search_service1),
            Arc::new(mock_search_service2),
        ])
        .await?;
        let cluster_client = ClusterClient::new(client_pool.clone());
        let search_response =
            root_search(&search_request, &metastore, &cluster_client, &client_pool).await?;
        assert_eq!(search_response.num_hits, 5);
        assert_eq!(search_response.hits.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_single_split() -> anyhow::Result<()> {
        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-index".to_string(),
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
                    "test-index",
                    "ram:///indexes/test-index",
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
                    ..Default::default()
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
        let client_pool = SearchClientPool::from_mocks(vec![Arc::new(mock_search_service)]).await?;
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
            index_id: "test-index".to_string(),
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
                    "test-index",
                    "ram:///indexes/test-index",
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
                    ..Default::default()
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
                    ..Default::default()
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
        let client_pool = SearchClientPool::from_mocks(vec![
            Arc::new(mock_search_service1),
            Arc::new(mock_search_service2),
        ])
        .await?;
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
            index_id: "test-index".to_string(),
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
                    "test-index",
                    "ram:///indexes/test-index",
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
                    ..Default::default()
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
                let split_ids: Vec<&str> = leaf_search_req
                    .split_offsets
                    .iter()
                    .map(|metadata| metadata.split_id.as_str())
                    .collect();
                if split_ids == ["split1"] {
                    Ok(quickwit_proto::LeafSearchResponse {
                        num_hits: 2,
                        partial_hits: vec![
                            mock_partial_hit("split1", 3, 1),
                            mock_partial_hit("split1", 1, 3),
                        ],
                        failed_splits: Vec::new(),
                        num_attempted_splits: 1,
                        ..Default::default()
                    })
                } else if split_ids == ["split2"] {
                    // RETRY REQUEST!
                    Ok(quickwit_proto::LeafSearchResponse {
                        num_hits: 1,
                        partial_hits: vec![mock_partial_hit("split2", 2, 2)],
                        failed_splits: Vec::new(),
                        num_attempted_splits: 1,
                        ..Default::default()
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
        let client_pool = SearchClientPool::from_mocks(vec![
            Arc::new(mock_search_service1),
            Arc::new(mock_search_service2),
        ])
        .await?;
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
            index_id: "test-index".to_string(),
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
                    "test-index",
                    "ram:///indexes/test-index",
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
            .withf(|leaf_search_req| leaf_search_req.split_offsets[0].split_id == "split2")
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
                    ..Default::default()
                })
            });
        mock_search_service1
            .expect_leaf_search()
            .withf(|leaf_search_req| leaf_search_req.split_offsets[0].split_id == "split1")
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
                    ..Default::default()
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
            .withf(|leaf_search_req| leaf_search_req.split_offsets[0].split_id == "split2")
            .return_once(|_| {
                println!("request from service2 split2?");
                // retry for split 2 arrive here, simulate success.
                Ok(quickwit_proto::LeafSearchResponse {
                    num_hits: 1,
                    partial_hits: vec![mock_partial_hit("split2", 2, 2)],
                    failed_splits: Vec::new(),
                    num_attempted_splits: 1,
                    ..Default::default()
                })
            });
        mock_search_service2
            .expect_leaf_search()
            .withf(|leaf_search_req| leaf_search_req.split_offsets[0].split_id == "split1")
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
                    ..Default::default()
                })
            });
        mock_search_service2.expect_fetch_docs().returning(
            |fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResponse {
                    hits: get_doc_for_fetch_req(fetch_docs_req),
                })
            },
        );
        let client_pool = SearchClientPool::from_mocks(vec![
            Arc::new(mock_search_service1),
            Arc::new(mock_search_service2),
        ])
        .await?;
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
            index_id: "test-index".to_string(),
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
                    "test-index",
                    "ram:///indexes/test-index",
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
                        ..Default::default()
                    })
                } else {
                    Ok(quickwit_proto::LeafSearchResponse {
                        num_hits: 1,
                        partial_hits: vec![mock_partial_hit("split1", 2, 2)],
                        failed_splits: Vec::new(),
                        num_attempted_splits: 1,
                        ..Default::default()
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
            SearchClientPool::from_mocks(vec![Arc::new(mock_search_service1)]).await?;
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
            index_id: "test-index".to_string(),
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
                    "test-index",
                    "ram:///indexes/test-index",
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
                    ..Default::default()
                })
            });
        mock_search_service1.expect_fetch_docs().returning(
            |_fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Err(SearchError::InternalError("mockerr docs".to_string()))
            },
        );
        let client_pool =
            SearchClientPool::from_mocks(vec![Arc::new(mock_search_service1)]).await?;
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
            index_id: "test-index".to_string(),
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
                    "test-index",
                    "ram:///indexes/test-index",
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
                    ..Default::default()
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
                    ..Default::default()
                })
            },
        );
        mock_search_service2.expect_fetch_docs().returning(
            |_fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Err(SearchError::InternalError("mockerr docs".to_string()))
            },
        );
        let client_pool = SearchClientPool::from_mocks(vec![
            Arc::new(mock_search_service1),
            Arc::new(mock_search_service2),
        ])
        .await?;
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
            index_id: "test-index".to_string(),
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
                    "test-index",
                    "ram:///indexes/test-index",
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
                    ..Default::default()
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
        let client_pool = SearchClientPool::from_mocks(vec![
            Arc::new(mock_search_service1),
            Arc::new(mock_search_service2),
        ])
        .await?;
        let cluster_client = ClusterClient::new(client_pool.clone());
        let search_response =
            root_search(&search_request, &metastore, &cluster_client, &client_pool).await?;
        assert_eq!(search_response.num_hits, 1);
        assert_eq!(search_response.hits.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_invalid_queries() -> anyhow::Result<()> {
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str, _split_state: SplitState, _time_range: Option<Range<i64>>, _tags| {
                Ok(vec![mock_split("split")])
            },
        );

        let client_pool =
            SearchClientPool::from_mocks(vec![Arc::new(MockSearchService::new())]).await?;
        let cluster_client = ClusterClient::new(client_pool.clone());

        assert!(root_search(
            &quickwit_proto::SearchRequest {
                index_id: "test-index".to_string(),
                query: r#"invalid_field:"test""#.to_string(),
                search_fields: vec!["body".to_string()],
                start_timestamp: None,
                end_timestamp: None,
                max_hits: 10,
                start_offset: 0,
                ..Default::default()
            },
            &metastore,
            &cluster_client,
            &client_pool,
        )
        .await
        .is_err());

        assert!(root_search(
            &quickwit_proto::SearchRequest {
                index_id: "test-index".to_string(),
                query: "test".to_string(),
                search_fields: vec!["invalid_field".to_string()],
                start_timestamp: None,
                end_timestamp: None,
                max_hits: 10,
                start_offset: 0,
                ..Default::default()
            },
            &metastore,
            &cluster_client,
            &client_pool,
        )
        .await
        .is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_invalid_aggregation() -> anyhow::Result<()> {
        let agg_req = r#"
            {
                "expensive_colors": {
                    "termss": {
                        "field": "color",
                        "order": {
                            "price_stats.max": "desc"
                        }
                    },
                    "aggs": {
                        "price_stats" : {
                            "stats": {
                                "field": "price"
                            }
                        }
                    }
                }
            }"#;

        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-index".to_string(),
            query: "test".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 10,
            start_offset: 0,
            aggregation_request: Some(agg_req.to_string()),
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str, _split_state: SplitState, _time_range: Option<Range<i64>>, _tags| {
                Ok(vec![mock_split("split1")])
            },
        );
        let client_pool =
            SearchClientPool::from_mocks(vec![Arc::new(MockSearchService::new())]).await?;
        let cluster_client = ClusterClient::new(client_pool.clone());
        let search_response =
            root_search(&search_request, &metastore, &cluster_client, &client_pool).await;
        assert!(search_response.is_err());
        assert_eq!(
            search_response.unwrap_err().to_string(),
            "Invalid aggregation request: data did not match any variant of untagged enum \
             Aggregation at line 18 column 13",
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_invalid_request() -> anyhow::Result<()> {
        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-index".to_string(),
            query: "test".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 10,
            start_offset: 20_000,
            ..Default::default()
        };
        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata::for_test(
                    "test-index",
                    "ram:///indexes/test-index",
                ))
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str, _split_state: SplitState, _time_range: Option<Range<i64>>, _tags| {
                Ok(vec![mock_split("split1")])
            },
        );
        let client_pool =
            SearchClientPool::from_mocks(vec![Arc::new(MockSearchService::new())]).await?;
        let cluster_client = ClusterClient::new(client_pool.clone());
        let search_response =
            root_search(&search_request, &metastore, &cluster_client, &client_pool).await;
        assert!(search_response.is_err());
        assert_eq!(
            search_response.unwrap_err().to_string(),
            "Invalid argument: max value for start_offset is 10_000, but got 20000",
        );

        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-index".to_string(),
            query: "test".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 20_000,
            ..Default::default()
        };

        let search_response =
            root_search(&search_request, &metastore, &cluster_client, &client_pool).await;
        assert!(search_response.is_err());
        assert_eq!(
            search_response.unwrap_err().to_string(),
            "Invalid argument: max value for max_hits is 10_000, but got 20000",
        );

        Ok(())
    }
}
