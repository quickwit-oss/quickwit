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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use mockall::predicate;
use quickwit_proto::search::{SearchRequest, SearchResponse};

use super::BatchingSearchService;
use super::combine::batch_grouping_key;
use super::dispatcher::{BatchEntry, batch_execute, dispatch_batch};
use super::normalize::normalize_request;
use crate::{MockSearchService, SearchError, SearchService};

fn make_search_request(query_ast: &str, max_hits: u64, aggregation: Option<&str>) -> SearchRequest {
    normalize_request(SearchRequest {
        index_id_patterns: vec!["test-index".to_string()],
        query_ast: query_ast.to_string(),
        start_timestamp: Some(1000),
        end_timestamp: Some(2000),
        max_hits,
        aggregation_request: aggregation.map(|s| s.to_string()),
        skip_aggregation_finalization: aggregation.is_some(),
        ..Default::default()
    })
}

const QUERY: &str = r#"{"type":"match_all"}"#;

#[tokio::test]
async fn test_dispatch_sort_is_stable_and_keeps_response_senders() {
    use tantivy::aggregation::intermediate_agg_result::{
        IntermediateAggregationResult, IntermediateAggregationResults, IntermediateMetricResult,
    };
    use tantivy::aggregation::metric::IntermediateSum;

    let batch_entry = |request| {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        (
            BatchEntry {
                request,
                result_tx,
                batch_key: 0,
                span: tracing::Span::none(),
            },
            result_rx,
        )
    };
    let assert_aggregation = |response: SearchResponse, expected_key: &str| {
        let aggregation_postcard = response.aggregation_postcard.unwrap();
        let aggregation: IntermediateAggregationResults =
            postcard::from_bytes(&aggregation_postcard).unwrap();
        assert_eq!(aggregation.keys().count(), 1);
        assert!(aggregation.get(expected_key).is_some());
    };

    let agg_a = make_search_request(QUERY, 0, Some(r#"{"sum_a":{"sum":{"field":"a"}}}"#));
    let agg_b = make_search_request(QUERY, 0, Some(r#"{"sum_b":{"sum":{"field":"b"}}}"#));
    let list = make_search_request(QUERY, 1, None);

    let combined_requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let combined_requests_clone = combined_requests.clone();
    let mut mock = MockSearchService::new();
    mock.expect_root_search()
        .times(2)
        .returning(move |request| {
            combined_requests_clone.lock().unwrap().push(request);
            let mut aggregation = IntermediateAggregationResults::default();
            aggregation
                .push(
                    "__b1_sum_a".to_string(),
                    IntermediateAggregationResult::Metric(IntermediateMetricResult::Sum(
                        IntermediateSum::default(),
                    )),
                )
                .unwrap();
            aggregation
                .push(
                    "__b2_sum_b".to_string(),
                    IntermediateAggregationResult::Metric(IntermediateMetricResult::Sum(
                        IntermediateSum::default(),
                    )),
                )
                .unwrap();
            Ok(SearchResponse {
                hits: vec![quickwit_proto::search::Hit {
                    json: r#"{"i":1}"#.to_string(),
                    ..Default::default()
                }],
                aggregation_postcard: Some(postcard::to_stdvec(&aggregation).unwrap()),
                ..Default::default()
            })
        });

    {
        let (agg_a, agg_a_rx) = batch_entry(agg_a.clone());
        let (list, list_rx) = batch_entry(list.clone());
        let (agg_b, agg_b_rx) = batch_entry(agg_b.clone());
        dispatch_batch(&mock, vec![agg_a, list, agg_b]).await;
        assert_aggregation(agg_a_rx.await.unwrap().unwrap(), "sum_a");
        assert_eq!(list_rx.await.unwrap().unwrap().hits.len(), 1);
        assert_aggregation(agg_b_rx.await.unwrap().unwrap(), "sum_b");
    }

    {
        let (agg_a, agg_a_rx) = batch_entry(agg_a.clone());
        let (list, list_rx) = batch_entry(list.clone());
        let (agg_b, agg_b_rx) = batch_entry(agg_b.clone());
        dispatch_batch(&mock, vec![list, agg_b, agg_a]).await;
        assert_aggregation(agg_a_rx.await.unwrap().unwrap(), "sum_a");
        assert_eq!(list_rx.await.unwrap().unwrap().hits.len(), 1);
        assert_aggregation(agg_b_rx.await.unwrap().unwrap(), "sum_b");
    }

    let combined_requests = combined_requests.lock().unwrap();
    assert_eq!(combined_requests.len(), 2);
    assert_eq!(combined_requests[0], combined_requests[1]);
}

#[tokio::test]
async fn test_batch_single_request_passthrough() {
    let mut mock = MockSearchService::new();
    mock.expect_root_search().times(1).returning(|_| {
        Ok(SearchResponse {
            num_hits: 42,
            ..Default::default()
        })
    });

    let results = batch_execute(&mock, vec![make_search_request(QUERY, 10, None)]).await;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_ref().unwrap().num_hits, 42);
}

#[tokio::test]
async fn test_batch_combines_aggregations() {
    let mut mock = MockSearchService::new();
    mock.expect_root_search()
        .times(1)
        .with(predicate::function(|req: &SearchRequest| {
            let agg_json = req.aggregation_request.as_ref().unwrap();
            let agg_map: HashMap<String, serde_json::Value> =
                serde_json::from_str(agg_json).unwrap();
            // keys are prefixed with __b{idx}_ to avoid collisions
            agg_map.contains_key("__b0_agg_a")
                && agg_map.contains_key("__b1_agg_b")
                && agg_map.contains_key("__b2_agg_c")
                && agg_map.len() == 3
        }))
        .returning(|_| {
            Ok(SearchResponse {
                num_hits: 100,
                ..Default::default()
            })
        });

    let requests = vec![
        make_search_request(QUERY, 0, Some(r#"{"agg_a": {"avg": {"field": "price"}}}"#)),
        make_search_request(
            QUERY,
            0,
            Some(r#"{"agg_b": {"terms": {"field": "color"}}}"#),
        ),
        make_search_request(QUERY, 0, Some(r#"{"agg_c": {"sum": {"field": "qty"}}}"#)),
    ];

    let results = batch_execute(&mock, requests).await;

    assert_eq!(results.len(), 3);
    for result in &results {
        let response = result.as_ref().unwrap();
        assert_eq!(response.num_hits, 100);
        assert!(response.hits.is_empty());
    }
}

#[tokio::test]
async fn test_batch_dispatches_when_request_limit_is_reached() {
    let mut mock = MockSearchService::new();
    mock.expect_root_search()
        .times(2)
        .returning(|_| Ok(SearchResponse::default()));

    let service = BatchingSearchService::new(Arc::new(mock), Duration::from_millis(10));
    let requests = (0..16).map(|idx| {
        let aggregation = format!(r#"{{"agg_{idx}":{{"avg":{{"field":"price"}}}}}}"#);
        let mut request = make_search_request(QUERY, 0, Some(&aggregation));
        request.enable_request_batching = true;
        request
    });

    let results =
        futures::future::join_all(requests.map(|request| service.root_search(request))).await;

    assert_eq!(results.len(), 16);
    assert!(results.iter().all(Result::is_ok));
}

#[tokio::test]
async fn test_batch_list_plus_aggregations() {
    let mut mock = MockSearchService::new();
    mock.expect_root_search()
        .times(1)
        .with(predicate::function(|req: &SearchRequest| {
            // combined request should have max_hits from the list request
            req.max_hits == 100 && req.aggregation_request.is_some() && {
                let agg: HashMap<String, serde_json::Value> =
                    serde_json::from_str(req.aggregation_request.as_ref().unwrap()).unwrap();
                agg.contains_key("__b1_histogram") && agg.contains_key("__b2_facets")
            }
        }))
        .returning(|_| {
            Ok(SearchResponse {
                num_hits: 500,
                hits: vec![quickwit_proto::search::Hit {
                    json: r#"{"msg":"hello"}"#.to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            })
        });

    let requests = vec![
        make_search_request(QUERY, 100, None),
        make_search_request(
            QUERY,
            0,
            Some(r#"{"histogram": {"date_histogram": {"field": "timestamp"}}}"#),
        ),
        make_search_request(
            QUERY,
            0,
            Some(r#"{"facets": {"terms": {"field": "service"}}}"#),
        ),
    ];

    let results = batch_execute(&mock, requests).await;

    assert_eq!(results.len(), 3);

    let list_response = results[0].as_ref().unwrap();
    assert_eq!(list_response.hits.len(), 1);
    assert_eq!(list_response.num_hits, 500);

    for result in &results[1..] {
        let response = result.as_ref().unwrap();
        assert!(response.hits.is_empty());
        assert_eq!(response.num_hits, 500);
    }
}

#[tokio::test]
async fn test_batch_falls_back_on_mismatched_queries() {
    let mut mock = MockSearchService::new();
    mock.expect_root_search().times(2).returning(|_| {
        Ok(SearchResponse {
            num_hits: 1,
            ..Default::default()
        })
    });

    let requests = vec![
        make_search_request(QUERY, 0, None),
        make_search_request(r#"{"type":"bool","must":[]}"#, 0, None),
    ];

    let results = batch_execute(&mock, requests).await;

    assert_eq!(results.len(), 2);
    for result in &results {
        assert!(result.is_ok(), "fallback should succeed individually");
    }
}

#[tokio::test]
async fn test_batch_falls_back_on_mismatched_timestamps() {
    let mut mock = MockSearchService::new();
    mock.expect_root_search().times(2).returning(|_| {
        Ok(SearchResponse {
            num_hits: 1,
            ..Default::default()
        })
    });

    let mut req_1 = make_search_request(QUERY, 0, None);
    req_1.start_timestamp = Some(1000);

    let mut req_2 = make_search_request(QUERY, 0, None);
    req_2.start_timestamp = Some(9999);

    let results = batch_execute(&mock, vec![req_1, req_2]).await;

    assert_eq!(results.len(), 2);
    for result in &results {
        assert!(result.is_ok(), "fallback should succeed individually");
    }
}

#[tokio::test]
async fn test_batch_falls_back_on_conflicting_list_sort_fields() {
    let mut mock = MockSearchService::new();
    mock.expect_root_search().times(2).returning(|_| {
        Ok(SearchResponse {
            num_hits: 1,
            ..Default::default()
        })
    });

    let mut req_1 = make_search_request(QUERY, 50, None);
    req_1.sort_fields = vec![quickwit_proto::search::SortField {
        field_name: "timestamp".to_string(),
        sort_order: 1, // desc
        ..Default::default()
    }];

    let mut req_2 = make_search_request(QUERY, 100, None);
    req_2.sort_fields = vec![quickwit_proto::search::SortField {
        field_name: "service".to_string(),
        sort_order: 0, // asc
        ..Default::default()
    }];

    let results = batch_execute(&mock, vec![req_1, req_2]).await;

    assert_eq!(results.len(), 2);
    for result in &results {
        assert!(result.is_ok(), "fallback should succeed individually");
    }
}

#[tokio::test]
async fn test_batch_compatible_list_requests() {
    let mut mock = MockSearchService::new();
    mock.expect_root_search()
        .times(1)
        .with(predicate::function(|req: &SearchRequest| {
            req.max_hits == 100
        }))
        .returning(|_| {
            Ok(SearchResponse {
                num_hits: 200,
                hits: vec![
                    quickwit_proto::search::Hit {
                        json: r#"{"i":1}"#.to_string(),
                        ..Default::default()
                    },
                    quickwit_proto::search::Hit {
                        json: r#"{"i":2}"#.to_string(),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            })
        });

    let mut req_1 = make_search_request(QUERY, 50, None);
    req_1.sort_fields = vec![quickwit_proto::search::SortField {
        field_name: "timestamp".to_string(),
        sort_order: 1,
        ..Default::default()
    }];

    let mut req_2 = make_search_request(QUERY, 100, None);
    req_2.sort_fields = req_1.sort_fields.clone();

    let results = batch_execute(&mock, vec![req_1, req_2]).await;

    assert_eq!(results.len(), 2);
    for result in &results {
        let response = result.as_ref().unwrap();
        assert_eq!(response.hits.len(), 2);
        assert_eq!(response.num_hits, 200);
    }
}

#[tokio::test]
async fn test_batch_root_search_failure() {
    let mut mock = MockSearchService::new();
    mock.expect_root_search()
        .times(1)
        .returning(|_| Err(SearchError::Internal("boom".to_string())));

    let requests = vec![
        make_search_request(QUERY, 0, Some(r#"{"agg_a": {"avg": {"field": "price"}}}"#)),
        make_search_request(QUERY, 0, Some(r#"{"agg_b": {"sum": {"field": "qty"}}}"#)),
    ];

    let results = batch_execute(&mock, requests).await;

    assert_eq!(results.len(), 2);
    for result in &results {
        assert!(result.is_err(), "all requests should get error");
    }
}

#[tokio::test]
async fn test_batch_parity_with_independent_searches() -> anyhow::Result<()> {
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::Arc;

    use quickwit_config::SearcherConfig;
    use quickwit_indexing::TestSandbox;
    use quickwit_proto::search::{SortField, SortOrder};
    use quickwit_query::query_ast::qast_json_helper;
    use serde_json::json;

    use crate::cluster_client::ClusterClient;
    use crate::search_job_placer::SearchJobPlacer;
    use crate::service::{SearchServiceImpl, SearcherContext};
    use crate::{SearchServiceClient, SearcherPool, single_node_search};

    let index_id = "batch-parity-test";
    let doc_mapping_yaml = r#"
            field_mappings:
              - name: body
                type: text
              - name: service
                type: text
                tokenizer: raw
                fast: true
              - name: status
                type: u64
                fast: true
              - name: timestamp
                type: datetime
                input_formats:
                  - unix_timestamp
                fast_precision: seconds
                fast: true
            timestamp_field: timestamp
        "#;

    let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "{}", &["body"]).await?;

    let now = tantivy::time::OffsetDateTime::now_utc().unix_timestamp();
    let docs = vec![
        // docs WITH service field (4 docs match "request")
        json!({"body": "request started", "service": "api", "status": 200, "timestamp": now - 100}),
        json!({"body": "request failed", "service": "api", "status": 500, "timestamp": now - 90}),
        json!({"body": "request started", "service": "web", "status": 200, "timestamp": now - 80}),
        json!({"body": "request failed", "service": "web", "status": 503, "timestamp": now - 60}),
        // docs WITHOUT service field (2 docs match "request")
        // these prove that stripping field_presence is safe:
        // the terms agg on "service" should skip them even without field_presence
        json!({"body": "request from unknown", "status": 200, "timestamp": now - 50}),
        json!({"body": "request orphan", "status": 404, "timestamp": now - 40}),
        // doc that doesn't match the base query (should be excluded regardless)
        json!({"body": "cache miss", "service": "api", "status": 200, "timestamp": now - 70}),
    ];
    test_sandbox.add_documents(docs).await?;

    let base_query = qast_json_helper("request", &["body"]);

    // list: bool wrapper without field_presence
    let list_query = format!(r#"{{"type":"bool","must":[{base_query}]}}"#);

    // facet: bool wrapper WITH field_presence (like real Log Explorer traffic)
    let service_query = format!(
        r#"{{"type":"bool","must":[{{"type":"field_presence","field":"service"}},{base_query}]}}"#
    );
    let histogram_query = format!(
        r#"{{"type":"bool","must":[{{"type":"field_presence","field":"timestamp"}},{base_query}]}}"#
    );
    let status_query = format!(
        r#"{{"type":"bool","must":[{{"type":"field_presence","field":"status"}},{base_query}]}}"#
    );

    let list_req = SearchRequest {
        index_id_patterns: vec![index_id.to_string()],
        query_ast: list_query,
        max_hits: 10,
        sort_fields: vec![SortField {
            field_name: "timestamp".to_string(),
            sort_order: SortOrder::Desc as i32,
            ..Default::default()
        }],
        ..Default::default()
    };
    let facet_req = SearchRequest {
        index_id_patterns: vec![index_id.to_string()],
        query_ast: service_query,
        max_hits: 0,
        aggregation_request: Some(r#"{"service_facet":{"terms":{"field":"service"}}}"#.to_string()),
        skip_aggregation_finalization: true,
        ..Default::default()
    };
    let histogram_req = SearchRequest {
        index_id_patterns: vec![index_id.to_string()],
        query_ast: histogram_query,
        max_hits: 0,
        aggregation_request: Some(
            r#"{"timeline":{"date_histogram":{"field":"timestamp","fixed_interval":"1h"}}}"#
                .to_string(),
        ),
        skip_aggregation_finalization: true,
        ..Default::default()
    };
    let status_req = SearchRequest {
        index_id_patterns: vec![index_id.to_string()],
        query_ast: status_query,
        max_hits: 0,
        aggregation_request: Some(r#"{"status_facet":{"terms":{"field":"status"}}}"#.to_string()),
        skip_aggregation_finalization: true,
        ..Default::default()
    };

    // --- run independently ---
    let metastore = test_sandbox.metastore();
    let storage_resolver = test_sandbox.storage_resolver();

    let indep_list = single_node_search(
        list_req.clone(),
        metastore.clone(),
        storage_resolver.clone(),
    )
    .await?;
    let indep_facet = single_node_search(
        facet_req.clone(),
        metastore.clone(),
        storage_resolver.clone(),
    )
    .await?;
    let indep_histogram = single_node_search(
        histogram_req.clone(),
        metastore.clone(),
        storage_resolver.clone(),
    )
    .await?;
    let indep_status = single_node_search(
        status_req.clone(),
        metastore.clone(),
        storage_resolver.clone(),
    )
    .await?;

    // --- run batched ---
    let socket_addr = SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 7280u16);
    let searcher_pool = SearcherPool::default();
    let search_job_placer = SearchJobPlacer::new(searcher_pool.clone());
    let cluster_client = ClusterClient::new(search_job_placer);
    let searcher_context = Arc::new(SearcherContext::new_without_invoker(
        SearcherConfig::default(),
        None,
    ));
    let search_service = Arc::new(SearchServiceImpl::new(
        metastore.clone(),
        storage_resolver.clone(),
        cluster_client,
        searcher_context,
    ));
    let search_service_client =
        SearchServiceClient::from_service(search_service.clone(), socket_addr);
    searcher_pool.insert(socket_addr, search_service_client);

    let batched_results = batch_execute(
        search_service.as_ref(),
        vec![list_req, facet_req, histogram_req, status_req]
            .into_iter()
            .map(normalize_request)
            .collect(),
    )
    .await;

    assert_eq!(batched_results.len(), 4, "should return 4 results");

    let batched_list = batched_results[0].as_ref().unwrap();
    let batched_facet = batched_results[1].as_ref().unwrap();
    let batched_histogram = batched_results[2].as_ref().unwrap();
    let batched_status = batched_results[3].as_ref().unwrap();

    // parity: list request should have same num_hits and hits
    assert_eq!(
        indep_list.num_hits, batched_list.num_hits,
        "list num_hits mismatch"
    );
    assert_eq!(
        indep_list.hits.len(),
        batched_list.hits.len(),
        "list hits count mismatch"
    );
    for (indep_hit, batched_hit) in indep_list.hits.iter().zip(batched_list.hits.iter()) {
        assert_eq!(
            indep_hit.json, batched_hit.json,
            "list hit content mismatch"
        );
    }

    // parity: facet aggregation results should match exactly.
    //
    // num_hits may differ because stripping field_presence widens the
    // predicate (includes docs without the facet field). But the
    // aggregation result (terms buckets) should be identical because
    // tantivy's terms agg skips docs without the field.
    //
    // This is the key safety proof: stripping field_presence is safe
    // for aggregation results even when docs are missing the field.
    assert_eq!(
        indep_facet.aggregation_postcard, batched_facet.aggregation_postcard,
        "facet aggregation mismatch — stripping field_presence changed the result"
    );
    assert_eq!(
        indep_histogram.aggregation_postcard, batched_histogram.aggregation_postcard,
        "histogram aggregation mismatch"
    );
    assert_eq!(
        indep_status.aggregation_postcard, batched_status.aggregation_postcard,
        "status aggregation mismatch"
    );

    // verify that num_hits DOES differ for facets (proving field_presence was stripped)
    assert!(
        batched_facet.num_hits >= indep_facet.num_hits,
        "batched should match at least as many docs as independent (field_presence stripped)"
    );

    test_sandbox.assert_quit().await;
    Ok(())
}

// -- regression test: count-only requests batched together must not produce "no aggregation result"
// --

/// Regression test for a bug where batching two count-only requests (aggregation_request =
/// Some("{}")) produced a combined request with aggregation_request = None.  The combined
/// root_search returned no aggregation_postcard, and unbatch_response returned agg_postcard = None
/// for every sub-request — causing the cloudprem layer to fail with "request generated no
/// aggregation result".
///
/// Fix: unbatch_response now returns an empty postcard (rather than None) when the sub-request had
/// an aggregation_request but the combined result contained no matching __b{idx}_ keys.
#[tokio::test]
async fn test_batch_count_only_requests_produce_non_none_agg_postcard() {
    let mut mock = MockSearchService::new();
    mock.expect_root_search().times(1).returning(|_| {
        Ok(SearchResponse {
            num_hits: 14,
            ..Default::default()
        })
    });

    // Two count-only requests: aggregation_request = Some("{}"), no actual aggregation keys.
    // This is the pattern sent by CloudPrem for count widgets.
    let requests = vec![
        make_search_request(QUERY, 0, Some("{}")),
        make_search_request(QUERY, 0, Some("{}")),
    ];

    let results = batch_execute(&mock, requests).await;

    assert_eq!(results.len(), 2);
    for (i, result) in results.iter().enumerate() {
        let response = result
            .as_ref()
            .unwrap_or_else(|e| panic!("request {i} failed: {e}"));
        assert_eq!(response.num_hits, 14);
        // Must have a non-None aggregation_postcard — the cloudprem layer requires it
        // when the original request had aggregation_request = Some("{}").
        assert!(
            response.aggregation_postcard.is_some(),
            "request {i}: expected non-None aggregation_postcard for count-only batched request"
        );
    }
}

// -- tests for field_presence stripping --

fn fp_query(field: &str) -> String {
    format!(
        r#"{{"type":"bool","must":[{{"type":"field_presence","field":"{field}"}},{{"type":"full_text","field":"body","text":"error","params":{{"mode":{{"type":"bool","operator":"And"}}}},"lenient":false}}]}}"#
    )
}

fn assert_stripped(query: &str, agg: Option<&str>, expect_stripped: bool, msg: &str) {
    let req = make_search_request(query, 0, agg);
    let normalized = normalize_request(req);
    let parsed: serde_json::Value = serde_json::from_str(&normalized.query_ast).unwrap();
    let must = parsed["must"].as_array().unwrap();
    let has_fp = must.iter().any(|c| c["type"] == "field_presence");
    if expect_stripped {
        assert!(!has_fp, "field_presence should be stripped: {msg}");
    } else {
        assert!(has_fp, "field_presence should be kept: {msg}");
    }
}

// scenario 1: single terms(service) + field_presence(service) → strip
#[test]
fn test_strip_fp_matching_agg() {
    let query = fp_query("service");
    let agg = r#"{"svc_facet": {"terms": {"field": "service"}}}"#;
    assert_stripped(&query, Some(agg), true, "agg targets service");
}

// scenario 2: terms(status) + field_presence(service) → don't strip
#[test]
fn test_strip_fp_no_matching_agg() {
    let query = fp_query("service");
    let agg = r#"{"status_facet": {"terms": {"field": "status"}}}"#;
    assert_stripped(&query, Some(agg), false, "agg targets status, not service");
}

// scenario 8: no agg (list request) → don't strip
#[test]
fn test_strip_fp_no_agg() {
    let query = fp_query("service");
    assert_stripped(&query, None, false, "list request, no agg");
}

// scenario 3: two top-level aggs both targeting service → strip
#[test]
fn test_strip_fp_two_aggs_same_field() {
    let query = fp_query("service");
    let agg =
        r#"{"a": {"terms": {"field": "service"}}, "b": {"cardinality": {"field": "service"}}}"#;
    assert_stripped(&query, Some(agg), true, "both aggs target service");
}

// scenario 4: two top-level aggs, different fields → don't strip
#[test]
fn test_strip_fp_two_aggs_different_fields() {
    let query = fp_query("service");
    let agg = r#"{"a": {"terms": {"field": "service"}}, "b": {"terms": {"field": "status"}}}"#;
    assert_stripped(&query, Some(agg), false, "aggs target different fields");
}

// scenario 5: nested sub-agg on different field, parent targets service → strip
#[test]
fn test_strip_fp_nested_sub_agg_different_field() {
    let query = fp_query("service");
    let agg =
        r#"{"a": {"terms": {"field": "service"}, "aggs": {"b": {"avg": {"field": "latency"}}}}}"#;
    assert_stripped(
        &query,
        Some(agg),
        true,
        "sub-agg shielded by parent targeting service",
    );
}

// scenario 6: date_histogram(timestamp) with sub terms(service) → don't strip
#[test]
fn test_strip_fp_parent_different_field() {
    let query = fp_query("service");
    let agg = r#"{"a": {"date_histogram": {"field": "timestamp", "fixed_interval": "3600s"}, "aggs": {"b": {"terms": {"field": "service"}}}}}"#;
    assert_stripped(
        &query,
        Some(agg),
        false,
        "top-level agg targets timestamp, not service",
    );
}

// scenario 7: filter agg → don't strip
#[test]
fn test_strip_fp_filter_agg() {
    let query = fp_query("service");
    let agg = r#"{"a": {"filter": {"term": {"status": "500"}}}}"#;
    assert_stripped(
        &query,
        Some(agg),
        false,
        "filter agg has no field guarantee",
    );
}

// scenario 9: metric agg (avg) on same field → strip
#[test]
fn test_strip_fp_metric_agg_same_field() {
    let query = fp_query("service");
    let agg = r#"{"a": {"avg": {"field": "service"}}}"#;
    assert_stripped(&query, Some(agg), true, "avg targets same field");
}

#[test]
fn test_grouping_key_ignores_field_presence_for_matching_agg() {
    let base = r#"{"type":"bool","must":[{"type":"range","field":"timestamp","lower_bound":{"Included":"2026-01-01"},"upper_bound":{"Excluded":"2026-01-02"}}]}"#;
    let with_service = r#"{"type":"bool","must":[{"type":"field_presence","field":"service"},{"type":"range","field":"timestamp","lower_bound":{"Included":"2026-01-01"},"upper_bound":{"Excluded":"2026-01-02"}}]}"#;
    let with_status = r#"{"type":"bool","must":[{"type":"field_presence","field":"status"},{"type":"range","field":"timestamp","lower_bound":{"Included":"2026-01-01"},"upper_bound":{"Excluded":"2026-01-02"}}]}"#;

    let req_base = make_search_request(base, 10, None);
    let req_service = make_search_request(
        with_service,
        0,
        Some(r#"{"svc": {"terms": {"field": "service"}}}"#),
    );
    let req_status = make_search_request(
        with_status,
        0,
        Some(r#"{"sts": {"terms": {"field": "status"}}}"#),
    );

    let key_base = batch_grouping_key(&normalize_request(req_base));
    let key_service = batch_grouping_key(&normalize_request(req_service));
    let key_status = batch_grouping_key(&normalize_request(req_status));

    assert_eq!(
        key_base, key_service,
        "list and service-facet should have same key"
    );
    assert_eq!(
        key_base, key_status,
        "list and status-facet should have same key"
    );
}

// 10 requests with timestamp lower bounds uniformly spread over 10 seconds.
// After quantization, they should land in 3-4 distinct grouping key buckets.
#[test]
fn test_timestamp_quantization_buckets() {
    let mut keys = std::collections::HashSet::new();
    for i in 0..10 {
        let query = format!(
            r#"{{"type":"bool","must":[{{"type":"range","field":"timestamp","lower_bound":{{"Included":"2026-04-22T12:00:{i:02}.000000000Z"}},"upper_bound":{{"Excluded":"2026-04-22T13:00:{i:02}.000000000Z"}}}}]}}"#,
            i = i
        );
        let req = make_search_request(&query, 10, None);
        let normalized = normalize_request(req);
        keys.insert(batch_grouping_key(&normalized));
    }
    assert!(
        keys.len() >= 3 && keys.len() <= 4,
        "expected 3-4 distinct grouping keys, got {}",
        keys.len()
    );
}

#[tokio::test]
async fn test_batch_facet_queries_with_different_field_presence() {
    // simulates real Log Explorer traffic: 1 list + 3 facet aggs
    // each facet has a different field_presence but same base query
    let mut mock = MockSearchService::new();
    mock.expect_root_search()
        .times(1)
        .with(predicate::function(|req: &SearchRequest| {
            // combined request should NOT contain field_presence
            let parsed: serde_json::Value = serde_json::from_str(&req.query_ast).unwrap();
            let must = parsed["must"].as_array().unwrap();
            let has_fp = must.iter().any(|c| c["type"] == "field_presence");
            !has_fp && req.max_hits == 10 && req.aggregation_request.is_some()
        }))
        .returning(|_| {
            Ok(SearchResponse {
                num_hits: 100,
                hits: vec![quickwit_proto::search::Hit {
                    json: r#"{"msg":"hello"}"#.to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            })
        });

    let base_query = r#"{"type":"bool","must":[{"type":"range","field":"timestamp","lower_bound":{"Included":"2026-01-01"},"upper_bound":{"Excluded":"2026-01-02"}}]}"#;
    let service_query = r#"{"type":"bool","must":[{"type":"field_presence","field":"service"},{"type":"range","field":"timestamp","lower_bound":{"Included":"2026-01-01"},"upper_bound":{"Excluded":"2026-01-02"}}]}"#;
    let status_query = r#"{"type":"bool","must":[{"type":"field_presence","field":"status"},{"type":"range","field":"timestamp","lower_bound":{"Included":"2026-01-01"},"upper_bound":{"Excluded":"2026-01-02"}}]}"#;
    let host_query = r#"{"type":"bool","must":[{"type":"field_presence","field":"host"},{"type":"range","field":"timestamp","lower_bound":{"Included":"2026-01-01"},"upper_bound":{"Excluded":"2026-01-02"}}]}"#;

    let requests = vec![
        make_search_request(base_query, 10, None), // list
        make_search_request(
            service_query,
            0,
            Some(r#"{"svc_facet": {"terms": {"field": "service"}}}"#),
        ),
        make_search_request(
            status_query,
            0,
            Some(r#"{"status_facet": {"terms": {"field": "status"}}}"#),
        ),
        make_search_request(
            host_query,
            0,
            Some(r#"{"host_facet": {"terms": {"field": "host"}}}"#),
        ),
    ];

    let results = batch_execute(&mock, requests).await;

    assert_eq!(results.len(), 4);
    assert!(results[0].is_ok());
    assert_eq!(results[0].as_ref().unwrap().hits.len(), 1);

    for result in &results[1..] {
        assert!(result.is_ok());
        assert!(result.as_ref().unwrap().hits.is_empty());
    }
}
