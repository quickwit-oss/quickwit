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

use std::sync::Arc;

use quickwit_config::SearcherConfig;
use quickwit_indexing::TestSandbox;
use quickwit_proto::search::{
    CountHits, LeafSearchResponse, SearchRequest, SortField, SortOrder, SortValue,
    SplitIdAndFooterOffsets,
};
use quickwit_query::query_ast::qast_json_helper;
use serde_json::json;

use crate::leaf::{rewrite_request, single_doc_mapping_leaf_search};
use crate::service::SearcherContext;
use crate::{extract_split_and_footer_offsets, list_all_splits};

const BASE_TIMESTAMP: i64 = 1_700_000_000;

async fn create_sandbox(index_id: &str) -> TestSandbox {
    TestSandbox::create(
        index_id,
        r#"
        field_mappings:
          - name: body
            type: text
          - name: ts
            type: datetime
            fast: true
        timestamp_field: ts
        "#,
        "{}",
        &["body"],
    )
    .await
    .unwrap()
}

// List splits newest first, setting maturity explicitly rather than depending on wall time.
async fn list_splits(sandbox: &TestSandbox) -> Vec<SplitIdAndFooterOffsets> {
    let metadata = list_all_splits(vec![sandbox.index_uid()], &sandbox.metastore())
        .await
        .unwrap();
    let mut splits: Vec<_> = metadata
        .iter()
        .map(extract_split_and_footer_offsets)
        .collect();
    for split in &mut splits {
        split.is_mature = split.timestamp_end() < BASE_TIMESTAMP + 400;
    }
    splits.sort_by_key(|split| std::cmp::Reverse(split.timestamp_end()));
    splits
}

fn searcher_context() -> Arc<SearcherContext> {
    let mut config = SearcherConfig::default();
    config.max_num_concurrent_split_searches = 100;
    Arc::new(SearcherContext::new_without_invoker(config, None))
}

fn search_request(max_hits: u64) -> SearchRequest {
    SearchRequest {
        query_ast: qast_json_helper("hello", &["body"]),
        start_timestamp: Some(BASE_TIMESTAMP - 10),
        end_timestamp: Some(BASE_TIMESTAMP + 500),
        max_hits,
        count_hits: CountHits::CountAll.into(),
        sort_fields: vec![SortField {
            field_name: "ts".to_string(),
            sort_order: SortOrder::Desc.into(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

async fn search(
    sandbox: &TestSandbox,
    context: &Arc<SearcherContext>,
    request: &SearchRequest,
    splits: Vec<SplitIdAndFooterOffsets>,
) -> LeafSearchResponse {
    let response = single_doc_mapping_leaf_search(
        context.clone(),
        Arc::new(request.clone()),
        sandbox.storage(),
        splits,
        sandbox.doc_mapper(),
    )
    .await
    .unwrap();
    assert!(response.failed_splits.is_empty(), "{response:?}");
    response
}

fn cached_response(
    context: &SearcherContext,
    split: &SplitIdAndFooterOffsets,
    request: &SearchRequest,
    count_only: bool,
) -> LeafSearchResponse {
    let mut request = request.clone();
    if count_only {
        request.max_hits = 0;
        request.start_offset = 0;
        request.sort_fields.clear();
    }
    rewrite_request(&mut request, split, Some("ts"));
    context
        .leaf_search_cache
        .get(split.clone(), request)
        .expect("the expected full-hit or count-only request should be cached")
}

#[tokio::test]
async fn test_mature_cache_survives_replaced_newest_split_and_sliding_window() {
    let sandbox = create_sandbox("mature-cache-replacement").await;
    for offset in [100, 200, 300, 400] {
        sandbox
            .add_documents(vec![
                json!({"body": "hello", "ts": BASE_TIMESTAMP + offset}),
            ])
            .await
            .unwrap();
    }
    let splits = list_splits(&sandbox).await;
    assert_eq!(splits.len(), 4);
    let context = searcher_context();
    let request = search_request(1);
    // Seed a newer hit so pruning is active regardless of task completion order.
    search(&sandbox, &context, &request, vec![splits[0].clone()]).await;
    let response = search(&sandbox, &context, &request, splits.clone()).await;
    assert_eq!(response.num_hits, 4);
    assert_eq!(response.partial_hits.len(), 1);
    assert_eq!(response.partial_hits[0].split_id, splits[0].split_id);

    let mature_response = cached_response(&context, &splits[1], &request, false);
    assert_eq!(mature_response.num_hits, 1);
    assert_eq!(mature_response.partial_hits.len(), 1);
    for split in &splits[2..] {
        let response = cached_response(&context, split, &request, true);
        assert_eq!(response.num_hits, 1);
        assert!(response.partial_hits.is_empty());
    }

    // Publish a replacement containing the old newest document plus a newer one.
    sandbox
        .add_documents(vec![
            json!({"body": "hello", "ts": BASE_TIMESTAMP + 400}),
            json!({"body": "hello", "ts": BASE_TIMESTAMP + 450}),
        ])
        .await
        .unwrap();
    let mut refreshed_splits = list_splits(&sandbox).await;
    refreshed_splits.retain(|split| split.split_id != splits[0].split_id);
    assert_eq!(refreshed_splits.len(), 4);
    let replacement_id = refreshed_splits[0].split_id.clone();
    assert_ne!(replacement_id, splits[0].split_id);
    let refreshed_request = SearchRequest {
        start_timestamp: Some(BASE_TIMESTAMP - 5),
        end_timestamp: Some(BASE_TIMESTAMP + 550),
        ..request
    };
    let response = search(&sandbox, &context, &refreshed_request, refreshed_splits).await;
    assert_eq!(response.num_hits, 5);
    assert_eq!(response.partial_hits.len(), 1);
    assert_eq!(response.partial_hits[0].split_id, replacement_id);
    assert_eq!(
        response.partial_hits[0].sort_value(),
        Some(SortValue::I64((BASE_TIMESTAMP + 450) * 1_000_000_000))
    );
    let stats = response.resource_stats.unwrap();
    assert_eq!(stats.localexec_num_splits, 1);
    assert_eq!(stats.partial_result_cache_num_splits, 3);
    sandbox.assert_quit().await;
}

#[tokio::test]
async fn test_mature_cache_preserves_hits_after_upfront_match_all_pruning() {
    let sandbox = create_sandbox("mature-cache-match-all").await;
    for offset in [100, 200, 400] {
        sandbox
            .add_documents(vec![
                json!({"body": "hello", "ts": BASE_TIMESTAMP + offset}),
            ])
            .await
            .unwrap();
    }
    let splits = list_splits(&sandbox).await;
    let context = searcher_context();
    let request = SearchRequest {
        query_ast: qast_json_helper("*", &[]),
        start_timestamp: None,
        end_timestamp: None,
        ..search_request(1)
    };
    let response = search(&sandbox, &context, &request, splits.clone()).await;
    assert_eq!(response.num_hits, 3);
    assert_eq!(response.partial_hits[0].split_id, splits[0].split_id);
    assert_eq!(
        cached_response(&context, &splits[1], &request, false)
            .partial_hits
            .len(),
        1
    );
    // The older tail is counted from metadata without opening its index or caching a response.
    assert_eq!(response.resource_stats.unwrap().localexec_num_splits, 2);
    sandbox.assert_quit().await;
}

#[tokio::test]
async fn test_mature_cache_protects_only_one_split_even_with_too_few_hits() {
    for body in ["absent", "hello"] {
        let sandbox = create_sandbox("mature-cache-sparse").await;
        for offset in [100, 200, 300] {
            let body = if offset == 300 { body } else { "hello" };
            sandbox
                .add_documents(vec![json!({"body": body, "ts": BASE_TIMESTAMP + offset})])
                .await
                .unwrap();
        }
        sandbox
            .add_documents(
                (400..402).map(|offset| json!({"body": "hello", "ts": BASE_TIMESTAMP + offset})),
            )
            .await
            .unwrap();
        let splits = list_splits(&sandbox).await;
        let context = searcher_context();
        let request = search_request(2);
        search(&sandbox, &context, &request, vec![splits[0].clone()]).await;
        let response = search(&sandbox, &context, &request, splits.clone()).await;
        let mature_hits = u64::from(body == "hello");
        assert_eq!(response.num_hits, 4 + mature_hits);
        assert_eq!(response.partial_hits.len(), 2);
        assert_eq!(
            cached_response(&context, &splits[1], &request, false).num_hits,
            mature_hits
        );
        for split in &splits[2..] {
            assert!(
                cached_response(&context, split, &request, true)
                    .partial_hits
                    .is_empty()
            );
        }
        // The selected split cannot fill top-2 by itself. Older splits must still be
        // searched for hits when the transient newest split disappears.
        let response = search(&sandbox, &context, &request, splits[1..].to_vec()).await;
        assert_eq!(response.num_hits, 2 + mature_hits);
        let expected_ids: Vec<_> = splits[if mature_hits == 0 { 2..4 } else { 1..3 }]
            .iter()
            .map(|split| split.split_id.as_str())
            .collect();
        let hit_ids: Vec<_> = response
            .partial_hits
            .iter()
            .map(|hit| hit.split_id.as_str())
            .collect();
        assert_eq!(hit_ids, expected_ids);
        assert!(response.resource_stats.unwrap().localexec_num_splits > 0);
        sandbox.assert_quit().await;
    }
}

#[tokio::test]
async fn test_mature_cache_skips_split_crossing_time_boundary() {
    let sandbox = create_sandbox("mature-cache-boundary").await;
    for offset in [100, 200, 400] {
        sandbox
            .add_documents(vec![
                json!({"body": "hello", "ts": BASE_TIMESTAMP + offset}),
            ])
            .await
            .unwrap();
    }
    sandbox
        .add_documents(vec![
            json!({"body": "hello", "ts": BASE_TIMESTAMP + 300}),
            json!({"body": "hello", "ts": BASE_TIMESTAMP + 500}),
        ])
        .await
        .unwrap();
    let mut splits = list_splits(&sandbox).await;
    assert_eq!(splits.len(), 4);
    splits[0].is_mature = true;
    let context = searcher_context();
    let request = search_request(1);
    search(&sandbox, &context, &request, vec![splits[1].clone()]).await;
    let response = search(&sandbox, &context, &request, splits.clone()).await;
    assert_eq!(response.num_hits, 4);
    assert_eq!(response.partial_hits[0].split_id, splits[1].split_id);
    // The newest-ending mature split crosses the exclusive upper time bound.
    // Protect the next mature split, which is entirely inside the window.
    assert_eq!(
        cached_response(&context, &splits[2], &request, false)
            .partial_hits
            .len(),
        1
    );
    assert!(
        cached_response(&context, &splits[3], &request, true)
            .partial_hits
            .is_empty()
    );
    sandbox.assert_quit().await;
}

#[tokio::test]
async fn test_mature_cache_disabled_preserves_upfront_pruning() {
    let sandbox = create_sandbox("mature-cache-disabled").await;
    sandbox
        .add_documents(vec![json!({"body": "hello", "ts": BASE_TIMESTAMP + 400})])
        .await
        .unwrap();
    let mut splits = list_splits(&sandbox).await;
    let mut older_split = splits[0].clone();
    older_split.split_id = "not-in-storage".to_string();
    older_split.timestamp_start = Some(BASE_TIMESTAMP + 100);
    older_split.timestamp_end = Some(BASE_TIMESTAMP + 100);
    older_split.is_mature = true;
    splits.push(older_split);

    let mut config = SearcherConfig::default();
    config.partial_request_cache = quickwit_config::CacheConfig::no_cache();
    let context = Arc::new(SearcherContext::new_without_invoker(config, None));
    let request = SearchRequest {
        query_ast: qast_json_helper("*", &[]),
        start_timestamp: None,
        end_timestamp: None,
        count_hits: CountHits::Underestimate.into(),
        ..search_request(1)
    };
    // With caching disabled, the older split must stay pruned rather than be opened.
    let response = search(&sandbox, &context, &request, splits).await;
    assert_eq!(response.num_hits, 1);
    assert_eq!(response.partial_hits.len(), 1);
    sandbox.assert_quit().await;
}

#[tokio::test]
async fn test_mature_cache_count_only_with_and_without_mature_splits() {
    let sandbox = create_sandbox("mature-cache-count-only").await;
    for offset in [100, 400] {
        sandbox
            .add_documents(vec![
                json!({"body": "hello", "ts": BASE_TIMESTAMP + offset}),
            ])
            .await
            .unwrap();
    }
    let mut splits = list_splits(&sandbox).await;
    for is_mature in [true, false] {
        for split in &mut splits {
            split.is_mature = is_mature;
        }
        let context = searcher_context();
        let request = search_request(0);
        let response = search(&sandbox, &context, &request, splits.clone()).await;
        assert_eq!(response.num_hits, 2);
        assert!(response.partial_hits.is_empty());
        let refreshed_request = SearchRequest {
            start_timestamp: Some(BASE_TIMESTAMP - 5),
            end_timestamp: Some(BASE_TIMESTAMP + 550),
            ..request
        };
        let response = search(&sandbox, &context, &refreshed_request, splits.clone()).await;
        assert_eq!(response.num_hits, 2);
        assert!(response.partial_hits.is_empty());
        let stats = response.resource_stats.unwrap();
        assert_eq!(stats.localexec_num_splits, 0);
        assert_eq!(stats.partial_result_cache_num_splits, 2);
    }
    sandbox.assert_quit().await;
}
