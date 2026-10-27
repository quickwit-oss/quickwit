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

use quickwit_config::SearcherConfig;
use quickwit_query::query_ast::{HitSet, PredicateCache};
use quickwit_storage::StorageResolver;

use super::*;
use crate::SearcherContext;

#[tokio::test]
async fn test_result_instances_use_cache_settings_and_preserve_semantics() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = SearcherConfig::default();
    config.disk_cache_directory = Some(directory.path().to_path_buf());
    config.disk_cache = Some(quickwit_config::DiskCacheConfig {
        path: None,
        disk_capacity: bytesize::ByteSize::mib(8),
        memory_capacity: bytesize::ByteSize::mib(1),
    });
    config.partial_request_cache = serde_json::from_value(serde_json::json!({
        "disk_capacity": "16MiB",
        "memory_capacity": "1MiB",
    }))
    .unwrap();
    config.predicate_cache = serde_json::from_value(serde_json::json!({
        "disk_capacity": "32MiB",
        "memory_capacity": "2MiB",
    }))
    .unwrap();
    let mut context = SearcherContext::new_without_invoker(config, None, None);
    context
        .initialize_disk_caches(&StorageResolver::unconfigured())
        .await
        .unwrap();
    assert!(context.disk_cache_opt.is_some());
    assert!(context.predicate_cache.is_enabled());
    for file_name in ["split_cache", "predicate_cache", "partial_response_cache"] {
        assert!(directory.path().join(file_name).is_file());
    }

    let split = SplitIdAndFooterOffsets {
        split_id: "split".into(),
        num_docs: 42,
        ..Default::default()
    };
    let request = SearchRequest::default();
    assert!(
        context
            .leaf_search_cache
            .get_async(split.clone(), request.clone())
            .await
            .is_none()
    );
    context
        .leaf_search_cache
        .put_async(
            split.clone(),
            request.clone(),
            LeafSearchResponse {
                num_hits: 7,
                ..Default::default()
            },
        )
        .await;
    let cached = context
        .leaf_search_cache
        .get_async(
            split,
            SearchRequest {
                priority: 10,
                ..request
            },
        )
        .await
        .unwrap();
    assert_eq!(cached.num_hits, 7);
    let stats = cached.resource_stats.unwrap();
    assert_eq!(stats.partial_result_cache_num_splits, 1);
    assert_eq!(stats.partial_result_cache_num_docs, 42);

    let predicate = context.predicate_cache;
    let segment = SegmentId::from_uuid_string("1686a000d4f7a91939d0e71df1646d7a").unwrap();
    predicate
        .put_async("split".into(), "term".into(), segment, HitSet::empty())
        .await;
    // Tantivy's synchronous trait is invoked on CPU threads even with a current-thread runtime.
    quickwit_common::thread_pool::run_cpu_intensive(move || {
        let (cached_segment, hits) = predicate.get("split".into(), "term".into()).unwrap();
        assert_eq!(cached_segment, segment);
        assert!(hits.is_empty());
        assert!(predicate.get("other-split".into(), "term".into()).is_none());
        predicate.put(
            "split".into(),
            "other-term".into(),
            segment,
            HitSet::empty(),
        );
        assert!(
            predicate
                .get("split".into(), "other-term".into())
                .unwrap()
                .1
                .is_empty()
        );
    })
    .await
    .unwrap();
}
