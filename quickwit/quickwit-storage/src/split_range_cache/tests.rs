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

use quickwit_config::SplitRangeCacheWritePolicy;

use super::*;

#[test]
fn test_flush_on_close_pairs_with_write_policy() {
    assert!(foyer_flush_on_close(
        SplitRangeCacheWritePolicy::WriteOnEviction
    ));
    assert!(!foyer_flush_on_close(
        SplitRangeCacheWritePolicy::WriteOnInsertion
    ));
}

#[tokio::test]
async fn test_split_range_cache_builder_uses_configured_policy_and_throttle() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache = FoyerSplitRangeCache::open(&config_for_test(temp_dir.path()))
        .await
        .unwrap();
    assert_eq!(
        cache.cache.policy(),
        foyer::HybridCachePolicy::WriteOnEviction
    );
    cache.close().await.unwrap();
}

#[tokio::test]
async fn test_split_range_cache_builder_write_on_insertion() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut config = config_for_test(temp_dir.path());
    config.write_policy = SplitRangeCacheWritePolicy::WriteOnInsertion;
    let cache = FoyerSplitRangeCache::open(&config).await.unwrap();
    assert_eq!(
        cache.cache.policy(),
        foyer::HybridCachePolicy::WriteOnInsertion
    );
    cache.close().await.unwrap();
}
