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

use std::time::Duration;

use bytesize::ByteSize;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};

use super::*;
use crate::FeuerSplitRangeCache;

#[tokio::test]
async fn test_memory_only_instance_does_not_initialize_io_queues() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = config(directory.path().join("unused"), 8);
    config.disk_capacity = ByteSize::b(0);
    let queues = FeuerIoQueues::default();
    let cache = FeuerCache::open(
        &config,
        &queues,
        "partial_response_cache",
        "partial_request",
    )
    .await
    .unwrap();
    cache
        .put("key".into(), OwnedBytes::new(&b"value"[..]))
        .await;
    assert_eq!(cache.get("key".into()).await.unwrap().as_slice(), b"value");
    assert!(queues.queues.get().is_none());
    assert!(!config.directory().unwrap().exists());
}

#[tokio::test]
async fn test_instances_share_queues_but_keep_contents_and_capacities_separate() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = ::metrics::set_default_local_recorder(&recorder);
    let directory = tempfile::tempdir().unwrap();
    let queues = FeuerIoQueues::default();
    let split_config = config(directory.path().to_path_buf(), 8);
    let partial_config = config(directory.path().to_path_buf(), 16);
    let predicate_config = config(directory.path().to_path_buf(), 32);
    let split = FeuerSplitRangeCache::open_with_io_queues(&split_config, &queues)
        .await
        .unwrap();
    let partial = FeuerCache::open(
        &partial_config,
        &queues,
        "partial_response_cache",
        "partial_request",
    )
    .await
    .unwrap();
    let predicate = FeuerCache::open(&predicate_config, &queues, "predicate_cache", "predicate")
        .await
        .unwrap();
    let capacities: std::collections::HashMap<_, _> = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, _, _, _)| key.key().name() == "feuer_memory_capacity_bytes")
        .map(|(key, _, _, value)| {
            let cache_name = key
                .key()
                .labels()
                .find(|label| label.key() == "cache")
                .unwrap()
                .value()
                .to_owned();
            (cache_name, value)
        })
        .collect();
    assert_eq!(capacities.len(), 3);
    for (cache_name, config) in [
        ("split_range", &split_config),
        ("partial_request", &partial_config),
        ("predicate", &predicate_config),
    ] {
        assert_eq!(
            capacities[cache_name],
            DebugValue::Gauge((config.memory_capacity.as_u64() as f64).into())
        );
    }
    assert_eq!(
        partial.cache.config().disk_capacity(),
        ByteSize::mib(16).as_u64()
    );
    assert_eq!(
        predicate.cache.config().disk_capacity(),
        ByteSize::mib(32).as_u64()
    );
    assert_eq!(
        partial.cache.config().memory_capacity(),
        partial_config.memory_capacity.as_u64()
    );
    assert_eq!(
        predicate.cache.config().memory_capacity(),
        predicate_config.memory_capacity.as_u64()
    );
    assert!(partial.get("missing".into()).await.is_none());
    assert_eq!(partial.cache.config().file_name(), "partial_response_cache");
    assert_eq!(predicate.cache.config().file_name(), "predicate_cache");
    for (config, file_name, cache_name) in [
        (&split_config, "split_cache", "split_range"),
        (&partial_config, "partial_response_cache", "partial_request"),
        (&predicate_config, "predicate_cache", "predicate"),
    ] {
        assert!(directory.path().join(file_name).is_file());
        assert!(
            directory
                .path()
                .join(format!(".feuer.{file_name}.lock"))
                .is_file()
        );
        let error = match FeuerCache::open(config, &queues, file_name, cache_name).await {
            Ok(_) => panic!("opening the same cache file twice must fail"),
            Err(error) => error,
        };
        assert!(matches!(
            error.downcast_ref::<feuer::DiskCacheError>(),
            Some(feuer::DiskCacheError::DataFile(source)) if source.kind().as_str() == "already_open"
        ));
    }

    // Large objects bypass Feuer's timed small-entry buffer, so disk publication is observable.
    let split_bytes = vec![3; 1 << 20];
    let download_bytes = split_bytes.clone();
    let bytes = split
        .get_or_fetch(
            "same-key".into(),
            0..split_bytes.len(),
            move |_| async move { Ok(feuer::Download::new(Bytes::from(download_bytes))) },
        )
        .await
        .unwrap();
    assert_eq!(bytes.as_ref(), split_bytes);
    let partial_bytes = vec![1; 1 << 20];
    let predicate_bytes = vec![2; 1 << 20];
    partial
        .put("same-key".into(), OwnedBytes::new(partial_bytes.clone()))
        .await;
    predicate
        .put("same-key".into(), OwnedBytes::new(predicate_bytes.clone()))
        .await;
    assert_eq!(
        partial.get("same-key".into()).await.unwrap().as_slice(),
        partial_bytes
    );
    assert_eq!(
        predicate.get("same-key".into()).await.unwrap().as_slice(),
        predicate_bytes
    );

    tokio::time::timeout(Duration::from_secs(10), async {
        let mut num_published = 0;
        loop {
            // Snapshots drain counters. Accumulate publications from the independent writers.
            for (key, _, _, value) in snapshotter.snapshot().into_vec() {
                if key.key().name() == "feuer_disk_write_entries_total"
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "outcome" && label.value() == "published")
                    && let DebugValue::Counter(count) = value
                {
                    num_published += count;
                }
            }
            if num_published == 3 {
                break;
            }
            assert!(num_published < 3, "unexpected extra disk publications");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("disk writes did not finish");

    drop(partial);
    drop(predicate);
    let partial = reopen(
        &partial_config,
        &queues,
        "partial_response_cache",
        "partial_request",
    )
    .await;
    let predicate = reopen(&predicate_config, &queues, "predicate_cache", "predicate").await;
    // Reopening result-cache files must not disturb the still-open split cache.
    let bytes = split
        .get_or_fetch("same-key".into(), 0..split_bytes.len(), |_| async {
            Err(crate::StorageErrorKind::Internal
                .with_error(anyhow::anyhow!("cache hit must not fetch")))
        })
        .await
        .unwrap();
    assert_eq!(bytes.as_ref(), split_bytes);
    // Memory is now empty: these lookups prove complete-object recovery from separate files.
    assert_eq!(
        partial.get("same-key".into()).await.unwrap().as_slice(),
        partial_bytes
    );
    assert_eq!(
        predicate.get("same-key".into()).await.unwrap().as_slice(),
        predicate_bytes
    );
    drop(split);
}

fn config(path: std::path::PathBuf, disk_mib: u64) -> DiskCacheConfig {
    DiskCacheConfig {
        path: Some(path),
        disk_capacity: ByteSize::mib(disk_mib),
        memory_capacity: ByteSize::mib(disk_mib / 8),
    }
}

async fn reopen(
    config: &DiskCacheConfig,
    queues: &FeuerIoQueues,
    file_name: &'static str,
    cache_name: &'static str,
) -> FeuerCache {
    // Feuer releases directory locks after its background tasks drain. Retry only this
    // expected shutdown race; all other failures must fail the test immediately.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match FeuerCache::open(config, queues, file_name, cache_name).await {
                Ok(cache) => return cache,
                Err(error) => match error.downcast_ref::<feuer::DiskCacheError>() {
                    Some(feuer::DiskCacheError::DataFile(source))
                        if source.kind().as_str() == "already_open" =>
                    {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    _ => panic!("failed to reopen cache: {error:?}"),
                },
            }
        }
    })
    .await
    .expect("cache directory lock was not released")
}
