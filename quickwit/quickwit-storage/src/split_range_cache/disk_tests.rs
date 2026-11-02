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

use bytes::Bytes;
use bytesize::ByteSize;
use feuer::Download;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};

use super::{FeuerSplitRangeCache, config_for_test};
use crate::StorageErrorKind;

#[tokio::test]
async fn test_disk_hit_after_memory_pressure() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = ::metrics::set_default_local_recorder(&recorder);
    let temp_dir = tempfile::tempdir().unwrap();
    let mut config = config_for_test(temp_dir.path());
    config.memory_capacity = ByteSize::kib(64);
    let cache = FeuerSplitRangeCache::open(&config).await.unwrap();
    // Bypass Feuer's 60-second small-entry buffer so this test observes a disk write,
    // rather than a hit from that buffer. Keep "data" at the start to identify the payload.
    cache
        .get_or_fetch("object".to_owned(), 0..512 * 1024, |_| async {
            let mut bytes = vec![0; 512 * 1024];
            bytes[..4].copy_from_slice(b"data");
            Ok(Download::new(Bytes::from(bytes)))
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let published = snapshotter
                .snapshot()
                .into_vec()
                .iter()
                .any(|(key, _, _, value)| {
                    key.key().name() == "feuer_disk_write_entries_total"
                        && key
                            .key()
                            .labels()
                            .any(|label| label.key() == "outcome" && label.value() == "published")
                        && *value == DebugValue::Counter(1)
                });
            if published {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("disk write did not finish");

    // Exact slices have independent keys and may select different memory shards.
    // Apply pressure with distinct oversized entries until the original slice is
    // evicted, without depending on Feuer's process-local shard assignment.
    tokio::time::timeout(Duration::from_secs(10), async {
        for index in 0..1024 {
            cache
                .get_or_fetch(format!("pressure-{index}"), 0..1024 * 1024, |_| async {
                    Ok(Download::new(Bytes::from(vec![0; 1024 * 1024])))
                })
                .await
                .unwrap();
            let bytes = cache
                .get_or_fetch("object".to_owned(), 0..512 * 1024, |_| async {
                    Err(StorageErrorKind::Internal
                        .with_error(anyhow::anyhow!("disk hit must not fetch")))
                })
                .await
                .unwrap();
            assert_eq!(bytes.len(), 512 * 1024);
            assert_eq!(&bytes[..4], b"data");
            let disk_hit = snapshotter
                .snapshot()
                .into_vec()
                .iter()
                .any(|(key, _, _, value)| {
                    key.key().name() == "feuer_lookup_total"
                        && key
                            .key()
                            .labels()
                            .any(|label| label.key() == "outcome" && label.value() == "disk_hit")
                        && *value == DebugValue::Counter(1)
                });
            if disk_hit {
                return;
            }
        }
        panic!("memory pressure did not evict the original exact slice");
    })
    .await
    .expect("disk hit did not follow memory pressure");
}
