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

//! Measures `MemorySizedCache` allocation overhead independently from a Quickwit server.
//!
//! Run each policy in a fresh process so allocator state from one policy does not affect another.
//! The workload is the protobuf representation stored by `LeafSearchCache` for a zero-hit split
//! response.

use std::env;
use std::hint::black_box;
use std::mem::size_of;
use std::sync::LazyLock;

use prost::Message;
use quickwit_config::{CacheConfig, CachePolicy};
use quickwit_proto::search::{LeafResourceStats, LeafSearchResponse};
use quickwit_storage::metrics::CacheMetrics;
use quickwit_storage::{MemorySizedCache, OwnedBytes};
use tikv_jemalloc_ctl::{epoch, stats};
use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

static CACHE_METRICS: LazyLock<CacheMetrics> =
    LazyLock::new(|| CacheMetrics::for_component("memory_benchmark"));

const BASELINE_CHUNK_NUM_ENTRIES: usize = 4_096;
const NUM_ENTRIES: usize = 1_560_000;
const SPLIT_NUM_DOCS: u64 = 1_000_000;

#[derive(Clone, Copy, Hash, Eq, PartialEq)]
struct CacheKeyHash(u128);

#[derive(Clone, Copy)]
struct AllocatorStats {
    allocated: usize,
    active: usize,
    resident: usize,
}

impl AllocatorStats {
    fn read() -> Self {
        epoch::advance().expect("failed to refresh jemalloc statistics");
        Self {
            allocated: stats::allocated::read().expect("failed to read stats.allocated"),
            active: stats::active::read().expect("failed to read stats.active"),
            resident: stats::resident::read().expect("failed to read stats.resident"),
        }
    }

    fn delta_from(self, before: Self) -> AllocatorStatsDelta {
        AllocatorStatsDelta {
            allocated: self.allocated as i128 - before.allocated as i128,
            active: self.active as i128 - before.active as i128,
            resident: self.resident as i128 - before.resident as i128,
        }
    }
}

struct AllocatorStatsDelta {
    allocated: i128,
    active: i128,
    resident: i128,
}

#[derive(Clone, Copy)]
enum Policy {
    KeyValueBaseline,
    Lru,
    S3Fifo,
    TinyLfu,
}

impl Policy {
    fn parse(value: &str) -> Self {
        match value {
            "key-value-baseline" => Self::KeyValueBaseline,
            "lru" => Self::Lru,
            "s3-fifo" => Self::S3Fifo,
            "tiny-lfu" => Self::TinyLfu,
            _ => panic!("unexpected child policy `{value}`"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::KeyValueBaseline => "key-value-baseline",
            Self::Lru => "lru",
            Self::S3Fifo => "s3-fifo",
            Self::TinyLfu => "tiny-lfu",
        }
    }

    fn cache_policy(self) -> Option<CachePolicy> {
        match self {
            Self::KeyValueBaseline => None,
            Self::Lru => Some(CachePolicy::Lru),
            Self::S3Fifo => Some(CachePolicy::S3Fifo),
            Self::TinyLfu => Some(CachePolicy::TinyLfu),
        }
    }
}

enum Entries {
    KeyValueBaseline(Vec<Vec<(CacheKeyHash, OwnedBytes)>>),
    Cache(MemorySizedCache<CacheKeyHash>),
}

impl Entries {
    fn retained_count(&self) -> usize {
        match self {
            Entries::KeyValueBaseline(chunks) => chunks.iter().map(Vec::len).sum(),
            Entries::Cache(cache) => {
                black_box(cache);
                CACHE_METRICS.in_cache_count() as usize
            }
        }
    }
}

fn make_zero_hit_value() -> OwnedBytes {
    let response = LeafSearchResponse {
        num_hits: 0,
        partial_hits: Vec::new(),
        failed_splits: Vec::new(),
        num_attempted_splits: 1,
        num_successful_splits: 1,
        intermediate_aggregation_result: None,
        // `LeafSearchCache::put` replaces execution stats with these cache-hit stats before
        // encoding the response.
        resource_stats: Some(LeafResourceStats {
            partial_result_cache_num_splits: 1,
            partial_result_cache_num_docs: SPLIT_NUM_DOCS,
            ..Default::default()
        }),
    };
    OwnedBytes::new(response.encode_to_vec())
}

fn run_policy(policy: Policy) {
    let value_num_bytes = make_zero_hit_value().len();
    let value_only_num_bytes = NUM_ENTRIES
        .checked_mul(value_num_bytes)
        .expect("value-only byte count overflowed usize");
    let entry_num_bytes = size_of::<CacheKeyHash>()
        .checked_add(value_num_bytes)
        .expect("entry byte count overflowed usize");
    let value_and_key_num_bytes = NUM_ENTRIES
        .checked_mul(entry_num_bytes)
        .expect("key-plus-value byte count overflowed usize");

    // Warm the jemalloc controls and cache metric handles before the allocation baseline.
    let _ = AllocatorStats::read();
    let _ = &*CACHE_METRICS;
    let before = AllocatorStats::read();

    let entries = match policy.cache_policy() {
        None => {
            let num_chunks = NUM_ENTRIES.div_ceil(BASELINE_CHUNK_NUM_ENTRIES);
            let mut chunks = Vec::with_capacity(num_chunks);
            for entry_ord in 0..NUM_ENTRIES {
                if entry_ord % BASELINE_CHUNK_NUM_ENTRIES == 0 {
                    let remaining_num_entries = NUM_ENTRIES - entry_ord;
                    chunks.push(Vec::with_capacity(
                        remaining_num_entries.min(BASELINE_CHUNK_NUM_ENTRIES),
                    ));
                }
                chunks
                    .last_mut()
                    .unwrap()
                    .push((CacheKeyHash(entry_ord as u128), make_zero_hit_value()));
            }
            Entries::KeyValueBaseline(chunks)
        }
        Some(cache_policy) => {
            let capacity =
                u64::try_from(value_and_key_num_bytes).expect("cache capacity does not fit in u64");
            let config: CacheConfig = serde_json::from_value(serde_json::json!({
                "capacity": capacity,
                "policy": cache_policy,
            }))
            .expect("failed to build cache configuration");
            let cache = MemorySizedCache::from_config(&config, &CACHE_METRICS);
            for entry_ord in 0..NUM_ENTRIES {
                cache.put(CacheKeyHash(entry_ord as u128), make_zero_hit_value());
            }
            cache.settle_for_memory_benchmark();
            Entries::Cache(cache)
        }
    };
    black_box(&entries);
    let after = AllocatorStats::read();
    let delta = after.delta_from(before);

    let retained_count = entries.retained_count();
    assert_eq!(
        retained_count, NUM_ENTRIES,
        "policy rejected or evicted entries during a no-eviction measurement"
    );
    let actual_metric_num_bytes = match &entries {
        Entries::KeyValueBaseline(_) => None,
        Entries::Cache(_) => Some(CACHE_METRICS.in_cache_num_bytes() as usize),
    };
    if let Some(metric_num_bytes) = actual_metric_num_bytes {
        assert!(
            metric_num_bytes == value_only_num_bytes || metric_num_bytes == value_and_key_num_bytes,
            "cache metric does not match either value-only or key-plus-value accounting"
        );
    }
    let (actual_metric_num_bytes, actual_metric_bytes_per_entry) = actual_metric_num_bytes
        .map(|num_bytes| {
            (
                num_bytes.to_string(),
                format!("{:.2}", num_bytes as f64 / retained_count as f64),
            )
        })
        .unwrap_or_default();

    println!(
        "{},{NUM_ENTRIES},{retained_count},{value_num_bytes},{actual_metric_num_bytes},\
         {value_only_num_bytes},{value_and_key_num_bytes},{entry_num_bytes},{},{},{},\
         {actual_metric_bytes_per_entry},{:.2}",
        policy.as_str(),
        delta.allocated,
        delta.active,
        delta.resident,
        delta.allocated as f64 / retained_count as f64,
    );
}

fn main() {
    let mut args = env::args().skip(1);
    let policy_flag = args.next();
    let policy_name = args.next();
    assert!(
        policy_flag.as_deref() == Some("--policy")
            && policy_name.is_some()
            && args.next().is_none(),
        "usage: cache-memory --policy <key-value-baseline|lru|s3-fifo|tiny-lfu>"
    );

    println!(
        "policy,requested_entries,retained_entries,value_bytes,actual_metric_bytes,\
         value_only_bytes,value_plus_key_bytes,accounted_bytes_per_entry,allocated_delta,\
         active_delta,resident_delta,actual_metric_bytes_per_entry,allocated_bytes_per_entry"
    );
    run_policy(Policy::parse(policy_name.as_deref().unwrap()));
}
