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

//! Per-payload aggregation: collapses per-connection `UsmStat` records
//! into two bucket maps — one fine-grained for `universal.*` emission,
//! one coarse for `trace.services_by_operation.*`.
//!
//! The aggregation sorts incoming stats by their fine-grained bucket key
//! before merging so that sketch `sum()` is bit-identical across runs on
//! the same input (Rust's `HashMap` iteration order is randomised).

use std::collections::HashMap;

use tracing::warn;
use vector_lib::metrics::AgentDDSketch;

use super::sketch;
use super::types::{
    BucketKey, ServiceIndexKey, StatusClass, UsmStat, full_operation, status_class_from_code,
};

#[derive(Debug, Default)]
pub(super) struct Bucket {
    pub(super) hits: u64,
    pub(super) errors: u64,
    pub(super) sketch: Option<AgentDDSketch>,
}

#[derive(Debug, Default)]
pub(super) struct Buckets {
    pub(super) fine_grained: HashMap<BucketKey, Bucket>,
    pub(super) service_index: HashMap<ServiceIndexKey, Bucket>,
}

/// Sort key mirroring the fine-grained `BucketKey` exactly so that two
/// stats landing in the same bucket arrive in a deterministic order.
fn sort_key(stat: &UsmStat) -> (String, String, String, String, u8, u8) {
    let env = stat.env.clone().unwrap_or_default();
    let op = full_operation(stat.operation, stat.direction);
    let status_class = status_class_from_code(stat.status)
        .map(|c| c as u8)
        .unwrap_or(255);
    let is_err = u8::from(stat.errors > 0);
    (
        stat.service.clone(),
        env,
        op,
        stat.resource.clone(),
        status_class,
        is_err,
    )
}

pub(super) fn aggregate(stats: &[UsmStat]) -> Buckets {
    let mut sorted: Vec<&UsmStat> = stats.iter().collect();
    sorted.sort_by_key(|s| sort_key(s));

    let mut fine_grained: HashMap<BucketKey, Bucket> = HashMap::new();
    let mut service_index: HashMap<ServiceIndexKey, Bucket> = HashMap::new();

    for stat in sorted {
        let sketch = build_sketch(stat);
        let status_class = status_class_from_code(stat.status);
        let operation = full_operation(stat.operation, stat.direction);
        let fine_key = BucketKey {
            service: stat.service.clone(),
            env: stat.env.clone(),
            operation: operation.clone(),
            resource: stat.resource.clone(),
            status_class,
            is_error: stat.errors > 0,
        };
        let index_key = ServiceIndexKey {
            service: stat.service.clone(),
            env: stat.env.clone(),
            operation,
        };

        merge_into(&mut fine_grained, fine_key, stat, sketch.as_ref());
        merge_into(&mut service_index, index_key, stat, sketch.as_ref());
    }

    // Suppress the "unused" warning on StatusClass exports in test-only builds.
    let _ = StatusClass::TwoXx;
    Buckets {
        fine_grained,
        service_index,
    }
}

fn build_sketch(stat: &UsmStat) -> Option<AgentDDSketch> {
    if let Some(bytes) = stat.latencies.as_ref() {
        return sketch::decode_proto(bytes);
    }
    if stat.hits == 1
        && let Some(v) = stat.first_latency_sample
    {
        return sketch::from_single_sample(v);
    }
    None
}

fn merge_into<K: std::hash::Hash + Eq>(
    map: &mut HashMap<K, Bucket>,
    key: K,
    stat: &UsmStat,
    incoming: Option<&AgentDDSketch>,
) {
    let entry = map.entry(key).or_default();
    entry.hits = entry.hits.saturating_add(u64::from(stat.hits));
    entry.errors = entry.errors.saturating_add(u64::from(stat.errors));
    match (entry.sketch.as_mut(), incoming) {
        (None, Some(incoming)) => entry.sketch = Some(incoming.clone()),
        (Some(existing), Some(incoming)) => {
            if let Err(err) = existing.merge(incoming) {
                warn!(%err, "failed to merge ddsketch; keeping existing bucket sketch");
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::super::types::{Direction, Operation, UsmStat};
    use super::*;

    #[allow(clippy::too_many_arguments)]
    fn stat(
        service: &str,
        op: Operation,
        dir: Direction,
        resource: &str,
        status: i32,
        hits: u32,
        errors: u32,
        latencies: Option<Bytes>,
    ) -> UsmStat {
        UsmStat {
            service: service.into(),
            env: Some("prod".into()),
            direction: dir,
            operation: op,
            resource: resource.into(),
            status,
            hits,
            errors,
            latencies,
            first_latency_sample: None,
        }
    }

    #[test]
    fn single_stat_one_bucket_each() {
        let s = stat(
            "web",
            Operation::Http,
            Direction::Server,
            "GET /x",
            200,
            1,
            0,
            None,
        );
        let buckets = aggregate(&[s]);
        assert_eq!(buckets.fine_grained.len(), 1);
        assert_eq!(buckets.service_index.len(), 1);
    }

    #[test]
    fn same_fine_key_merges() {
        let a = stat(
            "web",
            Operation::Http,
            Direction::Server,
            "GET /x",
            200,
            2,
            0,
            None,
        );
        let b = stat(
            "web",
            Operation::Http,
            Direction::Server,
            "GET /x",
            201,
            3,
            0,
            None,
        );
        let buckets = aggregate(&[a, b]);
        assert_eq!(buckets.fine_grained.len(), 1);
        let bucket = buckets.fine_grained.values().next().unwrap();
        assert_eq!(bucket.hits, 5);
    }

    #[test]
    fn different_resource_same_service_index() {
        let a = stat(
            "web",
            Operation::Http,
            Direction::Server,
            "GET /x",
            200,
            1,
            0,
            None,
        );
        let b = stat(
            "web",
            Operation::Http,
            Direction::Server,
            "GET /y",
            200,
            1,
            0,
            None,
        );
        let buckets = aggregate(&[a, b]);
        assert_eq!(buckets.fine_grained.len(), 2);
        assert_eq!(buckets.service_index.len(), 1);
        let idx = buckets.service_index.values().next().unwrap();
        assert_eq!(idx.hits, 2);
    }

    #[test]
    fn errors_separate_from_successes_in_fine_bucket() {
        let a = stat(
            "web",
            Operation::Http,
            Direction::Server,
            "GET /x",
            500,
            1,
            1,
            None,
        );
        let b = stat(
            "web",
            Operation::Http,
            Direction::Server,
            "GET /x",
            200,
            1,
            0,
            None,
        );
        let buckets = aggregate(&[a, b]);
        assert_eq!(buckets.fine_grained.len(), 2);
        assert_eq!(buckets.service_index.len(), 1);
    }
}
