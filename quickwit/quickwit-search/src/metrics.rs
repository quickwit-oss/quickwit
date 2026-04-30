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

// See https://prometheus.io/docs/practices/naming/

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use bytesize::ByteSize;
use quickwit_common::metrics::{
    Counter, Gauge, Histogram, counter, exponential_buckets, gauge, histogram, linear_buckets,
};

fn print_if_not_null(
    field_name: &'static str,
    counter: &SplitSearchOutcomeCounter,
    f: &mut fmt::Formatter,
) -> fmt::Result {
    let val = counter.get();
    if val > 0 {
        write!(f, "{}={} ", field_name, val)?;
    }
    Ok(())
}

#[derive(Clone)]
pub struct SplitSearchOutcomeCounter {
    inner: SplitSearchOutcomeCounterInner,
}

#[derive(Clone)]
enum SplitSearchOutcomeCounterInner {
    Registered(Counter),
    Local(Arc<AtomicU64>),
}

impl SplitSearchOutcomeCounter {
    fn registered(counter: Counter) -> Self {
        Self {
            inner: SplitSearchOutcomeCounterInner::Registered(counter),
        }
    }

    fn local() -> Self {
        Self {
            inner: SplitSearchOutcomeCounterInner::Local(Arc::new(AtomicU64::new(0))),
        }
    }

    pub fn increment(&self, value: u64) {
        match &self.inner {
            SplitSearchOutcomeCounterInner::Registered(counter) => counter.increment(value),
            SplitSearchOutcomeCounterInner::Local(value_ref) => {
                value_ref.fetch_add(value, Ordering::Relaxed);
            }
        }
    }

    pub fn get(&self) -> u64 {
        match &self.inner {
            SplitSearchOutcomeCounterInner::Registered(counter) => counter.get(),
            SplitSearchOutcomeCounterInner::Local(value_ref) => value_ref.load(Ordering::Relaxed),
        }
    }
}

pub struct SplitSearchOutcomeCounters {
    pub cancel_before_warmup: SplitSearchOutcomeCounter,
    pub cache_hit: SplitSearchOutcomeCounter,
    pub pruned_before_warmup: SplitSearchOutcomeCounter,
    pub cancel_warmup: SplitSearchOutcomeCounter,
    pub pruned_after_warmup: SplitSearchOutcomeCounter,
    pub cancel_cpu_queue: SplitSearchOutcomeCounter,
    pub cancel_cpu: SplitSearchOutcomeCounter,
    pub success: SplitSearchOutcomeCounter,
}

impl fmt::Display for SplitSearchOutcomeCounters {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        print_if_not_null("cancel_before_warmup", &self.cancel_before_warmup, f)?;
        print_if_not_null("cache_hit", &self.cache_hit, f)?;
        print_if_not_null("pruned_before_warmup", &self.pruned_before_warmup, f)?;
        print_if_not_null("cancel_warmup", &self.cancel_warmup, f)?;
        print_if_not_null("pruned_after_warmup", &self.pruned_after_warmup, f)?;
        print_if_not_null("cancel_cpu_queue", &self.cancel_cpu_queue, f)?;
        print_if_not_null("cancel_cpu", &self.cancel_cpu, f)?;
        print_if_not_null("success", &self.success, f)?;
        Ok(())
    }
}

impl SplitSearchOutcomeCounters {
    /// Create a new SplitSearchOutcomeCounters instance, registered in prometheus.
    pub fn new_registered() -> Self {
        Self::new_registered_from_counter(&*SPLIT_SEARCH_OUTCOME)
    }

    /// Create a new SplitSearchOutcomeCounters instance that is not reported.
    pub fn new_unregistered() -> Self {
        SplitSearchOutcomeCounters {
            cancel_before_warmup: SplitSearchOutcomeCounter::local(),
            cache_hit: SplitSearchOutcomeCounter::local(),
            pruned_before_warmup: SplitSearchOutcomeCounter::local(),
            cancel_warmup: SplitSearchOutcomeCounter::local(),
            pruned_after_warmup: SplitSearchOutcomeCounter::local(),
            cancel_cpu_queue: SplitSearchOutcomeCounter::local(),
            cancel_cpu: SplitSearchOutcomeCounter::local(),
            success: SplitSearchOutcomeCounter::local(),
        }
    }

    fn new_registered_from_counter(search_split_outcome: &Counter) -> Self {
        SplitSearchOutcomeCounters {
            cancel_before_warmup: SplitSearchOutcomeCounter::registered(counter!(
                parent: search_split_outcome,
                "category" => "cancel_before_warmup",
            )),
            cache_hit: SplitSearchOutcomeCounter::registered(counter!(
                parent: search_split_outcome,
                "category" => "cache_hit",
            )),
            pruned_before_warmup: SplitSearchOutcomeCounter::registered(counter!(
                parent: search_split_outcome,
                "category" => "pruned_before_warmup",
            )),
            cancel_warmup: SplitSearchOutcomeCounter::registered(counter!(
                parent: search_split_outcome,
                "category" => "cancel_warmup",
            )),
            pruned_after_warmup: SplitSearchOutcomeCounter::registered(counter!(
                parent: search_split_outcome,
                "category" => "pruned_after_warmup",
            )),
            cancel_cpu_queue: SplitSearchOutcomeCounter::registered(counter!(
                parent: search_split_outcome,
                "category" => "cancel_cpu_queue",
            )),
            cancel_cpu: SplitSearchOutcomeCounter::registered(counter!(
                parent: search_split_outcome,
                "category" => "cancel_cpu",
            )),
            success: SplitSearchOutcomeCounter::registered(counter!(
                parent: search_split_outcome,
                "category" => "success",
            )),
        }
    }
}

pub struct SearchMetrics {
    pub root_search_requests_total: Counter,
    pub root_search_request_duration_seconds: Histogram,
    pub root_search_targeted_splits: Histogram,
    pub leaf_search_requests_total: Counter,
    pub leaf_search_request_duration_seconds: Histogram,
    pub leaf_search_targeted_splits: Histogram,
    pub leaf_list_terms_splits_total: Counter,
    pub split_search_outcome_total: SplitSearchOutcomeCounters,
    pub leaf_search_split_duration_secs: Histogram,
    pub job_assigned_total: Counter,
    pub leaf_search_single_split_tasks_pending: Gauge,
    pub leaf_search_single_split_tasks_ongoing: Gauge,
    pub leaf_search_single_split_warmup_num_bytes: Histogram,
    pub searcher_local_kv_store_size_bytes: Gauge,
}

/// From 0.008s to 131.072s
fn duration_buckets() -> Vec<f64> {
    exponential_buckets(0.008, 2.0, 15).unwrap()
}

fn targeted_splits_buckets() -> Vec<f64> {
    [
        linear_buckets(0.0, 10.0, 10).unwrap(),
        linear_buckets(100.0, 100.0, 9).unwrap(),
        linear_buckets(1000.0, 1000.0, 9).unwrap(),
        linear_buckets(10000.0, 10000.0, 10).unwrap(),
    ]
    .iter()
    .flatten()
    .copied()
    .collect()
}

fn pseudo_exponential_bytes_buckets() -> Vec<f64> {
    vec![
        ByteSize::mb(10).as_u64() as f64,
        ByteSize::mb(20).as_u64() as f64,
        ByteSize::mb(50).as_u64() as f64,
        ByteSize::mb(100).as_u64() as f64,
        ByteSize::mb(200).as_u64() as f64,
        ByteSize::mb(500).as_u64() as f64,
        ByteSize::gb(1).as_u64() as f64,
        ByteSize::gb(2).as_u64() as f64,
        ByteSize::gb(5).as_u64() as f64,
    ]
}

static SPLIT_SEARCH_OUTCOME: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "split_search_outcome",
        description: "Count the state in which each leaf search split ended",
        subsystem: "search",
        observable: true,
    )
});

static LEAF_SEARCH_SINGLE_SPLIT_TASKS: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "leaf_search_single_split_tasks",
        description: "Number of single split search tasks pending or ongoing",
        subsystem: "search",
    )
});

static ROOT_SEARCH_REQUESTS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "root_search_requests_total",
        description: "Total number of root search gRPC requests processed.",
        subsystem: "search",
    )
});

static ROOT_SEARCH_REQUEST_DURATION_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "root_search_request_duration_seconds",
        description: "Duration of root search gRPC requests in seconds.",
        subsystem: "search",
        buckets: duration_buckets(),
    )
});

static ROOT_SEARCH_TARGETED_SPLITS: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "root_search_targeted_splits",
        description: "Number of splits targeted per root search GRPC request.",
        subsystem: "search",
        buckets: targeted_splits_buckets(),
    )
});

static LEAF_SEARCH_REQUESTS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "leaf_search_requests_total",
        description: "Total number of leaf search gRPC requests processed.",
        subsystem: "search",
    )
});

static LEAF_SEARCH_REQUEST_DURATION_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "leaf_search_request_duration_seconds",
        description: "Duration of leaf search gRPC requests in seconds.",
        subsystem: "search",
        buckets: duration_buckets(),
    )
});

static LEAF_SEARCH_TARGETED_SPLITS: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "leaf_search_targeted_splits",
        description: "Number of splits targeted per leaf search GRPC request.",
        subsystem: "search",
        buckets: targeted_splits_buckets(),
    )
});

static LEAF_LIST_TERMS_SPLITS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "leaf_list_terms_splits_total",
        description: "Number of list terms splits total",
        subsystem: "search",
    )
});

static LEAF_SEARCH_SPLIT_DURATION_SECS: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "leaf_search_split_duration_secs",
        description: "Number of seconds required to run a leaf search over a single split. The timer starts after the semaphore is obtained.",
        subsystem: "search",
        buckets: duration_buckets(),
    )
});

static LEAF_SEARCH_SINGLE_SPLIT_WARMUP_NUM_BYTES: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "leaf_search_single_split_warmup_num_bytes",
        description: "Size of the short lived cache for a single split once the warmup is done.",
        subsystem: "search",
        buckets: pseudo_exponential_bytes_buckets(),
    )
});

static JOB_ASSIGNED_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "job_assigned_total",
        description: "Number of job assigned to searchers, per affinity rank.",
        subsystem: "search",
    )
});

static SEARCHER_LOCAL_KV_STORE_SIZE_BYTES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "searcher_local_kv_store_size_bytes",
        description: "Size of the searcher kv store in bytes. This store is used to cache scroll contexts.",
        subsystem: "search",
    )
});

impl Default for SearchMetrics {
    fn default() -> Self {
        SearchMetrics {
            root_search_requests_total: counter!(
                parent: &*ROOT_SEARCH_REQUESTS_TOTAL,
                "kind" => "server",
            ),
            root_search_request_duration_seconds: histogram!(
                parent: &*ROOT_SEARCH_REQUEST_DURATION_SECONDS,
                "kind" => "server",
            ),
            root_search_targeted_splits: ROOT_SEARCH_TARGETED_SPLITS.clone(),
            leaf_search_requests_total: counter!(
                parent: &*LEAF_SEARCH_REQUESTS_TOTAL,
                "kind" => "server",
            ),
            leaf_search_request_duration_seconds: histogram!(
                parent: &*LEAF_SEARCH_REQUEST_DURATION_SECONDS,
                "kind" => "server",
            ),
            leaf_search_targeted_splits: LEAF_SEARCH_TARGETED_SPLITS.clone(),

            leaf_list_terms_splits_total: LEAF_LIST_TERMS_SPLITS_TOTAL.clone(),
            split_search_outcome_total: SplitSearchOutcomeCounters::new_registered(),

            leaf_search_split_duration_secs: LEAF_SEARCH_SPLIT_DURATION_SECS.clone(),
            leaf_search_single_split_tasks_ongoing: gauge!(
                parent: &*LEAF_SEARCH_SINGLE_SPLIT_TASKS,
                "status" => "ongoing",
            ),
            leaf_search_single_split_tasks_pending: gauge!(
                parent: &*LEAF_SEARCH_SINGLE_SPLIT_TASKS,
                "status" => "pending",
            ),
            leaf_search_single_split_warmup_num_bytes: LEAF_SEARCH_SINGLE_SPLIT_WARMUP_NUM_BYTES
                .clone(),
            job_assigned_total: JOB_ASSIGNED_TOTAL.clone(),
            searcher_local_kv_store_size_bytes: SEARCHER_LOCAL_KV_STORE_SIZE_BYTES.clone(),
        }
    }
}

/// `SEARCH_METRICS` exposes a bunch a set of storage/cache related metrics through a prometheus
/// endpoint.
pub static SEARCH_METRICS: LazyLock<SearchMetrics> = LazyLock::new(SearchMetrics::default);
