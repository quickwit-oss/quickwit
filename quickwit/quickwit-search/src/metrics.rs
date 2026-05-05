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
use std::sync::LazyLock;

use bytesize::ByteSize;
use quickwit_common::metrics::{MaybeRegisteredCounter, exponential_buckets, linear_buckets};
use quickwit_metrics::{
    Counter, Gauge, Histogram, LabelNames, counter, gauge, histogram, label_names,
};

pub(crate) const STATUS_LABELS: LabelNames<1> = label_names!("status");

fn print_if_not_null(
    field_name: &'static str,
    counter: &MaybeRegisteredCounter,
    f: &mut fmt::Formatter,
) -> fmt::Result {
    let val = counter.get();
    if val > 0 {
        write!(f, "{}={} ", field_name, val)?;
    }
    Ok(())
}

pub struct SplitSearchOutcomeCounters {
    pub cancel_before_warmup: MaybeRegisteredCounter,
    pub cache_hit: MaybeRegisteredCounter,
    pub pruned_before_warmup: MaybeRegisteredCounter,
    pub cancel_warmup: MaybeRegisteredCounter,
    pub pruned_after_warmup: MaybeRegisteredCounter,
    pub cancel_cpu_queue: MaybeRegisteredCounter,
    pub cancel_cpu: MaybeRegisteredCounter,
    pub success: MaybeRegisteredCounter,
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
        Self::new_registered_from_counter(&SPLIT_SEARCH_OUTCOME)
    }

    /// Create a new SplitSearchOutcomeCounters instance that is not reported.
    pub fn new_unregistered() -> Self {
        SplitSearchOutcomeCounters {
            cancel_before_warmup: MaybeRegisteredCounter::local(),
            cache_hit: MaybeRegisteredCounter::local(),
            pruned_before_warmup: MaybeRegisteredCounter::local(),
            cancel_warmup: MaybeRegisteredCounter::local(),
            pruned_after_warmup: MaybeRegisteredCounter::local(),
            cancel_cpu_queue: MaybeRegisteredCounter::local(),
            cancel_cpu: MaybeRegisteredCounter::local(),
            success: MaybeRegisteredCounter::local(),
        }
    }

    fn new_registered_from_counter(search_split_outcome: &Counter) -> Self {
        SplitSearchOutcomeCounters {
            cancel_before_warmup: MaybeRegisteredCounter::registered(counter!(
                parent: search_split_outcome,
                "category" => "cancel_before_warmup",
            )),
            cache_hit: MaybeRegisteredCounter::registered(counter!(
                parent: search_split_outcome,
                "category" => "cache_hit",
            )),
            pruned_before_warmup: MaybeRegisteredCounter::registered(counter!(
                parent: search_split_outcome,
                "category" => "pruned_before_warmup",
            )),
            cancel_warmup: MaybeRegisteredCounter::registered(counter!(
                parent: search_split_outcome,
                "category" => "cancel_warmup",
            )),
            pruned_after_warmup: MaybeRegisteredCounter::registered(counter!(
                parent: search_split_outcome,
                "category" => "pruned_after_warmup",
            )),
            cancel_cpu_queue: MaybeRegisteredCounter::registered(counter!(
                parent: search_split_outcome,
                "category" => "cancel_cpu_queue",
            )),
            cancel_cpu: MaybeRegisteredCounter::registered(counter!(
                parent: search_split_outcome,
                "category" => "cancel_cpu",
            )),
            success: MaybeRegisteredCounter::registered(counter!(
                parent: search_split_outcome,
                "category" => "success",
            )),
        }
    }
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
    )
});

pub(crate) static SPLIT_SEARCH_OUTCOME_TOTAL: LazyLock<SplitSearchOutcomeCounters> =
    LazyLock::new(SplitSearchOutcomeCounters::new_registered);

static LEAF_SEARCH_SINGLE_SPLIT_TASKS_BASE: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "leaf_search_single_split_tasks",
        description: "Number of single split search tasks pending or ongoing",
        subsystem: "search",
    )
});

pub(crate) static LEAF_SEARCH_SINGLE_SPLIT_TASKS_ONGOING: LazyLock<Gauge> =
    LazyLock::new(|| gauge!(parent: LEAF_SEARCH_SINGLE_SPLIT_TASKS_BASE, "status" => "ongoing"));

pub(crate) static LEAF_SEARCH_SINGLE_SPLIT_TASKS_PENDING: LazyLock<Gauge> =
    LazyLock::new(|| gauge!(parent: LEAF_SEARCH_SINGLE_SPLIT_TASKS_BASE, "status" => "pending"));

static ROOT_SEARCH_REQUESTS_TOTAL_BASE: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "root_search_requests_total",
        description: "Total number of root search gRPC requests processed.",
        subsystem: "search",
    )
});

pub(crate) static ROOT_SEARCH_REQUESTS_TOTAL: LazyLock<Counter> =
    LazyLock::new(|| counter!(parent: ROOT_SEARCH_REQUESTS_TOTAL_BASE, "kind" => "server"));

static ROOT_SEARCH_REQUEST_DURATION_SECONDS_BASE: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "root_search_request_duration_seconds",
        description: "Duration of root search gRPC requests in seconds.",
        subsystem: "search",
        buckets: duration_buckets(),
    )
});

pub(crate) static ROOT_SEARCH_REQUEST_DURATION_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        parent: ROOT_SEARCH_REQUEST_DURATION_SECONDS_BASE,
        "kind" => "server",
    )
});

pub(crate) static ROOT_SEARCH_TARGETED_SPLITS: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "root_search_targeted_splits",
        description: "Number of splits targeted per root search GRPC request.",
        subsystem: "search",
        buckets: targeted_splits_buckets(),
    )
});

static LEAF_SEARCH_REQUESTS_TOTAL_BASE: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "leaf_search_requests_total",
        description: "Total number of leaf search gRPC requests processed.",
        subsystem: "search",
    )
});

pub(crate) static LEAF_SEARCH_REQUESTS_TOTAL: LazyLock<Counter> =
    LazyLock::new(|| counter!(parent: LEAF_SEARCH_REQUESTS_TOTAL_BASE, "kind" => "server"));

static LEAF_SEARCH_REQUEST_DURATION_SECONDS_BASE: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "leaf_search_request_duration_seconds",
        description: "Duration of leaf search gRPC requests in seconds.",
        subsystem: "search",
        buckets: duration_buckets(),
    )
});

pub(crate) static LEAF_SEARCH_REQUEST_DURATION_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        parent: LEAF_SEARCH_REQUEST_DURATION_SECONDS_BASE,
        "kind" => "server",
    )
});

pub(crate) static LEAF_SEARCH_TARGETED_SPLITS: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "leaf_search_targeted_splits",
        description: "Number of splits targeted per leaf search GRPC request.",
        subsystem: "search",
        buckets: targeted_splits_buckets(),
    )
});

pub(crate) static LEAF_LIST_TERMS_SPLITS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "leaf_list_terms_splits_total",
        description: "Number of list terms splits total",
        subsystem: "search",
    )
});

pub(crate) static LEAF_SEARCH_SPLIT_DURATION_SECS: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "leaf_search_split_duration_secs",
        description: "Number of seconds required to run a leaf search over a single split. The timer starts after the semaphore is obtained.",
        subsystem: "search",
        buckets: duration_buckets(),
    )
});

pub(crate) static LEAF_SEARCH_SINGLE_SPLIT_WARMUP_NUM_BYTES: LazyLock<Histogram> = LazyLock::new(
    || {
        histogram!(
            name: "leaf_search_single_split_warmup_num_bytes",
            description: "Size of the short lived cache for a single split once the warmup is done.",
            subsystem: "search",
            buckets: pseudo_exponential_bytes_buckets(),
        )
    },
);

pub(crate) static JOB_ASSIGNED_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "job_assigned_total",
        description: "Number of job assigned to searchers, per affinity rank.",
        subsystem: "search",
    )
});

pub(crate) static SEARCHER_LOCAL_KV_STORE_SIZE_BYTES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "searcher_local_kv_store_size_bytes",
        description: "Size of the searcher kv store in bytes. This store is used to cache scroll contexts.",
        subsystem: "search",
    )
});
