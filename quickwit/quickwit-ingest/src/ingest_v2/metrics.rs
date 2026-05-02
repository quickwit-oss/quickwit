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

use std::sync::LazyLock;

use mrecordlog::ResourceUsage;
use quickwit_common::metrics::{exponential_buckets, linear_buckets};
use quickwit_metrics::{Counter, Gauge, Histogram, counter, gauge, histogram};

// Counter vec counting the different outcomes of ingest requests as
// measure at the end of the router work.
//
// The counter are counting persist subrequests.
pub(crate) struct IngestResultMetrics {
    pub success: Counter,
    pub circuit_breaker: Counter,
    pub unspecified: Counter,
    pub index_not_found: Counter,
    pub source_not_found: Counter,
    pub internal: Counter,
    pub no_shards_available: Counter,
    pub shard_rate_limited: Counter,
    pub wal_full: Counter,
    pub timeout: Counter,
    pub router_timeout: Counter,
    pub router_load_shedding: Counter,
    pub load_shedding: Counter,
    pub shard_not_found: Counter,
    pub unavailable: Counter,
}

static INGEST_RESULT_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "ingest_result_total",
        description: "Number of ingest requests by result",
        subsystem: "ingest",
    )
});

impl Default for IngestResultMetrics {
    fn default() -> Self {
        Self {
            success: counter!(parent: &*INGEST_RESULT_TOTAL, "result" => "success"),
            circuit_breaker: counter!(parent: &*INGEST_RESULT_TOTAL, "result" => "circuit_breaker"),
            unspecified: counter!(parent: &*INGEST_RESULT_TOTAL, "result" => "unspecified"),
            index_not_found: counter!(parent: &*INGEST_RESULT_TOTAL, "result" => "index_not_found"),
            source_not_found: counter!(
                parent: &*INGEST_RESULT_TOTAL,
                "result" => "source_not_found",
            ),
            internal: counter!(parent: &*INGEST_RESULT_TOTAL, "result" => "internal"),
            no_shards_available: counter!(
                parent: &*INGEST_RESULT_TOTAL,
                "result" => "no_shards_available",
            ),
            shard_rate_limited: counter!(
                parent: &*INGEST_RESULT_TOTAL,
                "result" => "shard_rate_limited",
            ),
            wal_full: counter!(parent: &*INGEST_RESULT_TOTAL, "result" => "wal_full"),
            timeout: counter!(parent: &*INGEST_RESULT_TOTAL, "result" => "timeout"),
            router_timeout: counter!(parent: &*INGEST_RESULT_TOTAL, "result" => "router_timeout"),
            router_load_shedding: counter!(
                parent: &*INGEST_RESULT_TOTAL,
                "result" => "router_load_shedding",
            ),
            load_shedding: counter!(parent: &*INGEST_RESULT_TOTAL, "result" => "load_shedding"),
            unavailable: counter!(parent: &*INGEST_RESULT_TOTAL, "result" => "unavailable"),
            shard_not_found: counter!(parent: &*INGEST_RESULT_TOTAL, "result" => "shard_not_found"),
        }
    }
}

pub(super) struct IngestV2Metrics {
    pub reset_shards_operations_total: Counter,
    pub open_shards: Gauge,
    pub closed_shards: Gauge,
    pub shard_lt_throughput_mib: Histogram,
    pub shard_st_throughput_mib: Histogram,
    pub wal_acquire_lock_requests_in_flight: Gauge,
    pub wal_acquire_lock_request_duration_secs: Histogram,
    pub wal_disk_used_bytes: Gauge,
    pub wal_memory_used_bytes: Gauge,
    pub ingest_results: IngestResultMetrics,
    pub ingest_attempts: Counter,
}

static INGEST_ATTEMPTS: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "ingest_attempts",
        description: "Number of routing attempts by AZ locality",
        subsystem: "ingest",
    )
});

static RESET_SHARDS_OPERATIONS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "reset_shards_operations_total",
        description: "Total number of reset shards operations performed.",
        subsystem: "ingest",
    )
});

static SHARDS: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "shards",
        description: "Number of shards hosted by the ingester.",
        subsystem: "ingest",
    )
});

static SHARD_LT_THROUGHPUT_MIB: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "shard_lt_throughput_mib",
        description: "Shard long term throughput as reported through chitchat",
        subsystem: "ingest",
        buckets: linear_buckets(0.0f64, 1.0f64, 15).unwrap(),
    )
});

static SHARD_ST_THROUGHPUT_MIB: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "shard_st_throughput_mib",
        description: "Shard short term throughput as reported through chitchat",
        subsystem: "ingest",
        buckets: linear_buckets(0.0f64, 1.0f64, 15).unwrap(),
    )
});

static WAL_ACQUIRE_LOCK_REQUESTS_IN_FLIGHT: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "wal_acquire_lock_requests_in_flight",
        description: "Number of acquire lock requests in-flight.",
        subsystem: "ingest",
    )
});

static WAL_ACQUIRE_LOCK_REQUEST_DURATION_SECS: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "wal_acquire_lock_request_duration_secs",
        description: "Duration of acquire lock requests in seconds.",
        subsystem: "ingest",
        buckets: exponential_buckets(0.001, 2.0, 12).unwrap(),
    )
});

static WAL_DISK_USED_BYTES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "wal_disk_used_bytes",
        description: "WAL disk space used in bytes.",
        subsystem: "ingest",
    )
});

static WAL_MEMORY_USED_BYTES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "wal_memory_used_bytes",
        description: "WAL memory used in bytes.",
        subsystem: "ingest",
    )
});

impl Default for IngestV2Metrics {
    fn default() -> Self {
        Self {
            ingest_results: IngestResultMetrics::default(),
            ingest_attempts: INGEST_ATTEMPTS.clone(),
            reset_shards_operations_total: RESET_SHARDS_OPERATIONS_TOTAL.clone(),
            open_shards: gauge!(parent: &*SHARDS, "state" => "open"),
            closed_shards: gauge!(parent: &*SHARDS, "state" => "closed"),
            shard_lt_throughput_mib: SHARD_LT_THROUGHPUT_MIB.clone(),
            shard_st_throughput_mib: SHARD_ST_THROUGHPUT_MIB.clone(),
            wal_acquire_lock_requests_in_flight: WAL_ACQUIRE_LOCK_REQUESTS_IN_FLIGHT.clone(),
            wal_acquire_lock_request_duration_secs: WAL_ACQUIRE_LOCK_REQUEST_DURATION_SECS.clone(),
            wal_disk_used_bytes: WAL_DISK_USED_BYTES.clone(),
            wal_memory_used_bytes: WAL_MEMORY_USED_BYTES.clone(),
        }
    }
}

pub(super) fn report_wal_usage(wal_usage: ResourceUsage) {
    INGEST_V2_METRICS
        .wal_disk_used_bytes
        .set(wal_usage.disk_used_bytes as f64);
    quickwit_common::metrics::MEMORY_METRICS
        .in_flight
        .wal
        .set(wal_usage.memory_allocated_bytes as f64);
    INGEST_V2_METRICS
        .wal_memory_used_bytes
        .set(wal_usage.memory_used_bytes as f64);
}

pub(super) static INGEST_V2_METRICS: LazyLock<IngestV2Metrics> =
    LazyLock::new(IngestV2Metrics::default);
