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
use quickwit_common::metrics::{IN_FLIGHT_WAL, exponential_buckets, linear_buckets};
use quickwit_metrics::{
    Counter, Gauge, Histogram, LabelNames, counter, gauge, histogram, label_names,
};

pub(super) const STATUS: LabelNames<1> = label_names!("status");

static INGEST_RESULT_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "ingest_result_total",
        description: "Number of ingest requests by result",
        subsystem: "ingest",
    )
});

pub(super) static INGEST_RESULT_SUCCESS: LazyLock<Counter> =
    LazyLock::new(|| counter!(parent: INGEST_RESULT_TOTAL, "result" => "success"));

pub(super) static INGEST_RESULT_CIRCUIT_BREAKER: LazyLock<Counter> =
    LazyLock::new(|| counter!(parent: INGEST_RESULT_TOTAL, "result" => "circuit_breaker"));

pub(super) static INGEST_RESULT_UNSPECIFIED: LazyLock<Counter> =
    LazyLock::new(|| counter!(parent: INGEST_RESULT_TOTAL, "result" => "unspecified"));

pub(super) static INGEST_RESULT_INDEX_NOT_FOUND: LazyLock<Counter> =
    LazyLock::new(|| counter!(parent: INGEST_RESULT_TOTAL, "result" => "index_not_found"));

pub(super) static INGEST_RESULT_SOURCE_NOT_FOUND: LazyLock<Counter> =
    LazyLock::new(|| counter!(parent: INGEST_RESULT_TOTAL, "result" => "source_not_found"));

pub(super) static INGEST_RESULT_INTERNAL: LazyLock<Counter> =
    LazyLock::new(|| counter!(parent: INGEST_RESULT_TOTAL, "result" => "internal"));

pub(super) static INGEST_RESULT_NO_SHARDS_AVAILABLE: LazyLock<Counter> =
    LazyLock::new(|| counter!(parent: INGEST_RESULT_TOTAL, "result" => "no_shards_available"));

pub(super) static INGEST_RESULT_SHARD_RATE_LIMITED: LazyLock<Counter> =
    LazyLock::new(|| counter!(parent: INGEST_RESULT_TOTAL, "result" => "shard_rate_limited"));

pub(super) static INGEST_RESULT_WAL_FULL: LazyLock<Counter> =
    LazyLock::new(|| counter!(parent: INGEST_RESULT_TOTAL, "result" => "wal_full"));

pub(super) static INGEST_RESULT_TIMEOUT: LazyLock<Counter> =
    LazyLock::new(|| counter!(parent: INGEST_RESULT_TOTAL, "result" => "timeout"));

pub(super) static INGEST_RESULT_ROUTER_TIMEOUT: LazyLock<Counter> =
    LazyLock::new(|| counter!(parent: INGEST_RESULT_TOTAL, "result" => "router_timeout"));

pub(super) static INGEST_RESULT_ROUTER_LOAD_SHEDDING: LazyLock<Counter> =
    LazyLock::new(|| counter!(parent: INGEST_RESULT_TOTAL, "result" => "router_load_shedding"));

pub(super) static INGEST_RESULT_LOAD_SHEDDING: LazyLock<Counter> =
    LazyLock::new(|| counter!(parent: INGEST_RESULT_TOTAL, "result" => "load_shedding"));

pub(super) static INGEST_RESULT_SHARD_NOT_FOUND: LazyLock<Counter> =
    LazyLock::new(|| counter!(parent: INGEST_RESULT_TOTAL, "result" => "shard_not_found"));

pub(super) static INGEST_RESULT_UNAVAILABLE: LazyLock<Counter> =
    LazyLock::new(|| counter!(parent: INGEST_RESULT_TOTAL, "result" => "unavailable"));

pub(super) static INGEST_ATTEMPTS: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "ingest_attempts",
        description: "Number of routing attempts by AZ locality",
        subsystem: "ingest",
    )
});

pub(super) static RESET_SHARDS_OPERATIONS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
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

pub(super) static OPEN_SHARDS: LazyLock<Gauge> =
    LazyLock::new(|| gauge!(parent: SHARDS, "state" => "open"));

pub(super) static CLOSED_SHARDS: LazyLock<Gauge> =
    LazyLock::new(|| gauge!(parent: SHARDS, "state" => "closed"));

pub(super) static SHARD_LT_THROUGHPUT_MIB: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "shard_lt_throughput_mib",
        description: "Shard long term throughput as reported through chitchat",
        subsystem: "ingest",
        buckets: linear_buckets(0.0f64, 1.0f64, 15).unwrap(),
    )
});

pub(super) static SHARD_ST_THROUGHPUT_MIB: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "shard_st_throughput_mib",
        description: "Shard short term throughput as reported through chitchat",
        subsystem: "ingest",
        buckets: linear_buckets(0.0f64, 1.0f64, 15).unwrap(),
    )
});

pub(super) static WAL_ACQUIRE_LOCK_REQUESTS_IN_FLIGHT: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "wal_acquire_lock_requests_in_flight",
        description: "Number of acquire lock requests in-flight.",
        subsystem: "ingest",
    )
});

pub(super) static WAL_ACQUIRE_LOCK_REQUEST_DURATION_SECS: LazyLock<Histogram> =
    LazyLock::new(|| {
        histogram!(
            name: "wal_acquire_lock_request_duration_secs",
            description: "Duration of acquire lock requests in seconds.",
            subsystem: "ingest",
            buckets: exponential_buckets(0.001, 2.0, 12).unwrap(),
        )
    });

pub(super) static WAL_DISK_USED_BYTES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "wal_disk_used_bytes",
        description: "WAL disk space used in bytes.",
        subsystem: "ingest",
    )
});

pub(super) static WAL_MEMORY_USED_BYTES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "wal_memory_used_bytes",
        description: "WAL memory used in bytes.",
        subsystem: "ingest",
    )
});

pub(super) fn report_wal_usage(wal_usage: ResourceUsage) {
    WAL_DISK_USED_BYTES.set(wal_usage.disk_used_bytes as f64);
    IN_FLIGHT_WAL.set(wal_usage.memory_allocated_bytes as f64);
    WAL_MEMORY_USED_BYTES.set(wal_usage.memory_used_bytes as f64);
}
