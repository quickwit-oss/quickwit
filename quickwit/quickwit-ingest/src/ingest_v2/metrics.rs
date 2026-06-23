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

use mrecordlog::ResourceUsage;
use quickwit_common::metrics::{IN_FLIGHT_WAL, exponential_buckets, linear_buckets};
use quickwit_metrics::{
    LabelNames, LazyCounter, LazyGauge, LazyHistogram, label_names, lazy_counter, lazy_gauge,
    lazy_histogram,
};

pub(super) const STATUS: LabelNames<1> = label_names!("status");

static INGEST_RESULT_TOTAL: LazyCounter = lazy_counter!(
        name: "ingest_result_total",
        description: "Number of ingest requests by result",
        subsystem: "ingest",
);

pub(super) static INGEST_RESULT_SUCCESS: LazyCounter =
    lazy_counter!(parent: INGEST_RESULT_TOTAL, "result" => "success");

pub(super) static INGEST_RESULT_CIRCUIT_BREAKER: LazyCounter =
    lazy_counter!(parent: INGEST_RESULT_TOTAL, "result" => "circuit_breaker");

pub(super) static INGEST_RESULT_UNSPECIFIED: LazyCounter =
    lazy_counter!(parent: INGEST_RESULT_TOTAL, "result" => "unspecified");

pub(super) static INGEST_RESULT_INDEX_NOT_FOUND: LazyCounter =
    lazy_counter!(parent: INGEST_RESULT_TOTAL, "result" => "index_not_found");

pub(super) static INGEST_RESULT_SOURCE_NOT_FOUND: LazyCounter =
    lazy_counter!(parent: INGEST_RESULT_TOTAL, "result" => "source_not_found");

pub(super) static INGEST_RESULT_INTERNAL: LazyCounter =
    lazy_counter!(parent: INGEST_RESULT_TOTAL, "result" => "internal");

pub(super) static INGEST_RESULT_NO_SHARDS_AVAILABLE: LazyCounter =
    lazy_counter!(parent: INGEST_RESULT_TOTAL, "result" => "no_shards_available");

pub(super) static INGEST_RESULT_SHARD_RATE_LIMITED: LazyCounter =
    lazy_counter!(parent: INGEST_RESULT_TOTAL, "result" => "shard_rate_limited");

pub(super) static INGEST_RESULT_WAL_FULL: LazyCounter =
    lazy_counter!(parent: INGEST_RESULT_TOTAL, "result" => "wal_full");

pub(super) static INGEST_RESULT_TIMEOUT: LazyCounter =
    lazy_counter!(parent: INGEST_RESULT_TOTAL, "result" => "timeout");

pub(super) static INGEST_RESULT_ROUTER_TIMEOUT: LazyCounter =
    lazy_counter!(parent: INGEST_RESULT_TOTAL, "result" => "router_timeout");

pub(super) static INGEST_RESULT_ROUTER_LOAD_SHEDDING: LazyCounter =
    lazy_counter!(parent: INGEST_RESULT_TOTAL, "result" => "router_load_shedding");

pub(super) static INGEST_RESULT_LOAD_SHEDDING: LazyCounter =
    lazy_counter!(parent: INGEST_RESULT_TOTAL, "result" => "load_shedding");

pub(super) static INGEST_RESULT_SHARD_NOT_FOUND: LazyCounter =
    lazy_counter!(parent: INGEST_RESULT_TOTAL, "result" => "shard_not_found");

pub(super) static INGEST_RESULT_UNAVAILABLE: LazyCounter =
    lazy_counter!(parent: INGEST_RESULT_TOTAL, "result" => "unavailable");

static SKIPPED_MRECORDS_TOTAL: LazyCounter = lazy_counter!(
        name: "skipped_mrecords_total",
        description: "Number of mrecords skipped during decoding, by reason (e.g. an unknown \
                      header version or record type written by a newer binary).",
        subsystem: "ingest",
);

pub(super) static SKIPPED_MRECORDS_UNKNOWN_HEADER_VERSION: LazyCounter =
    lazy_counter!(parent: SKIPPED_MRECORDS_TOTAL, "reason" => "unknown_header_version");

pub(super) static SKIPPED_MRECORDS_UNKNOWN_RECORD_TYPE: LazyCounter =
    lazy_counter!(parent: SKIPPED_MRECORDS_TOTAL, "reason" => "unknown_record_type");

pub(super) static SKIPPED_MRECORDS_MALFORMED: LazyCounter =
    lazy_counter!(parent: SKIPPED_MRECORDS_TOTAL, "reason" => "malformed");

pub(super) static INGEST_ATTEMPTS: LazyCounter = lazy_counter!(
        name: "ingest_attempts",
        description: "Number of routing attempts by AZ locality",
        subsystem: "ingest",
);

pub(super) static RESET_SHARDS_OPERATIONS_TOTAL: LazyCounter = lazy_counter!(
        name: "reset_shards_operations_total",
        description: "Total number of reset shards operations performed.",
        subsystem: "ingest",
);

static SHARDS: LazyGauge = lazy_gauge!(
        name: "shards",
        description: "Number of shards hosted by the ingester.",
        subsystem: "ingest",
);

pub(super) static OPEN_SHARDS: LazyGauge = lazy_gauge!(parent: SHARDS, "state" => "open");

pub(super) static CLOSED_SHARDS: LazyGauge = lazy_gauge!(parent: SHARDS, "state" => "closed");

pub(super) static SHARD_LT_THROUGHPUT_MIB: LazyHistogram = lazy_histogram!(
        name: "shard_lt_throughput_mib",
        description: "Shard long term throughput as reported through chitchat",
        subsystem: "ingest",
        buckets: linear_buckets(0.0f64, 1.0f64, 15).unwrap(),
);

pub(super) static SHARD_ST_THROUGHPUT_MIB: LazyHistogram = lazy_histogram!(
        name: "shard_st_throughput_mib",
        description: "Shard short term throughput as reported through chitchat",
        subsystem: "ingest",
        buckets: linear_buckets(0.0f64, 1.0f64, 15).unwrap(),
);

pub(super) static WAL_ACQUIRE_LOCK_REQUESTS_IN_FLIGHT: LazyGauge = lazy_gauge!(
        name: "wal_acquire_lock_requests_in_flight",
        description: "Number of acquire lock requests in-flight.",
        subsystem: "ingest",
);

pub(super) static WAL_ACQUIRE_LOCK_REQUEST_DURATION_SECS: LazyHistogram = lazy_histogram!(
    name: "wal_acquire_lock_request_duration_secs",
    description: "Duration of acquire lock requests in seconds.",
    subsystem: "ingest",
    buckets: exponential_buckets(0.001, 2.0, 12).unwrap(),
);

pub(super) static WAL_LOCK_HOLD_DURATION_SECS: LazyHistogram = lazy_histogram!(
        name: "wal_lock_hold_duration_secs",
        description: "Duration for which the WAL lock was held in seconds.",
        subsystem: "ingest",
        buckets: exponential_buckets(0.001, 2.0, 12).unwrap(),
);

pub(super) static WAL_DISK_USED_BYTES: LazyGauge = lazy_gauge!(
        name: "wal_disk_used_bytes",
        description: "WAL disk space used in bytes.",
        subsystem: "ingest",
);

pub(super) static WAL_MEMORY_USED_BYTES: LazyGauge = lazy_gauge!(
        name: "wal_memory_used_bytes",
        description: "WAL memory used in bytes.",
        subsystem: "ingest",
);

static WAL_BYTES_WRITTEN_TOTAL: LazyCounter = lazy_counter!(
    name: "wal_bytes_written_total",
    description: "Total number of bytes written to the WAL by write operations (create_queue, append_records, truncate_queue, delete_queue), including frame headers and end-of-block padding.",
    subsystem: "ingest",
);

pub(crate) static WAL_BYTES_WRITTEN_CREATE_QUEUE: LazyCounter =
    lazy_counter!(parent: WAL_BYTES_WRITTEN_TOTAL, "operation" => "create_queue");

pub(crate) static WAL_BYTES_WRITTEN_DELETE_QUEUE: LazyCounter =
    lazy_counter!(parent: WAL_BYTES_WRITTEN_TOTAL, "operation" => "delete_queue");

pub(crate) static WAL_BYTES_WRITTEN_APPEND: LazyCounter =
    lazy_counter!(parent: WAL_BYTES_WRITTEN_TOTAL, "operation" => "append");

pub(crate) static WAL_BYTES_WRITTEN_TRUNCATE: LazyCounter =
    lazy_counter!(parent: WAL_BYTES_WRITTEN_TOTAL, "operation" => "truncate");

pub(super) fn report_wal_usage(wal_usage: ResourceUsage) {
    WAL_DISK_USED_BYTES.set(wal_usage.disk_used_bytes as f64);
    IN_FLIGHT_WAL.set(wal_usage.memory_allocated_bytes as f64);
    WAL_MEMORY_USED_BYTES.set(wal_usage.memory_used_bytes as f64);
}
