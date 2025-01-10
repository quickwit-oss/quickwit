// Copyright (C) 2024 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use mrecordlog::ResourceUsage;
use once_cell::sync::Lazy;
use quickwit_common::metrics::{
    exponential_buckets, linear_buckets, new_counter_vec, new_gauge, new_gauge_vec, new_histogram,
    new_histogram_vec, Histogram, IntCounter, IntCounterVec, IntGauge, Vector,
};

// Counter vec counting the different outcomes of ingest requests as
// measure at the end of the router work.
//
// The counter are counting persist subrequests.
pub(crate) struct IngestResultMetrics {
    pub success: IntCounter,
    pub circuit_breaker: IntCounter,
    pub unspecified: IntCounter,
    pub index_not_found: IntCounter,
    pub source_not_found: IntCounter,
    pub internal: IntCounter,
    pub no_shards_available: IntCounter,
    pub shard_rate_limited: IntCounter,
    pub wal_full: IntCounter,
    pub timeout: IntCounter,
    pub router_timeout: IntCounter,
    pub router_load_shedding: IntCounter,
    pub load_shedding: IntCounter,
    pub shard_not_found: IntCounter,
    pub unavailable: IntCounter,
}

impl Default for IngestResultMetrics {
    fn default() -> Self {
        let ingest_result_total_vec = new_counter_vec::<1>(
            "ingest_result_total",
            "Number of ingest requests by result",
            "ingest",
            &[],
            ["result"],
        );
        Self {
            success: ingest_result_total_vec.with_label_values(["success"]),
            circuit_breaker: ingest_result_total_vec.with_label_values(["circuit_breaker"]),
            unspecified: ingest_result_total_vec.with_label_values(["unspecified"]),
            index_not_found: ingest_result_total_vec.with_label_values(["index_not_found"]),
            source_not_found: ingest_result_total_vec.with_label_values(["source_not_found"]),
            internal: ingest_result_total_vec.with_label_values(["internal"]),
            no_shards_available: ingest_result_total_vec.with_label_values(["no_shards_available"]),
            shard_rate_limited: ingest_result_total_vec.with_label_values(["shard_rate_limited"]),
            wal_full: ingest_result_total_vec.with_label_values(["wal_full"]),
            timeout: ingest_result_total_vec.with_label_values(["timeout"]),
            router_timeout: ingest_result_total_vec.with_label_values(["router_timeout"]),
            router_load_shedding: ingest_result_total_vec
                .with_label_values(["router_load_shedding"]),
            load_shedding: ingest_result_total_vec.with_label_values(["load_shedding"]),
            unavailable: ingest_result_total_vec.with_label_values(["unavailable"]),
            shard_not_found: ingest_result_total_vec.with_label_values(["shard_not_found"]),
        }
    }
}

pub(crate) struct IngestV2Metrics {
    pub reset_shards_operations_total: IntCounterVec<1>,
    pub open_shards: IntGauge,
    pub closed_shards: IntGauge,
    pub shard_lt_throughput_mib: Histogram,
    pub shard_st_throughput_mib: Histogram,
    pub wal_acquire_lock_requests_in_flight: LockMetric<IntGauge>,
    pub wal_acquire_lock_request_duration_secs: LockMetric<Histogram>,
    pub wal_hold_lock_duration_secs: LockMetric<Histogram>,
    pub wal_disk_used_bytes: IntGauge,
    pub wal_memory_used_bytes: IntGauge,
    pub ingest_results: IngestResultMetrics,
}

pub(crate) struct ReadMetric<T> {
    pub read: T,
}

pub(crate) struct WriteMetric<T> {
    pub write: T,
}

pub(crate) struct RWMetric<T> {
    pub read: T,
    pub write: T,
}

pub(crate) struct LockMetric<T> {
    pub reset_shards: RWMetric<T>,
    pub fetch: ReadMetric<T>,
    pub persist: WriteMetric<T>,
    pub init_shards: WriteMetric<T>,
    pub truncate_shards: WriteMetric<T>,
    pub close_shards: WriteMetric<T>,
    pub retain_shards: WriteMetric<T>,
    pub gc_shards: WriteMetric<T>,
    pub init_replica: WriteMetric<T>,
    pub replicate: WriteMetric<T>,
    pub close_idle_shards: WriteMetric<T>,
}

impl<T> LockMetric<T> {
    fn new<V>(dynamic_vector: V) -> Self
    where V: Vector<2, T> {
        LockMetric {
            reset_shards: RWMetric {
                read: dynamic_vector.with_label_values(["reset_shards", "read"]),
                write: dynamic_vector.with_label_values(["reset_shards", "write"]),
            },
            fetch: ReadMetric {
                read: dynamic_vector.with_label_values(["fetch", "read"]),
            },
            persist: WriteMetric {
                write: dynamic_vector.with_label_values(["persit", "read"]),
            },
            init_shards: WriteMetric {
                write: dynamic_vector.with_label_values(["init_shards", "read"]),
            },
            truncate_shards: WriteMetric {
                write: dynamic_vector.with_label_values(["truncate_shards", "read"]),
            },
            close_shards: WriteMetric {
                write: dynamic_vector.with_label_values(["close_shards", "read"]),
            },
            retain_shards: WriteMetric {
                write: dynamic_vector.with_label_values(["retain_shards", "read"]),
            },
            gc_shards: WriteMetric {
                write: dynamic_vector.with_label_values(["gc_shards", "read"]),
            },
            init_replica: WriteMetric {
                write: dynamic_vector.with_label_values(["init_replica", "read"]),
            },
            replicate: WriteMetric {
                write: dynamic_vector.with_label_values(["replicate", "read"]),
            },
            close_idle_shards: WriteMetric {
                write: dynamic_vector.with_label_values(["close_idle_shards", "read"]),
            },
        }
    }
}

impl Default for IngestV2Metrics {
    fn default() -> Self {
        Self {
            ingest_results: IngestResultMetrics::default(),
            reset_shards_operations_total: new_counter_vec(
                "reset_shards_operations_total",
                "Total number of reset shards operations performed.",
                "ingest",
                &[],
                ["status"],
            ),
            open_shards: new_gauge(
                "shards",
                "Number of shards hosted by the ingester.",
                "ingest",
                &[("state", "open")],
            ),
            closed_shards: new_gauge(
                "shards",
                "Number of shards hosted by the ingester.",
                "ingest",
                &[("state", "closed")],
            ),
            shard_lt_throughput_mib: new_histogram(
                "shard_lt_throughput_mib",
                "Shard long term throughput as reported through chitchat",
                "ingest",
                linear_buckets(0.0f64, 1.0f64, 15).unwrap(),
            ),
            shard_st_throughput_mib: new_histogram(
                "shard_st_throughput_mib",
                "Shard short term throughput as reported through chitchat",
                "ingest",
                linear_buckets(0.0f64, 1.0f64, 15).unwrap(),
            ),
            wal_acquire_lock_requests_in_flight: LockMetric::new(new_gauge_vec(
                "wal_acquire_lock_requests_in_flight",
                "Number of acquire lock requests in-flight.",
                "ingest",
                &[],
                ["operation", "type"],
            )),
            wal_acquire_lock_request_duration_secs: LockMetric::new(new_histogram_vec(
                "wal_acquire_lock_request_duration_secs",
                "Duration of acquire lock requests in seconds.",
                "ingest",
                &[],
                ["operation", "type"],
                exponential_buckets(0.001, 2.0, 12).unwrap(),
            )),
            wal_hold_lock_duration_secs: LockMetric::new(new_histogram_vec(
                "wal_hold_lock_duration_secs",
                "Duration for which a lock was held in seconds.",
                "ingest",
                &[],
                ["operation", "type"],
                exponential_buckets(0.001, 2.0, 12).unwrap(),
            )),
            wal_disk_used_bytes: new_gauge(
                "wal_disk_used_bytes",
                "WAL disk space used in bytes.",
                "ingest",
                &[],
            ),
            wal_memory_used_bytes: new_gauge(
                "wal_memory_used_bytes",
                "WAL memory used in bytes.",
                "ingest",
                &[],
            ),
        }
    }
}

pub(super) fn report_wal_usage(wal_usage: ResourceUsage) {
    INGEST_V2_METRICS
        .wal_disk_used_bytes
        .set(wal_usage.disk_used_bytes as i64);
    quickwit_common::metrics::MEMORY_METRICS
        .in_flight
        .wal
        .set(wal_usage.memory_allocated_bytes as i64);
    INGEST_V2_METRICS
        .wal_memory_used_bytes
        .set(wal_usage.memory_used_bytes as i64);
}

pub(crate) static INGEST_V2_METRICS: Lazy<IngestV2Metrics> = Lazy::new(IngestV2Metrics::default);
