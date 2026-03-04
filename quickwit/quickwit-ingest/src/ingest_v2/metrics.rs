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
use once_cell::sync::Lazy;
use quickwit_common::metrics::{
    Histogram, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, exponential_buckets,
    linear_buckets, new_counter_vec, new_gauge, new_gauge_vec, new_histogram, new_histogram_vec,
};

// Counter vec counting the different outcomes of ingest requests as
// measured at the end of the router work.
//
// The counters count persist subrequests, broken down by result and AZ routing locality.
pub(crate) struct IngestResultMetrics {
    counter_vec: IntCounterVec<2>,
}

impl IngestResultMetrics {
    fn new() -> Self {
        Self {
            counter_vec: new_counter_vec::<2>(
                "ingest_result_total",
                "Number of ingest requests by result and AZ routing locality",
                "ingest",
                &[],
                ["result", "az_routing"],
            ),
        }
    }

    pub fn inc(&self, result: &str, az_routing: &str) {
        self.counter_vec
            .with_label_values([result, az_routing])
            .inc();
    }

    pub fn inc_by(&self, result: &str, az_routing: &str, count: u64) {
        self.counter_vec
            .with_label_values([result, az_routing])
            .inc_by(count);
    }
}

pub(super) struct IngestV2Metrics {
    pub reset_shards_operations_total: IntCounterVec<1>,
    pub open_shards: IntGauge,
    pub closed_shards: IntGauge,
    pub shard_lt_throughput_mib: Histogram,
    pub shard_st_throughput_mib: Histogram,
    pub wal_acquire_lock_requests_in_flight: IntGaugeVec<2>,
    pub wal_acquire_lock_request_duration_secs: HistogramVec<2>,
    pub wal_disk_used_bytes: IntGauge,
    pub wal_memory_used_bytes: IntGauge,
    pub ingest_results: IngestResultMetrics,
}

impl Default for IngestV2Metrics {
    fn default() -> Self {
        Self {
            ingest_results: IngestResultMetrics::new(),
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
            wal_acquire_lock_requests_in_flight: new_gauge_vec(
                "wal_acquire_lock_requests_in_flight",
                "Number of acquire lock requests in-flight.",
                "ingest",
                &[],
                ["operation", "type"],
            ),
            wal_acquire_lock_request_duration_secs: new_histogram_vec(
                "wal_acquire_lock_request_duration_secs",
                "Duration of acquire lock requests in seconds.",
                "ingest",
                &[],
                ["operation", "type"],
                exponential_buckets(0.001, 2.0, 12).unwrap(),
            ),
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

pub(super) static INGEST_V2_METRICS: Lazy<IngestV2Metrics> = Lazy::new(IngestV2Metrics::default);
