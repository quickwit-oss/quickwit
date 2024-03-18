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
    new_counter_vec, new_gauge, new_gauge_vec, new_histogram_vec, HistogramVec, IntCounterVec,
    IntGauge, IntGaugeVec,
};

pub(super) struct IngestV2Metrics {
    pub reset_shards_operations_total: IntCounterVec<1>,
    pub open_shards: IntGauge,
    pub closed_shards: IntGauge,
    pub wal_acquire_lock_requests_in_flight: IntGaugeVec<2>,
    pub wal_acquire_lock_request_duration_secs: HistogramVec<2>,
    pub wal_disk_used_bytes: IntGauge,
    pub wal_memory_used_bytes: IntGauge,
}

impl Default for IngestV2Metrics {
    fn default() -> Self {
        Self {
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
        .in_flight_data
        .wal
        .set(wal_usage.memory_allocated_bytes as i64);
    INGEST_V2_METRICS
        .wal_memory_used_bytes
        .set(wal_usage.memory_used_bytes as i64);
}

pub(super) static INGEST_V2_METRICS: Lazy<IngestV2Metrics> = Lazy::new(IngestV2Metrics::default);
