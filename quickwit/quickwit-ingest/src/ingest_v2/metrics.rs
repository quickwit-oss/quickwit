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

use once_cell::sync::Lazy;
use quickwit_common::metrics::{
    new_counter_vec, new_gauge, new_gauge_vec, new_histogram_vec, HistogramVec, IntCounterVec,
    IntGauge, IntGaugeVec,
};

pub(super) struct IngestV2Metrics {
    pub reset_shards_operations_total: IntCounterVec<1>,
    pub shards: IntGaugeVec<2>,
    pub wal_acquire_lock_requests_in_flight: IntGaugeVec<2>,
    pub wal_acquire_lock_request_duration_secs: HistogramVec<2>,
    pub wal_disk_usage_bytes: IntGauge,
    pub wal_memory_usage_bytes: IntGauge,
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
            shards: new_gauge_vec(
                "shards",
                "Number of shards.",
                "ingest",
                &[],
                ["state", "index_id"],
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
            wal_disk_usage_bytes: new_gauge(
                "wal_disk_usage_bytes",
                "WAL disk usage in bytes.",
                "ingest",
            ),
            wal_memory_usage_bytes: new_gauge(
                "wal_memory_usage_bytes",
                "WAL memory usage in bytes.",
                "ingest",
            ),
        }
    }
}

pub(super) static INGEST_V2_METRICS: Lazy<IngestV2Metrics> = Lazy::new(IngestV2Metrics::default);
