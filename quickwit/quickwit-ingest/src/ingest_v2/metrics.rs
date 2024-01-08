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
    pub grpc_requests_total: IntCounterVec<4>,
    pub grpc_requests_in_flight: IntGaugeVec<3>,
    pub grpc_request_duration_secs: HistogramVec<4>,
    pub shards: IntGaugeVec<2>,
    pub wal_acquire_lock_requests_in_flight: IntGaugeVec<2>,
    pub wal_acquire_lock_request_duration_secs: HistogramVec<2>,
    pub wal_disk_usage_bytes: IntGauge,
    pub wal_memory_usage_bytes: IntGauge,
}

impl Default for IngestV2Metrics {
    fn default() -> Self {
        Self {
            grpc_requests_total: new_counter_vec(
                "grpc_requests_total",
                "Total number of gRPC requests processed.",
                "quickwit_ingest",
                ["component", "kind", "operation", "status"],
            ),
            grpc_requests_in_flight: new_gauge_vec(
                "grpc_requests_in_flight",
                "Number of gRPC requests in flight.",
                "quickwit_ingest",
                ["component", "kind", "operation"],
            ),
            grpc_request_duration_secs: new_histogram_vec(
                "grpc_request_duration_secs",
                "Duration of gRPC requests in seconds.",
                "quickwit_ingest",
                ["component", "kind", "operation", "status"],
            ),
            shards: new_gauge_vec(
                "shards",
                "Number of shards.",
                "quickwit_ingest",
                ["state", "index_id"],
            ),
            wal_acquire_lock_requests_in_flight: new_gauge_vec(
                "wal_acquire_lock_requests_in_flight",
                "Number of acquire lock requests in flight.",
                "quickwit_ingest",
                ["operation", "type"],
            ),
            wal_acquire_lock_request_duration_secs: new_histogram_vec(
                "wal_acquire_lock_request_duration_secs",
                "Duration of acquire lock requests in seconds.",
                "quickwit_ingest",
                ["operation", "type"],
            ),
            wal_disk_usage_bytes: new_gauge(
                "wal_disk_usage_bytes",
                "Disk usage of the write-ahead log in bytes.",
                "quickwit_ingest",
            ),
            wal_memory_usage_bytes: new_gauge(
                "wal_memory_usage_bytes",
                "Memory usage of the write-ahead log in bytes.",
                "quickwit_ingest",
            ),
        }
    }
}

pub(super) static INGEST_V2_METRICS: Lazy<IngestV2Metrics> = Lazy::new(IngestV2Metrics::default);
