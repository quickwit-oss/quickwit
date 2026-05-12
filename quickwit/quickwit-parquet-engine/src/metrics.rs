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

//! Prometheus metrics for the Quickwit Parquet Engine.
//!
//! Provides counters, histograms, and gauges for monitoring ingest throughput,
//! query performance, and error rates in production.

use std::sync::LazyLock;

use quickwit_common::metrics::{
    Histogram, IntCounter, IntCounterVec, new_counter, new_counter_vec, new_histogram,
};

/// Subsystem name for all metrics engine metrics.
const SUBSYSTEM: &str = "metrics_engine";

/// Histogram buckets for duration measurements (in seconds).
/// Covers sub-millisecond to multi-second operations.
fn duration_buckets() -> Vec<f64> {
    vec![
        0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ]
}

/// Metrics for the Quickwit Parquet Engine.
#[derive(Clone)]
pub struct ParquetEngineMetrics {
    /// Total number of batches accumulated during indexing.
    pub index_batches_total: IntCounter,
    /// Total number of rows accumulated during indexing.
    pub index_rows_total: IntCounter,
    /// Total number of bytes received from IPC payloads during ingestion, by kind
    /// (points/sketches).
    pub ingest_bytes_total: IntCounterVec<1>,
    /// Histogram of add_batch durations (seconds), including any triggered flush.
    pub index_batch_duration_seconds: Histogram,
    /// Total number of splits written to storage.
    pub splits_written_total: IntCounter,
    /// Total bytes written to split files.
    pub splits_bytes_written: IntCounter,
    /// Histogram of query execution durations (seconds).
    pub query_duration_seconds: Histogram,
    /// Total number of rows returned from queries.
    pub query_rows_returned: IntCounter,
    /// Errors by operation type and kind (points/sketches).
    pub errors_total: IntCounterVec<2>,
}

impl Default for ParquetEngineMetrics {
    fn default() -> Self {
        Self {
            index_batches_total: new_counter(
                "index_batches_total",
                "Total number of batches accumulated during indexing.",
                SUBSYSTEM,
                &[],
            ),
            index_rows_total: new_counter(
                "index_rows_total",
                "Total number of rows accumulated during indexing.",
                SUBSYSTEM,
                &[],
            ),
            ingest_bytes_total: new_counter_vec(
                "ingest_bytes_total",
                "Total number of bytes received from IPC payloads during ingestion.",
                SUBSYSTEM,
                &[],
                ["kind"],
            ),
            index_batch_duration_seconds: new_histogram(
                "index_batch_duration_seconds",
                "Histogram of add_batch durations in seconds, including any triggered flush.",
                SUBSYSTEM,
                duration_buckets(),
            ),
            splits_written_total: new_counter(
                "splits_written_total",
                "Total number of splits written to storage.",
                SUBSYSTEM,
                &[],
            ),
            splits_bytes_written: new_counter(
                "splits_bytes_written",
                "Total bytes written to split files.",
                SUBSYSTEM,
                &[],
            ),
            query_duration_seconds: new_histogram(
                "query_duration_seconds",
                "Histogram of query execution durations in seconds.",
                SUBSYSTEM,
                duration_buckets(),
            ),
            query_rows_returned: new_counter(
                "query_rows_returned",
                "Total number of rows returned from queries.",
                SUBSYSTEM,
                &[],
            ),
            errors_total: new_counter_vec(
                "errors_total",
                "Total errors by operation type and kind.",
                SUBSYSTEM,
                &[],
                ["operation", "kind"],
            ),
        }
    }
}

/// Global metrics instance for the metrics engine.
pub static PARQUET_ENGINE_METRICS: LazyLock<ParquetEngineMetrics> =
    LazyLock::new(ParquetEngineMetrics::default);
