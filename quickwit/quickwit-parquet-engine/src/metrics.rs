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

//! Prometheus metrics for the Pomsky Parquet Engine.
//!
//! Provides counters, histograms, and gauges for monitoring ingest throughput,
//! query performance, and error rates in production.

use std::sync::LazyLock;

use quickwit_metrics::{Counter, Histogram, counter, histogram};

/// Histogram buckets for duration measurements (in seconds).
/// Covers sub-millisecond to multi-second operations.
fn duration_buckets() -> Vec<f64> {
    vec![
        0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ]
}

/// Metrics for the Pomsky Parquet Engine.
#[derive(Clone)]
pub struct ParquetEngineMetrics {
    /// Total number of batches accumulated during indexing.
    pub index_batches_total: Counter,
    /// Total number of rows accumulated during indexing.
    pub index_rows_total: Counter,
    /// Total number of bytes received from IPC payloads during ingestion, by kind
    /// (points/sketches).
    pub ingest_bytes_total: Counter,
    /// Histogram of add_batch durations (seconds), including any triggered flush.
    pub index_batch_duration_seconds: Histogram,
    /// Total number of splits written to storage.
    pub splits_written_total: Counter,
    /// Total bytes written to split files.
    pub splits_bytes_written: Counter,
    /// Histogram of query execution durations (seconds).
    pub query_duration_seconds: Histogram,
    /// Total number of rows returned from queries.
    pub query_rows_returned: Counter,
    /// Errors by operation type and kind (points/sketches).
    pub errors_total: Counter,
}

static INDEX_BATCHES_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "index_batches_total",
        description: "Total number of batches accumulated during indexing.",
        subsystem: "metrics_engine",
    )
});

static INDEX_ROWS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "index_rows_total",
        description: "Total number of rows accumulated during indexing.",
        subsystem: "metrics_engine",
    )
});

static INGEST_BYTES_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "ingest_bytes_total",
        description: "Total number of bytes received from IPC payloads during ingestion.",
        subsystem: "metrics_engine",
    )
});

static INDEX_BATCH_DURATION_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "index_batch_duration_seconds",
        description: "Histogram of add_batch durations in seconds, including any triggered flush.",
        subsystem: "metrics_engine",
        buckets: duration_buckets(),
    )
});

static SPLITS_WRITTEN_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "splits_written_total",
        description: "Total number of splits written to storage.",
        subsystem: "metrics_engine",
    )
});

static SPLITS_BYTES_WRITTEN: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "splits_bytes_written",
        description: "Total bytes written to split files.",
        subsystem: "metrics_engine",
    )
});

static QUERY_DURATION_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "query_duration_seconds",
        description: "Histogram of query execution durations in seconds.",
        subsystem: "metrics_engine",
        buckets: duration_buckets(),
    )
});

static QUERY_ROWS_RETURNED: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "query_rows_returned",
        description: "Total number of rows returned from queries.",
        subsystem: "metrics_engine",
    )
});

static ERRORS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "errors_total",
        description: "Total errors by operation type and kind.",
        subsystem: "metrics_engine",
    )
});

impl Default for ParquetEngineMetrics {
    fn default() -> Self {
        Self {
            index_batches_total: INDEX_BATCHES_TOTAL.clone(),
            index_rows_total: INDEX_ROWS_TOTAL.clone(),
            ingest_bytes_total: INGEST_BYTES_TOTAL.clone(),
            index_batch_duration_seconds: INDEX_BATCH_DURATION_SECONDS.clone(),
            splits_written_total: SPLITS_WRITTEN_TOTAL.clone(),
            splits_bytes_written: SPLITS_BYTES_WRITTEN.clone(),
            query_duration_seconds: QUERY_DURATION_SECONDS.clone(),
            query_rows_returned: QUERY_ROWS_RETURNED.clone(),
            errors_total: ERRORS_TOTAL.clone(),
        }
    }
}

/// Global metrics instance for the metrics engine.
pub static PARQUET_ENGINE_METRICS: LazyLock<ParquetEngineMetrics> =
    LazyLock::new(ParquetEngineMetrics::default);
