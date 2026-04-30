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

use quickwit_common::metrics::{Counter, Gauge, counter, gauge};

pub struct IndexerMetrics {
    pub processed_docs_total: Counter,
    pub processed_bytes: Counter,
    pub indexing_pipelines: Gauge,
    pub backpressure_micros: Counter,
    pub available_concurrent_upload_permits: Gauge,
    pub split_builders: Gauge,
    pub ongoing_merge_operations: Gauge,
    pub pending_merge_operations: Gauge,
    pub pending_merge_bytes: Gauge,
    // We use a lazy counter, as most users do not use Kafka.
    #[cfg_attr(not(feature = "kafka"), allow(dead_code))]
    pub kafka_rebalance_total: LazyLock<Counter>,
}

static PROCESSED_DOCS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "processed_docs_total",
        description: "Number of processed docs by index, source and processed status in [valid, schema_error, parse_error, transform_error]",
        subsystem: "indexing",
    )
});

static PROCESSED_BYTES: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "processed_bytes",
        description: "Number of bytes of processed documents by index, source and processed status in [valid, schema_error, parse_error, transform_error]",
        subsystem: "indexing",
    )
});

static INDEXING_PIPELINES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "indexing_pipelines",
        description: "Number of running indexing pipelines",
        subsystem: "indexing",
    )
});

static BACKPRESSURE_MICROS: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "backpressure_micros",
        description: "Amount of time spent in backpressure (in micros). This time only includes the amount of time spent waiting for a place in the queue of another actor.",
        subsystem: "indexing",
    )
});

static AVAILABLE_CONCURRENT_UPLOAD_PERMITS: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "concurrent_upload_available_permits_num",
        description: "Number of available concurrent upload permits by component in [merger, indexer]",
        subsystem: "indexing",
    )
});

static SPLIT_BUILDERS: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "split_builders",
        description: "Number of existing index writer instances.",
        subsystem: "indexing",
    )
});

static ONGOING_MERGE_OPERATIONS: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "ongoing_merge_operations",
        description: "Number of ongoing merge operations",
        subsystem: "indexing",
        observable: true,
    )
});

static PENDING_MERGE_OPERATIONS: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "pending_merge_operations",
        description: "Number of pending merge operations",
        subsystem: "indexing",
    )
});

static PENDING_MERGE_BYTES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "pending_merge_bytes",
        description: "Number of pending merge bytes",
        subsystem: "indexing",
    )
});

static KAFKA_REBALANCE_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "kafka_rebalance_total",
        description: "Number of kafka rebalances",
        subsystem: "indexing",
    )
});

impl Default for IndexerMetrics {
    fn default() -> Self {
        IndexerMetrics {
            processed_docs_total: PROCESSED_DOCS_TOTAL.clone(),
            processed_bytes: PROCESSED_BYTES.clone(),
            indexing_pipelines: INDEXING_PIPELINES.clone(),
            backpressure_micros: BACKPRESSURE_MICROS.clone(),
            available_concurrent_upload_permits: AVAILABLE_CONCURRENT_UPLOAD_PERMITS.clone(),
            split_builders: SPLIT_BUILDERS.clone(),
            ongoing_merge_operations: ONGOING_MERGE_OPERATIONS.clone(),
            pending_merge_operations: PENDING_MERGE_OPERATIONS.clone(),
            pending_merge_bytes: PENDING_MERGE_BYTES.clone(),
            kafka_rebalance_total: LazyLock::new(|| KAFKA_REBALANCE_TOTAL.clone()),
        }
    }
}

/// `INDEXER_METRICS` exposes indexing related metrics through a prometheus
/// endpoint.
pub static INDEXER_METRICS: LazyLock<IndexerMetrics> = LazyLock::new(IndexerMetrics::default);
