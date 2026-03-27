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

use once_cell::sync::Lazy;
use quickwit_common::metrics::{
    IntCounter, IntCounterVec, IntGauge, IntGaugeVec, new_counter, new_counter_vec, new_gauge,
    new_gauge_vec,
};

pub struct IndexerMetrics {
    pub processed_docs_total: IntCounterVec<2>,
    pub processed_bytes: IntCounterVec<2>,
    pub indexing_pipelines: IntGaugeVec<1>,
    pub backpressure_micros: IntCounterVec<1>,
    pub available_concurrent_upload_permits: IntGaugeVec<1>,
    pub split_builders: IntGauge,
    pub ongoing_merge_operations: IntGauge,
    pub pending_merge_operations: IntGauge,
    pub pending_merge_bytes: IntGauge,
    // We use a lazy counter, as most users do not use Kafka.
    #[cfg_attr(not(feature = "kafka"), allow(dead_code))]
    pub kafka_rebalance_total: Lazy<IntCounter>,
}

impl Default for IndexerMetrics {
    fn default() -> Self {
        IndexerMetrics {
            processed_docs_total: new_counter_vec(
                "processed_docs_total",
                "Number of processed docs by index, source and processed status in [valid, \
                 schema_error, parse_error, transform_error]",
                "indexing",
                &[],
                ["index", "docs_processed_status"],
            ),
            processed_bytes: new_counter_vec(
                "processed_bytes",
                "Number of bytes of processed documents by index, source and processed status in \
                 [valid, schema_error, parse_error, transform_error]",
                "indexing",
                &[],
                ["index", "docs_processed_status"],
            ),
            indexing_pipelines: new_gauge_vec(
                "indexing_pipelines",
                "Number of running indexing pipelines",
                "indexing",
                &[],
                ["index"],
            ),
            backpressure_micros: new_counter_vec(
                "backpressure_micros",
                "Amount of time spent in backpressure (in micros). This time only includes the \
                 amount of time spent waiting for a place in the queue of another actor.",
                "indexing",
                &[],
                ["actor_name"],
            ),
            available_concurrent_upload_permits: new_gauge_vec(
                "concurrent_upload_available_permits_num",
                "Number of available concurrent upload permits by component in [merger, indexer]",
                "indexing",
                &[],
                ["component"],
            ),
            split_builders: new_gauge(
                "split_builders",
                "Number of existing index writer instances.",
                "indexing",
                &[],
            ),
            ongoing_merge_operations: new_gauge(
                "ongoing_merge_operations",
                "Number of ongoing merge operations",
                "indexing",
                &[],
            ),
            pending_merge_operations: new_gauge(
                "pending_merge_operations",
                "Number of pending merge operations",
                "indexing",
                &[],
            ),
            pending_merge_bytes: new_gauge(
                "pending_merge_bytes",
                "Number of pending merge bytes",
                "indexing",
                &[],
            ),
            kafka_rebalance_total: Lazy::new(|| {
                new_counter(
                    "kafka_rebalance_total",
                    "Number of kafka rebalances",
                    "indexing",
                    &[],
                )
            }),
        }
    }
}

/// `INDEXER_METRICS` exposes indexing related metrics through a prometheus
/// endpoint.
pub static INDEXER_METRICS: Lazy<IndexerMetrics> = Lazy::new(IndexerMetrics::default);
