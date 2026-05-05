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

use quickwit_metrics::{Counter, Gauge, LabelNames, counter, gauge};

pub(crate) const ACTOR_NAME: LabelNames<1> = LabelNames::new(["actor_name"]);
pub(crate) const COMPONENT: LabelNames<1> = LabelNames::new(["component"]);

pub(crate) static PROCESSED_DOCS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "processed_docs_total",
        description: "Number of processed docs by index, source and processed status in [valid, schema_error, parse_error, transform_error]",
        subsystem: "indexing",
    )
});

pub(crate) static PROCESSED_BYTES: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "processed_bytes",
        description: "Number of bytes of processed documents by index, source and processed status in [valid, schema_error, parse_error, transform_error]",
        subsystem: "indexing",
    )
});

pub(crate) static INDEXING_PIPELINES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "indexing_pipelines",
        description: "Number of running indexing pipelines",
        subsystem: "indexing",
    )
});

pub(crate) static BACKPRESSURE_MICROS: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "backpressure_micros",
        description: "Amount of time spent in backpressure (in micros). This time only includes the amount of time spent waiting for a place in the queue of another actor.",
        subsystem: "indexing",
    )
});

pub(crate) static AVAILABLE_CONCURRENT_UPLOAD_PERMITS: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "concurrent_upload_available_permits_num",
        description: "Number of available concurrent upload permits by component in [merger, indexer]",
        subsystem: "indexing",
    )
});

pub(crate) static SPLIT_BUILDERS: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "split_builders",
        description: "Number of existing index writer instances.",
        subsystem: "indexing",
    )
});

pub(crate) static ONGOING_MERGE_OPERATIONS: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "ongoing_merge_operations",
        description: "Number of ongoing merge operations",
        subsystem: "indexing",
    )
});

pub(crate) static PENDING_MERGE_OPERATIONS: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "pending_merge_operations",
        description: "Number of pending merge operations",
        subsystem: "indexing",
    )
});

pub(crate) static PENDING_MERGE_BYTES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "pending_merge_bytes",
        description: "Number of pending merge bytes",
        subsystem: "indexing",
    )
});

// We use a lazy counter, as most users do not use Kafka.
#[cfg_attr(not(feature = "kafka"), allow(dead_code))]
pub(crate) static KAFKA_REBALANCE_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "kafka_rebalance_total",
        description: "Number of kafka rebalances",
        subsystem: "indexing",
    )
});
