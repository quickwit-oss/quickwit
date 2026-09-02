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

use quickwit_common::metrics::exponential_buckets;
use quickwit_metastore::SplitMetadata;
use quickwit_metrics::{
    LabelNames, LazyCounter, LazyGauge, LazyHistogram, counter, histogram, label_names,
    label_values, lazy_counter, lazy_gauge, lazy_histogram,
};

pub(crate) const ACTOR_NAME: LabelNames<1> = label_names!("actor_name");
pub(crate) const COMPONENT: LabelNames<1> = label_names!("component");
pub(crate) const INDEX_SOURCE: LabelNames<2> = label_names!("index", "source");
pub(crate) const PUBLISHED_SPLIT: LabelNames<3> = label_names!("index", "source", "merge_ops");

pub(crate) static PROCESSED_DOCS_TOTAL: LazyCounter = lazy_counter!(
        name: "processed_docs_total",
        description: "Number of processed docs by index, source and processed status in [valid, schema_error, parse_error, transform_error]",
        subsystem: "indexing",
);

pub(crate) static PROCESSED_BYTES: LazyCounter = lazy_counter!(
        name: "processed_bytes",
        description: "Number of bytes of processed documents by index, source and processed status in [valid, schema_error, parse_error, transform_error]",
        subsystem: "indexing",
);

pub(crate) static DOCS_CLUSTER_GROUP_SIZE: LazyHistogram = lazy_histogram!(
        name: "docs_cluster_group_size",
        description: "Document cluster group size when finalizing an indexed split.",
        subsystem: "indexing",
        buckets: exponential_buckets(1.0, 10.0, 8).unwrap(),
);

pub(crate) static PUBLISHED_SPLIT_BYTES_TOTAL: LazyCounter = lazy_counter!(
    name: "published_split_bytes_total",
    description: "Compressed bytes in successfully published splits.",
    subsystem: "indexing",
);

pub(crate) static PUBLISHED_SPLIT_DOCS_TOTAL: LazyCounter = lazy_counter!(
    name: "published_split_docs_total",
    description: "Documents in successfully published splits.",
    subsystem: "indexing",
);

pub(crate) static PUBLISHED_SPLITS_TOTAL: LazyCounter = lazy_counter!(
    name: "published_splits_total",
    description: "Number of successfully published splits.",
    subsystem: "indexing",
);

pub(crate) static PUBLISHED_SPLIT_UNCOMPRESSED_BYTES_TOTAL: LazyCounter = lazy_counter!(
    name: "published_split_uncompressed_bytes_total",
    description: "Uncompressed document bytes in successfully published splits.",
    subsystem: "indexing",
);

pub(crate) static PUBLISHED_SPLIT_SIZE_BYTES: LazyHistogram = lazy_histogram!(
    name: "published_split_size_bytes",
    description: "Compressed size in bytes of successfully published splits.",
    subsystem: "indexing",
    buckets: exponential_buckets(1_000_000.0, 2.0, 14).unwrap(),
);

/// Records one split after the metastore has successfully published it.
///
/// All metrics deliberately use the same labels so ratios computed over a time
/// window describe the same set of splits.
pub(crate) fn record_published_split(index_id: &str, split: &SplitMetadata) {
    let index = quickwit_common::metrics::index_label(index_id);
    let labels = label_values!(
        PUBLISHED_SPLIT =>
        index.to_string(),
        split.source_id.to_string(),
        split.num_merge_ops.to_string()
    );
    let split_size_bytes = split.footer_offsets.end;

    counter!(parent: PUBLISHED_SPLIT_BYTES_TOTAL, labels: [labels.clone()])
        .inc_by(split_size_bytes);
    counter!(parent: PUBLISHED_SPLIT_DOCS_TOTAL, labels: [labels.clone()])
        .inc_by(split.num_docs as u64);
    counter!(parent: PUBLISHED_SPLITS_TOTAL, labels: [labels.clone()]).inc();
    counter!(parent: PUBLISHED_SPLIT_UNCOMPRESSED_BYTES_TOTAL, labels: [labels.clone()])
        .inc_by(split.uncompressed_docs_size_in_bytes);
    histogram!(parent: PUBLISHED_SPLIT_SIZE_BYTES, labels: [labels])
        .observe(split_size_bytes as f64);
}

pub(crate) static INDEXING_PIPELINES: LazyGauge = lazy_gauge!(
        name: "indexing_pipelines",
        description: "Number of running indexing pipelines",
        subsystem: "indexing",
);

pub(crate) static BACKPRESSURE_MICROS: LazyCounter = lazy_counter!(
        name: "backpressure_micros",
        description: "Amount of time spent in backpressure (in micros). This time only includes the amount of time spent waiting for a place in the queue of another actor.",
        subsystem: "indexing",
);

pub(crate) static AVAILABLE_CONCURRENT_UPLOAD_PERMITS: LazyGauge = lazy_gauge!(
        name: "concurrent_upload_available_permits_num",
        description: "Number of available concurrent upload permits by component in [merger, indexer]",
        subsystem: "indexing",
);

pub(crate) static SPLIT_BUILDERS: LazyGauge = lazy_gauge!(
        name: "split_builders",
        description: "Number of existing index writer instances.",
        subsystem: "indexing",
);

pub(crate) static ONGOING_MERGE_OPERATIONS: LazyGauge = lazy_gauge!(
        name: "ongoing_merge_operations",
        description: "Number of ongoing merge operations",
        subsystem: "indexing",
);

pub(crate) static PENDING_MERGE_OPERATIONS: LazyGauge = lazy_gauge!(
        name: "pending_merge_operations",
        description: "Number of pending merge operations",
        subsystem: "indexing",
);

pub(crate) static PENDING_MERGE_BYTES: LazyGauge = lazy_gauge!(
        name: "pending_merge_bytes",
        description: "Number of pending merge bytes",
        subsystem: "indexing",
);

// We use a lazy counter, as most users do not use Kafka.
#[cfg_attr(not(feature = "kafka"), allow(dead_code))]
pub(crate) static KAFKA_REBALANCE_TOTAL: LazyCounter = lazy_counter!(
        name: "kafka_rebalance_total",
        description: "Number of kafka rebalances",
        subsystem: "indexing",
);
