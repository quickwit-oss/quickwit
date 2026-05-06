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

//! DogStatsD metrics for the Parquet compaction path.
//!
//! Quickwit installs the DogStatsD exporter in the CLI binary. This module uses
//! the `metrics` crate directly so these measurements are emitted to Datadog
//! instead of registering Prometheus collectors.

use std::time::Duration;

use quickwit_parquet_engine::merge::policy::ParquetMergeOperation;
use quickwit_parquet_engine::split::{ParquetSplitKind, ParquetSplitMetadata};

const STATUS_PLANNED: &str = "planned";
const STATUS_COMPLETED: &str = "completed";
const STATUS_FAILED: &str = "failed";

const STAGE_SCHEDULE: &str = "schedule";
const STAGE_DOWNLOAD: &str = "download";
const STAGE_MERGE: &str = "merge";
const STAGE_STAGING: &str = "staging";
const STAGE_UPLOAD: &str = "upload";
const STAGE_PUBLISH: &str = "publish";
const STAGE_SEQUENCER: &str = "sequencer";

const REASON_MATURE: &str = "mature";
const REASON_TIME_MATURED: &str = "time_matured";
const REASON_DUPLICATE: &str = "duplicate";
const REASON_MISSING_SCOPE: &str = "missing_scope";

const METRIC_OPERATIONS: &str = "parquet.compaction.operations";
const METRIC_FAILURES: &str = "parquet.compaction.failures";
const METRIC_SKIPPED_SPLITS: &str = "parquet.compaction.planner.skipped_splits";
const METRIC_INPUT_BYTES: &str = "parquet.compaction.input_bytes";
const METRIC_OUTPUT_BYTES: &str = "parquet.compaction.output_bytes";
const METRIC_INPUT_SPLITS: &str = "parquet.compaction.input_splits";
const METRIC_OUTPUT_SPLITS: &str = "parquet.compaction.output_splits";
const METRIC_ROWS: &str = "parquet.compaction.rows";
const METRIC_STAGED_SPLITS: &str = "parquet.compaction.staged_splits";
const METRIC_UPLOADED_SPLITS: &str = "parquet.compaction.uploaded_splits";
const METRIC_UPLOADED_BYTES: &str = "parquet.compaction.uploaded_bytes";
const METRIC_PUBLISHED_SPLITS: &str = "parquet.compaction.published_splits";
const METRIC_REPLACED_SPLITS: &str = "parquet.compaction.replaced_splits";
const METRIC_DURATION_SECONDS: &str = "parquet.compaction.duration_seconds";
const METRIC_INPUT_BYTES_PER_OPERATION: &str = "parquet.compaction.input_bytes_per_operation";
const METRIC_OUTPUT_BYTES_PER_OPERATION: &str = "parquet.compaction.output_bytes_per_operation";
const METRIC_INPUT_SPLITS_PER_OPERATION: &str = "parquet.compaction.input_splits_per_operation";
const METRIC_OUTPUT_SPLITS_PER_OPERATION: &str = "parquet.compaction.output_splits_per_operation";
const METRIC_ROWS_PER_OPERATION: &str = "parquet.compaction.rows_per_operation";
const METRIC_INPUT_OUTPUT_BYTES_RATIO: &str = "parquet.compaction.input_output_bytes_ratio";
const METRIC_PLANNER_ELIGIBLE_SPLITS: &str = "parquet.compaction.planner.eligible_splits";
const METRIC_PLANNER_ONGOING_OPERATIONS: &str = "parquet.compaction.planner.ongoing_operations";

fn kind_label(kind: ParquetSplitKind) -> &'static str {
    match kind {
        ParquetSplitKind::Metrics => "points",
        ParquetSplitKind::Sketches => "sketches",
    }
}

pub(super) fn index_id_from_uid(index_uid: &str) -> &str {
    index_uid
        .split_once(':')
        .map(|(index_id, _)| index_id)
        .unwrap_or(index_uid)
}

fn operation_labels(merge_operation: &ParquetMergeOperation) -> (&str, ParquetSplitKind) {
    merge_operation
        .splits
        .first()
        .map(|split| (index_id_from_uid(&split.index_uid), split.kind))
        .unwrap_or(("unknown", ParquetSplitKind::Metrics))
}

fn split_labels(split: &ParquetSplitMetadata) -> (&str, ParquetSplitKind) {
    (index_id_from_uid(&split.index_uid), split.kind)
}

pub(super) fn kind_from_index_id(index_id: &str) -> ParquetSplitKind {
    if quickwit_common::is_sketches_index(index_id) {
        ParquetSplitKind::Sketches
    } else {
        ParquetSplitKind::Metrics
    }
}

pub(super) struct ParquetCompactionMetrics;

impl ParquetCompactionMetrics {
    pub(super) fn record_mature_split(&self, split: &ParquetSplitMetadata) {
        self.record_skipped_split(split, REASON_MATURE);
    }

    pub(super) fn record_time_matured_split(&self, split: &ParquetSplitMetadata) {
        self.record_skipped_split(split, REASON_TIME_MATURED);
    }

    pub(super) fn record_duplicate_split(&self, split: &ParquetSplitMetadata) {
        self.record_skipped_split(split, REASON_DUPLICATE);
    }

    pub(super) fn record_missing_scope_split(&self, split: &ParquetSplitMetadata) {
        self.record_skipped_split(split, REASON_MISSING_SCOPE);
    }

    pub(super) fn record_planned_operation(&self, merge_operation: &ParquetMergeOperation) {
        let (index_id, kind) = operation_labels(merge_operation);
        increment_status_counter(METRIC_OPERATIONS, index_id, kind, STATUS_PLANNED, 1);
        increment_gauge(METRIC_PLANNER_ONGOING_OPERATIONS, index_id, kind, 1.0);
        increment_labeled_counter(
            METRIC_INPUT_BYTES,
            index_id,
            kind,
            merge_operation.total_size_bytes(),
        );
        increment_labeled_counter(
            METRIC_INPUT_SPLITS,
            index_id,
            kind,
            merge_operation.splits.len() as u64,
        );
        record_labeled_histogram(
            METRIC_INPUT_BYTES_PER_OPERATION,
            index_id,
            kind,
            merge_operation.total_size_bytes() as f64,
        );
        record_labeled_histogram(
            METRIC_INPUT_SPLITS_PER_OPERATION,
            index_id,
            kind,
            merge_operation.splits.len() as f64,
        );
    }

    pub(super) fn record_download_success(
        &self,
        merge_operation: &ParquetMergeOperation,
        duration: Duration,
    ) {
        self.record_stage_duration(merge_operation, STAGE_DOWNLOAD, duration);
    }

    pub(super) fn record_schedule_failure(
        &self,
        merge_operation: &ParquetMergeOperation,
        duration: Duration,
    ) {
        self.record_operation_failure(merge_operation, STAGE_SCHEDULE, duration);
    }

    pub(super) fn record_download_failure(
        &self,
        merge_operation: &ParquetMergeOperation,
        duration: Duration,
    ) {
        self.record_operation_failure(merge_operation, STAGE_DOWNLOAD, duration);
    }

    pub(super) fn record_merge_success(
        &self,
        merge_operation: &ParquetMergeOperation,
        output_splits: u64,
        output_bytes: u64,
        duration: Duration,
    ) {
        let (index_id, kind) = operation_labels(merge_operation);
        self.record_stage_duration(merge_operation, STAGE_MERGE, duration);
        increment_labeled_counter(METRIC_OUTPUT_BYTES, index_id, kind, output_bytes);
        increment_labeled_counter(METRIC_OUTPUT_SPLITS, index_id, kind, output_splits);
        increment_labeled_counter(
            METRIC_ROWS,
            index_id,
            kind,
            merge_operation.total_num_rows(),
        );
        record_labeled_histogram(
            METRIC_OUTPUT_BYTES_PER_OPERATION,
            index_id,
            kind,
            output_bytes as f64,
        );
        record_labeled_histogram(
            METRIC_OUTPUT_SPLITS_PER_OPERATION,
            index_id,
            kind,
            output_splits as f64,
        );
        record_labeled_histogram(
            METRIC_ROWS_PER_OPERATION,
            index_id,
            kind,
            merge_operation.total_num_rows() as f64,
        );
        if output_bytes > 0 {
            record_labeled_histogram(
                METRIC_INPUT_OUTPUT_BYTES_RATIO,
                index_id,
                kind,
                merge_operation.total_size_bytes() as f64 / output_bytes as f64,
            );
        }
    }

    pub(super) fn record_merge_failure(
        &self,
        merge_operation: &ParquetMergeOperation,
        duration: Duration,
    ) {
        self.record_operation_failure(merge_operation, STAGE_MERGE, duration);
    }

    pub(super) fn record_stage_failure(
        &self,
        index_id: &str,
        kind: ParquetSplitKind,
        duration: Duration,
    ) {
        self.record_operation_failure_for_labels(index_id, kind, STAGE_STAGING, duration);
    }

    pub(super) fn record_stage_success(
        &self,
        index_id: &str,
        kind: ParquetSplitKind,
        num_splits: usize,
        duration: Duration,
    ) {
        increment_labeled_counter(METRIC_STAGED_SPLITS, index_id, kind, num_splits as u64);
        record_stage_duration_for_labels(index_id, kind, STAGE_STAGING, duration);
    }

    pub(super) fn record_upload_success(
        &self,
        index_id: &str,
        kind: ParquetSplitKind,
        num_splits: usize,
        num_bytes: u64,
        duration: Duration,
    ) {
        increment_labeled_counter(METRIC_UPLOADED_SPLITS, index_id, kind, num_splits as u64);
        increment_labeled_counter(METRIC_UPLOADED_BYTES, index_id, kind, num_bytes);
        record_stage_duration_for_labels(index_id, kind, STAGE_UPLOAD, duration);
    }

    pub(super) fn record_upload_failure(
        &self,
        index_id: &str,
        kind: ParquetSplitKind,
        duration: Duration,
    ) {
        self.record_operation_failure_for_labels(index_id, kind, STAGE_UPLOAD, duration);
    }

    pub(super) fn record_sequencer_failure(
        &self,
        index_id: &str,
        kind: ParquetSplitKind,
        duration: Duration,
    ) {
        self.record_operation_failure_for_labels(index_id, kind, STAGE_SEQUENCER, duration);
    }

    pub(super) fn record_publish_success(
        &self,
        index_id: &str,
        kind: ParquetSplitKind,
        published_splits: usize,
        replaced_splits: usize,
        duration: Duration,
    ) {
        increment_status_counter(METRIC_OPERATIONS, index_id, kind, STATUS_COMPLETED, 1);
        increment_labeled_counter(
            METRIC_PUBLISHED_SPLITS,
            index_id,
            kind,
            published_splits as u64,
        );
        increment_labeled_counter(
            METRIC_REPLACED_SPLITS,
            index_id,
            kind,
            replaced_splits as u64,
        );
        decrement_gauge(METRIC_PLANNER_ONGOING_OPERATIONS, index_id, kind, 1.0);
        record_stage_duration_for_labels(index_id, kind, STAGE_PUBLISH, duration);
    }

    pub(super) fn record_publish_failure(
        &self,
        index_id: &str,
        kind: ParquetSplitKind,
        duration: Duration,
    ) {
        self.record_operation_failure_for_labels(index_id, kind, STAGE_PUBLISH, duration);
    }

    pub(super) fn set_planner_eligible_splits(
        &self,
        index_id: &str,
        kind: ParquetSplitKind,
        count: usize,
    ) {
        set_labeled_gauge(METRIC_PLANNER_ELIGIBLE_SPLITS, index_id, kind, count as f64);
    }

    fn record_skipped_split(&self, split: &ParquetSplitMetadata, reason: &'static str) {
        let (index_id, kind) = split_labels(split);
        metrics::counter!(
            METRIC_SKIPPED_SPLITS,
            "index" => index_id.to_string(),
            "kind" => kind_label(kind),
            "reason" => reason,
        )
        .increment(1);
    }

    fn record_stage_duration(
        &self,
        merge_operation: &ParquetMergeOperation,
        stage: &'static str,
        duration: Duration,
    ) {
        let (index_id, kind) = operation_labels(merge_operation);
        record_stage_duration_for_labels(index_id, kind, stage, duration);
    }

    fn record_operation_failure(
        &self,
        merge_operation: &ParquetMergeOperation,
        stage: &'static str,
        duration: Duration,
    ) {
        let (index_id, kind) = operation_labels(merge_operation);
        self.record_operation_failure_for_labels(index_id, kind, stage, duration);
    }

    fn record_operation_failure_for_labels(
        &self,
        index_id: &str,
        kind: ParquetSplitKind,
        stage: &'static str,
        duration: Duration,
    ) {
        increment_status_counter(METRIC_OPERATIONS, index_id, kind, STATUS_FAILED, 1);
        decrement_gauge(METRIC_PLANNER_ONGOING_OPERATIONS, index_id, kind, 1.0);
        metrics::counter!(
            METRIC_FAILURES,
            "index" => index_id.to_string(),
            "kind" => kind_label(kind),
            "stage" => stage,
        )
        .increment(1);
        record_stage_duration_for_labels(index_id, kind, stage, duration);
    }
}

fn increment_labeled_counter(
    name: &'static str,
    index_id: &str,
    kind: ParquetSplitKind,
    value: u64,
) {
    metrics::counter!(
        name,
        "index" => index_id.to_string(),
        "kind" => kind_label(kind),
    )
    .increment(value);
}

fn increment_status_counter(
    name: &'static str,
    index_id: &str,
    kind: ParquetSplitKind,
    status: &'static str,
    value: u64,
) {
    metrics::counter!(
        name,
        "index" => index_id.to_string(),
        "kind" => kind_label(kind),
        "status" => status,
    )
    .increment(value);
}

fn record_labeled_histogram(
    name: &'static str,
    index_id: &str,
    kind: ParquetSplitKind,
    value: f64,
) {
    metrics::histogram!(
        name,
        "index" => index_id.to_string(),
        "kind" => kind_label(kind),
    )
    .record(value);
}

fn record_stage_duration_for_labels(
    index_id: &str,
    kind: ParquetSplitKind,
    stage: &'static str,
    duration: Duration,
) {
    metrics::histogram!(
        METRIC_DURATION_SECONDS,
        "index" => index_id.to_string(),
        "kind" => kind_label(kind),
        "stage" => stage,
    )
    .record(duration.as_secs_f64());
}

fn set_labeled_gauge(name: &'static str, index_id: &str, kind: ParquetSplitKind, value: f64) {
    metrics::gauge!(
        name,
        "index" => index_id.to_string(),
        "kind" => kind_label(kind),
    )
    .set(value);
}

fn increment_gauge(name: &'static str, index_id: &str, kind: ParquetSplitKind, value: f64) {
    metrics::gauge!(
        name,
        "index" => index_id.to_string(),
        "kind" => kind_label(kind),
    )
    .increment(value);
}

fn decrement_gauge(name: &'static str, index_id: &str, kind: ParquetSplitKind, value: f64) {
    metrics::gauge!(
        name,
        "index" => index_id.to_string(),
        "kind" => kind_label(kind),
    )
    .decrement(value);
}

pub(super) static PARQUET_COMPACTION_METRICS: ParquetCompactionMetrics = ParquetCompactionMetrics;
