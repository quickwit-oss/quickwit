// Copyright (C) 2022 Quickwit, Inc.
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
use quickwit_common::metrics::{new_counter_vec, new_histogram_vec, HistogramVec, IntCounterVec};

pub struct MetastoreMetrics {
    // Index API
    pub create_index_requests_total: IntCounterVec,
    pub create_index_errors_total: IntCounterVec,
    pub create_index_duration_seconds: HistogramVec,

    pub index_exists_requests_total: IntCounterVec,
    pub index_exists_errors_total: IntCounterVec,
    pub index_exists_duration_seconds: HistogramVec,

    pub index_metadata_requests_total: IntCounterVec,
    pub index_metadata_errors_total: IntCounterVec,
    pub index_metadata_duration_seconds: HistogramVec,

    pub list_indexes_metadatas_requests_total: IntCounterVec,
    pub list_indexes_metadatas_errors_total: IntCounterVec,
    pub list_indexes_metadatas_duration_seconds: HistogramVec,

    pub delete_index_requests_total: IntCounterVec,
    pub delete_index_errors_total: IntCounterVec,
    pub delete_index_duration_seconds: HistogramVec,

    // Split API
    pub stage_split_requests_total: IntCounterVec,
    pub stage_split_errors_total: IntCounterVec,
    pub stage_split_duration_seconds: HistogramVec,

    pub publish_splits_requests_total: IntCounterVec,
    pub publish_splits_errors_total: IntCounterVec,
    pub publish_splits_duration_seconds: HistogramVec,

    pub list_splits_requests_total: IntCounterVec,
    pub list_splits_errors_total: IntCounterVec,
    pub list_splits_duration_seconds: HistogramVec,

    pub list_all_splits_requests_total: IntCounterVec,
    pub list_all_splits_errors_total: IntCounterVec,
    pub list_all_splits_duration_seconds: HistogramVec,

    pub mark_splits_for_deletion_requests_total: IntCounterVec,
    pub mark_splits_for_deletion_errors_total: IntCounterVec,
    pub mark_splits_for_deletion_duration_seconds: HistogramVec,

    pub delete_splits_requests_total: IntCounterVec,
    pub delete_splits_errors_total: IntCounterVec,
    pub delete_splits_duration_seconds: HistogramVec,

    // Source API
    pub add_source_requests_total: IntCounterVec,
    pub add_source_errors_total: IntCounterVec,
    pub add_source_duration_seconds: HistogramVec,

    pub toggle_source_requests_total: IntCounterVec,
    pub toggle_source_errors_total: IntCounterVec,
    pub toggle_source_duration_seconds: HistogramVec,

    pub reset_source_checkpoint_requests_total: IntCounterVec,
    pub reset_source_checkpoint_errors_total: IntCounterVec,
    pub reset_source_checkpoint_duration_seconds: HistogramVec,

    pub delete_source_requests_total: IntCounterVec,
    pub delete_source_errors_total: IntCounterVec,
    pub delete_source_duration_seconds: HistogramVec,

    // Delete tasks API
    pub create_delete_task_requests_total: IntCounterVec,
    pub create_delete_task_errors_total: IntCounterVec,
    pub create_delete_task_duration_seconds: HistogramVec,

    pub list_delete_tasks_requests_total: IntCounterVec,
    pub list_delete_tasks_errors_total: IntCounterVec,
    pub list_delete_tasks_duration_seconds: HistogramVec,

    pub last_delete_opstamp_requests_total: IntCounterVec,
    pub last_delete_opstamp_errors_total: IntCounterVec,
    pub last_delete_opstamp_duration_seconds: HistogramVec,

    pub update_splits_delete_opstamp_requests_total: IntCounterVec,
    pub update_splits_delete_opstamp_errors_total: IntCounterVec,
    pub update_splits_delete_opstamp_duration_seconds: HistogramVec,

    pub list_stale_splits_requests_total: IntCounterVec,
    pub list_stale_splits_errors_total: IntCounterVec,
    pub list_stale_splits_duration_seconds: HistogramVec,
}

impl Default for MetastoreMetrics {
    fn default() -> Self {
        MetastoreMetrics {
            create_index_requests_total: new_counter_vec(
                "create_index_requests_total",
                "Number of create index requests",
                "quickwit_metastore",
                &["index"],
            ),
            create_index_errors_total: new_counter_vec(
                "create_index_errors_total",
                "Number of failed create index requests",
                "quickwit_metastore",
                &["index"],
            ),
            create_index_duration_seconds: new_histogram_vec(
                "create_index_duration_seconds",
                "Duration of create index requests",
                "quickwit_metastore",
                &["index", "error"],
            ),

            index_exists_requests_total: new_counter_vec(
                "index_exists_requests_total",
                "Number of index exists requests",
                "quickwit_metastore",
                &["index"],
            ),
            index_exists_errors_total: new_counter_vec(
                "index_exists_errors_total",
                "Number of failed index exists requests",
                "quickwit_metastore",
                &["index"],
            ),
            index_exists_duration_seconds: new_histogram_vec(
                "index_exists_duration_seconds",
                "Duration of a index exists requests",
                "quickwit_metastore",
                &["index", "error"],
            ),

            index_metadata_requests_total: new_counter_vec(
                "index_metadata_requests_total",
                "Number of index metadata requests",
                "quickwit_metastore",
                &["index"],
            ),
            index_metadata_errors_total: new_counter_vec(
                "index_metadata_errors_total",
                "Number of failed index metadata requests",
                "quickwit_metastore",
                &["index"],
            ),
            index_metadata_duration_seconds: new_histogram_vec(
                "index_metadata_duration_seconds",
                "Duration of index metadata requests",
                "quickwit_metastore",
                &["index", "error"],
            ),

            list_indexes_metadatas_requests_total: new_counter_vec(
                "list_indexes_metadatas_requests_total",
                "Number of list indexes metadatas requests",
                "quickwit_metastore",
                &[],
            ),
            list_indexes_metadatas_errors_total: new_counter_vec(
                "list_indexes_metadatas_errors_total",
                "Number of failed list indexes metadatas requests",
                "quickwit_metastore",
                &[],
            ),
            list_indexes_metadatas_duration_seconds: new_histogram_vec(
                "list_indexes_metadatas_duration_seconds",
                "Duration of list indexes metadatas requests",
                "quickwit_metastore",
                &["error"],
            ),

            delete_index_requests_total: new_counter_vec(
                "delete_index_requests_total",
                "Number of delete index requests",
                "quickwit_metastore",
                &["index"],
            ),
            delete_index_errors_total: new_counter_vec(
                "delete_index_errors_total",
                "Number of failed delete index requests",
                "quickwit_metastore",
                &["index"],
            ),
            delete_index_duration_seconds: new_histogram_vec(
                "delete_index_duration_seconds",
                "Duration of delete index requests",
                "quickwit_metastore",
                &["index", "error"],
            ),

            stage_split_requests_total: new_counter_vec(
                "stage_split_requests_total",
                "Number of stage split requests",
                "quickwit_metastore",
                &["index"],
            ),
            stage_split_errors_total: new_counter_vec(
                "stage_split_errors_total",
                "Number of failed stage split requests",
                "quickwit_metastore",
                &["index"],
            ),
            stage_split_duration_seconds: new_histogram_vec(
                "stage_split_duration_seconds",
                "Duration of stage split requests",
                "quickwit_metastore",
                &["index", "error"],
            ),

            publish_splits_requests_total: new_counter_vec(
                "publish_splits_requests_total",
                "Number of publish splits requests",
                "quickwit_metastore",
                &["index"],
            ),
            publish_splits_errors_total: new_counter_vec(
                "publish_splits_errors_total",
                "Number of failed publish splits requests",
                "quickwit_metastore",
                &["index"],
            ),
            publish_splits_duration_seconds: new_histogram_vec(
                "publish_splits_duration_seconds",
                "Duration of publish splits requests",
                "quickwit_metastore",
                &["index", "error"],
            ),

            list_splits_requests_total: new_counter_vec(
                "list_splits_requests_total",
                "Number of list splits requests",
                "quickwit_metastore",
                &["index"],
            ),
            list_splits_errors_total: new_counter_vec(
                "list_splits_errors_total",
                "Number of failed list splits requests",
                "quickwit_metastore",
                &["index"],
            ),
            list_splits_duration_seconds: new_histogram_vec(
                "list_splits_duration_seconds",
                "Duration of list splits requests",
                "quickwit_metastore",
                &["index", "error"],
            ),

            list_all_splits_requests_total: new_counter_vec(
                "list_all_splits_requests_total",
                "Number of list all splits requests",
                "quickwit_metastore",
                &["index"],
            ),
            list_all_splits_errors_total: new_counter_vec(
                "list_all_splits_errors_total",
                "Number of failed list all splits requests",
                "quickwit_metastore",
                &["index"],
            ),
            list_all_splits_duration_seconds: new_histogram_vec(
                "list_all_splits_duration_seconds",
                "Duration of list all splits requests",
                "quickwit_metastore",
                &["index", "error"],
            ),

            mark_splits_for_deletion_requests_total: new_counter_vec(
                "mark_splits_for_deletion_requests_total",
                "Number of mark splits for deletion requests",
                "quickwit_metastore",
                &["index"],
            ),
            mark_splits_for_deletion_errors_total: new_counter_vec(
                "mark_splits_for_deletion_errors_total",
                "Number of failed mark splits for deletion requests",
                "quickwit_metastore",
                &["index"],
            ),
            mark_splits_for_deletion_duration_seconds: new_histogram_vec(
                "mark_splits_for_deletion_duration_seconds",
                "Duration of mark splits for deletion requests",
                "quickwit_metastore",
                &["index", "error"],
            ),

            delete_splits_requests_total: new_counter_vec(
                "delete_splits_requests_total",
                "Number of delete splits requests",
                "quickwit_metastore",
                &["index"],
            ),
            delete_splits_errors_total: new_counter_vec(
                "delete_splits_errors_total",
                "Number of failed delete splits requests",
                "quickwit_metastore",
                &["index"],
            ),
            delete_splits_duration_seconds: new_histogram_vec(
                "delete_splits_duration_seconds",
                "Duration of delete splits requests",
                "quickwit_metastore",
                &["index", "error"],
            ),

            add_source_requests_total: new_counter_vec(
                "add_source_requests_total",
                "Number of add source requests",
                "quickwit_metastore",
                &["index", "source"],
            ),
            add_source_errors_total: new_counter_vec(
                "add_source_errors_total",
                "Number of failed add source requests",
                "quickwit_metastore",
                &["index", "source"],
            ),
            add_source_duration_seconds: new_histogram_vec(
                "add_source_duration_seconds",
                "Duration of a add source requests",
                "quickwit_metastore",
                &["index", "source", "error"],
            ),

            toggle_source_requests_total: new_counter_vec(
                "toggle_source_requests_total",
                "Number of toggle source requests",
                "quickwit_metastore",
                &["index", "source"],
            ),
            toggle_source_errors_total: new_counter_vec(
                "toggle_source_errors_total",
                "Number of failed toggle source requests",
                "quickwit_metastore",
                &["index", "source"],
            ),
            toggle_source_duration_seconds: new_histogram_vec(
                "toggle_source_duration_seconds",
                "Duration of toggle source requests",
                "quickwit_metastore",
                &["index", "source", "error"],
            ),

            reset_source_checkpoint_requests_total: new_counter_vec(
                "reset_source_checkpoint_requests_total",
                "Number of reset source checkpoint requests",
                "quickwit_metastore",
                &["index", "source"],
            ),
            reset_source_checkpoint_errors_total: new_counter_vec(
                "reset_source_checkpoint_errors_total",
                "Number of failed reset source checkpoint requests",
                "quickwit_metastore",
                &["index", "source"],
            ),
            reset_source_checkpoint_duration_seconds: new_histogram_vec(
                "reset_source_checkpoint_duration_seconds",
                "Duration of reset source checkpoint requests",
                "quickwit_metastore",
                &["index", "source", "error"],
            ),

            delete_source_requests_total: new_counter_vec(
                "delete_source_requests_total",
                "Number of delete source requests",
                "quickwit_metastore",
                &["index", "source"],
            ),
            delete_source_errors_total: new_counter_vec(
                "delete_source_errors_total",
                "Number of failed delete source requests",
                "quickwit_metastore",
                &["index", "source"],
            ),
            delete_source_duration_seconds: new_histogram_vec(
                "delete_source_duration_seconds",
                "Duration of delete source requests",
                "quickwit_metastore",
                &["index", "source", "error"],
            ),

            create_delete_task_requests_total: new_counter_vec(
                "create_delete_task_requests_total",
                "Number of create delete task requests",
                "quickwit_metastore",
                &["index"],
            ),
            create_delete_task_errors_total: new_counter_vec(
                "create_delete_task_errors_total",
                "Number of failed create delete task requests",
                "quickwit_metastore",
                &["index"],
            ),
            create_delete_task_duration_seconds: new_histogram_vec(
                "create_delete_task_duration_seconds",
                "Duration of create delete task requests",
                "quickwit_metastore",
                &["index", "error"],
            ),

            list_delete_tasks_requests_total: new_counter_vec(
                "list_delete_tasks_requests_total",
                "Number of list delete tasks requests",
                "quickwit_metastore",
                &["index"],
            ),
            list_delete_tasks_errors_total: new_counter_vec(
                "list_delete_tasks_errors_total",
                "Number of failed list delete tasks requests",
                "quickwit_metastore",
                &["index"],
            ),
            list_delete_tasks_duration_seconds: new_histogram_vec(
                "list_delete_tasks_duration_seconds",
                "Duration of list delete tasks requests",
                "quickwit_metastore",
                &["index", "error"],
            ),

            last_delete_opstamp_requests_total: new_counter_vec(
                "last_delete_opstamp_requests_total",
                "Number of last delete opstamp requests",
                "quickwit_metastore",
                &["index"],
            ),
            last_delete_opstamp_errors_total: new_counter_vec(
                "last_delete_opstamp_errors_total",
                "Number of failed last delete opstamp requests",
                "quickwit_metastore",
                &["index"],
            ),
            last_delete_opstamp_duration_seconds: new_histogram_vec(
                "last_delete_opstamp_duration_seconds",
                "Duration of last delete opstamp requests",
                "quickwit_metastore",
                &["index", "error"],
            ),

            update_splits_delete_opstamp_requests_total: new_counter_vec(
                "update_splits_delete_opstamp_requests_total",
                "Number of update splits delete opstamp requests",
                "quickwit_metastore",
                &["index"],
            ),
            update_splits_delete_opstamp_errors_total: new_counter_vec(
                "update_splits_delete_opstamp_errors_total",
                "Number of failed update splits delete opstamp requests",
                "quickwit_metastore",
                &["index"],
            ),
            update_splits_delete_opstamp_duration_seconds: new_histogram_vec(
                "update_splits_delete_opstamp_duration_seconds",
                "Duration of a update splits delete opstamp requests",
                "quickwit_metastore",
                &["index", "error"],
            ),

            list_stale_splits_requests_total: new_counter_vec(
                "list_stale_splits_requests_total",
                "Number of list stale splits requests",
                "quickwit_metastore",
                &["index"],
            ),
            list_stale_splits_errors_total: new_counter_vec(
                "list_stale_splits_errors_total",
                "Number of failed list stale splits requests",
                "quickwit_metastore",
                &["index"],
            ),
            list_stale_splits_duration_seconds: new_histogram_vec(
                "list_stale_splits_duration_seconds",
                "Duration of list stale splits requests",
                "quickwit_metastore",
                &["index", "error"],
            ),
        }
    }
}

/// `METASTORE_METRICS` exposes a bunch of metastore-related metrics through a Prometheus
/// endpoint.
pub static METASTORE_METRICS: Lazy<MetastoreMetrics> = Lazy::new(MetastoreMetrics::default);
