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

use std::ops::Range;

use async_trait::async_trait;
use concat_idents::concat_idents;
use quickwit_common::uri::Uri;
use quickwit_config::SourceConfig;
use quickwit_doc_mapper::tag_pruning::TagFilterAst;
use quickwit_proto::metastore_api::{DeleteQuery, DeleteTask};

use crate::checkpoint::IndexCheckpointDelta;
use crate::{IndexMetadata, Metastore, MetastoreResult, Split, SplitMetadata, SplitState};

macro_rules! instrument {
    ($method_name:ident, $expr:expr, $($label:expr),*) => {
        let labels = &[$($label,)*];
        concat_idents!(metric_name = $method_name, _requests_total {
            crate::metrics::METASTORE_METRICS.metric_name.with_label_values(labels).inc();
        });
        let start = std::time::Instant::now();
        let (res, is_error) = match $expr {
            ok @ Ok(_) => {
                (ok, "false")
            },
            err @ Err(_) => {
                concat_idents!(metric_name = $method_name, _errors_total {
                    crate::metrics::METASTORE_METRICS.metric_name.with_label_values(labels).inc();
                });
                (err, "true")
            },
        };
        let elapsed = start.elapsed();
        let labels = &[$($label,)* is_error];
        concat_idents!(metric_name = $method_name, _duration_seconds {
            crate::metrics::METASTORE_METRICS.metric_name.with_label_values(labels).observe(elapsed.as_secs_f64());
        });
        if elapsed.as_secs() > 1 {
            let index_id = if labels.len() > 1 {
                labels[0]
            } else {
                ""
            };
            tracing::warn!(
                operation=stringify!($method_name),
                duration_millis=elapsed.as_millis(),
                index_id=index_id,
                "Slow metastore operation"
            );
        }
        return res;
    };
}

pub(crate) struct InstrumentedMetastore {
    underlying: Box<dyn Metastore>,
}

impl InstrumentedMetastore {
    pub fn new(metastore: Box<dyn Metastore>) -> Self {
        Self {
            underlying: metastore,
        }
    }
}

#[async_trait]
impl Metastore for InstrumentedMetastore {
    fn uri(&self) -> &Uri {
        self.underlying.uri()
    }

    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.underlying.check_connectivity().await
    }

    // Index API

    async fn create_index(&self, index_metadata: IndexMetadata) -> MetastoreResult<()> {
        let index_id = index_metadata.index_id.clone();
        instrument!(
            create_index,
            self.underlying.create_index(index_metadata).await,
            index_id.as_ref()
        );
    }

    async fn index_exists(&self, index_id: &str) -> MetastoreResult<bool> {
        instrument!(
            index_exists,
            self.underlying.index_exists(index_id).await,
            index_id
        );
    }

    async fn index_metadata(&self, index_id: &str) -> MetastoreResult<IndexMetadata> {
        instrument!(
            index_metadata,
            self.underlying.index_metadata(index_id).await,
            index_id
        );
    }

    async fn list_indexes_metadatas(&self) -> MetastoreResult<Vec<IndexMetadata>> {
        instrument!(
            list_indexes_metadatas,
            self.underlying.list_indexes_metadatas().await,
        );
    }

    async fn delete_index(&self, index_id: &str) -> MetastoreResult<()> {
        instrument!(
            delete_index,
            self.underlying.delete_index(index_id).await,
            index_id
        );
    }

    // Split API

    async fn stage_split(
        &self,
        index_id: &str,
        split_metadata: SplitMetadata,
    ) -> MetastoreResult<()> {
        instrument!(
            stage_split,
            self.underlying.stage_split(index_id, split_metadata).await,
            index_id
        );
    }

    async fn publish_splits<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
        replaced_split_ids: &[&'a str],
        checkpoint_delta_opt: Option<IndexCheckpointDelta>,
    ) -> MetastoreResult<()> {
        instrument!(
            publish_splits,
            self.underlying
                .publish_splits(
                    index_id,
                    split_ids,
                    replaced_split_ids,
                    checkpoint_delta_opt,
                )
                .await,
            index_id
        );
    }

    async fn list_splits(
        &self,
        index_id: &str,
        split_state: SplitState,
        time_range: Option<Range<i64>>,
        tags: Option<TagFilterAst>,
    ) -> MetastoreResult<Vec<Split>> {
        instrument!(
            list_splits,
            self.underlying
                .list_splits(index_id, split_state, time_range, tags)
                .await,
            index_id
        );
    }

    async fn list_all_splits(&self, index_id: &str) -> MetastoreResult<Vec<Split>> {
        instrument!(
            list_all_splits,
            self.underlying.list_all_splits(index_id).await,
            index_id
        );
    }

    async fn mark_splits_for_deletion<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        instrument!(
            mark_splits_for_deletion,
            self.underlying
                .mark_splits_for_deletion(index_id, split_ids)
                .await,
            index_id
        );
    }

    async fn delete_splits<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        instrument!(
            delete_splits,
            self.underlying.delete_splits(index_id, split_ids).await,
            index_id
        );
    }

    // Source API

    async fn add_source(&self, index_id: &str, source: SourceConfig) -> MetastoreResult<()> {
        let source_id = source.source_id.clone();
        instrument!(
            add_source,
            self.underlying.add_source(index_id, source).await,
            index_id,
            source_id.as_ref()
        );
    }

    async fn toggle_source(
        &self,
        index_id: &str,
        source_id: &str,
        enable: bool,
    ) -> MetastoreResult<()> {
        instrument!(
            toggle_source,
            self.underlying
                .toggle_source(index_id, source_id, enable)
                .await,
            index_id,
            source_id
        );
    }

    async fn reset_source_checkpoint(
        &self,
        index_id: &str,
        source_id: &str,
    ) -> MetastoreResult<()> {
        instrument!(
            reset_source_checkpoint,
            self.underlying
                .reset_source_checkpoint(index_id, source_id)
                .await,
            index_id,
            source_id
        );
    }

    async fn delete_source(&self, index_id: &str, source_id: &str) -> MetastoreResult<()> {
        instrument!(
            delete_source,
            self.underlying.delete_source(index_id, source_id).await,
            index_id,
            source_id
        );
    }

    // Delete tasks API
    async fn create_delete_task(&self, delete_query: DeleteQuery) -> MetastoreResult<DeleteTask> {
        let index_id = delete_query.index_id.clone();
        instrument!(
            create_delete_task,
            self.underlying.create_delete_task(delete_query).await,
            index_id.as_ref()
        );
    }

    async fn list_delete_tasks(
        &self,
        index_id: &str,
        opstamp_start: u64,
    ) -> MetastoreResult<Vec<DeleteTask>> {
        instrument!(
            list_delete_tasks,
            self.underlying
                .list_delete_tasks(index_id, opstamp_start)
                .await,
            index_id
        );
    }

    async fn last_delete_opstamp(&self, index_id: &str) -> MetastoreResult<u64> {
        instrument!(
            last_delete_opstamp,
            self.underlying.last_delete_opstamp(index_id).await,
            index_id
        );
    }

    async fn update_splits_delete_opstamp<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
        delete_opstamp: u64,
    ) -> MetastoreResult<()> {
        instrument!(
            update_splits_delete_opstamp,
            self.underlying
                .update_splits_delete_opstamp(index_id, split_ids, delete_opstamp)
                .await,
            index_id
        );
    }

    async fn list_stale_splits(
        &self,
        index_id: &str,
        delete_opstamp: u64,
        num_splits: usize,
    ) -> MetastoreResult<Vec<Split>> {
        instrument!(
            list_stale_splits,
            self.underlying
                .list_stale_splits(index_id, delete_opstamp, num_splits)
                .await,
            index_id
        );
    }
}
