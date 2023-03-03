// Copyright (C) 2023 Quickwit, Inc.
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

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_config::{IndexConfig, SourceConfig};
use quickwit_proto::metastore_api::{DeleteQuery, DeleteTask};

use crate::checkpoint::IndexCheckpointDelta;
use crate::{IndexMetadata, ListSplitsQuery, Metastore, MetastoreResult, Split, SplitMetadata};

macro_rules! instrument {
    ($expr:expr, [$operation:ident, $($label:expr),*]) => {
        let start = std::time::Instant::now();
        let labels = [stringify!($operation), $($label,)*];
        crate::metrics::METASTORE_METRICS.requests_total.with_label_values(labels).inc();
        let (res, is_error) = match $expr {
            ok @ Ok(_) => {
                (ok, "false")
            },
            err @ Err(_) => {
                crate::metrics::METASTORE_METRICS.request_errors_total.with_label_values(labels).inc();
                (err, "true")
            },
        };
        let elapsed = start.elapsed();
        let labels = [stringify!($operation), $($label,)* is_error];
        crate::metrics::METASTORE_METRICS.request_duration_seconds.with_label_values(labels).observe(elapsed.as_secs_f64());

        if elapsed.as_secs() > 1 {
            let index_id = if labels.len() > 2 {
                labels[1]
            } else {
                ""
            };
            tracing::warn!(
                operation=stringify!($operation),
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
    async fn create_index(&self, index_config: IndexConfig) -> MetastoreResult<()> {
        let index_id = index_config.index_id.to_string();
        instrument!(
            self.underlying.create_index(index_config).await,
            [create_index, index_id.as_str()]
        );
    }

    async fn index_exists(&self, index_id: &str) -> MetastoreResult<bool> {
        instrument!(
            self.underlying.index_exists(index_id).await,
            [index_exists, index_id]
        );
    }

    async fn index_metadata(&self, index_id: &str) -> MetastoreResult<IndexMetadata> {
        instrument!(
            self.underlying.index_metadata(index_id).await,
            [index_metadata, index_id]
        );
    }

    async fn list_indexes_metadatas(&self) -> MetastoreResult<Vec<IndexMetadata>> {
        instrument!(
            self.underlying.list_indexes_metadatas().await,
            [list_indexes_metadatas, ""]
        );
    }

    async fn delete_index(&self, index_id: &str) -> MetastoreResult<()> {
        instrument!(
            self.underlying.delete_index(index_id).await,
            [delete_index, index_id]
        );
    }

    // Split API

    async fn stage_splits(
        &self,
        index_id: &str,
        split_metadata_list: Vec<SplitMetadata>,
    ) -> MetastoreResult<()> {
        instrument!(
            self.underlying
                .stage_splits(index_id, split_metadata_list)
                .await,
            [stage_splits, index_id]
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
            self.underlying
                .publish_splits(
                    index_id,
                    split_ids,
                    replaced_split_ids,
                    checkpoint_delta_opt,
                )
                .await,
            [publish_splits, index_id]
        );
    }

    async fn list_splits<'a>(&self, query: ListSplitsQuery<'a>) -> MetastoreResult<Vec<Split>> {
        let index_id = query.index_id;
        instrument!(
            self.underlying.list_splits(query).await,
            [list_splits, index_id]
        );
    }

    async fn list_all_splits(&self, index_id: &str) -> MetastoreResult<Vec<Split>> {
        instrument!(
            self.underlying.list_all_splits(index_id).await,
            [list_all_splits, index_id]
        );
    }

    async fn mark_splits_for_deletion<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        instrument!(
            self.underlying
                .mark_splits_for_deletion(index_id, split_ids)
                .await,
            [mark_splits_for_deletion, index_id]
        );
    }

    async fn delete_splits<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        instrument!(
            self.underlying.delete_splits(index_id, split_ids).await,
            [delete_splits, index_id]
        );
    }

    // Source API

    async fn add_source(&self, index_id: &str, source: SourceConfig) -> MetastoreResult<()> {
        instrument!(
            self.underlying.add_source(index_id, source).await,
            [add_source, index_id]
        );
    }

    async fn toggle_source(
        &self,
        index_id: &str,
        source_id: &str,
        enable: bool,
    ) -> MetastoreResult<()> {
        instrument!(
            self.underlying
                .toggle_source(index_id, source_id, enable)
                .await,
            [toggle_source, index_id]
        );
    }

    async fn reset_source_checkpoint(
        &self,
        index_id: &str,
        source_id: &str,
    ) -> MetastoreResult<()> {
        instrument!(
            self.underlying
                .reset_source_checkpoint(index_id, source_id)
                .await,
            [reset_source_checkpoint, index_id]
        );
    }

    async fn delete_source(&self, index_id: &str, source_id: &str) -> MetastoreResult<()> {
        instrument!(
            self.underlying.delete_source(index_id, source_id).await,
            [delete_source, index_id]
        );
    }

    // Delete task API
    async fn create_delete_task(&self, delete_query: DeleteQuery) -> MetastoreResult<DeleteTask> {
        let index_id = delete_query.index_id.clone();
        instrument!(
            self.underlying.create_delete_task(delete_query).await,
            [create_delete_task, index_id.as_ref()]
        );
    }

    async fn list_delete_tasks(
        &self,
        index_id: &str,
        opstamp_start: u64,
    ) -> MetastoreResult<Vec<DeleteTask>> {
        instrument!(
            self.underlying
                .list_delete_tasks(index_id, opstamp_start)
                .await,
            [list_delete_tasks, index_id]
        );
    }

    async fn last_delete_opstamp(&self, index_id: &str) -> MetastoreResult<u64> {
        instrument!(
            self.underlying.last_delete_opstamp(index_id).await,
            [last_delete_opstamp, index_id]
        );
    }

    async fn update_splits_delete_opstamp<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
        delete_opstamp: u64,
    ) -> MetastoreResult<()> {
        instrument!(
            self.underlying
                .update_splits_delete_opstamp(index_id, split_ids, delete_opstamp)
                .await,
            [update_splits_delete_opstamp, index_id]
        );
    }

    async fn list_stale_splits(
        &self,
        index_id: &str,
        delete_opstamp: u64,
        num_splits: usize,
    ) -> MetastoreResult<Vec<Split>> {
        instrument!(
            self.underlying
                .list_stale_splits(index_id, delete_opstamp, num_splits)
                .await,
            [list_stale_splits, index_id]
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use quickwit_storage::RamStorage;

    use super::*;
    use crate::tests::test_suite::DefaultForTest;
    use crate::FileBackedMetastore;

    #[async_trait]
    impl DefaultForTest for InstrumentedMetastore {
        async fn default_for_test() -> Self {
            InstrumentedMetastore {
                underlying: Box::new(FileBackedMetastore::for_test(Arc::new(
                    RamStorage::default(),
                ))),
            }
        }
    }

    metastore_test_suite!(crate::metastore::instrumented_metastore::InstrumentedMetastore);
}
