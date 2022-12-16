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

mod error;
mod retry;
#[cfg(test)]
mod test;

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_config::{IndexConfig, SourceConfig};
use quickwit_proto::metastore_api::{DeleteQuery, DeleteTask};

use self::retry::{retry, RetryParams};
use crate::checkpoint::IndexCheckpointDelta;
use crate::{IndexMetadata, ListSplitsQuery, Metastore, MetastoreResult, Split, SplitMetadata};

/// Retry layer for a [`Metastore`].
/// This is a band-aid solution for now. This will be removed after retry can be usable on
/// tonic level.
/// Tracking Issue: <https://github.com/tower-rs/tower/issues/682>
pub struct RetryingMetastore {
    inner: Box<dyn Metastore>,
    retry_params: RetryParams,
}

impl RetryingMetastore {
    /// Creates a retry layer for a [`Metastore`]
    pub fn new(metastore: Box<dyn Metastore>) -> Self {
        Self {
            inner: metastore,
            retry_params: RetryParams {
                max_attempts: 5,
                ..Default::default()
            },
        }
    }
}

#[async_trait]
impl Metastore for RetryingMetastore {
    fn uri(&self) -> &Uri {
        self.inner.uri()
    }

    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.inner.check_connectivity().await
    }

    async fn create_index(&self, index_config: IndexConfig) -> MetastoreResult<()> {
        retry(&self.retry_params, || async {
            self.inner.create_index(index_config.clone()).await
        })
        .await
    }

    async fn index_exists(&self, index_id: &str) -> MetastoreResult<bool> {
        retry(&self.retry_params, || async {
            self.inner.index_exists(index_id).await
        })
        .await
    }

    async fn index_metadata(&self, index_id: &str) -> MetastoreResult<IndexMetadata> {
        retry(&self.retry_params, || async {
            self.inner.index_metadata(index_id).await
        })
        .await
    }

    async fn list_indexes_metadatas(&self) -> MetastoreResult<Vec<IndexMetadata>> {
        retry(&self.retry_params, || async {
            self.inner.list_indexes_metadatas().await
        })
        .await
    }

    async fn delete_index(&self, index_id: &str) -> MetastoreResult<()> {
        retry(&self.retry_params, || async {
            self.inner.delete_index(index_id).await
        })
        .await
    }

    async fn stage_splits(
        &self,
        index_id: &str,
        split_metadata_list: Vec<SplitMetadata>,
    ) -> MetastoreResult<()> {
        retry(&self.retry_params, || async {
            self.inner
                .stage_splits(index_id, split_metadata_list.clone())
                .await
        })
        .await
    }

    async fn publish_splits<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
        replaced_split_ids: &[&'a str],
        checkpoint_delta_opt: Option<IndexCheckpointDelta>,
    ) -> MetastoreResult<()> {
        retry(&self.retry_params, || async {
            self.inner
                .publish_splits(
                    index_id,
                    split_ids,
                    replaced_split_ids,
                    checkpoint_delta_opt.clone(),
                )
                .await
        })
        .await
    }

    async fn list_splits<'a>(&self, query: ListSplitsQuery<'a>) -> MetastoreResult<Vec<Split>> {
        retry(&self.retry_params, || async {
            self.inner.list_splits(query.clone()).await
        })
        .await
    }

    async fn list_all_splits(&self, index_id: &str) -> MetastoreResult<Vec<Split>> {
        retry(&self.retry_params, || async {
            self.inner.list_all_splits(index_id).await
        })
        .await
    }

    async fn list_stale_splits(
        &self,
        index_id: &str,
        delete_opstamp: u64,
        num_splits: usize,
    ) -> MetastoreResult<Vec<Split>> {
        retry(&self.retry_params, || async {
            self.inner
                .list_stale_splits(index_id, delete_opstamp, num_splits)
                .await
        })
        .await
    }

    async fn mark_splits_for_deletion<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        retry(&self.retry_params, || async {
            self.inner
                .mark_splits_for_deletion(index_id, split_ids)
                .await
        })
        .await
    }

    async fn delete_splits<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        retry(&self.retry_params, || async {
            self.inner.delete_splits(index_id, split_ids).await
        })
        .await
    }

    async fn add_source(&self, index_id: &str, source: SourceConfig) -> MetastoreResult<()> {
        retry(&self.retry_params, || async {
            self.inner.add_source(index_id, source.clone()).await
        })
        .await
    }

    async fn toggle_source(
        &self,
        index_id: &str,
        source_id: &str,
        enable: bool,
    ) -> MetastoreResult<()> {
        retry(&self.retry_params, || async {
            self.inner.toggle_source(index_id, source_id, enable).await
        })
        .await
    }

    async fn reset_source_checkpoint(
        &self,
        index_id: &str,
        source_id: &str,
    ) -> MetastoreResult<()> {
        retry(&self.retry_params, || async {
            self.inner
                .reset_source_checkpoint(index_id, source_id)
                .await
        })
        .await
    }

    async fn delete_source(&self, index_id: &str, source_id: &str) -> MetastoreResult<()> {
        retry(&self.retry_params, || async {
            self.inner.delete_source(index_id, source_id).await
        })
        .await
    }

    async fn create_delete_task(&self, delete_query: DeleteQuery) -> MetastoreResult<DeleteTask> {
        retry(&self.retry_params, || async {
            self.inner.create_delete_task(delete_query.clone()).await
        })
        .await
    }

    async fn last_delete_opstamp(&self, index_id: &str) -> MetastoreResult<u64> {
        retry(&self.retry_params, || async {
            self.inner.last_delete_opstamp(index_id).await
        })
        .await
    }

    async fn update_splits_delete_opstamp<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
        delete_opstamp: u64,
    ) -> MetastoreResult<()> {
        retry(&self.retry_params, || async {
            self.inner
                .update_splits_delete_opstamp(index_id, split_ids, delete_opstamp)
                .await
        })
        .await
    }

    async fn list_delete_tasks(
        &self,
        index_id: &str,
        opstamp_start: u64,
    ) -> MetastoreResult<Vec<DeleteTask>> {
        retry(&self.retry_params, || async {
            self.inner.list_delete_tasks(index_id, opstamp_start).await
        })
        .await
    }
}
