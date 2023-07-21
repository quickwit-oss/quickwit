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

mod error;
mod retry;
#[cfg(test)]
mod test;

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_proto::metastore::{
    AddSourceRequest, CreateIndexRequest, CreateIndexResponse, DeleteIndexRequest, DeleteQuery,
    DeleteSourceRequest, DeleteSplitsRequest, DeleteTask, EmptyResponse, IndexMetadataRequest,
    IndexMetadataResponse, ListDeleteTasksRequest, ListDeleteTasksResponse, ListIndexesRequest,
    ListIndexesResponse, ListSplitsRequest, ListSplitsResponse, MarkSplitsForDeletionRequest,
    PublishSplitsRequest, ResetSourceCheckpointRequest, StageSplitsRequest, ToggleSourceRequest,
    UpdateSplitsDeleteOpstampRequest,
};
use quickwit_proto::IndexUid;

use self::retry::{retry, RetryParams};
use crate::{Metastore, MetastoreResult};

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

    async fn create_index(
        &self,
        request: CreateIndexRequest,
    ) -> MetastoreResult<CreateIndexResponse> {
        retry(&self.retry_params, || async {
            self.inner.create_index(request.clone()).await
        })
        .await
    }

    async fn index_metadata(
        &self,
        request: IndexMetadataRequest,
    ) -> MetastoreResult<IndexMetadataResponse> {
        retry(&self.retry_params, || async {
            self.inner.index_metadata(request.clone()).await
        })
        .await
    }

    async fn list_indexes(
        &self,
        request: ListIndexesRequest,
    ) -> MetastoreResult<ListIndexesResponse> {
        retry(&self.retry_params, || async {
            self.inner.list_indexes(request.clone()).await
        })
        .await
    }

    async fn delete_index(&self, request: DeleteIndexRequest) -> MetastoreResult<EmptyResponse> {
        retry(&self.retry_params, || async {
            self.inner.delete_index(request.clone()).await
        })
        .await
    }

    async fn stage_splits(&self, request: StageSplitsRequest) -> MetastoreResult<EmptyResponse> {
        retry(&self.retry_params, || async {
            self.inner.stage_splits(request.clone()).await
        })
        .await
    }

    async fn publish_splits(
        &self,
        request: PublishSplitsRequest,
    ) -> MetastoreResult<EmptyResponse> {
        retry(&self.retry_params, || async {
            self.inner.publish_splits(request.clone()).await
        })
        .await
    }

    async fn list_splits(&self, request: ListSplitsRequest) -> MetastoreResult<ListSplitsResponse> {
        retry(&self.retry_params, || async {
            self.inner.list_splits(request.clone()).await
        })
        .await
    }

    // async fn list_stale_splits(
    //     &self,
    //     index_uid: IndexUid,
    //     delete_opstamp: u64,
    //     num_splits: usize,
    // ) -> MetastoreResult<Vec<Split>> { retry(&self.retry_params, || async { self.inner
    //   .list_stale_splits(index_uid.clone(), delete_opstamp, num_splits) .await }) .await
    // }

    async fn mark_splits_for_deletion(
        &self,
        request: MarkSplitsForDeletionRequest,
    ) -> MetastoreResult<EmptyResponse> {
        retry(&self.retry_params, || async {
            self.inner.mark_splits_for_deletion(request.clone()).await
        })
        .await
    }

    async fn delete_splits(&self, request: DeleteSplitsRequest) -> MetastoreResult<EmptyResponse> {
        retry(&self.retry_params, || async {
            self.inner.delete_splits(request.clone()).await
        })
        .await
    }

    async fn add_source(&self, request: AddSourceRequest) -> MetastoreResult<EmptyResponse> {
        retry(&self.retry_params, || async {
            self.inner.add_source(request.clone()).await
        })
        .await
    }

    async fn toggle_source(&self, request: ToggleSourceRequest) -> MetastoreResult<EmptyResponse> {
        retry(&self.retry_params, || async {
            self.inner.toggle_source(request.clone()).await
        })
        .await
    }

    async fn reset_source_checkpoint(
        &self,
        request: ResetSourceCheckpointRequest,
    ) -> MetastoreResult<EmptyResponse> {
        retry(&self.retry_params, || async {
            self.inner.reset_source_checkpoint(request.clone()).await
        })
        .await
    }

    async fn delete_source(&self, request: DeleteSourceRequest) -> MetastoreResult<EmptyResponse> {
        retry(&self.retry_params, || async {
            self.inner.delete_source(request.clone()).await
        })
        .await
    }

    async fn create_delete_task(&self, delete_query: DeleteQuery) -> MetastoreResult<DeleteTask> {
        retry(&self.retry_params, || async {
            self.inner.create_delete_task(delete_query.clone()).await
        })
        .await
    }

    async fn last_delete_opstamp(&self, index_uid: IndexUid) -> MetastoreResult<u64> {
        retry(&self.retry_params, || async {
            self.inner.last_delete_opstamp(index_uid.clone()).await
        })
        .await
    }

    async fn update_splits_delete_opstamp(
        &self,
        request: UpdateSplitsDeleteOpstampRequest,
    ) -> MetastoreResult<EmptyResponse> {
        retry(&self.retry_params, || async {
            self.inner
                .update_splits_delete_opstamp(request.clone())
                .await
        })
        .await
    }

    async fn list_delete_tasks(
        &self,
        request: ListDeleteTasksRequest,
    ) -> MetastoreResult<ListDeleteTasksResponse> {
        retry(&self.retry_params, || async {
            self.inner.list_delete_tasks(request.clone()).await
        })
        .await
    }
}
