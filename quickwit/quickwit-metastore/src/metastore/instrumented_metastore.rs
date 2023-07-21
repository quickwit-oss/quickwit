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
use quickwit_proto::metastore::{
    AddSourceRequest, CreateIndexRequest, CreateIndexResponse, DeleteIndexRequest, DeleteQuery,
    DeleteSourceRequest, DeleteSplitsRequest, DeleteTask, EmptyResponse, IndexMetadataRequest,
    IndexMetadataResponse, ListDeleteTasksRequest, ListDeleteTasksResponse, ListIndexesRequest,
    ListIndexesResponse, ListSplitsRequest, ListSplitsResponse, MarkSplitsForDeletionRequest,
    PublishSplitsRequest, ResetSourceCheckpointRequest, StageSplitsRequest, ToggleSourceRequest,
    UpdateSplitsDeleteOpstampRequest,
};
use quickwit_proto::IndexUid;

use crate::{Metastore, MetastoreResult};

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

    async fn create_index(
        &self,
        request: CreateIndexRequest,
    ) -> MetastoreResult<CreateIndexResponse> {
        let index_id = "FIXME";
        instrument!(
            self.underlying.create_index(request).await,
            [create_index, index_id]
        );
    }

    async fn index_metadata(
        &self,
        request: IndexMetadataRequest,
    ) -> MetastoreResult<IndexMetadataResponse> {
        let index_id = request.index_id.clone();
        instrument!(
            self.underlying.index_metadata(request).await,
            [index_metadata, &index_id]
        );
    }

    async fn list_indexes(
        &self,
        request: ListIndexesRequest,
    ) -> MetastoreResult<ListIndexesResponse> {
        instrument!(
            self.underlying.list_indexes(request).await,
            [list_indexes_metadatas, ""]
        );
    }

    async fn delete_index(&self, request: DeleteIndexRequest) -> MetastoreResult<EmptyResponse> {
        let index_uid: IndexUid = request.index_uid.clone().into();
        instrument!(
            self.underlying.delete_index(request).await,
            [delete_index, index_uid.index_id()]
        );
    }

    // Split API

    async fn stage_splits(&self, request: StageSplitsRequest) -> MetastoreResult<EmptyResponse> {
        let index_uid: IndexUid = request.index_uid.clone().into();
        instrument!(
            self.underlying.stage_splits(request).await,
            [stage_splits, index_uid.index_id()]
        );
    }

    async fn publish_splits(
        &self,
        request: PublishSplitsRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_uid: IndexUid = request.index_uid.clone().into();
        instrument!(
            self.underlying.publish_splits(request).await,
            [publish_splits, index_uid.index_id()]
        );
    }

    async fn list_splits(&self, request: ListSplitsRequest) -> MetastoreResult<ListSplitsResponse> {
        let index_uid: IndexUid = request.index_uid.clone().into();
        instrument!(
            self.underlying.list_splits(request).await,
            [list_splits, index_uid.index_id()]
        );
    }

    async fn mark_splits_for_deletion(
        &self,
        request: MarkSplitsForDeletionRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_uid: IndexUid = request.index_uid.clone().into();
        instrument!(
            self.underlying.mark_splits_for_deletion(request).await,
            [mark_splits_for_deletion, index_uid.index_id()]
        );
    }

    async fn delete_splits(&self, request: DeleteSplitsRequest) -> MetastoreResult<EmptyResponse> {
        let index_uid: IndexUid = request.index_uid.clone().into();
        instrument!(
            self.underlying.delete_splits(request).await,
            [delete_splits, index_uid.index_id()]
        );
    }

    // Source API

    async fn add_source(&self, request: AddSourceRequest) -> MetastoreResult<EmptyResponse> {
        let index_uid: IndexUid = request.index_uid.clone().into();
        instrument!(
            self.underlying.add_source(request).await,
            [add_source, index_uid.index_id()]
        );
    }

    async fn toggle_source(&self, request: ToggleSourceRequest) -> MetastoreResult<EmptyResponse> {
        let index_uid: IndexUid = request.index_uid.clone().into();
        instrument!(
            self.underlying.toggle_source(request).await,
            [toggle_source, index_uid.index_id()]
        );
    }

    async fn reset_source_checkpoint(
        &self,
        request: ResetSourceCheckpointRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_uid: IndexUid = request.index_uid.clone().into();
        instrument!(
            self.underlying.reset_source_checkpoint(request).await,
            [reset_source_checkpoint, index_uid.index_id()]
        );
    }

    async fn delete_source(&self, request: DeleteSourceRequest) -> MetastoreResult<EmptyResponse> {
        let index_uid: IndexUid = request.index_uid.clone().into();
        instrument!(
            self.underlying.delete_source(request).await,
            [delete_source, index_uid.index_id()]
        );
    }

    // Delete task API

    async fn create_delete_task(&self, delete_query: DeleteQuery) -> MetastoreResult<DeleteTask> {
        let index_uid: IndexUid = delete_query.index_uid.clone().into();
        instrument!(
            self.underlying.create_delete_task(delete_query).await,
            [create_delete_task, index_uid.index_id()]
        );
    }

    async fn list_delete_tasks(
        &self,
        request: ListDeleteTasksRequest,
    ) -> MetastoreResult<ListDeleteTasksResponse> {
        let index_uid: IndexUid = request.index_uid.clone().into();
        instrument!(
            self.underlying.list_delete_tasks(request).await,
            [list_delete_tasks, index_uid.index_id()]
        );
    }

    async fn last_delete_opstamp(&self, index_uid: IndexUid) -> MetastoreResult<u64> {
        instrument!(
            self.underlying.last_delete_opstamp(index_uid.clone()).await,
            [last_delete_opstamp, index_uid.index_id()]
        );
    }

    async fn update_splits_delete_opstamp(
        &self,
        request: UpdateSplitsDeleteOpstampRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let index_uid: IndexUid = request.index_uid.clone().into();
        instrument!(
            self.underlying.update_splits_delete_opstamp(request).await,
            [update_splits_delete_opstamp, index_uid.index_id()]
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
