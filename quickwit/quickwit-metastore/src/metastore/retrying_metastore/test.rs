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

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use quickwit_common::retry::RetryParams;
use quickwit_common::uri::Uri;
use quickwit_config::{IndexConfig, SourceConfig};
use quickwit_proto::metastore::{
    AcquireShardsRequest, AcquireShardsResponse, CloseShardsRequest, CloseShardsResponse,
    DeleteQuery, DeleteShardsRequest, DeleteShardsResponse, DeleteTask, EntityKind,
    ListShardsRequest, ListShardsResponse, MetastoreError, MetastoreResult, OpenShardsRequest,
    OpenShardsResponse,
};
use quickwit_proto::{IndexUid, PublishToken};

use crate::checkpoint::IndexCheckpointDelta;
use crate::{
    IndexMetadata, ListIndexesQuery, ListSplitsQuery, Metastore, RetryingMetastore, Split,
    SplitMetadata,
};

struct RetryTestMetastore {
    retry_count: AtomicUsize,
    error_count: usize,
    errors_to_return: Vec<MetastoreError>,
}

impl fmt::Debug for RetryTestMetastore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetryTestMetastore").finish()
    }
}

impl RetryTestMetastore {
    fn new_with_errors(errors: &[MetastoreError]) -> Self {
        Self {
            retry_count: AtomicUsize::new(0),
            error_count: errors.len(),
            errors_to_return: errors.to_vec(),
        }
    }

    fn new_retrying_with_errors(
        max_attempts: usize,
        errors: &[MetastoreError],
    ) -> RetryingMetastore {
        RetryingMetastore {
            inner: Box::new(RetryTestMetastore::new_with_errors(errors)),
            retry_params: RetryParams {
                max_attempts,
                ..Default::default()
            },
        }
    }

    fn try_success(&self) -> MetastoreResult<()> {
        let retry_count = self.retry_count.load(Ordering::SeqCst);
        if retry_count < self.error_count {
            self.retry_count.fetch_add(1, Ordering::SeqCst);
            Err(self.errors_to_return[retry_count].clone())
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl Metastore for RetryTestMetastore {
    fn uri(&self) -> &Uri {
        unimplemented!()
    }

    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.try_success().map_err(anyhow::Error::from)
    }

    async fn create_index(&self, _index_config: IndexConfig) -> MetastoreResult<IndexUid> {
        let result = self.try_success();
        match result {
            Ok(_) => Ok(IndexUid::new("")),
            Err(err) => Err(err),
        }
    }

    async fn index_metadata(&self, index_id: &str) -> MetastoreResult<IndexMetadata> {
        let result = self.try_success();
        match result {
            Ok(_) => Ok(IndexMetadata::for_test(index_id, "")),
            Err(err) => Err(err),
        }
    }

    async fn list_indexes_metadatas(
        &self,
        _query: ListIndexesQuery,
    ) -> MetastoreResult<Vec<IndexMetadata>> {
        let result = self.try_success();
        match result {
            Ok(_) => Ok(Vec::new()),
            Err(err) => Err(err),
        }
    }

    async fn delete_index(&self, _index_uid: IndexUid) -> MetastoreResult<()> {
        self.try_success()
    }

    async fn stage_splits(
        &self,
        _index_uid: IndexUid,
        _split_metadata_list: Vec<SplitMetadata>,
    ) -> MetastoreResult<()> {
        self.try_success()
    }

    async fn publish_splits<'a>(
        &self,
        _index_uid: IndexUid,
        _staged_split_ids: &[&'a str],
        _replaced_split_ids: &[&'a str],
        _checkpoint_delta_opt: Option<IndexCheckpointDelta>,
        _publish_token_opt: Option<PublishToken>,
    ) -> MetastoreResult<()> {
        self.try_success()
    }

    async fn list_splits(&self, _query: ListSplitsQuery) -> MetastoreResult<Vec<Split>> {
        let result = self.try_success();
        match result {
            Ok(_) => Ok(Vec::new()),
            Err(err) => Err(err),
        }
    }

    async fn mark_splits_for_deletion<'a>(
        &self,
        _index_uid: IndexUid,
        _split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        self.try_success()
    }

    async fn delete_splits<'a>(
        &self,
        _index_uid: IndexUid,
        _split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        self.try_success()
    }

    async fn add_source(&self, _index_uid: IndexUid, _source: SourceConfig) -> MetastoreResult<()> {
        self.try_success()
    }

    async fn toggle_source(
        &self,
        _index_uid: IndexUid,
        _source_id: &str,
        _enable: bool,
    ) -> MetastoreResult<()> {
        self.try_success()
    }

    async fn reset_source_checkpoint(
        &self,
        _index_uid: IndexUid,
        _source_id: &str,
    ) -> MetastoreResult<()> {
        self.try_success()
    }

    async fn delete_source(&self, _index_uid: IndexUid, _source_id: &str) -> MetastoreResult<()> {
        self.try_success()
    }

    async fn create_delete_task(&self, _delete_query: DeleteQuery) -> MetastoreResult<DeleteTask> {
        let result = self.try_success();
        match result {
            Ok(_) => Ok(DeleteTask {
                create_timestamp: 0,
                opstamp: 0,
                delete_query: None,
            }),
            Err(err) => Err(err),
        }
    }

    async fn last_delete_opstamp(&self, _index_uid: IndexUid) -> MetastoreResult<u64> {
        let result = self.try_success();
        match result {
            Ok(_) => Ok(0),
            Err(err) => Err(err),
        }
    }

    async fn update_splits_delete_opstamp<'a>(
        &self,
        _index_uid: IndexUid,
        _split_ids: &[&'a str],
        _delete_opstamp: u64,
    ) -> MetastoreResult<()> {
        self.try_success()
    }

    async fn list_delete_tasks(
        &self,
        _index_uid: IndexUid,
        _opstamp_start: u64,
    ) -> MetastoreResult<Vec<DeleteTask>> {
        let result = self.try_success();
        match result {
            Ok(_) => Ok(Vec::new()),
            Err(err) => Err(err),
        }
    }

    async fn open_shards(
        &self,
        _request: OpenShardsRequest,
    ) -> MetastoreResult<OpenShardsResponse> {
        self.try_success().map(|_| Default::default())
    }

    async fn acquire_shards(
        &self,
        _request: AcquireShardsRequest,
    ) -> MetastoreResult<AcquireShardsResponse> {
        self.try_success().map(|_| Default::default())
    }

    async fn close_shards(
        &self,
        _request: CloseShardsRequest,
    ) -> MetastoreResult<CloseShardsResponse> {
        self.try_success().map(|_| Default::default())
    }

    async fn list_shards(
        &self,
        _request: ListShardsRequest,
    ) -> MetastoreResult<ListShardsResponse> {
        self.try_success().map(|_| Default::default())
    }

    async fn delete_shards(
        &self,
        _request: DeleteShardsRequest,
    ) -> MetastoreResult<DeleteShardsResponse> {
        self.try_success().map(|_| Default::default())
    }
}

#[tokio::test]
async fn test_retryable_metastore_errors() {
    let metastore: RetryingMetastore = RetryTestMetastore::new_retrying_with_errors(
        5,
        &[
            MetastoreError::Connection {
                message: "".to_string(),
            },
            MetastoreError::Io {
                message: "".to_string(),
            },
            MetastoreError::Db {
                message: "".to_string(),
            },
            MetastoreError::Internal {
                message: "".to_string(),
                cause: "".to_string(),
            },
        ],
    );

    // On retryable errors, if max retry count is not achieved, RetryingMetastore should retry until
    // success
    assert!(metastore
        .list_indexes_metadatas(ListIndexesQuery::All)
        .await
        .is_ok());

    let metastore: RetryingMetastore = RetryTestMetastore::new_retrying_with_errors(
        5,
        &[MetastoreError::NotFound(EntityKind::Index {
            index_id: "".to_string(),
        })],
    );

    // On non-retryable errors, RetryingMetastore should exit with an error.
    assert!(metastore
        .list_indexes_metadatas(ListIndexesQuery::All)
        .await
        .is_err());
}

#[tokio::test]
async fn test_retryable_more_than_max_retry() {
    let metastore: RetryingMetastore = RetryTestMetastore::new_retrying_with_errors(
        3,
        &(0..4)
            .collect::<Vec<_>>()
            .iter()
            .map(|index| MetastoreError::Connection {
                message: format!("{index}"),
            })
            .collect::<Vec<_>>(),
    );

    let error = metastore
        .list_indexes_metadatas(ListIndexesQuery::All)
        .await
        .unwrap_err();
    assert_eq!(
        error,
        MetastoreError::Connection {
            message: "2".to_string() // Max 3 retries, last error index is 2
        }
    )
}

#[tokio::test]
async fn test_mixed_retryable_metastore_errors() {
    let metastore: RetryingMetastore = RetryTestMetastore::new_retrying_with_errors(
        5,
        &[
            MetastoreError::Connection {
                message: "".to_string(),
            },
            MetastoreError::Io {
                message: "".to_string(),
            },
            // Non-retryable
            MetastoreError::AlreadyExists(EntityKind::Source {
                index_id: "".to_string(),
                source_id: "".to_string(),
            }),
            MetastoreError::Internal {
                message: "".to_string(),
                cause: "".to_string(),
            },
        ],
    );

    let error = metastore
        .list_indexes_metadatas(ListIndexesQuery::All)
        .await
        .unwrap_err();

    assert_eq!(
        error,
        MetastoreError::AlreadyExists(EntityKind::Source {
            index_id: "".to_string(),
            source_id: "".to_string()
        }),
    )
}
