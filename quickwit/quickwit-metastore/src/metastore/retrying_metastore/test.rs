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

use std::sync::atomic::{AtomicUsize, Ordering};

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

use super::retry::RetryParams;
use crate::{Metastore, MetastoreError, MetastoreResult, RetryingMetastore};

struct RetryTestMetastore {
    retry_count: AtomicUsize,
    error_count: usize,
    errors_to_return: Vec<MetastoreError>,
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

    async fn create_index(
        &self,
        _request: CreateIndexRequest,
    ) -> MetastoreResult<CreateIndexResponse> {
        self.try_success()?;
        Ok(CreateIndexResponse::default())
    }

    async fn index_metadata(
        &self,
        _request: IndexMetadataRequest,
    ) -> MetastoreResult<IndexMetadataResponse> {
        self.try_success()?;
        Ok(IndexMetadataResponse::default())
    }

    async fn list_indexes(
        &self,
        _request: ListIndexesRequest,
    ) -> MetastoreResult<ListIndexesResponse> {
        self.try_success()?;
        Ok(ListIndexesResponse::default())
    }

    async fn delete_index(&self, _request: DeleteIndexRequest) -> MetastoreResult<EmptyResponse> {
        self.try_success()?;
        Ok(EmptyResponse {})
    }

    async fn stage_splits(&self, request: StageSplitsRequest) -> MetastoreResult<EmptyResponse> {
        self.try_success()?;
        Ok(EmptyResponse {})
    }

    async fn publish_splits(
        &self,
        _request: PublishSplitsRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.try_success()?;
        Ok(EmptyResponse {})
    }

    async fn list_splits(
        &self,
        _request: ListSplitsRequest,
    ) -> MetastoreResult<ListSplitsResponse> {
        self.try_success()?;
        Ok(ListSplitsResponse::default())
    }

    async fn mark_splits_for_deletion(
        &self,
        _request: MarkSplitsForDeletionRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.try_success()?;
        Ok(EmptyResponse {})
    }

    async fn delete_splits(&self, request: DeleteSplitsRequest) -> MetastoreResult<EmptyResponse> {
        self.try_success()?;
        Ok(EmptyResponse {})
    }

    async fn add_source(&self, _request: AddSourceRequest) -> MetastoreResult<EmptyResponse> {
        self.try_success()?;
        Ok(EmptyResponse {})
    }

    async fn toggle_source(&self, _request: ToggleSourceRequest) -> MetastoreResult<EmptyResponse> {
        self.try_success()?;
        Ok(EmptyResponse {})
    }

    async fn reset_source_checkpoint(
        &self,
        _request: ResetSourceCheckpointRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.try_success()?;
        Ok(EmptyResponse {})
    }

    async fn delete_source(&self, _request: DeleteSourceRequest) -> MetastoreResult<EmptyResponse> {
        self.try_success()?;
        Ok(EmptyResponse {})
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

    async fn update_splits_delete_opstamp(
        &self,
        _request: UpdateSplitsDeleteOpstampRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.try_success()?;
        Ok(EmptyResponse {})
    }

    async fn list_delete_tasks(
        &self,
        _request: ListDeleteTasksRequest,
    ) -> MetastoreResult<ListDeleteTasksResponse> {
        self.try_success()?;
        Ok(ListDeleteTasksResponse::default())
    }
}

#[tokio::test]
async fn test_retryable_metastore_errors() {
    let metastore: RetryingMetastore = RetryTestMetastore::new_retrying_with_errors(
        5,
        &[
            MetastoreError::ConnectionError {
                message: "".to_string(),
            },
            MetastoreError::Io {
                message: "".to_string(),
            },
            MetastoreError::DbError {
                message: "".to_string(),
            },
            MetastoreError::InternalError {
                message: "".to_string(),
                cause: "".to_string(),
            },
        ],
    );

    // On retryable errors, if max retry count is not achieved, RetryingMetastore should retry until
    // success
    assert!(metastore.list_indexes(ListIndexesRequest {}).await.is_ok());

    let metastore: RetryingMetastore = RetryTestMetastore::new_retrying_with_errors(
        5,
        &[MetastoreError::IndexDoesNotExist {
            index_id: "".to_string(),
        }],
    );

    // On non-retryable errors, RetryingMetastore should exit with an error.
    assert!(metastore.list_indexes(ListIndexesRequest {}).await.is_err());
}

#[tokio::test]
async fn test_retryable_more_than_max_retry() {
    let metastore: RetryingMetastore = RetryTestMetastore::new_retrying_with_errors(
        3,
        &(0..4)
            .collect::<Vec<_>>()
            .iter()
            .map(|index| MetastoreError::ConnectionError {
                message: format!("{index}"),
            })
            .collect::<Vec<_>>(),
    );

    let error = metastore
        .list_indexes(ListIndexesRequest {})
        .await
        .unwrap_err();
    assert_eq!(
        error,
        MetastoreError::ConnectionError {
            message: "2".to_string() // Max 3 retries, last error index is 2
        }
    )
}

#[tokio::test]
async fn test_mixed_retryable_metastore_errors() {
    let metastore: RetryingMetastore = RetryTestMetastore::new_retrying_with_errors(
        5,
        &[
            MetastoreError::ConnectionError {
                message: "".to_string(),
            },
            MetastoreError::Io {
                message: "".to_string(),
            },
            // Non-retryable
            MetastoreError::SourceAlreadyExists {
                source_id: "".to_string(),
                source_type: "".to_string(),
            },
            MetastoreError::InternalError {
                message: "".to_string(),
                cause: "".to_string(),
            },
        ],
    );

    let error = metastore
        .list_indexes(ListIndexesRequest {})
        .await
        .unwrap_err();

    assert_eq!(
        error,
        MetastoreError::SourceAlreadyExists {
            source_id: "".to_string(),
            source_type: "".to_string(),
        }
    )
}
