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
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use itertools::Itertools;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, QueueCapacity};
use quickwit_common::uri::Uri;
use quickwit_config::IndexConfig;
use quickwit_doc_mapper::tag_pruning::TagFilterAst;
use quickwit_metastore::checkpoint::CheckpointDelta;
use quickwit_metastore::{
    IndexMetadata, Metastore, MetastoreError, Split, SplitMetadata, SplitState,
};
use quickwit_proto::{
    CreateIndexRequest, DeleteIndexRequest, FileEntry, IndexMetadataRequest, ListAllSplitsRequest,
    ListIndexesMetadatasRequest, ListSplitsRequest, MarkSplitsForDeletionRequest,
    PublishSplitsRequest, ReplaceSplitsRequest, StageSplitRequest,
};
use quickwit_storage::{Storage, StorageUriResolver};
use time::OffsetDateTime;
use tracing::{error, info};

use crate::error::IndexManagementError;
use crate::split_metadata_to_file_entry;

const MAX_CONCURRENT_STORAGE_REQUESTS: usize = if cfg!(test) { 2 } else { 10 };

pub type IndexManagementResult<T> = Result<T, IndexManagementError>;

/// The [`IndexManagementService`] is responsible for executing index CRUD operations.
/// This implies:
/// - Updating the metastore accordingly.
/// - Deleting splits on the index storage on index delete, splits replacement,...
///
/// It does not take care of splits marked as deleted as it is handled by the garbage
/// collector runned by the Janitor service.
///
/// Comments: I did not implement the [`Metastore`] trait for 2 reasons:
/// - The signature is a bit different for the delete_index and for the error type.
/// - Instead of using the [`Metastore`] trait, I'm thinking more aboug adding
///   an `IndexManagementService` trait that is implemented by the service and by
///   the client.
/// 
pub struct IndexManagementService {
    metastore: Arc<dyn Metastore>,
    storage_resolver: StorageUriResolver,
    default_index_root_uri: Uri,
}

#[async_trait]
impl Actor for IndexManagementService {
    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {}

    fn name(&self) -> String {
        "IndexManagementService".to_string()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(3)
    }

    async fn initialize(&mut self, _ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        Ok(())
    }
}

impl IndexManagementService {
    /// Creates an [`IndexManagementService`].
    pub fn new(
        metastore: Arc<dyn Metastore>,
        storage_resolver: StorageUriResolver,
        default_index_root_uri: Uri,
    ) -> Self {
        Self {
            metastore,
            storage_resolver,
            default_index_root_uri,
        }
    }
}

// Implements almost [`Metastore`] trait with two differences:
// - the `create_index` has an additional overwrite bool params.
// - the `delete_index` method also as deletes splits files and returnes files metadata.
impl IndexManagementService {
    /// Creates an index.
    /// If `overwrite` is true and if an index of the same name exists,
    /// the operation will delete the index in the [`Metastore`] and all
    /// related split in the related storage.
    pub async fn create_index(
        &self,
        index_config: IndexConfig,
        overwrite: bool,
    ) -> IndexManagementResult<IndexMetadata> {
        // Delete existing index if it exists.
        if overwrite {
            match self.delete_index(&index_config.index_id, false).await {
                Ok(_)
                | Err(IndexManagementError::MetastoreError(MetastoreError::IndexDoesNotExist {
                    index_id: _,
                })) => {
                    // Ignore IndexDoesNotExist error.
                }
                Err(error) => {
                    return Err(error);
                }
            }
        }
        index_config
            .validate()
            .map_err(|error| IndexManagementError::InvalidIndexConfig(error.to_string()))?;
        let index_id = index_config.index_id.clone();
        let index_uri = if let Some(index_uri) = &index_config.index_uri {
            index_uri.clone()
        } else {
            let index_uri = self.default_index_root_uri.join(&index_id).expect(
              "Failed to create default index URI. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.",
          );
            info!(
                index_id = %index_id,
                index_uri = %index_uri,
                "Index config does not specify `index_uri`, falling back to default value.",
            );
            index_uri
        };
        let index_metadata = IndexMetadata {
            index_id,
            index_uri,
            checkpoint: Default::default(),
            sources: index_config.sources(),
            doc_mapping: index_config.doc_mapping,
            indexing_settings: index_config.indexing_settings,
            search_settings: index_config.search_settings,
            create_timestamp: OffsetDateTime::now_utc().unix_timestamp(),
            update_timestamp: OffsetDateTime::now_utc().unix_timestamp(),
        };
        self.metastore.create_index(index_metadata).await?;
        let index_metadata = self
            .metastore
            .index_metadata(&index_config.index_id)
            .await?;
        Ok(index_metadata)
    }

    //// Gets all [`IndexMetadata`].
    pub async fn list_indexes_metadatas(&self) -> IndexManagementResult<Vec<IndexMetadata>> {
        let indexes_metadatas = self.metastore.list_indexes_metadatas().await?;
        Ok(indexes_metadatas)
    }

    /// Gets [`IndexMetadata`] for `index_id`.
    pub async fn index_metadata(&self, index_id: &str) -> IndexManagementResult<IndexMetadata> {
        let index_metadata = self.metastore.index_metadata(index_id).await?;
        Ok(index_metadata)
    }

    /// Deletes the index specified with `index_id`.
    /// This is equivalent to running `rm -rf <index path>` for a local index or
    /// `aws s3 rm --recursive <index path>` for a remote Amazon S3 index.
    ///
    /// * `index_id` - The target index Id.
    /// * `dry_run` - Should this only return a list of affected files without performing deletion.
    pub async fn delete_index(
        &self,
        index_id: &str,
        dry_run: bool,
    ) -> IndexManagementResult<Vec<FileEntry>> {
        let index_uri = self.metastore.index_metadata(index_id).await?.index_uri;
        let storage = self.storage_resolver.resolve(&index_uri)?;

        if dry_run {
            let all_splits = self
                .metastore
                .list_all_splits(index_id)
                .await?
                .into_iter()
                .map(|metadata| metadata.split_metadata)
                .collect::<Vec<_>>();

            let file_entries_to_delete: Vec<FileEntry> = all_splits
                .iter()
                .map(split_metadata_to_file_entry)
                .collect();
            return Ok(file_entries_to_delete);
        }

        // Schedule staged and published splits for deletion.
        let staged_splits = self
            .metastore
            .list_splits(index_id, SplitState::Staged, None, None)
            .await?;
        let published_splits = self
            .metastore
            .list_splits(index_id, SplitState::Published, None, None)
            .await?;
        let split_ids = staged_splits
            .iter()
            .chain(published_splits.iter())
            .map(|meta| meta.split_id())
            .collect::<Vec<_>>();
        self.metastore
            .mark_splits_for_deletion(index_id, &split_ids)
            .await?;

        // Select splits to delete
        let splits_to_delete = self
            .metastore
            .list_splits(index_id, SplitState::MarkedForDeletion, None, None)
            .await?
            .into_iter()
            .map(|metadata| metadata.split_metadata)
            .collect::<Vec<_>>();

        let deleted_entries = delete_splits_with_files(
            index_id,
            storage.clone(),
            self.metastore.clone(),
            splits_to_delete,
        )
        .await?;
        self.metastore.delete_index(index_id).await?;
        Ok(deleted_entries)
    }

    /// Stages split `index_id`.
    pub async fn stage_split(
        &self,
        index_id: &str,
        split_metadata: SplitMetadata,
    ) -> IndexManagementResult<()> {
        let result = self.metastore.stage_split(index_id, split_metadata).await?;
        Ok(result)
    }

    /// Publishes a list of splits.
    pub async fn publish_splits<'a>(
        &self,
        index_id: &str,
        source_id: &str,
        split_ids: &[&'a str],
        checkpoint_delta: CheckpointDelta,
    ) -> IndexManagementResult<()> {
        let result = self
            .metastore
            .publish_splits(index_id, source_id, split_ids, checkpoint_delta)
            .await?;
        Ok(result)
    }

    /// Replaces a list of splits with another list.
    pub async fn replace_splits<'a>(
        &self,
        index_id: &str,
        new_split_ids: &[&'a str],
        replaced_split_ids: &[&'a str],
    ) -> IndexManagementResult<()> {
        let result = self
            .metastore
            .replace_splits(index_id, new_split_ids, replaced_split_ids)
            .await?;
        Ok(result)
    }

    /// Lists all splits from index `index_id`.
    pub async fn list_all_splits(&self, index_id: &str) -> IndexManagementResult<Vec<Split>> {
        let splits = self.metastore.list_all_splits(index_id).await?;
        Ok(splits)
    }

    /// Lists the splits.
    pub async fn list_splits<'a>(
        &self,
        index_id: &str,
        split_state: SplitState,
        time_range: Option<Range<i64>>,
        tags: Option<TagFilterAst>,
    ) -> IndexManagementResult<Vec<Split>> {
        let splits = self
            .metastore
            .list_splits(index_id, split_state, time_range, tags)
            .await?;
        Ok(splits)
    }

    /// Marks a list of splits for deletion.
    pub async fn mark_splits_for_deletion<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> IndexManagementResult<()> {
        self.metastore
            .mark_splits_for_deletion(index_id, split_ids)
            .await?;
        Ok(())
    }
}

/// TODO: It's a copy/paste from quickwit-indexing where currently
/// garbage collection is done. We do not want a dependency
/// quickwit-indexing -> quickwit-index-management but the reverse.
/// Delete a list of splits from the storage and the metastore.
///
/// It should leave the index and the metastore in good state.
///
/// * `index_id` - The target index id.
/// * `storage - The storage managing the target index.
/// * `metastore` - The metastore managing the target index.
/// * `splits`  - The list of splits to delete.
/// * `ctx_opt` - A context for reporting progress (only useful within quickwit actor).
pub async fn delete_splits_with_files(
    index_id: &str,
    storage: Arc<dyn Storage>,
    metastore: Arc<dyn Metastore>,
    splits: Vec<SplitMetadata>,
) -> anyhow::Result<Vec<FileEntry>, IndexManagementError> {
    let mut deleted_file_entries = Vec::new();
    let mut deleted_split_ids = Vec::new();
    let mut failed_split_ids_to_error = Vec::new();

    let mut delete_splits_results_stream = tokio_stream::iter(splits.into_iter())
        .map(|split| {
            let storage_clone = storage.clone();
            async move {
                let file_entry = split_metadata_to_file_entry(&split);
                let split_filename = quickwit_common::split_file(split.split_id());
                let split_path = Path::new(&split_filename);
                let delete_result = storage_clone.delete(split_path).await;
                (split.split_id().to_string(), file_entry, delete_result)
            }
        })
        .buffer_unordered(MAX_CONCURRENT_STORAGE_REQUESTS);

    while let Some((split_id, file_entry, delete_split_res)) =
        delete_splits_results_stream.next().await
    {
        if let Err(error) = delete_split_res {
            error!(error = ?error, index_id = ?index_id, split_id = ?split_id, "Failed to delete split.");
            failed_split_ids_to_error.push((split_id, error));
        } else {
            deleted_split_ids.push(split_id);
            deleted_file_entries.push(file_entry);
        };
    }

    if !failed_split_ids_to_error.is_empty() {
        error!(index_id = ?index_id, failed_split_ids_to_error = ?failed_split_ids_to_error, "Failed to delete splits.");
        return Err(IndexManagementError::StorageError(
            failed_split_ids_to_error,
        ));
    }

    if !deleted_split_ids.is_empty() {
        let split_ids: Vec<&str> = deleted_split_ids.iter().map(String::as_str).collect();
        metastore
            .delete_splits(index_id, &split_ids)
            .await
            .map_err(IndexManagementError::MetastoreError)?;
    }

    Ok(deleted_file_entries)
}

#[async_trait]
impl Handler<IndexMetadataRequest> for IndexManagementService {
    type Reply = Result<IndexMetadata, IndexManagementError>;
    async fn handle(
        &mut self,
        req: IndexMetadataRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.index_metadata(&req.index_id).await)
    }
}

#[async_trait]
impl Handler<ListIndexesMetadatasRequest> for IndexManagementService {
    type Reply = Result<Vec<IndexMetadata>, IndexManagementError>;
    async fn handle(
        &mut self,
        _req: ListIndexesMetadatasRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.list_indexes_metadatas().await)
    }
}

#[async_trait]
impl Handler<CreateIndexRequest> for IndexManagementService {
    type Reply = Result<IndexMetadata, IndexManagementError>;
    async fn handle(
        &mut self,
        req: CreateIndexRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let reply = match serde_json::from_str(&req.index_config_serialized_json) {
            Ok(index_config) => self.create_index(index_config, false).await,
            Err(error) => Err(IndexManagementError::InternalError(format!(
                "Cannot deserialized `IndexConfig`: {}",
                error.to_string()
            ))),
        };
        Ok(reply)
    }
}

#[async_trait]
impl Handler<DeleteIndexRequest> for IndexManagementService {
    type Reply = Result<Vec<FileEntry>, IndexManagementError>;
    async fn handle(
        &mut self,
        req: DeleteIndexRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.delete_index(&req.index_id, false).await)
    }
}

#[async_trait]
impl Handler<ListAllSplitsRequest> for IndexManagementService {
    type Reply = Result<Vec<Split>, IndexManagementError>;
    async fn handle(
        &mut self,
        req: ListAllSplitsRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.list_all_splits(&req.index_id).await)
    }
}

#[async_trait]
impl Handler<ListSplitsRequest> for IndexManagementService {
    type Reply = Result<Vec<Split>, IndexManagementError>;
    async fn handle(
        &mut self,
        req: ListSplitsRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        // TODO: implement time range and tags filters and remove unwrap.
        let split_state = SplitState::from_str(&req.split_state).unwrap();
        Ok(self
            .list_splits(&req.index_id, split_state, None, None)
            .await)
    }
}

#[async_trait]
impl Handler<StageSplitRequest> for IndexManagementService {
    type Reply = Result<(), IndexManagementError>;
    async fn handle(
        &mut self,
        stage_split_req: StageSplitRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let split_metadata =
            match serde_json::from_str(&stage_split_req.split_metadata_serialized_json) {
                Ok(split_metadata) => split_metadata,
                Err(error) => {
                    return Ok(Err(IndexManagementError::InternalError(format!(
                        "Cannot deserialize split metadata: {}.",
                        error.to_string()
                    ))))
                }
            };
        Ok(self
            .stage_split(&stage_split_req.index_id, split_metadata)
            .await)
    }
}

#[async_trait]
impl Handler<PublishSplitsRequest> for IndexManagementService {
    type Reply = Result<(), IndexManagementError>;
    async fn handle(
        &mut self,
        publish_split_req: PublishSplitsRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let split_ids = publish_split_req
            .split_ids
            .iter()
            .map(String::as_str)
            .collect_vec();
        let checkpoint_delta =
            match serde_json::from_str(&publish_split_req.checkpoint_delta_serialized_json) {
                Ok(checkpoint_delta) => checkpoint_delta,
                Err(error) => {
                    return Ok(Err(IndexManagementError::InternalError(error.to_string())))
                }
            };
        Ok(self
            .publish_splits(
                &publish_split_req.index_id,
                &publish_split_req.source_id,
                &split_ids,
                checkpoint_delta,
            )
            .await)
    }
}

#[async_trait]
impl Handler<ReplaceSplitsRequest> for IndexManagementService {
    type Reply = Result<(), IndexManagementError>;
    async fn handle(
        &mut self,
        replace_splits_req: ReplaceSplitsRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let new_split_ids_ref: Vec<&str> = replace_splits_req
            .new_split_ids
            .iter()
            .map(String::as_str)
            .collect();
        let replaced_split_ids_ref: Vec<&str> = replace_splits_req
            .replaced_split_ids
            .iter()
            .map(String::as_str)
            .collect();
        Ok(self
            .replace_splits(
                &replace_splits_req.index_id,
                &new_split_ids_ref,
                &replaced_split_ids_ref,
            )
            .await)
    }
}

#[async_trait]
impl Handler<MarkSplitsForDeletionRequest> for IndexManagementService {
    type Reply = Result<(), IndexManagementError>;
    async fn handle(
        &mut self,
        mark_splits_for_deletion_req: MarkSplitsForDeletionRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let split_ids_ref: Vec<&str> = mark_splits_for_deletion_req
            .split_ids
            .iter()
            .map(String::as_str)
            .collect();
        Ok(self
            .mark_splits_for_deletion(&mark_splits_for_deletion_req.index_id, &split_ids_ref)
            .await)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use quickwit_actors::Universe;
    use quickwit_common::uri::Uri as QuickiwtUri;
    use quickwit_metastore::{IndexMetadata, MockMetastore};
    use quickwit_proto::IndexMetadataRequest;
    use quickwit_storage::StorageUriResolver;

    use super::IndexManagementService;

    #[tokio::test]
    async fn test_index_management_service() -> anyhow::Result<()> {
        let mut mock_metastore = MockMetastore::default();
        mock_metastore.expect_index_metadata().returning(move |_| {
            Ok(IndexMetadata::for_test(
                "test-index",
                "ram:///indexes/test-index",
            ))
        });
        let storage_resolver = StorageUriResolver::for_test();
        let default_index_root_uri = QuickiwtUri::new("ram:///test".to_string());
        let index_management_service = IndexManagementService::new(
            Arc::new(mock_metastore),
            storage_resolver,
            default_index_root_uri,
        );
        let universe = Universe::new();
        let (service_mailbox, service_handle) =
            universe.spawn_actor(index_management_service).spawn();
        let response = service_mailbox
            .ask_for_res(IndexMetadataRequest {
                index_id: "my-index".to_string(),
            })
            .await;
        let _ = service_handle.process_pending_and_observe().await;
        assert!(response.is_err());
        Ok(())
    }
}
