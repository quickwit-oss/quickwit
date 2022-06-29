// Copyright (C) 2021 Quickwit, Inc.
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

use std::sync::Arc;

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_cluster::Cluster;
use quickwit_common::uri::Uri;
use quickwit_config::IndexConfig;
use quickwit_core::IndexServiceError;
use quickwit_metastore::{IndexMetadata, Metastore, Split, SplitMetadata};
use quickwit_proto::{
    GetIndexMetadataRequest, GetSplitsMetadatasRequest, PublishSplitRequest, ReplaceSplitsRequest,
    StageSplitRequest,
};
use quickwit_storage::StorageUriResolver;
use serde::Serialize;

use crate::actors::planner::RefreshIndexingPlan;

use super::IndexingPlanner;

/// IndexService's role is to:
/// - Update the metastore for CRUD requests on index, source and split resources.
/// - Send a message EvaluateIndexingPlan on index create/delete/update.
///
/// Moreover, the `IndexService` should be aware of all publish tokens to accept write requests.
/// A publish token is hash of (index_id, source_id, num_total_tasks, task_idx)
pub struct IndexService {
    metastore: Arc<dyn Metastore>,
    cluster: Arc<Cluster>,
    indexing_planner_mailbox: Mailbox<IndexingPlanner>,
    storage_resolver: StorageUriResolver,
    default_index_root_uri: Uri,
}

#[async_trait]
impl Actor for IndexService {
    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {}

    fn name(&self) -> String {
        "IndexService".to_string()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(3)
    }

    async fn initialize(&mut self, _ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        Ok(())
    }
}

impl IndexService {
    pub fn new(
        metastore: Arc<dyn Metastore>,
        cluster: Arc<Cluster>,
        indexing_planner_mailbox: Mailbox<IndexingPlanner>,
        storage_resolver: StorageUriResolver,
        default_index_root_uri: Uri,
    ) -> Self {
        Self {
            metastore,
            cluster,
            indexing_planner_mailbox,
            storage_resolver,
            default_index_root_uri,
        }
    }

    /// Get an index from `index_id`.
    pub async fn get_index(&self, _index_id: &str) -> Result<IndexMetadata, IndexServiceError> {
        todo!();
    }

    /// Get all splits from index `index_id`.
    pub async fn get_all_splits(&self, _index_id: &str) -> Result<Vec<Split>, IndexServiceError> {
        todo!();
    }

    /// Get all indexes.
    pub async fn get_indexes(&self) -> Result<Vec<IndexMetadata>, IndexServiceError> {
        todo!()
    }

    /// Creates an index from `IndexConfig`.
    pub async fn create_index(
        &self,
        _index_config: IndexConfig,
        _publish_token: &str,
    ) -> Result<IndexMetadata, IndexServiceError> {
        // TODO: how to handle a sending error?
        let _ = self.indexing_planner_mailbox.send_message(RefreshIndexingPlan {}).await;
        todo!();
    }

    /// Deletes the index specified with `index_id`.
    /// This is equivalent to running `rm -rf <index path>` for a local index or
    /// `aws s3 rm --recursive <index path>` for a remote Amazon S3 index.
    ///
    /// * `index_id` - The target index Id.
    /// * `dry_run` - Should this only return a list of affected files without performing deletion.
    pub async fn delete_index(
        &self,
        _index_id: &str,
        _dry_run: bool,
        _publish_token: &str,
    ) -> Result<Vec<FileEntry>, IndexServiceError> {
        // TODO: how to handle a sending error?
        let _ = self.indexing_planner_mailbox.send_message(RefreshIndexingPlan {}).await;
        todo!();
    }

    pub async fn stage_split(
        &self,
        _index_id: &str,
        _split_metadata: SplitMetadata,
        _publish_token: &str,
    ) -> Result<(), IndexServiceError> {
        todo!();
    }

    pub async fn publish_split(
        &self,
        _index_id: &str,
        _split_metadata: SplitMetadata,
        _publish_token: &str,
    ) -> Result<(), IndexServiceError> {
        todo!();
    }

    pub async fn replace_splits<'a>(
        &self,
        _index_id: &str,
        _new_split_ids: &[&'a str],
        _replaced_split_ids: &[&'a str],
        _publish_token: &str,
    ) -> Result<(), IndexServiceError> {
        todo!();
    }
}

// Dumb struct to define signature of `delete_index`.
// To be deleted once this function is implemented.
#[derive(Serialize)]
pub struct FileEntry {}

#[async_trait]
impl Handler<GetIndexMetadataRequest> for IndexService {
    type Reply = Result<IndexMetadata, IndexServiceError>;
    async fn handle(
        &mut self,
        get_index_metadata_req: GetIndexMetadataRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.get_index(&get_index_metadata_req.index_id).await)
    }
}

#[async_trait]
impl Handler<GetSplitsMetadatasRequest> for IndexService {
    type Reply = Result<Vec<Split>, IndexServiceError>;
    async fn handle(
        &mut self,
        get_splits_metadatas_req: GetSplitsMetadatasRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self
            .get_all_splits(&get_splits_metadatas_req.index_id)
            .await)
    }
}

#[async_trait]
impl Handler<StageSplitRequest> for IndexService {
    type Reply = Result<(), IndexServiceError>;
    async fn handle(
        &mut self,
        stage_split_req: StageSplitRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let split_metadata: SplitMetadata =
            serde_json::from_str(&stage_split_req.split_metadata_serialized_json).unwrap();
        Ok(self
            .stage_split(
                &stage_split_req.index_id,
                split_metadata,
                &stage_split_req.publish_token,
            )
            .await)
    }
}

#[async_trait]
impl Handler<PublishSplitRequest> for IndexService {
    type Reply = Result<(), IndexServiceError>;
    async fn handle(
        &mut self,
        publish_split_req: PublishSplitRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let split_metadata: SplitMetadata =
            serde_json::from_str(&publish_split_req.split_metadata_serialized_json).unwrap();
        Ok(self
            .publish_split(
                &publish_split_req.index_id,
                split_metadata,
                &publish_split_req.publish_token,
            )
            .await)
    }
}

#[async_trait]
impl Handler<ReplaceSplitsRequest> for IndexService {
    type Reply = Result<(), IndexServiceError>;
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
                &replace_splits_req.publish_token,
            )
            .await)
    }
}

#[derive(Debug)]
pub struct GetIndexesRequest {}

#[async_trait]
impl Handler<GetIndexesRequest> for IndexService {
    type Reply = Result<Vec<IndexMetadata>, IndexServiceError>;
    async fn handle(
        &mut self,
        _get_all_index_metadata_req: GetIndexesRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.get_indexes().await)
    }
}

#[derive(Debug)]
pub struct CreateIndexRequest {
    pub index_config: IndexConfig,
    pub publish_token: String,
}

#[async_trait]
impl Handler<CreateIndexRequest> for IndexService {
    type Reply = Result<IndexMetadata, IndexServiceError>;
    async fn handle(
        &mut self,
        create_index_req: CreateIndexRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self
            .create_index(
                create_index_req.index_config,
                &create_index_req.publish_token,
            )
            .await)
    }
}

#[derive(Debug)]
pub struct DeleteIndexRequest {
    pub index_id: String,
    pub dry_run: bool,
    pub publish_token: String,
}

#[async_trait]
impl Handler<DeleteIndexRequest> for IndexService {
    type Reply = Result<Vec<FileEntry>, IndexServiceError>;
    async fn handle(
        &mut self,
        delete_index_req: DeleteIndexRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self
            .delete_index(
                &delete_index_req.index_id,
                delete_index_req.dry_run,
                &delete_index_req.publish_token,
            )
            .await)
    }
}
