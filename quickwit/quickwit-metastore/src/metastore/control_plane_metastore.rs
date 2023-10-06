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
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_config::{IndexConfig, SourceConfig};
use quickwit_proto::control_plane::{ControlPlaneService, ControlPlaneServiceClient};
use quickwit_proto::metastore::{
    serde_utils as metastore_serde_utils, AcquireShardsRequest, AcquireShardsResponse,
    AddSourceRequest, CloseShardsRequest, CloseShardsResponse, CreateIndexRequest,
    DeleteIndexRequest, DeleteQuery, DeleteShardsRequest, DeleteShardsResponse,
    DeleteSourceRequest, DeleteTask, ListShardsRequest, ListShardsResponse, MetastoreResult,
    OpenShardsRequest, OpenShardsResponse, ToggleSourceRequest,
};
use quickwit_proto::{IndexUid, PublishToken};

use crate::checkpoint::IndexCheckpointDelta;
use crate::{IndexMetadata, ListIndexesQuery, ListSplitsQuery, Metastore, Split, SplitMetadata};

/// A [`Metastore`] implementation that proxies some requests to the control plane so it can
/// track the state of the metastore accurately and react to events in real-time.
pub struct ControlPlaneMetastore {
    control_plane: ControlPlaneServiceClient,
    metastore: Arc<dyn Metastore>,
}

impl fmt::Debug for ControlPlaneMetastore {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ControlPlaneMetastore").finish()
    }
}

impl ControlPlaneMetastore {
    /// Creates a new [`ControlPlaneMetastore`].
    pub fn new(control_plane: ControlPlaneServiceClient, metastore: Arc<dyn Metastore>) -> Self {
        Self {
            control_plane,
            metastore,
        }
    }
}

#[async_trait]
impl Metastore for ControlPlaneMetastore {
    fn uri(&self) -> &Uri {
        self.metastore.uri()
    }

    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.metastore.check_connectivity().await
    }

    // Proxied metastore API calls.

    async fn create_index(&self, index_config: IndexConfig) -> MetastoreResult<IndexUid> {
        let index_config_json = metastore_serde_utils::to_json_str(&index_config)?;
        let request = CreateIndexRequest { index_config_json };
        let response = self.control_plane.clone().create_index(request).await?;
        let index_uid: IndexUid = response.index_uid.into();
        Ok(index_uid)
    }

    async fn delete_index(&self, index_uid: IndexUid) -> MetastoreResult<()> {
        let request = DeleteIndexRequest {
            index_uid: index_uid.into(),
        };
        self.control_plane.clone().delete_index(request).await?;
        Ok(())
    }

    async fn add_source(&self, index_uid: IndexUid, source: SourceConfig) -> MetastoreResult<()> {
        let request = AddSourceRequest {
            index_uid: index_uid.into(),
            source_config_json: metastore_serde_utils::to_json_str(&source)?,
        };
        self.control_plane.clone().add_source(request).await?;
        Ok(())
    }

    async fn toggle_source(
        &self,
        index_uid: IndexUid,
        source_id: &str,
        enable: bool,
    ) -> MetastoreResult<()> {
        let request = ToggleSourceRequest {
            index_uid: index_uid.into(),
            source_id: source_id.to_string(),
            enable,
        };
        self.control_plane.clone().toggle_source(request).await?;
        Ok(())
    }

    async fn delete_source(&self, index_uid: IndexUid, source_id: &str) -> MetastoreResult<()> {
        let request = DeleteSourceRequest {
            index_uid: index_uid.into(),
            source_id: source_id.to_string(),
        };
        self.control_plane.clone().delete_source(request).await?;
        Ok(())
    }

    // Other metastore API calls.

    async fn index_metadata(&self, index_id: &str) -> MetastoreResult<IndexMetadata> {
        self.metastore.index_metadata(index_id).await
    }

    async fn list_indexes_metadatas(
        &self,
        query: ListIndexesQuery,
    ) -> MetastoreResult<Vec<IndexMetadata>> {
        self.metastore.list_indexes_metadatas(query).await
    }

    async fn stage_splits(
        &self,
        index_uid: IndexUid,
        splits_metadata: Vec<SplitMetadata>,
    ) -> MetastoreResult<()> {
        self.metastore
            .stage_splits(index_uid, splits_metadata)
            .await
    }

    async fn publish_splits<'a>(
        &self,
        index_uid: IndexUid,
        staged_split_ids: &[&'a str],
        replaced_split_ids: &[&'a str],
        checkpoint_delta_opt: Option<IndexCheckpointDelta>,
        publish_token_opt: Option<PublishToken>,
    ) -> MetastoreResult<()> {
        self.metastore
            .publish_splits(
                index_uid,
                staged_split_ids,
                replaced_split_ids,
                checkpoint_delta_opt,
                publish_token_opt,
            )
            .await
    }

    async fn list_splits(&self, query: ListSplitsQuery) -> MetastoreResult<Vec<Split>> {
        self.metastore.list_splits(query).await
    }

    async fn mark_splits_for_deletion<'a>(
        &self,
        index_uid: IndexUid,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        self.metastore
            .mark_splits_for_deletion(index_uid, split_ids)
            .await
    }

    async fn delete_splits<'a>(
        &self,
        index_uid: IndexUid,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        self.metastore.delete_splits(index_uid, split_ids).await
    }

    async fn reset_source_checkpoint(
        &self,
        index_uid: IndexUid,
        source_id: &str,
    ) -> MetastoreResult<()> {
        self.metastore
            .reset_source_checkpoint(index_uid, source_id)
            .await
    }

    // Delete tasks API

    async fn create_delete_task(&self, delete_query: DeleteQuery) -> MetastoreResult<DeleteTask> {
        self.metastore.create_delete_task(delete_query).await
    }

    async fn last_delete_opstamp(&self, index_uid: IndexUid) -> MetastoreResult<u64> {
        self.metastore.last_delete_opstamp(index_uid).await
    }

    async fn update_splits_delete_opstamp<'a>(
        &self,
        index_uid: IndexUid,
        split_ids: &[&'a str],
        delete_opstamp: u64,
    ) -> MetastoreResult<()> {
        self.metastore
            .update_splits_delete_opstamp(index_uid, split_ids, delete_opstamp)
            .await
    }

    async fn list_delete_tasks(
        &self,
        index_uid: IndexUid,
        opstamp_start: u64,
    ) -> MetastoreResult<Vec<DeleteTask>> {
        self.metastore
            .list_delete_tasks(index_uid, opstamp_start)
            .await
    }

    // Shard API

    async fn open_shards(&self, request: OpenShardsRequest) -> MetastoreResult<OpenShardsResponse> {
        self.metastore.open_shards(request).await
    }

    async fn acquire_shards(
        &self,
        request: AcquireShardsRequest,
    ) -> MetastoreResult<AcquireShardsResponse> {
        self.metastore.acquire_shards(request).await
    }

    async fn list_shards(&self, request: ListShardsRequest) -> MetastoreResult<ListShardsResponse> {
        self.metastore.list_shards(request).await
    }

    async fn close_shards(
        &self,
        request: CloseShardsRequest,
    ) -> MetastoreResult<CloseShardsResponse> {
        self.metastore.close_shards(request).await
    }

    async fn delete_shards(
        &self,
        request: DeleteShardsRequest,
    ) -> MetastoreResult<DeleteShardsResponse> {
        self.metastore.delete_shards(request).await
    }
}
