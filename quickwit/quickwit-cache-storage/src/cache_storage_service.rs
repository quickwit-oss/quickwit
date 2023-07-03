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
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, Universe};
use quickwit_common::fs::get_cache_directory_path;
use quickwit_common::temp_dir;
use quickwit_config::{CacheStorageConfig, NodeConfig};
use quickwit_metastore::{Metastore, MetastoreError};
use quickwit_proto::cache_storage::{
    NotifySplitsChangeRequest, NotifySplitsChangeResponse, SplitsChangeNotification,
};
use quickwit_proto::{ServiceError, ServiceErrorCode};
use quickwit_storage::{StorageError, StorageResolver, StorageResolverError};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, error, info};

/// Name of the cache directory, usually located at `<data_dir_path>/searching`.
pub const CACHE_DIR_NAME: &str = "searching";

#[derive(Error, Debug)]
pub enum CacheStorageServiceError {
    #[error("Failed to resolve the storage `{0}`.")]
    StorageResolverError(#[from] StorageResolverError),
    #[error("Storage error `{0}`.")]
    StorageError(#[from] StorageError),
    #[error("Metastore error `{0}`.")]
    MetastoreError(#[from] MetastoreError),
    #[error("Invalid params `{0}`.")]
    InvalidParams(anyhow::Error),
}

impl ServiceError for CacheStorageServiceError {
    fn status_code(&self) -> ServiceErrorCode {
        match self {
            Self::StorageResolverError(_) | Self::StorageError(_) => ServiceErrorCode::Internal,
            Self::MetastoreError(_) => ServiceErrorCode::Internal,
            Self::InvalidParams(_) => ServiceErrorCode::BadRequest,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CacheStorageServiceCounters {
    pub num_cached_splits: usize,
    pub num_downloaded_splits: usize,
    pub num_hits: usize,
    pub num_misses: usize,
}

pub struct CacheStorageService {
    node_id: String,
    cache_root_directory: PathBuf,
    metastore: Arc<dyn Metastore>,
    counters: CacheStorageServiceCounters,
    // local_split_store: Arc<LocalSplitStore>,
    max_concurrent_split_downloads: usize,
    storage_resolver: StorageResolver,
}

impl CacheStorageService {
    pub async fn new(
        node_id: String,
        data_dir_path: PathBuf,
        _cache_storage_config: CacheStorageConfig,
        metastore: Arc<dyn Metastore>,
        storage_resolver: StorageResolver,
    ) -> anyhow::Result<CacheStorageService> {
        // let split_store_space_quota = SplitStoreQuota::new(
        //     usize::MAX, // no limits for the number of splits
        //     cache_storage_config.max_cache_storage_disk_usage,
        // );
        let _split_cache_dir_path = get_cache_directory_path(&data_dir_path);
        // let local_split_store =
        //     LocalSplitStore::open(split_cache_dir_path, split_store_space_quota).await?;
        let cache_root_directory =
            temp_dir::create_or_purge_directory(&data_dir_path.join(CACHE_DIR_NAME)).await?;
        Ok(Self {
            node_id,
            cache_root_directory,
            metastore,
            counters: Default::default(),
            max_concurrent_split_downloads: 5, // TODO: replace with cache_storage_config.something
            storage_resolver,
        })
    }

    async fn handle_supervise(&mut self) -> Result<(), ActorExitStatus> {
        Ok(())
    }

    async fn update_split_cache(
        &self,
        splits: Vec<SplitsChangeNotification>,
    ) -> Result<(), ActorExitStatus> {
        if let Some(cache_storage_factory) = self.storage_resolver.cache_storage_factory() {
            if let Err(_err) = cache_storage_factory
                .update_split_cache(&self.storage_resolver, splits)
                .await
            {
                // TODO: log this
            }
        }
        Ok(())
    }
}

impl fmt::Debug for CacheStorageService {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("CacheStorageService")
            .field("node_id", &self.node_id)
            .finish()
    }
}

#[derive(Debug)]
struct SuperviseLoop;

#[async_trait]
impl Handler<SuperviseLoop> for CacheStorageService {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: SuperviseLoop,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.handle_supervise().await?;
        ctx.schedule_self_msg(*quickwit_actors::HEARTBEAT, SuperviseLoop)
            .await;
        Ok(())
    }
}

#[async_trait]
impl Handler<NotifySplitsChangeRequest> for CacheStorageService {
    type Reply = quickwit_proto::cache_storage::Result<NotifySplitsChangeResponse>;

    async fn handle(
        &mut self,
        request: NotifySplitsChangeRequest,
        _: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        debug!("Split change notification: schedule indexing plan.");
        self.update_split_cache(request.splits_change).await?;
        Ok(Ok(NotifySplitsChangeResponse {}))
    }
}

#[async_trait]
impl Actor for CacheStorageService {
    type ObservableState = CacheStorageServiceCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.handle(SuperviseLoop, ctx).await
    }
}

pub async fn start_cache_storage_service(
    universe: &Universe,
    config: &NodeConfig,
    metastore: Arc<dyn Metastore>,
    storage_resolver: StorageResolver,
) -> anyhow::Result<Mailbox<CacheStorageService>> {
    info!("Starting indexer service.");

    // Spawn indexing service.
    let cache_storage_service = CacheStorageService::new(
        config.node_id.clone(),
        config.data_dir_path.to_path_buf(),
        CacheStorageConfig::default(), // TODO: Replace with real
        metastore.clone(),
        storage_resolver,
    )
    .await?;

    let (cache_storage_service, _) = universe.spawn_builder().spawn(cache_storage_service);

    Ok(cache_storage_service)
}
#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test() {}
}
