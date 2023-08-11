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

use anyhow::anyhow;
use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, Universe};
// use quickwit_common::fs::get_cache_directory_path;
// use quickwit_common::temp_dir;
use quickwit_config::NodeConfig;
use quickwit_metastore::MetastoreError;
use quickwit_proto::cache_storage::{
    NotifySplitsChangeRequest, NotifySplitsChangeResponse, SplitsChangeNotification,
};
use quickwit_proto::{ServiceError, ServiceErrorCode};
use quickwit_storage::{
    CacheStorageCounters, CacheStorageFactory, StorageError, StorageResolver, StorageResolverError,
};
use thiserror::Error;
use tracing::{debug, error, info};

/// Name of the cache directory, usually located at `<data_dir_path>/searching`.
// pub const CACHE_DIR_NAME: &str = "searching";

#[derive(Error, Debug)]
pub enum CacheStorageServiceError {
    #[error("Failed to resolve the storage `{0}`.")]
    StorageResolverError(#[from] StorageResolverError),
    #[error("Storage error `{0}`.")]
    StorageError(#[from] StorageError),
    #[error("Metastore error `{0}`.")]
    MetastoreError(#[from] MetastoreError),
    #[error("Invalid params `{0}`.")]
    InvalidParams(String),
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

pub struct CacheStorageService {
    node_id: String,
    storage_resolver: StorageResolver,
    cache_storage_factory: CacheStorageFactory,
}

impl CacheStorageService {
    pub async fn new(
        node_id: String,
        storage_resolver: StorageResolver,
    ) -> anyhow::Result<CacheStorageService> {
        if let Some(cache_storage_factory) = storage_resolver.cache_storage_factory() {
            Ok(Self {
                node_id,
                storage_resolver,
                cache_storage_factory,
            })
        } else {
            Err(anyhow!(CacheStorageServiceError::InvalidParams(
                "The cache storage factory is not available.".to_string()
            )))
        }
    }

    async fn handle_supervise(&mut self) -> Result<(), ActorExitStatus> {
        Ok(())
    }

    pub async fn update_split_cache(
        &self,
        splits: Vec<SplitsChangeNotification>,
    ) -> Result<(), ActorExitStatus> {
        self.cache_storage_factory
            .update_split_cache(&self.storage_resolver, splits)
            .await
            .map_err(|e| ActorExitStatus::from(anyhow!("Failed to update split cache: {:?}", e)))
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
    type ObservableState = CacheStorageCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.cache_storage_factory.counters()
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.handle(SuperviseLoop, ctx).await
    }
}

pub async fn start_cache_storage_service(
    universe: &Universe,
    config: &NodeConfig,
    storage_resolver: StorageResolver,
) -> anyhow::Result<Option<Mailbox<CacheStorageService>>> {
    if config.is_cache_storage_enabled() {
        info!("Starting cache storage service.");
        // Spawn indexing service.
        let cache_storage_service =
            CacheStorageService::new(config.node_id.clone(), storage_resolver).await?;

        let (cache_storage_service, _) = universe.spawn_builder().spawn(cache_storage_service);

        Ok(Some(cache_storage_service))
    } else {
        Ok(None)
    }
}
#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test() {}
}
