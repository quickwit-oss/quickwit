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

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, Handler, Health, Supervisable,
};
use quickwit_metastore::{Metastore, MetastoreError};
use quickwit_storage::{StorageResolverError, StorageUriResolver};
use thiserror::Error;
use tracing::{error, info};

use super::garbage_collector::{GarbageCollector, GarbageCollectorCounters};

const OBSERVATION_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Error, Debug)]
pub enum JanitorServiceError {
    #[error("Garbage collector already exists.")]
    GarbageCollectionAlreadyExists,
    #[error("Failed to resolve the storage `{0}`.")]
    StorageError(#[from] StorageResolverError),
    #[error("Metastore error `{0}`.")]
    MetastoreError(#[from] MetastoreError),
}

#[derive(Clone, Copy, Debug, Default)]
struct SpawnGarbageCollector;

#[derive(Clone, Copy, Debug, Default)]
struct Observe;

#[derive(Clone, Copy, Debug, Default)]
struct Supervise;

#[derive(Clone, Debug, Default)]
pub struct JanitorServiceCounters {
    pub garbage_collector_counters: GarbageCollectorCounters,
}

pub struct JanitorService {
    node_id: String,
    metastore: Arc<dyn Metastore>,
    storage_resolver: StorageUriResolver,
    garbage_collector_handle_opt: Option<ActorHandle<GarbageCollector>>,
    counters: JanitorServiceCounters,
}

impl JanitorService {
    pub fn new(
        node_id: String,
        metastore: Arc<dyn Metastore>,
        storage_resolver: StorageUriResolver,
    ) -> Self {
        Self {
            node_id,
            metastore,
            storage_resolver,
            garbage_collector_handle_opt: None,
            counters: JanitorServiceCounters::default(),
        }
    }

    async fn spawn_garbage_collector(
        &mut self,
        ctx: &ActorContext<Self>,
    ) -> Result<(), JanitorServiceError> {
        if self.garbage_collector_handle_opt.is_some() {
            return Err(JanitorServiceError::GarbageCollectionAlreadyExists);
        }

        let garbage_collector =
            GarbageCollector::new(self.metastore.clone(), self.storage_resolver.clone());
        let (_, garbage_collector_handle) = ctx.spawn_actor(garbage_collector).spawn();

        self.garbage_collector_handle_opt = Some(garbage_collector_handle);
        Ok(())
    }
}

#[async_trait]
impl Actor for JanitorService {
    type ObservableState = JanitorServiceCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    async fn initialize(
        &mut self,
        ctx: &ActorContext<Self>,
    ) -> Result<(), quickwit_actors::ActorExitStatus> {
        self.handle(SpawnGarbageCollector, ctx).await?;
        self.handle(Observe, ctx).await?;
        self.handle(Supervise, ctx).await
    }
}

#[async_trait]
impl Handler<Supervise> for JanitorService {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: Supervise,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if let Some(gc_handle) = &self.garbage_collector_handle_opt {
            let supervisable_gc_handle: &dyn Supervisable = gc_handle;
            match supervisable_gc_handle.health() {
                Health::Success => info!(node_id=%self.node_id, "Garbage collector completed."),
                Health::FailureOrUnhealthy => {
                    error!(node_id=%self.node_id, "Garbage collector failed.")
                }
                Health::Healthy => (),
            };
        }
        ctx.schedule_self_msg(quickwit_actors::HEARTBEAT, Supervise)
            .await;
        Ok(())
    }
}

#[async_trait]
impl Handler<SpawnGarbageCollector> for JanitorService {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: SpawnGarbageCollector,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if let Err(spawn_error) = self.spawn_garbage_collector(ctx).await {
            if !matches!(
                spawn_error,
                JanitorServiceError::GarbageCollectionAlreadyExists
            ) {
                error!(error = ?spawn_error, "Could not spawn garbage collector.");
                return Err(ActorExitStatus::Success);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<Observe> for JanitorService {
    type Reply = ();

    async fn handle(
        &mut self,
        _: Observe,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if let Some(handle) = &self.garbage_collector_handle_opt {
            let observation_counters = handle.observe().await;
            self.counters = JanitorServiceCounters {
                garbage_collector_counters: observation_counters.clone(),
            };
        }
        ctx.schedule_self_msg(OBSERVATION_INTERVAL, Observe).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use quickwit_actors::Universe;
    use quickwit_common::uri::Uri;
    use quickwit_metastore::quickwit_metastore_uri_resolver;

    use super::*;
    use crate::actors::garbage_collector::RUN_INTERVAL;

    #[tokio::test]
    async fn test_janitor_service() {
        let metastore_uri = Uri::new("ram:///metastore".to_string());
        let metastore = quickwit_metastore_uri_resolver()
            .resolve(&metastore_uri)
            .await
            .unwrap();

        let storage_resolver = StorageUriResolver::for_test();
        let janitor_service =
            JanitorService::new("one".to_string(), metastore.clone(), storage_resolver);
        let universe = Universe::new();
        let (_, handle) = universe.spawn_actor(janitor_service).spawn();
        let counters = handle.process_pending_and_observe().await.state;
        assert_eq!(counters.garbage_collector_counters.num_passes, 1);

        // 30 secs later
        universe.simulate_time_shift(Duration::from_secs(30)).await;
        let counters = handle.process_pending_and_observe().await.state;
        assert_eq!(counters.garbage_collector_counters.num_passes, 1);

        // 60 secs later
        universe.simulate_time_shift(RUN_INTERVAL).await;
        let counters = handle.process_pending_and_observe().await.state;
        assert_eq!(counters.garbage_collector_counters.num_passes, 2);
    }
}
