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

use anyhow::Context;
use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox};
use quickwit_proto::control_plane::{
    CloseShardsRequest, CloseShardsResponse, ControlPlaneResult, GetOpenShardsRequest,
    GetOpenShardsResponse, NotifyIndexChangeRequest, NotifyIndexChangeResponse,
    NotifySplitsChangeRequest, NotifySplitsChangeResponse,
};
use tracing::debug;

use crate::cache_storage_controller::{CacheStorageController, CacheUpdateRequest};
use crate::scheduler::IndexingScheduler;

#[derive(Debug)]
pub struct ControlPlane {
    index_scheduler_mailbox: Mailbox<IndexingScheduler>,
    cache_storage_manager_mailbox: Mailbox<CacheStorageController>,
}

#[async_trait]
impl Actor for ControlPlane {
    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {}

    fn name(&self) -> String {
        "ControlPlane".to_string()
    }
}

impl ControlPlane {
    pub fn new(
        index_scheduler_mailbox: Mailbox<IndexingScheduler>,
        cache_storage_manager_mailbox: Mailbox<CacheStorageController>,
    ) -> Self {
        Self {
            index_scheduler_mailbox,
            cache_storage_manager_mailbox,
        }
    }
}

#[async_trait]
impl Handler<NotifyIndexChangeRequest> for ControlPlane {
    type Reply = ControlPlaneResult<NotifyIndexChangeResponse>;

    async fn handle(
        &mut self,
        request: NotifyIndexChangeRequest,
        _: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        debug!("Index change notification: schedule indexing plan.");
        self.index_scheduler_mailbox
            .send_message(request)
            .await
            .context("Error sending index change notification to index scheduler.")?;
        self.cache_storage_manager_mailbox
            .send_message(CacheUpdateRequest {})
            .await
            .context("Error sending index change notification to index scheduler.")?;
        Ok(Ok(NotifyIndexChangeResponse {}))
    }
}

#[async_trait]
impl Handler<GetOpenShardsRequest> for ControlPlane {
    type Reply = ControlPlaneResult<GetOpenShardsResponse>;

    async fn handle(
        &mut self,
        _request: GetOpenShardsRequest,
        _: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        unimplemented!()
    }
}

#[async_trait]
impl Handler<CloseShardsRequest> for ControlPlane {
    type Reply = ControlPlaneResult<CloseShardsResponse>;

    async fn handle(
        &mut self,
        _request: CloseShardsRequest,
        _: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        unimplemented!()
    }
}

#[async_trait]
impl Handler<NotifySplitsChangeRequest> for ControlPlane {
    type Reply = ControlPlaneResult<NotifySplitsChangeResponse>;

    async fn handle(
        &mut self,
        _request: NotifySplitsChangeRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        debug!("Split change notification: notifying availalbe cache storage clients.");
        self.cache_storage_manager_mailbox
            .send_message(CacheUpdateRequest {})
            .await
            .context("Error sending index change notification to index scheduler.")?;
        Ok(Ok(NotifySplitsChangeResponse {}))
    }
}
