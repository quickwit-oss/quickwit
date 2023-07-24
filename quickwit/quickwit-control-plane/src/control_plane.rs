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
use quickwit_proto::control_plane::{NotifyIndexChangeRequest, NotifyIndexChangeResponse};
use tracing::debug;

use crate::scheduler::IndexingScheduler;

#[derive(Debug)]
pub struct ControlPlane {
    index_scheduler_mailbox: Mailbox<IndexingScheduler>,
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
    pub fn new(index_scheduler_mailbox: Mailbox<IndexingScheduler>) -> Self {
        Self {
            index_scheduler_mailbox,
        }
    }
}

#[async_trait]
impl Handler<NotifyIndexChangeRequest> for ControlPlane {
    type Reply = quickwit_proto::control_plane::Result<NotifyIndexChangeResponse>;

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
        Ok(Ok(NotifyIndexChangeResponse {}))
    }
}
