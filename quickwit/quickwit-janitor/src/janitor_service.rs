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

use async_trait::async_trait;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, ActorState, Handler, Healthz,
};
use serde_json::{json, Value as JsonValue};

use crate::actors::{DeleteTaskService, GarbageCollector, RetentionPolicyExecutor};

pub struct JanitorService {
    delete_task_service_handle: ActorHandle<DeleteTaskService>,
    garbage_collector_handle: ActorHandle<GarbageCollector>,
    retention_policy_executor_handle: ActorHandle<RetentionPolicyExecutor>,
}

impl JanitorService {
    pub fn new(
        delete_task_service_handle: ActorHandle<DeleteTaskService>,
        garbage_collector_handle: ActorHandle<GarbageCollector>,
        retention_policy_executor_handle: ActorHandle<RetentionPolicyExecutor>,
    ) -> Self {
        Self {
            delete_task_service_handle,
            garbage_collector_handle,
            retention_policy_executor_handle,
        }
    }

    fn is_healthy(&self) -> bool {
        self.delete_task_service_handle.state() != ActorState::Failure
            && self.garbage_collector_handle.state() != ActorState::Failure
            && self.retention_policy_executor_handle.state() != ActorState::Failure
    }
}

#[async_trait]
impl Actor for JanitorService {
    type ObservableState = JsonValue;

    fn name(&self) -> String {
        "JanitorService".to_string()
    }

    fn observable_state(&self) -> Self::ObservableState {
        json!({})
    }
}

#[async_trait]
impl Handler<Healthz> for JanitorService {
    type Reply = bool;

    async fn handle(
        &mut self,
        _message: Healthz,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.is_healthy())
    }
}
