// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_trait::async_trait;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, ActorState, Handler, Healthz,
};
use serde_json::{json, Value as JsonValue};

use crate::actors::{DeleteTaskService, GarbageCollector, RetentionPolicyExecutor};

pub struct JanitorService {
    delete_task_service_handle: Option<ActorHandle<DeleteTaskService>>,
    garbage_collector_handle: ActorHandle<GarbageCollector>,
    retention_policy_executor_handle: ActorHandle<RetentionPolicyExecutor>,
}

impl JanitorService {
    pub fn new(
        delete_task_service_handle: Option<ActorHandle<DeleteTaskService>>,
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
        self.delete_task_service_handle
            .as_ref()
            .map_or(true, |delete_task_service_handle| {
                delete_task_service_handle.state() != ActorState::Failure
            })
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
