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

//! Distributed DataFusion worker session setup.
//!
//! The worker builder mirrors the coordinator's native OSS registration flow:
//! runtime plugins contribute `SessionStateBuilder` state, and catalogs/schemas
//! are created fresh per worker session via the same factory registrations used
//! on the coordinator.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion_distributed::{Worker, WorkerQueryContext, WorkerSessionBuilder};

use crate::session::DataFusionSessionBuilder;

#[derive(Clone)]
pub struct QuickwitWorkerSessionBuilder {
    session_builder: Arc<DataFusionSessionBuilder>,
}

impl QuickwitWorkerSessionBuilder {
    pub fn new(session_builder: Arc<DataFusionSessionBuilder>) -> Self {
        Self { session_builder }
    }
}

#[async_trait]
impl WorkerSessionBuilder for QuickwitWorkerSessionBuilder {
    async fn build_session_state(
        &self,
        mut ctx: WorkerQueryContext,
    ) -> Result<SessionState, DataFusionError> {
        let registration = self.session_builder.merged_runtime_registration();
        if let Some(config) = ctx.builder.config() {
            registration.apply_to_config(config);
        }

        let state = registration
            .apply_to_builder(
                ctx.builder
                    .with_runtime_env(Arc::clone(self.session_builder.runtime()))
                    .with_catalog_list(self.session_builder.build_catalog_list()?),
            )
            .build();

        for plugin in self.session_builder.runtime_plugins() {
            plugin.register_for_worker(&state).await?;
        }

        Ok(state)
    }
}

pub fn build_worker(session_builder: Arc<DataFusionSessionBuilder>) -> Worker {
    Worker::from_session_builder(QuickwitWorkerSessionBuilder::new(session_builder))
}
