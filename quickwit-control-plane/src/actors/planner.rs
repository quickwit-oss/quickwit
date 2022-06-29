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
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox};
use quickwit_config::SourceConfig;
use quickwit_metastore::{Metastore, MetastoreError};

use super::scheduler::IndexingScheduler;

// The IndexerPlanner is responsible for building the `IndexingPlan`
// and sending it to the IndexingScheduler.
// An IndexingPlan is the list of (index_id, source_id) to index.
// It is derived from the indexes sources registered in the
// metastore. If not specified in the index config, a source ".ingest-api"
// is always added to an index.
//
// The build of an IndexingPlan is triggered by the following events:
// - `IndexingPlanner` init.
// - Index creation / update / deletion messages, the `IndexService` is responsible for detecting
//   these events and sending a refresh event to the `IndexingPlanner`.
// - `ClusterState` changes, the `ClusterStateIndexingWatcher` is responsible for detecting changes
//   and sending a refresh event to the `IndexingPlanner`.
// - Every minute, the `IndexingPlanner` send to himself an message to reevaluate the `IndexingPlan`.
//
pub struct IndexingPlanner {
    metastore: Arc<dyn Metastore>,
    scheduler_mailbox: Mailbox<IndexingScheduler>,
}

#[derive(Debug)]
pub struct SourceTask {
    pub index_id: String,
    pub source: SourceConfig,
}

#[derive(Debug)]
pub struct IndexingPlan {
    pub tasks: Vec<SourceTask>,
}

#[async_trait]
impl Actor for IndexingPlanner {
    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {}

    fn name(&self) -> String {
        "IndexerPlanner".to_string()
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.submit_indexing_plan(ctx).await?;
        Ok(())
    }
}

impl IndexingPlanner {
    pub fn new(
        metastore: Arc<dyn Metastore>,
        scheduler_mailbox: Mailbox<IndexingScheduler>,
    ) -> Self {
        Self {
            metastore,
            scheduler_mailbox,
        }
    }

    async fn build_indexing_plan(&self) -> Result<IndexingPlan, MetastoreError> {
        todo!()
    }

    async fn submit_indexing_plan(
        &mut self,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        // TODO: handle error.
        let indexing_plan = self.build_indexing_plan().await.unwrap();
        ctx.send_message(&self.scheduler_mailbox, indexing_plan)
            .await?;
        Ok(())
    }
}


/// The IndexService will send this message to the IndexingPlanner.
#[derive(Debug)]
pub struct RefreshIndexingPlan;

#[async_trait]
impl Handler<RefreshIndexingPlan> for IndexingPlanner {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: RefreshIndexingPlan,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.submit_indexing_plan(ctx).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct RefreshIndexingPlanLoop;

#[async_trait]
impl Handler<RefreshIndexingPlanLoop> for IndexingPlanner {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: RefreshIndexingPlanLoop,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.submit_indexing_plan(ctx).await?;
        ctx.schedule_self_msg(quickwit_actors::HEARTBEAT * 20, RefreshIndexingPlanLoop)
            .await;
        Ok(())
    }
}
