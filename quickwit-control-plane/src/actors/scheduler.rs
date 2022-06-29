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
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, QueueCapacity};
use quickwit_proto::PhysicalIndexingPlanRequest;

use crate::cluster_client::ClusterClient;

use super::planner::IndexingPlan;


///
/// The IndexingScheduler is reponsible for allocating IndexingPlan's tasks to available indexers.
/// It executes the following steps:
/// - on receiving an `IndexingPlan` from the IndexingPlaner, it builds a `PhysicalIndexingPlan`.
/// - It send gRPC requests to apply the physical plan to each indexers.
/// 
/// TODO: 
/// - what happends if an indexer is returning an error? => nothing for now, the IndexingPlaner
///   will resend a new plan every minute.
/// - If several IndexingPlan are in a queue, we should not apply all of them and just keep the last one.
pub struct IndexingScheduler {
    cluster_client: Arc<ClusterClient>,
}

#[async_trait]
impl Actor for IndexingScheduler {
    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {}

    fn name(&self) -> String {
        "IndexerScheduler".to_string()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        // TODO: what would be a good capacity?
        QueueCapacity::Bounded(3)
    }
}

impl IndexingScheduler {
    pub fn new(cluster_client: Arc<ClusterClient>) -> Self {
        Self {
            cluster_client
        }
    }
    async fn build_physical_indexing_plan(indexing_plan: IndexingPlan) -> anyhow::Result<PhysicalIndexingPlanRequest> {
        todo!()
    }

    async fn apply_physical_indexing_plan(physhical_plan: PhysicalIndexingPlanRequest) -> anyhow::Result<()> {
        todo!()
    }
}

#[async_trait]
impl Handler<IndexingPlan> for IndexingScheduler {
    type Reply = ();

    async fn handle(
        &mut self,
        message: IndexingPlan,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        Ok(())
    }
}
