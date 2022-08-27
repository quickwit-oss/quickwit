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

use actors::IndexingScheduler;
use quickwit_actors::{Mailbox, Universe};
use quickwit_cluster::Cluster;
use quickwit_metastore::Metastore;

pub mod actors;
pub mod grpc_adapter;

/// Starting Control Plane.
pub async fn start_control_plane_service(
    universe: &Universe,
    cluster: Arc<Cluster>,
    metastore: Arc<dyn Metastore>,
) -> anyhow::Result<Mailbox<IndexingScheduler>> {
    let scheduler = IndexingScheduler::new(cluster, metastore);
    let (scheduler_mailbox, _) = universe.spawn_builder().spawn(scheduler);
    Ok(scheduler_mailbox)
}
