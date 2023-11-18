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

pub mod control_plane;
pub(crate) mod control_plane_model;
pub mod indexing_plan;
pub mod indexing_scheduler;
pub mod ingest;
pub(crate) mod metrics;

use quickwit_common::tower::Pool;
use quickwit_proto::indexing::{CpuCapacity, IndexingServiceClient, IndexingTask};

/// Indexer-node specific information stored in the pool of available indexer nodes
#[derive(Debug, Clone)]
pub struct IndexerNodeInfo {
    pub client: IndexingServiceClient,
    pub indexing_tasks: Vec<IndexingTask>,
    pub indexing_capacity: CpuCapacity,
}

pub type IndexerPool = Pool<String, IndexerNodeInfo>;

#[cfg(test)]
mod tests;
