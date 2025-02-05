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

pub mod control_plane;
pub mod indexing_plan;
pub mod indexing_scheduler;
pub mod ingest;
pub(crate) mod metrics;
pub(crate) mod model;

use quickwit_common::tower::Pool;
use quickwit_proto::indexing::{CpuCapacity, IndexingServiceClient, IndexingTask};
use quickwit_proto::types::NodeId;

/// Indexer-node specific information stored in the pool of available indexer nodes
#[derive(Debug, Clone)]
pub struct IndexerNodeInfo {
    pub node_id: NodeId,
    pub generation_id: u64,
    pub client: IndexingServiceClient,
    pub indexing_tasks: Vec<IndexingTask>,
    pub indexing_capacity: CpuCapacity,
}

pub type IndexerPool = Pool<NodeId, IndexerNodeInfo>;

mod cooldown_map;
mod debouncer;
#[cfg(test)]
mod tests;
