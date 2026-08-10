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

#![deny(clippy::disallowed_methods)]

mod compaction_pipeline;
mod compactor_supervisor;
mod metrics;
pub mod planner;

pub type TaskId = String;

use std::sync::Arc;

pub use compactor_supervisor::{
    CompactorSupervisor, notify_compactor_decommission, wait_for_compactor_decommission,
};
use quickwit_actors::{Mailbox, Universe};
use quickwit_common::pubsub::EventBroker;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_config::CompactorConfig;
use quickwit_indexing::IndexingSplitCache;
use quickwit_proto::compaction::CompactionPlannerServiceClient;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_proto::types::{IndexUid, NodeId, SourceId};
use quickwit_storage::StorageResolver;
use tracing::info;

pub fn source_uid_metrics_label(index_uid: &IndexUid, source_id: &SourceId) -> String {
    let source_uid = format!("{}-{}", index_uid, source_id);
    quickwit_common::metrics::index_label(&source_uid).to_string()
}

#[allow(clippy::too_many_arguments)]
pub async fn start_compactor_service(
    universe: &Universe,
    node_id: NodeId,
    compaction_client: CompactionPlannerServiceClient,
    compactor_config: &CompactorConfig,
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
    split_cache: Arc<IndexingSplitCache>,
    event_broker: EventBroker,
    compaction_root_directory: TempDirectory,
) -> anyhow::Result<Mailbox<CompactorSupervisor>> {
    info!("starting compactor service");
    let compactor_config = compactor_config.clone();
    let (mailbox, _supervisor_handle) = universe.spawn_builder().supervise_fn(move || {
        CompactorSupervisor::new(
            node_id.clone(),
            compaction_client.clone(),
            &compactor_config,
            metastore.clone(),
            storage_resolver.clone(),
            split_cache.clone(),
            event_broker.clone(),
            compaction_root_directory.clone(),
        )
    });
    Ok(mailbox)
}
