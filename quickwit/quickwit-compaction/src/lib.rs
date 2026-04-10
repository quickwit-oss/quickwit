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

#[allow(dead_code)]
mod compaction_pipeline;
#[allow(dead_code)]
mod compactor_supervisor;
pub mod planner;

use quickwit_actors::{Mailbox, Universe};
use quickwit_common::io;
use quickwit_common::pubsub::EventBroker;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_config::CompactorConfig;
use quickwit_indexing::IndexingSplitStore;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_storage::StorageResolver;
use tracing::info;

pub use compactor_supervisor::CompactorSupervisor;

pub async fn start_compactor_service(
    universe: &Universe,
    compactor_config: &CompactorConfig,
    split_store: IndexingSplitStore,
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
    event_broker: EventBroker,
    compaction_root_directory: TempDirectory,
) -> anyhow::Result<Mailbox<CompactorSupervisor>> {
    info!("starting compactor service");
    let io_throughput_limiter = compactor_config.max_merge_write_throughput.map(io::limiter);
    let supervisor = CompactorSupervisor::new(
        compactor_config.max_concurrent_pipelines.get(),
        io_throughput_limiter,
        split_store,
        metastore,
        storage_resolver,
        compactor_config.max_concurrent_split_uploads,
        event_broker,
        compaction_root_directory,
        compactor_config.max_local_retries,
    );
    let (mailbox, _handle) = universe.spawn_builder().spawn(supervisor);
    Ok(mailbox)
}
