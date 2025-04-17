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

use quickwit_actors::{Mailbox, Universe};
use quickwit_cluster::Cluster;
use quickwit_common::pubsub::EventBroker;
use quickwit_config::NodeConfig;
use quickwit_ingest::{IngestApiService, IngesterPool};
use quickwit_proto::indexing::PipelineMetrics;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_storage::StorageResolver;
use tracing::info;

use crate::actors::MergeSchedulerService;
pub use crate::actors::{
    FinishPendingMergesAndShutdownPipeline, IndexingError, IndexingPipeline,
    IndexingPipelineParams, IndexingService, PublisherType, Sequencer, SplitsUpdateMailbox,
};
pub use crate::controlled_directory::ControlledDirectory;
use crate::models::IndexingStatistics;
pub use crate::split_store::{IndexingSplitStore, get_tantivy_directory_from_split_bundle};

pub mod actors;
mod controlled_directory;
pub mod merge_policy;
mod metrics;
pub mod models;
pub mod source;
mod split_store;
#[cfg(any(test, feature = "testsuite"))]
mod test_utils;

use quickwit_proto::indexing::CpuCapacity;
#[cfg(any(test, feature = "testsuite"))]
pub use test_utils::{MockSplitBuilder, TestSandbox, mock_split, mock_split_meta};

use self::merge_policy::MergePolicy;
pub use self::source::check_source_connectivity;

#[derive(utoipa::OpenApi)]
#[openapi(components(schemas(IndexingStatistics, PipelineMetrics, CpuCapacity)))]
/// Schema used for the OpenAPI generation which are apart of this crate.
pub struct IndexingApiSchemas;

pub fn new_split_id() -> String {
    ulid::Ulid::new().to_string()
}

#[allow(clippy::too_many_arguments)]
pub async fn start_indexing_service(
    universe: &Universe,
    config: &NodeConfig,
    num_blocking_threads: usize,
    cluster: Cluster,
    metastore: MetastoreServiceClient,
    ingester_pool: IngesterPool,
    storage_resolver: StorageResolver,
    event_broker: EventBroker,
) -> anyhow::Result<Mailbox<IndexingService>> {
    info!("starting indexer service");
    let ingest_api_service_mailbox = universe.get_one::<IngestApiService>();
    let (merge_scheduler_mailbox, _) = universe.spawn_builder().spawn(MergeSchedulerService::new(
        config.indexer_config.merge_concurrency.get(),
    ));
    // Spawn indexing service.
    let indexing_service = IndexingService::new(
        config.node_id.clone(),
        config.data_dir_path.to_path_buf(),
        config.indexer_config.clone(),
        num_blocking_threads,
        cluster,
        metastore.clone(),
        ingest_api_service_mailbox,
        merge_scheduler_mailbox,
        ingester_pool,
        storage_resolver,
        event_broker,
    )
    .await?;
    let (indexing_service, _) = universe.spawn_builder().spawn(indexing_service);
    Ok(indexing_service)
}
