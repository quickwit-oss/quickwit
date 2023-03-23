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

#![deny(clippy::disallowed_methods)]

use std::sync::Arc;

use quickwit_actors::{Mailbox, Universe};
use quickwit_cluster::Cluster;
use quickwit_config::QuickwitConfig;
use quickwit_ingest::IngestApiService;
use quickwit_metastore::Metastore;
use quickwit_storage::StorageUriResolver;
use tracing::info;

pub use crate::actors::{
    IndexingPipeline, IndexingPipelineParams, IndexingService, IndexingServiceError, PublisherType,
    Sequencer, SplitsUpdateMailbox,
};
pub use crate::controlled_directory::ControlledDirectory;
use crate::models::IndexingStatistics;
pub use crate::split_store::{get_tantivy_directory_from_split_bundle, IndexingSplitStore};

pub mod actors;
mod controlled_directory;
pub mod grpc_adapter;
pub mod indexing_client;
pub mod merge_policy;
mod metrics;
pub mod models;
pub mod source;
mod split_store;
#[cfg(any(test, feature = "testsuite"))]
mod test_utils;

#[cfg(any(test, feature = "testsuite"))]
pub use test_utils::{mock_split, mock_split_meta, TestSandbox};

use self::merge_policy::MergePolicy;
pub use self::source::check_source_connectivity;

#[derive(utoipa::OpenApi)]
#[openapi(components(schemas(IndexingStatistics)))]
/// Schema used for the OpenAPI generation which are apart of this crate.
pub struct IndexingApiSchemas;

pub fn new_split_id() -> String {
    ulid::Ulid::new().to_string()
}

pub async fn start_indexing_service(
    universe: &Universe,
    config: &QuickwitConfig,
    cluster: Arc<Cluster>,
    metastore: Arc<dyn Metastore>,
    ingest_api_service: Mailbox<IngestApiService>,
    storage_resolver: StorageUriResolver,
) -> anyhow::Result<Mailbox<IndexingService>> {
    info!("Starting indexer service.");

    // Spawn indexing service.
    let indexing_service = IndexingService::new(
        config.node_id.clone(),
        config.data_dir_path.to_path_buf(),
        config.indexer_config.clone(),
        cluster,
        metastore.clone(),
        Some(ingest_api_service),
        storage_resolver,
    )
    .await?;
    let (indexing_service, _) = universe.spawn_builder().spawn(indexing_service);

    Ok(indexing_service)
}
