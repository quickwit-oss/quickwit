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

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use actors::INDEXING_DIR_NAME;
use itertools::Itertools;
use models::CACHE;
use quickwit_actors::{Mailbox, Universe};
use quickwit_common::fs::empty_dir;
use quickwit_config::QuickwitConfig;
use quickwit_index_management::IndexManagementClient;
use quickwit_ingest_api::IngestApiService;
use quickwit_metastore::Metastore;
use quickwit_storage::StorageUriResolver;
use tracing::info;

pub use crate::actors::IndexingServiceError;
use crate::actors::{
    IndexingPipeline, IndexingPipelineParams, IndexingService, IngestApiGarbageCollector,
};
use crate::models::{IndexingStatistics, SpawnPipelinesForIndex};
pub use crate::split_store::{
    get_tantivy_directory_from_split_bundle, IndexingSplitStore, IndexingSplitStoreParams,
    SplitFolder,
};

pub mod actors;
mod controlled_directory;
mod garbage_collection;
pub mod merge_policy;
pub mod models;
pub mod source;
mod split_store;
mod test_utils;

pub use test_utils::{mock_split, mock_split_meta, TestSandbox};

pub use self::garbage_collection::{
    delete_splits_with_files, run_garbage_collect, FileEntry, SplitDeletionError,
};
use self::merge_policy::{MergePolicy, StableMultitenantWithTimestampMergePolicy};
pub use self::source::check_source_connectivity;

pub fn new_split_id() -> String {
    ulid::Ulid::new().to_string()
}

pub async fn start_indexer_service(
    universe: &Universe,
    config: &QuickwitConfig,
    metastore: Arc<dyn Metastore>,
    index_management_client: IndexManagementClient,
    storage_uri_resolver: StorageUriResolver,
    ingest_api_service: Option<Mailbox<IngestApiService>>,
) -> anyhow::Result<Mailbox<IndexingService>> {
    info!("Starting indexer service.");
    let indexing_server = IndexingService::new(
        config.data_dir_path.to_path_buf(),
        config.indexer_config.clone(),
        index_management_client.clone(),
        storage_uri_resolver,
        ingest_api_service.clone(),
    );
    let (indexer_service_mailbox, _) = universe.spawn_actor(indexing_server).spawn();

    let index_metadatas = metastore.list_indexes_metadatas().await?;
    info!(index_ids = %index_metadatas.iter().map(|im| &im.index_id).join(", "), "Spawning indexing pipeline(s).");

    for index_metadata in index_metadatas {
        indexer_service_mailbox
            .ask_for_res(SpawnPipelinesForIndex {
                index_id: index_metadata.index_id,
            })
            .await?;
    }

    // IngestApi garbage collector
    if let Some(ingest_api_service_mailbox) = ingest_api_service {
        let ingest_api_garbage_collector = IngestApiGarbageCollector::new(
            metastore,
            ingest_api_service_mailbox,
            indexer_service_mailbox.clone(),
        );
        universe.spawn_actor(ingest_api_garbage_collector).spawn();
    }

    Ok(indexer_service_mailbox)
}

/// Helper function to get the cache path.
pub fn get_cache_directory_path(data_dir_path: &Path, index_id: &str, source_id: &str) -> PathBuf {
    data_dir_path
        .join(INDEXING_DIR_NAME)
        .join(index_id)
        .join(source_id)
        .join(CACHE)
}

/// Clears the cache directory of a given source.
///
/// * `data_dir_path` - Path to directory where data (tmp data, splits kept for caching purpose) is
///   persisted.
/// * `index_id` - The target index Id.
/// * `source_id` -  The source Id.
pub async fn clear_cache_directory(
    data_dir_path: &Path,
    index_id: String,
    source_id: String,
) -> anyhow::Result<()> {
    let cache_directory_path = get_cache_directory_path(data_dir_path, &index_id, &source_id);
    info!(path = %cache_directory_path.display(), "Clearing cache directory.");
    empty_dir(&cache_directory_path).await?;
    Ok(())
}

/// Removes the indexing directory of a given index.
///
/// * `data_dir_path` - Path to directory where data (tmp data, splits kept for caching purpose) is
///   persisted.
/// * `index_id` - The target index ID.
pub async fn remove_indexing_directory(data_dir_path: &Path, index_id: String) -> io::Result<()> {
    let indexing_directory_path = data_dir_path.join(INDEXING_DIR_NAME).join(index_id);
    info!(path = %indexing_directory_path.display(), "Clearing indexing directory.");
    match tokio::fs::remove_dir_all(indexing_directory_path.as_path()).await {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}
