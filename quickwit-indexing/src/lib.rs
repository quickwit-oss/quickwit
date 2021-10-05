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

use anyhow::bail;
use quickwit_actors::Universe;
use quickwit_metastore::Metastore;
use quickwit_storage::StorageUriResolver;

use crate::actors::{IndexerParams, IndexingPipelineParams, IndexingPipelineSupervisor};
use crate::models::IndexingStatistics;
use crate::source::SourceConfig;
pub use crate::split_store::{IndexingSplitStore, IndexingSplitStoreParams};

pub mod actors;
mod garbage_collection;
mod merge_policy;
pub mod models;
pub(crate) mod semaphore;
pub mod source;
mod split_store;
mod test_utils;

pub use test_utils::{mock_split_meta, TestSandbox};

pub use self::garbage_collection::{
    delete_splits_with_files, run_garbage_collect, FileEntry, SplitDeletionStats,
};
pub use self::merge_policy::{MergePolicy, StableMultitenantWithTimestampMergePolicy};

pub async fn index_data(
    index_id: String,
    metastore: Arc<dyn Metastore>,
    indexer_params: IndexerParams,
    source_config: SourceConfig,
    storage_uri_resolver: StorageUriResolver,
) -> anyhow::Result<IndexingStatistics> {
    let universe = Universe::new();
    let indexing_pipeline_params = IndexingPipelineParams {
        index_id,
        source_config,
        indexer_params,
        metastore,
        storage_uri_resolver,
    };
    let indexing_supervisor = IndexingPipelineSupervisor::new(indexing_pipeline_params);
    let (_pipeline_mailbox, pipeline_handler) =
        universe.spawn_actor(indexing_supervisor).spawn_async();
    let (pipeline_termination, statistics) = pipeline_handler.join().await;
    if !pipeline_termination.is_success() {
        bail!(pipeline_termination);
    }
    Ok(statistics)
}

pub(crate) fn new_split_id() -> String {
    ulid::Ulid::new().to_string()
}
