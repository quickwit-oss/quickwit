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

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use futures::future::join_all;
use anyhow::{Context, bail};
use counters::COUNTERS;
use itertools::Itertools;
use quickwit_actors::{ActorExitStatus, ActorHandle, ObservationType, Universe};
use quickwit_metastore::{Metastore, MetastoreUriResolver};
use quickwit_storage::{Storage, StorageUriResolver, quickwit_storage_uri_resolver};
use tracing::{debug, info, warn};

use crate::actors::{IndexerParams, IndexingPipelineParams, IndexingPipelineSupervisor};
use crate::models::{CommitPolicy, IndexingStatistics, ScratchDirectory};
use crate::rest::start_rest_service;
use crate::source::SourceConfig;

pub mod actors;
mod garbage_collection;
mod merge_policy;
pub mod models;
pub(crate) mod semaphore;
pub mod source;
pub mod counters;
pub mod rest;
pub mod args;
mod test_utils;

pub use test_utils::{mock_split_meta, TestSandbox};
pub use crate::args::ServeIndexingArgs;
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

/// Fetch source configs from storage for each index id.
/// The source path must have the form `{index_id}.json`.
// TODO: accept other format.
async fn load_sources(source_storage: Arc<dyn Storage>, index_ids: &[String]) -> anyhow::Result<Vec<(&String, SourceConfig)>> {
    let mut source_configs = vec![];
    for index_id in index_ids {
        let source_config_filename = format!("{}.json", index_id);
        let source_config_path = Path::new(&source_config_filename);
        if !source_storage.exists(source_config_path).await? {
            info!(index_id=?index_id, "No source file found for index.");
            continue;
        }
        let source_config_bytes = source_storage.get_all(source_config_path).await?;
        let source_config: SourceConfig = serde_json::from_slice(&source_config_bytes)
            .with_context(|| {
                format!(
                    "Failed to parse source config file `{}`.",
                    source_config_path.display()
                )
            })?;
        debug!(index_id=?index_id, "Source config built.");
        source_configs.push((index_id, source_config));
    }
    Ok(source_configs)
}

/// Start Quickwit indexing server.
pub async fn serve_indexing_cli(args: ServeIndexingArgs) -> anyhow::Result<()> {
    debug!(args=?args.rest_socket_addr, "serve-indexing-cli");

    let storage_uri_resolver = quickwit_storage_uri_resolver();
    let metastore_resolver = MetastoreUriResolver::default();
    let metastore = metastore_resolver.resolve(&args.metastore_uri).await?;
    let source_storage = storage_uri_resolver.resolve(&args.source_config_uri)?;
    
    // Load sources from storage.
    let sources = load_sources(source_storage, &args.index_ids).await?;
    if sources.is_empty() {
        info!("No source for indexes. Exiting...");
        return Ok(());
    }

    let scratch_directory = if let Some(scratch_root_path) = args.temp_dir.as_ref() {
        ScratchDirectory::new_in_path(scratch_root_path.clone())
    } else {
        ScratchDirectory::try_new_temp()
            .with_context(|| "Failed to create a tempdir for the indexer")?
    };

    // Create indexing pilelines.
    let universe = Universe::new();
    let mut supervisor_handlers = vec![];
    let mut supervisor_index_ids = vec![];
    for (index_id, source_config) in sources.into_iter() {
        debug!(index_id=?index_id, "instantiate-indexing-pipeline");
        let indexer_params = IndexerParams {
            scratch_directory: scratch_directory.temp_child()?,
            heap_size: args.heap_size,
            commit_policy: CommitPolicy::default(), //< TODO make the commit policy configurable
        };
        let indexing_pipeline_params = IndexingPipelineParams {
            index_id: index_id.clone(),
            source_config,
            indexer_params,
            metastore: metastore.clone(),
            storage_uri_resolver: storage_uri_resolver.clone(),
        };
        let indexing_supervisor = IndexingPipelineSupervisor::new(indexing_pipeline_params);
        let (_supervisor_mailbox, supervisor_handler) = universe.spawn_actor(indexing_supervisor).spawn_async();
        supervisor_handlers.push(supervisor_handler);
        supervisor_index_ids.push(index_id);
    }

    let rest_server = start_rest_service(args.rest_socket_addr);
    let observe_result = observe_supervisors(&supervisor_index_ids, supervisor_handlers);
    tokio::try_join!(rest_server, observe_result)?;
    Ok(())
}

/// Loop on supervisor handler observe and update prometheus metrics with observation.
// TODO: remove statistics and use counters directly into actors?
async fn observe_supervisors(index_ids: &[&String], supervisor_handlers: Vec<ActorHandle<IndexingPipelineSupervisor>>) -> anyhow::Result<()> {
    let mut report_interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        report_interval.tick().await;
        let futures = supervisor_handlers
            .iter()
            .map(|handler| handler.observe());
        let observations = join_all(futures).await;

        for (index_id, observation) in index_ids.iter().zip(observations.iter()) {
            if observation.obs_type != ObservationType::PostMortem {
                COUNTERS.num_docs.with_label_values(&[index_id]).set(observation.num_docs as i64);
                COUNTERS.num_invalid_docs.with_label_values(&[index_id]).set(observation.num_invalid_docs as i64);
                COUNTERS.num_local_splits.with_label_values(&[index_id]).set(observation.num_local_splits as i64);
                COUNTERS.num_published_splits.with_label_values(&[index_id]).set(observation.num_published_splits as i64);
                COUNTERS.num_staged_splits.with_label_values(&[index_id]).set(observation.num_staged_splits as i64);
                COUNTERS.num_uploaded_splits.with_label_values(&[index_id]).set(observation.num_uploaded_splits as i64);
                COUNTERS.total_bytes_processed.with_label_values(&[index_id]).set(observation.total_bytes_processed as i64);
                COUNTERS.total_size_splits.with_label_values(&[index_id]).set(observation.total_size_splits as i64);
            }   
        }
        if observations.iter().all(|observation| {
            observation.obs_type == ObservationType::PostMortem
        }) {
            warn!("all supervisors are down, stopping observations.");
            break;
        }
    }
    let join_handles = supervisor_handlers.into_iter()
        .map(|handler| handler.join());
    let exit_statuses_futures = join_all(join_handles).await;
    let exit_statuses = exit_statuses_futures
        .iter()
        .map(|(status, _)| status)
        .collect_vec();
    if exit_statuses.iter().all(|exit_status| {matches!(exit_status, ActorExitStatus::Success)}) {
        return Ok(());
    }
    Ok(())
}
