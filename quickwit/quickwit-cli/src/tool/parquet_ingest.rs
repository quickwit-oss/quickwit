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

//! Bulk loads a local Parquet file with several indexing pipelines. See
//! `docs/internals/parquet-bulk-load.md`.
//!
//! A load is all or nothing: it requires an index without published splits, and the index is
//! cleared if anything fails once the pipelines are spawned.

use std::io::stdout;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use bytesize::ByteSize;
use colored::Colorize;
use quickwit_actors::{ActorHandle, Mailbox, Universe};
use quickwit_config::{CLI_SOURCE_ID, NodeConfig, SourceConfig, SourceParams, TransformConfig};
use quickwit_index_management::{IndexService, clear_cache_directory};
use quickwit_indexing::actors::{IndexingService, MergePipeline};
use quickwit_indexing::models::{
    DetachIndexingPipeline, DetachMergePipeline, IndexingStatistics, SpawnPipeline,
};
use quickwit_indexing::source::SourceLoader;
use quickwit_indexing::source::parquet_file::{
    DEFAULT_PARQUET_BATCH_NUM_ROWS, ParquetLoadPlan, ParquetSourceFactory,
};
use quickwit_indexing::{FinishPendingMergesAndShutdownPipeline, IndexingPipeline};
use quickwit_metastore::{
    IndexMetadata, IndexMetadataResponseExt, ListSplitsQuery, ListSplitsRequestExt,
    MetastoreServiceStreamSplitsExt, SplitMetadata, SplitState,
};
use quickwit_proto::metastore::{
    IndexMetadataRequest, ListSplitsRequest, MetastoreService, MetastoreServiceClient, SourceType,
};
use quickwit_proto::types::PipelineUid;
use thousands::Separable;

use super::parquet_report::LoadReport;
use super::{
    LocalIngestDocsArgs, ThroughputCalculator, display_statistics, spawn_indexing_service,
};
use crate::checklist::GREEN_COLOR;
use crate::{get_resolvers, load_node_config, run_index_checklist};

const REPORT_INTERVAL: Duration = Duration::from_secs(1);

pub(super) async fn local_ingest_parquet_cli(args: LocalIngestDocsArgs) -> anyhow::Result<()> {
    let input_uri = args
        .input_path_opt
        .clone()
        .expect("the Parquet input is detected from `--input-path`");
    let batch_num_rows = args
        .batch_num_rows_opt
        .map(|batch_num_rows| batch_num_rows.get())
        .unwrap_or(DEFAULT_PARQUET_BATCH_NUM_ROWS);
    println!(
        "❯ Ingesting Parquet file locally with {} pipeline(s)...",
        args.num_pipelines
    );
    let config = load_node_config(&args.config_uri, None).await?;
    let (storage_resolver, metastore_resolver) =
        get_resolvers(&config.storage_configs, &config.metastore_configs);
    let mut metastore = metastore_resolver.resolve(&config.metastore_uri).await?;

    let source_config = SourceConfig {
        source_id: CLI_SOURCE_ID.to_string(),
        num_pipelines: args.num_pipelines,
        enabled: true,
        source_params: SourceParams::file_from_uri(input_uri.clone()),
        transform_config: args
            .vrl_script
            .clone()
            .map(|vrl_script| TransformConfig::new(vrl_script, None)),
        input_format: args.input_format,
    };
    run_index_checklist(
        &mut metastore,
        &storage_resolver,
        &args.index_id,
        Some(&source_config),
    )
    .await?;

    let mut index_service = IndexService::new(metastore.clone(), storage_resolver.clone());
    if args.overwrite {
        index_service.clear_index(&args.index_id).await?;
    }
    let index_metadata = fetch_index_metadata(&metastore, &args.index_id).await?;
    if !list_published_splits(&metastore, &index_metadata)
        .await?
        .is_empty()
    {
        bail!(
            "index `{}` already has published splits: a Parquet load requires an empty index, use \
             `--overwrite` to clear it",
            args.index_id
        );
    }
    let plan =
        tokio::task::spawn_blocking(move || ParquetLoadPlan::try_new(input_uri, batch_num_rows))
            .await??;
    println!(
        "Parquet file: {} rows, {} row groups, {} uncompressed.",
        plan.num_rows().separate_with_commas(),
        plan.num_row_groups().separate_with_commas(),
        ByteSize(plan.num_uncompressed_bytes()),
    );
    let plan = Arc::new(plan);
    // The indexing service creates the sources of the pipelines with this loader: they all read
    // from the plan.
    let mut source_loader = SourceLoader::default();
    source_loader.add_source(SourceType::File, ParquetSourceFactory::new(plan.clone()));
    let universe = Universe::new();
    let (indexing_server_mailbox, _indexing_server_handle) = spawn_indexing_service(
        &universe,
        &config,
        metastore.clone(),
        storage_resolver,
        Arc::new(source_loader),
    )
    .await?;
    let load_res = run_load(
        &indexing_server_mailbox,
        &plan,
        &args,
        &config,
        &metastore,
        source_config,
        &index_metadata,
    )
    .await;
    // Kills all the actors, including the detached pipelines, before the index is cleared.
    universe.quit().await;

    if let Err(load_error) = load_res {
        println!("Load failed, clearing index `{}`...", args.index_id);
        index_service
            .clear_index(&args.index_id)
            .await
            .context("failed to clear the index after the load failed")?;
        clear_cache_directory_if_requested(&args, &config).await?;
        return Err(load_error);
    }
    clear_cache_directory_if_requested(&args, &config).await
}

/// Indexes the file, then shuts down the merge pipeline. The caller must clear the index if this
/// fails.
async fn run_load(
    indexing_server_mailbox: &Mailbox<IndexingService>,
    plan: &ParquetLoadPlan,
    args: &LocalIngestDocsArgs,
    config: &NodeConfig,
    metastore: &MetastoreServiceClient,
    source_config: SourceConfig,
    index_metadata: &IndexMetadata,
) -> anyhow::Result<()> {
    let start_time = Instant::now();
    let (pipeline_handles, merge_pipeline_handle) =
        spawn_pipelines(indexing_server_mailbox, args, source_config).await?;
    let indexing_statistics = wait_for_indexing_pipelines(pipeline_handles).await?;

    // With the `no_merge` merge policy, there is no merge to finish.
    merge_pipeline_handle
        .mailbox()
        .ask(FinishPendingMergesAndShutdownPipeline)
        .await?;
    let (exit_status, _) = merge_pipeline_handle.join().await;
    if !exit_status.is_success() {
        bail!("merge pipeline failed: {exit_status:?}");
    }
    let report = LoadReport {
        num_pipelines: args.num_pipelines.get(),
        plan,
        indexing_statistics,
        elapsed: start_time.elapsed(),
        splits: list_published_splits(metastore, index_metadata).await?,
        index_config: &index_metadata.index_config,
        metastore_uri: &config.metastore_uri,
    };
    report.print();
    report.check()
}

/// Spawns the indexing pipelines and detaches them, along with their shared merge pipeline.
async fn spawn_pipelines(
    indexing_server_mailbox: &Mailbox<IndexingService>,
    args: &LocalIngestDocsArgs,
    source_config: SourceConfig,
) -> anyhow::Result<(
    Vec<ActorHandle<IndexingPipeline>>,
    ActorHandle<MergePipeline>,
)> {
    let mut pipeline_ids = Vec::with_capacity(args.num_pipelines.get());
    for _ in 0..args.num_pipelines.get() {
        let pipeline_id = indexing_server_mailbox
            .ask_for_res(SpawnPipeline {
                index_id: args.index_id.clone(),
                source_config: source_config.clone(),
                pipeline_uid: PipelineUid::random(),
            })
            .await?;
        pipeline_ids.push(pipeline_id);
    }
    // All the pipelines share the same merge pipeline. It must be detached after all the pipelines
    // are spawned, otherwise the indexing service would spawn a new one.
    let merge_pipeline_handle = indexing_server_mailbox
        .ask_for_res(DetachMergePipeline {
            pipeline_id: pipeline_ids[0].merge_pipeline_id(),
        })
        .await?;
    let mut pipeline_handles = Vec::with_capacity(pipeline_ids.len());
    for pipeline_id in pipeline_ids {
        let pipeline_handle = indexing_server_mailbox
            .ask_for_res(DetachIndexingPipeline { pipeline_id })
            .await?;
        pipeline_handles.push(pipeline_handle);
    }
    Ok((pipeline_handles, merge_pipeline_handle))
}

/// Displays the progress of the indexing pipelines until they all exit, then returns their
/// summed statistics.
///
/// Fails as soon as a pipeline restarts: the row groups handed out to the failed source are lost.
async fn wait_for_indexing_pipelines(
    pipeline_handles: Vec<ActorHandle<IndexingPipeline>>,
) -> anyhow::Result<IndexingStatistics> {
    let mut stdout_handle = stdout();
    let mut throughput_calculator = ThroughputCalculator::new(Instant::now());
    let mut report_interval = tokio::time::interval(REPORT_INTERVAL);

    loop {
        report_interval.tick().await;
        let mut statistics = IndexingStatistics::default();
        let mut num_running_pipelines = 0;

        for pipeline_handle in &pipeline_handles {
            pipeline_handle.refresh_observe();
            let pipeline_statistics = pipeline_handle.last_observation();
            check_no_restart(&pipeline_statistics)?;
            add_indexing_statistics(&mut statistics, &pipeline_statistics);
            if !pipeline_handle.state().is_exit() {
                num_running_pipelines += 1;
            }
        }
        if num_running_pipelines == 0 {
            break;
        }
        print!(" {} {num_running_pipelines}", "Pipelines".bright_blue());
        display_statistics(&mut stdout_handle, &mut throughput_calculator, &statistics)?;
    }
    let mut statistics = IndexingStatistics::default();

    for pipeline_handle in pipeline_handles {
        let (exit_status, pipeline_statistics) = pipeline_handle.join().await;
        if !exit_status.is_success() {
            bail!("indexing pipeline failed: {exit_status:?}");
        }
        check_no_restart(&pipeline_statistics)?;
        add_indexing_statistics(&mut statistics, &pipeline_statistics);
    }
    Ok(statistics)
}

/// A failed pipeline is respawned, and a failed spawn is retried, forever: in both cases, the
/// load is aborted.
fn check_no_restart(pipeline_statistics: &IndexingStatistics) -> anyhow::Result<()> {
    if pipeline_statistics.generation > 1 || pipeline_statistics.num_spawn_attempts > 1 {
        bail!("an indexing pipeline failed and restarted, see the logs for the cause");
    }
    Ok(())
}

fn add_indexing_statistics(total: &mut IndexingStatistics, statistics: &IndexingStatistics) {
    total.num_docs += statistics.num_docs;
    total.num_invalid_docs += statistics.num_invalid_docs;
    total.num_local_splits += statistics.num_local_splits;
    total.num_staged_splits += statistics.num_staged_splits;
    total.num_uploaded_splits += statistics.num_uploaded_splits;
    total.num_published_splits += statistics.num_published_splits;
    total.num_empty_splits += statistics.num_empty_splits;
    total.total_bytes_processed += statistics.total_bytes_processed;
    total.total_size_splits += statistics.total_size_splits;
}

async fn fetch_index_metadata(
    metastore: &MetastoreServiceClient,
    index_id: &str,
) -> anyhow::Result<IndexMetadata> {
    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await?
        .deserialize_index_metadata()?;
    Ok(index_metadata)
}

async fn list_published_splits(
    metastore: &MetastoreServiceClient,
    index_metadata: &IndexMetadata,
) -> anyhow::Result<Vec<SplitMetadata>> {
    let query = ListSplitsQuery::for_index(index_metadata.index_uid.clone())
        .with_split_state(SplitState::Published);
    let request = ListSplitsRequest::try_from_list_splits_query(&query)?;
    let splits = metastore
        .list_splits(request)
        .await?
        .collect_splits_metadata()
        .await?;
    Ok(splits)
}

async fn clear_cache_directory_if_requested(
    args: &LocalIngestDocsArgs,
    config: &NodeConfig,
) -> anyhow::Result<()> {
    if !args.clear_cache {
        return Ok(());
    }
    println!("Clearing local cache directory...");
    clear_cache_directory(&config.data_dir_path).await?;
    println!("{} Local cache directory cleared.", "✔".color(GREEN_COLOR));
    Ok(())
}
