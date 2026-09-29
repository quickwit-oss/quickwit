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

//! Local Parquet bulk load. See `docs/internals/parquet-bulk-load.md`.
//! Requires no published splits; failures trigger best-effort index cleanup.

use std::io::stdout;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::bail;
use bytesize::ByteSize;
use colored::Colorize;
use quickwit_actors::{ActorHandle, Mailbox, Universe};
use quickwit_config::{CLI_SOURCE_ID, NodeConfig, SourceConfig, SourceParams, TransformConfig};
use quickwit_index_management::IndexService;
use quickwit_indexing::IndexingPipeline;
use quickwit_indexing::actors::IndexingService;
use quickwit_indexing::models::{DetachIndexingPipeline, IndexingStatistics, SpawnPipeline};
use quickwit_indexing::source::SourceLoader;
use quickwit_indexing::source::parquet_file::{
    DEFAULT_PARQUET_BATCH_NUM_ROWS, ParquetLoadPlan, ParquetSourceFactory,
};
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
use crate::{get_resolvers, load_node_config, run_index_checklist};

mod lifecycle;
#[cfg(test)]
mod tests;

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
    let index_metadata = fetch_index_metadata(&metastore, &args.index_id).await?;
    if args.overwrite {
        lifecycle::clear_index_checked(&mut index_service, &index_metadata.index_uid).await?;
    }
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
    // All sources share the same row-group plan.
    let mut source_loader = SourceLoader::default();
    source_loader.add_source(SourceType::File, ParquetSourceFactory::new(plan.clone()));
    let universe = Universe::new();
    let (indexing_server_mailbox, indexing_server_handle) = spawn_indexing_service(
        &universe,
        &config,
        metastore.clone(),
        storage_resolver,
        Arc::new(source_loader),
        false,
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
    universe.kill();
    indexing_server_handle.kill().await;
    lifecycle::quiesce(&universe).await;
    lifecycle::finish_load(
        load_res,
        &mut index_service,
        &args,
        &config,
        &index_metadata.index_uid,
    )
    .await
}

/// Runs indexing without merges. The caller handles cleanup on failure.
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
    let pipeline_handles = spawn_pipelines(indexing_server_mailbox, args, source_config).await?;
    let indexing_statistics = wait_for_indexing_pipelines(pipeline_handles).await?;
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

/// Detaches each pipeline immediately after spawning it.
async fn spawn_pipelines(
    indexing_server_mailbox: &Mailbox<IndexingService>,
    args: &LocalIngestDocsArgs,
    source_config: SourceConfig,
) -> anyhow::Result<Vec<ActorHandle<IndexingPipeline>>> {
    let mut handles = Vec::with_capacity(args.num_pipelines.get());
    for _ in 0..args.num_pipelines.get() {
        let pipeline_id = indexing_server_mailbox
            .ask_for_res(SpawnPipeline {
                index_id: args.index_id.clone(),
                source_config: source_config.clone(),
                pipeline_uid: PipelineUid::random(),
            })
            .await?;
        let handle = indexing_server_mailbox
            .ask_for_res(DetachIndexingPipeline { pipeline_id })
            .await?;
        handles.push(handle);
    }
    Ok(handles)
}

/// Waits for all pipelines, reporting progress and summing statistics.
/// Aborts on restart because claimed row groups cannot be replayed.
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

/// Aborts on restart or spawn retry; supervisors otherwise retry indefinitely.
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
