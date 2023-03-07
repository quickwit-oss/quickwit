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

use std::collections::{HashSet, VecDeque};
use std::io::{stdout, Stdout, Write};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{env, fmt, io};

use anyhow::{bail, Context};
use clap::{arg, ArgMatches, Command};
use colored::{ColoredString, Colorize};
use humantime::format_duration;
use quickwit_actors::{ActorExitStatus, ActorHandle, ObservationType, Universe};
use quickwit_cluster::create_fake_cluster_for_cli;
use quickwit_common::uri::Uri;
use quickwit_common::{GREEN_COLOR, RED_COLOR};
use quickwit_config::service::QuickwitService;
use quickwit_config::{
    IndexerConfig, SourceConfig, SourceParams, TransformConfig, VecSourceParams,
    CLI_INGEST_SOURCE_ID,
};
use quickwit_core::{clear_cache_directory, IndexService};
use quickwit_indexing::actors::{IndexingService, MergePipeline, MergePipelineId};
use quickwit_indexing::models::{
    DetachIndexingPipeline, DetachMergePipeline, IndexingStatistics, SpawnPipeline,
};
use quickwit_indexing::IndexingPipeline;
use quickwit_metastore::quickwit_metastore_uri_resolver;
use quickwit_storage::{quickwit_storage_uri_resolver, BundleStorage, Storage};
use quickwit_telemetry::payload::TelemetryEvent;
use thousands::Separable;
use tracing::{debug, info};

use crate::{
    config_cli_arg, load_quickwit_config, parse_duration_with_unit, run_index_checklist,
    start_actor_runtimes, THROUGHPUT_WINDOW_SIZE,
};

pub fn build_tool_command<'a>() -> Command<'a> {
    Command::new("tool")
        .about("Performs utility operations. Requires a node config.")
        .arg(config_cli_arg())
        .subcommand(
            Command::new("local-ingest")
                .display_order(10)
                .about("Indexes NDJSON documents locally.")
                .long_about("Local ingest indexes locally NDJSON documents from a file or from stdin and uploads splits on the configured storage.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index")
                        .display_order(1),
                    arg!(--"input-path" <INPUT_PATH> "Location of the input file.")
                        .required(false),
                    arg!(--overwrite "Overwrites pre-existing index.")
                        .required(false),
                    arg!(--"transform-script" <SCRIPT> "VRL program to transform docs before ingesting.")
                        .required(false),
                    arg!(--"keep-cache" "Does not clear local cache directory upon completion.")
                        .required(false),
                ])
            )
        .subcommand(
            Command::new("extract-split")
                .about("Downloads and extracts a split to a directory.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index")
                        .display_order(1),
                    arg!(--split <SPLIT> "ID of the target split")
                        .display_order(2),
                    arg!(--"target-dir" <TARGET_DIR> "Directory to extract the split to."),
                ])
            )
        .subcommand(
            Command::new("gc")
                .display_order(10)
                .about("Garbage collects stale staged splits and splits marked for deletion.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index")
                        .display_order(1),
                    arg!(--"grace-period" <GRACE_PERIOD> "Threshold period after which stale staged splits are garbage collected.")
                        .default_value("1h")
                        .required(false),
                    arg!(--"dry-run" "Executes the command in dry run mode and only displays the list of splits candidates for garbage collection.")
                        .required(false),
                ])
            )
        .subcommand(
            Command::new("merge")
                .display_order(10)
                .about("Merges all the splits for a given Node ID, index ID, source ID.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index.")
                        .display_order(1),
                    arg!(--source <SOURCE_ID> "ID of the target source."),
                ])
            )
        .arg_required_else_help(true)
}

#[derive(Debug, Eq, PartialEq)]
pub struct LocalIngestDocsArgs {
    pub config_uri: Uri,
    pub index_id: String,
    pub input_path_opt: Option<PathBuf>,
    pub overwrite: bool,
    pub vrl_script: Option<String>,
    pub clear_cache: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct GarbageCollectIndexArgs {
    pub config_uri: Uri,
    pub index_id: String,
    pub grace_period: Duration,
    pub dry_run: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct MergeArgs {
    pub config_uri: Uri,
    pub index_id: String,
    pub source_id: String,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ExtractSplitArgs {
    pub config_uri: Uri,
    pub index_id: String,
    pub split_id: String,
    pub target_dir: PathBuf,
}

#[derive(Debug, Eq, PartialEq)]
pub enum ToolCliCommand {
    GarbageCollect(GarbageCollectIndexArgs),
    LocalIngest(LocalIngestDocsArgs),
    Merge(MergeArgs),
    ExtractSplit(ExtractSplitArgs),
}

impl ToolCliCommand {
    pub fn parse_cli_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches) = matches
            .subcommand()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse sub-matches."))?;
        match subcommand {
            "gc" => Self::parse_garbage_collect_args(submatches),
            "local-ingest" => Self::parse_local_ingest_args(submatches),
            "merge" => Self::parse_merge_args(submatches),
            "extract-split" => Self::parse_extract_split_args(submatches),
            _ => bail!("Tool subcommand `{}` is not implemented.", subcommand),
        }
    }

    fn parse_local_ingest_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let config_uri = matches
            .value_of("config")
            .map(Uri::from_str)
            .expect("`config` is a required arg.")?;
        let index_id = matches
            .value_of("index")
            .expect("`index` is a required arg.")
            .to_string();
        let input_path_opt = if let Some(input_path) = matches.value_of("input-path") {
            Uri::from_str(input_path)?
                .filepath()
                .map(|path| path.to_path_buf())
        } else {
            None
        };
        let overwrite = matches.is_present("overwrite");
        let vrl_script = matches
            .value_of("transform-script")
            .map(|source| source.to_string());
        let clear_cache = !matches.is_present("keep-cache");

        Ok(Self::LocalIngest(LocalIngestDocsArgs {
            config_uri,
            index_id,
            input_path_opt,
            overwrite,
            vrl_script,
            clear_cache,
        }))
    }

    fn parse_merge_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let config_uri = matches
            .value_of("config")
            .map(Uri::from_str)
            .expect("`config` is a required arg.")?;
        let index_id = matches
            .value_of("index")
            .context("'index-id' is a required arg.")?
            .to_string();
        let source_id = matches
            .value_of("source")
            .context("'source-id' is a required arg.")?
            .to_string();
        Ok(Self::Merge(MergeArgs {
            index_id,
            source_id,
            config_uri,
        }))
    }

    fn parse_garbage_collect_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let config_uri = matches
            .value_of("config")
            .map(Uri::from_str)
            .expect("`config` is a required arg.")?;
        let index_id = matches
            .value_of("index")
            .expect("`index` is a required arg.")
            .to_string();
        let grace_period = matches
            .value_of("grace-period")
            .map(parse_duration_with_unit)
            .expect("`grace-period` should have a default value.")?;
        let dry_run = matches.is_present("dry-run");
        Ok(Self::GarbageCollect(GarbageCollectIndexArgs {
            index_id,
            grace_period,
            dry_run,
            config_uri,
        }))
    }

    fn parse_extract_split_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_id = matches
            .value_of("index")
            .map(String::from)
            .expect("`index` is a required arg.");
        let split_id = matches
            .value_of("split")
            .map(String::from)
            .expect("`split` is a required arg.");
        let config_uri = matches
            .value_of("config")
            .map(Uri::from_str)
            .expect("`config` is a required arg.")?;
        let target_dir = matches
            .value_of("target-dir")
            .map(PathBuf::from)
            .expect("`target-dir` is a required arg.");
        Ok(Self::ExtractSplit(ExtractSplitArgs {
            config_uri,
            index_id,
            split_id,
            target_dir,
        }))
    }

    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            Self::GarbageCollect(args) => garbage_collect_index_cli(args).await,
            Self::LocalIngest(args) => local_ingest_docs_cli(args).await,
            Self::Merge(args) => merge_cli(args).await,
            Self::ExtractSplit(args) => extract_split_cli(args).await,
        }
    }
}

pub async fn local_ingest_docs_cli(args: LocalIngestDocsArgs) -> anyhow::Result<()> {
    debug!(args=?args, "local-ingest-docs");
    println!("❯ Ingesting documents locally...");
    quickwit_telemetry::send_telemetry_event(TelemetryEvent::Ingest).await;

    let config = load_quickwit_config(&args.config_uri).await?;

    let source_params = if let Some(filepath) = args.input_path_opt.as_ref() {
        SourceParams::file(filepath)
    } else {
        SourceParams::stdin()
    };
    let transform_config = args
        .vrl_script
        .map(|vrl_script| TransformConfig::new(vrl_script, None));
    let source_config = SourceConfig {
        source_id: CLI_INGEST_SOURCE_ID.to_string(),
        max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
        desired_num_pipelines: NonZeroUsize::new(1).unwrap(),
        enabled: true,
        source_params,
        transform_config,
    };
    run_index_checklist(&config.metastore_uri, &args.index_id, Some(&source_config)).await?;
    let metastore_uri_resolver = quickwit_metastore_uri_resolver();
    let metastore = metastore_uri_resolver
        .resolve(&config.metastore_uri)
        .await?;

    if args.overwrite {
        let index_service = IndexService::from_config(config.clone()).await?;
        index_service.clear_index(&args.index_id).await?;
    }
    // The indexing service needs to update its cluster chitchat state so that the control plane is
    // aware of the running tasks. We thus create a fake cluster to instantiate the indexing service
    // and avoid impacting potential control plane running on the cluster.
    let fake_cluster = create_fake_cluster_for_cli().await?;
    let indexer_config = IndexerConfig {
        ..Default::default()
    };
    start_actor_runtimes(&HashSet::from_iter([QuickwitService::Indexer]))?;
    let indexing_server = IndexingService::new(
        config.node_id.clone(),
        config.data_dir_path.clone(),
        indexer_config,
        Arc::new(fake_cluster),
        metastore,
        None,
        quickwit_storage_uri_resolver().clone(),
    )
    .await?;
    let universe = Universe::new();
    let (indexing_server_mailbox, indexing_server_handle) =
        universe.spawn_builder().spawn(indexing_server);
    let pipeline_id = indexing_server_mailbox
        .ask_for_res(SpawnPipeline {
            index_id: args.index_id.clone(),
            source_config,
            pipeline_ord: 0,
        })
        .await?;
    let merge_pipeline_handle = indexing_server_mailbox
        .ask_for_res(DetachMergePipeline {
            pipeline_id: MergePipelineId::from(&pipeline_id),
        })
        .await?;
    let indexing_pipeline_handle = indexing_server_mailbox
        .ask_for_res(DetachIndexingPipeline { pipeline_id })
        .await?;

    let is_stdin_atty = atty::is(atty::Stream::Stdin);
    if args.input_path_opt.is_none() && is_stdin_atty {
        let eof_shortcut = match env::consts::OS {
            "windows" => "CTRL+Z",
            _ => "CTRL+D",
        };
        println!(
            "Please, enter JSON documents one line at a time.\nEnd your input using \
             {eof_shortcut}."
        );
    }
    let statistics =
        start_statistics_reporting_loop(indexing_pipeline_handle, args.input_path_opt.is_none())
            .await?;
    merge_pipeline_handle.quit().await;
    // Shutdown the indexing server.
    universe
        .send_exit_with_success(&indexing_server_mailbox)
        .await?;
    indexing_server_handle.join().await;
    universe.quit().await;
    if statistics.num_published_splits > 0 {
        println!(
            "Now, you can query the index with the following command:\nquickwit index search \
             --index {} --config ./config/quickwit.yaml --query \"my query\"",
            args.index_id
        );
    }

    if args.clear_cache {
        println!("Clearing local cache directory...");
        clear_cache_directory(&config.data_dir_path).await?;
        println!("{} Local cache directory cleared.", "✔".color(GREEN_COLOR));
    }

    match statistics.num_invalid_docs {
        0 => {
            println!("{} Documents successfully indexed.", "✔".color(GREEN_COLOR));
            Ok(())
        }
        _ => bail!("Failed to ingest all the documents."),
    }
}

pub async fn merge_cli(args: MergeArgs) -> anyhow::Result<()> {
    debug!(args=?args, "run-merge-operations");
    println!("❯ Merging splits locally...");
    let config = load_quickwit_config(&args.config_uri).await?;
    run_index_checklist(&config.metastore_uri, &args.index_id, None).await?;
    let indexer_config = IndexerConfig {
        ..Default::default()
    };
    // The indexing service needs to update its cluster chitchat state so that the control plane is
    // aware of the running tasks. We thus create a fake cluster to instantiate the indexing service
    // and avoid impacting potential control plane running on the cluster.
    let fake_cluster = create_fake_cluster_for_cli().await?;
    let metastore_uri_resolver = quickwit_metastore_uri_resolver();
    let metastore = metastore_uri_resolver
        .resolve(&config.metastore_uri)
        .await?;
    let storage_resolver = quickwit_storage_uri_resolver().clone();
    start_actor_runtimes(&HashSet::from_iter([QuickwitService::Indexer]))?;
    let universe = Universe::new();
    let indexing_server = IndexingService::new(
        config.node_id,
        config.data_dir_path,
        indexer_config,
        Arc::new(fake_cluster),
        metastore,
        None,
        storage_resolver,
    )
    .await?;
    let (indexing_service_mailbox, indexing_service_handle) =
        universe.spawn_builder().spawn(indexing_server);
    let pipeline_id = indexing_service_mailbox
        .ask_for_res(SpawnPipeline {
            index_id: args.index_id,
            source_config: SourceConfig {
                source_id: args.source_id,
                max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
                desired_num_pipelines: NonZeroUsize::new(1).unwrap(),
                enabled: true,
                source_params: SourceParams::Vec(VecSourceParams::default()),
                transform_config: None,
            },
            pipeline_ord: 0,
        })
        .await?;
    let pipeline_handle: ActorHandle<MergePipeline> = indexing_service_mailbox
        .ask_for_res(DetachMergePipeline {
            pipeline_id: MergePipelineId::from(&pipeline_id),
        })
        .await?;

    let mut check_interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        check_interval.tick().await;

        let observation = pipeline_handle.observe().await;

        if observation.num_ongoing_merges == 0 {
            info!("Merge pipeline has no more ongoing merges, Exiting.");
            break;
        }

        if observation.obs_type == ObservationType::PostMortem {
            info!("Merge pipeline has exited, Exiting.");
            break;
        }
    }

    let (pipeline_exit_status, _pipeline_statistics) = pipeline_handle.quit().await;
    indexing_service_handle.quit().await;
    if !matches!(
        pipeline_exit_status,
        ActorExitStatus::Success | ActorExitStatus::Quit
    ) {
        bail!(pipeline_exit_status);
    }
    println!("{} Merge successful.", "✔".color(GREEN_COLOR));
    Ok(())
}

pub async fn garbage_collect_index_cli(args: GarbageCollectIndexArgs) -> anyhow::Result<()> {
    debug!(args=?args, "garbage-collect-index");
    println!("❯ Garbage collecting index...");
    quickwit_telemetry::send_telemetry_event(TelemetryEvent::GarbageCollect).await;

    let quickwit_config = load_quickwit_config(&args.config_uri).await?;
    let index_service = IndexService::from_config(quickwit_config.clone()).await?;
    let removal_info = index_service
        .garbage_collect_index(&args.index_id, args.grace_period, args.dry_run)
        .await?;
    if removal_info.removed_split_entries.is_empty() && removal_info.failed_split_ids.is_empty() {
        println!("No dangling files to garbage collect.");
        return Ok(());
    }

    if args.dry_run {
        println!("The following files will be garbage collected.");
        for file_entry in removal_info.removed_split_entries {
            println!(" - {}", file_entry.file_name);
        }
        return Ok(());
    }

    if !removal_info.failed_split_ids.is_empty() {
        println!("The following splits were attempted to be removed, but failed.");
        for split_id in removal_info.failed_split_ids.iter() {
            println!(" - {split_id}");
        }
        println!(
            "{} Splits were unable to be removed.",
            removal_info.failed_split_ids.len()
        );
    }

    let deleted_bytes: u64 = removal_info
        .removed_split_entries
        .iter()
        .map(|entry| entry.file_size_in_bytes)
        .sum();
    println!(
        "{}MB of storage garbage collected.",
        deleted_bytes / 1_000_000
    );

    if removal_info.failed_split_ids.is_empty() {
        println!(
            "{} Index successfully garbage collected.",
            "✔".color(GREEN_COLOR)
        );
    } else if removal_info.removed_split_entries.is_empty()
        && !removal_info.failed_split_ids.is_empty()
    {
        println!("{} Failed to garbage collect index.", "✘".color(RED_COLOR));
    } else {
        println!(
            "{} Index partially garbage collected.",
            "✘".color(RED_COLOR)
        );
    }

    Ok(())
}

async fn extract_split_cli(args: ExtractSplitArgs) -> anyhow::Result<()> {
    debug!(args=?args, "extract-split");
    println!("❯ Extracting split...");

    let quickwit_config = load_quickwit_config(&args.config_uri).await?;
    let storage_uri_resolver = quickwit_storage_uri_resolver();
    let metastore_uri_resolver = quickwit_metastore_uri_resolver();
    let metastore = metastore_uri_resolver
        .resolve(&quickwit_config.metastore_uri)
        .await?;
    let index_metadata = metastore.index_metadata(&args.index_id).await?;
    let index_storage = storage_uri_resolver.resolve(index_metadata.index_uri())?;
    let split_file = PathBuf::from(format!("{}.split", args.split_id));
    let split_data = index_storage.get_all(split_file.as_path()).await?;
    let (_hotcache_bytes, bundle_storage) = BundleStorage::open_from_split_data_with_owned_bytes(
        index_storage,
        split_file,
        split_data,
    )?;
    std::fs::create_dir_all(&args.target_dir)?;
    for path in bundle_storage.iter_files() {
        let mut out_path = args.target_dir.to_owned();
        out_path.push(path);
        println!("Copying {out_path:?}");
        bundle_storage.copy_to_file(path, &out_path).await?;
    }

    println!("{} Split successfully extracted.", "✔".color(GREEN_COLOR));
    Ok(())
}

/// Starts a tokio task that displays the indexing statistics
/// every once in awhile.
pub async fn start_statistics_reporting_loop(
    pipeline_handle: ActorHandle<IndexingPipeline>,
    is_stdin: bool,
) -> anyhow::Result<IndexingStatistics> {
    let mut stdout_handle = stdout();
    let start_time = Instant::now();
    let mut throughput_calculator = ThroughputCalculator::new(start_time);
    let mut report_interval = tokio::time::interval(Duration::from_secs(1));

    loop {
        // TODO fixme. The way we wait today is a bit lame: if the indexing pipeline exits, we will
        // still wait up to an entire heartbeat...  Ideally we should  select between two
        // futures.
        report_interval.tick().await;
        // Try to receive with a timeout of 1 second.
        // 1 second is also the frequency at which we update statistic in the console
        let observation = pipeline_handle.observe().await;

        // Let's not display live statistics to allow screen to scroll.
        if observation.state.num_docs > 0 {
            display_statistics(
                &mut stdout_handle,
                &mut throughput_calculator,
                &observation.state,
            )?;
        }

        if observation.obs_type == ObservationType::PostMortem {
            break;
        }
    }
    let (pipeline_exit_status, pipeline_statistics) = pipeline_handle.join().await;
    if !pipeline_exit_status.is_success() {
        bail!(pipeline_exit_status);
    }
    // If we have received zero docs at this point,
    // there is no point in displaying report.
    if pipeline_statistics.num_docs == 0 {
        return Ok(pipeline_statistics);
    }

    if is_stdin {
        display_statistics(
            &mut stdout_handle,
            &mut throughput_calculator,
            &pipeline_statistics,
        )?;
    }
    // display end of task report
    println!();
    let secs = Duration::from_secs(start_time.elapsed().as_secs());
    if pipeline_statistics.num_invalid_docs == 0 {
        println!(
            "Indexed {} documents in {}.",
            pipeline_statistics.num_docs.separate_with_commas(),
            format_duration(secs)
        );
    } else {
        let num_indexed_docs = (pipeline_statistics.num_docs
            - pipeline_statistics.num_invalid_docs)
            .separate_with_commas();

        let error_rate = (pipeline_statistics.num_invalid_docs as f64
            / pipeline_statistics.num_docs as f64)
            * 100.0;

        println!(
            "Indexed {} out of {} documents in {}. Failed to index {} document(s). {}\n",
            num_indexed_docs,
            pipeline_statistics.num_docs.separate_with_commas(),
            format_duration(secs),
            pipeline_statistics.num_invalid_docs.separate_with_commas(),
            colorize_error_rate(error_rate),
        );
    }

    Ok(pipeline_statistics)
}

fn colorize_error_rate(error_rate: f64) -> ColoredString {
    let error_rate_message = format!("({error_rate:.1}% error rate)");
    if error_rate < 1.0 {
        error_rate_message.yellow()
    } else if error_rate < 5.0 {
        error_rate_message.truecolor(255, 181, 46) //< Orange
    } else {
        error_rate_message.red()
    }
}

/// A struct to print data on the standard output.
struct Printer<'a> {
    pub stdout: &'a mut Stdout,
}

impl<'a> Printer<'a> {
    pub fn print_header(&mut self, header: &str) -> io::Result<()> {
        write!(&mut self.stdout, " {}", header.bright_blue())?;
        Ok(())
    }

    pub fn print_value(&mut self, fmt_args: fmt::Arguments) -> io::Result<()> {
        write!(&mut self.stdout, " {fmt_args}")
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.stdout.flush()
    }
}

fn display_statistics(
    stdout: &mut Stdout,
    throughput_calculator: &mut ThroughputCalculator,
    statistics: &IndexingStatistics,
) -> anyhow::Result<()> {
    let elapsed_duration = time::Duration::try_from(throughput_calculator.elapsed_time())?;
    let elapsed_time = format!(
        "{:02}:{:02}:{:02}",
        elapsed_duration.whole_hours(),
        elapsed_duration.whole_minutes() % 60,
        elapsed_duration.whole_seconds() % 60
    );
    let throughput_mb_s = throughput_calculator.calculate(statistics.total_bytes_processed);
    let mut printer = Printer { stdout };
    printer.print_header("Num docs")?;
    printer.print_value(format_args!("{:>7}", statistics.num_docs))?;
    printer.print_header("Parse errs")?;
    printer.print_value(format_args!("{:>5}", statistics.num_invalid_docs))?;
    printer.print_header("PublSplits")?;
    printer.print_value(format_args!("{:>3}", statistics.num_published_splits))?;
    printer.print_header("Input size")?;
    printer.print_value(format_args!(
        "{:>5}MB",
        statistics.total_bytes_processed / 1_000_000
    ))?;
    printer.print_header("Thrghput")?;
    printer.print_value(format_args!("{throughput_mb_s:>5.2}MB/s"))?;
    printer.print_header("Time")?;
    printer.print_value(format_args!("{elapsed_time}\n"))?;
    printer.flush()?;
    Ok(())
}

/// ThroughputCalculator is used to calculate throughput.
struct ThroughputCalculator {
    /// Stores the time series of processed bytes value.
    processed_bytes_values: VecDeque<(Instant, u64)>,
    /// Store the time this calculator started
    start_time: Instant,
}

impl ThroughputCalculator {
    /// Creates new instance.
    pub fn new(start_time: Instant) -> Self {
        let processed_bytes_values: VecDeque<(Instant, u64)> = (0..THROUGHPUT_WINDOW_SIZE)
            .map(|_| (start_time, 0u64))
            .collect();
        Self {
            processed_bytes_values,
            start_time,
        }
    }

    /// Calculates the throughput.
    pub fn calculate(&mut self, current_processed_bytes: u64) -> f64 {
        self.processed_bytes_values.pop_front();
        let current_instant = Instant::now();
        let (first_instant, first_processed_bytes) = *self.processed_bytes_values.front().unwrap();
        let elapsed_time = (current_instant - first_instant).as_millis() as f64 / 1_000f64;
        self.processed_bytes_values
            .push_back((current_instant, current_processed_bytes));
        (current_processed_bytes - first_processed_bytes) as f64
            / 1_000_000f64
            / elapsed_time.max(1f64)
    }

    pub fn elapsed_time(&self) -> Duration {
        self.start_time.elapsed()
    }
}
