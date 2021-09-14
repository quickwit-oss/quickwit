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

use std::collections::VecDeque;
use std::convert::TryFrom;
use std::env;
use std::io::{stdout, Stdout, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use byte_unit::Byte;
use crossterm::style::{Print, PrintStyledContent, Stylize};
use crossterm::QueueableCommand;
use json_comments::StripComments;
use quickwit_actors::{ActorExitStatus, ActorHandle, ObservationType, Universe};
use quickwit_common::extract_index_id_from_index_uri;
use quickwit_core::{create_index, delete_index, garbage_collect_index, reset_index};
use quickwit_index_config::{DefaultIndexConfigBuilder, IndexConfig};
use quickwit_indexing::actors::{
    IndexerParams, IndexingPipelineParams, IndexingPipelineSupervisor,
};
use quickwit_indexing::models::{CommitPolicy, IndexingStatistics, ScratchDirectory};
use quickwit_indexing::source::{FileSourceParams, SourceConfig};
use quickwit_metastore::checkpoint::Checkpoint;
use quickwit_metastore::{IndexMetadata, MetastoreUriResolver};
use quickwit_proto::{SearchRequest, SearchResponse};
use quickwit_search::{single_node_search, SearchResponseRest};
use quickwit_storage::quickwit_storage_uri_resolver;
use quickwit_telemetry::payload::TelemetryEvent;
use tracing::debug;

/// Throughput calculation window size.
const THROUGHPUT_WINDOW_SIZE: usize = 5;

#[derive(Debug)]
pub struct CreateIndexArgs {
    metastore_uri: String,
    index_uri: String,
    index_config: Arc<dyn IndexConfig>,
    overwrite: bool,
}
impl PartialEq for CreateIndexArgs {
    // index_config is opaque and not compared currently, need to change the trait to enable
    // IndexConfig comparison
    fn eq(&self, other: &Self) -> bool {
        self.index_uri == other.index_uri && self.overwrite == other.overwrite
    }
}

impl CreateIndexArgs {
    pub fn new(
        metastore_uri: String,
        index_uri: String,
        index_config_path: PathBuf,
        overwrite: bool,
    ) -> anyhow::Result<Self> {
        let json_file = std::fs::File::open(index_config_path.clone())
            .with_context(|| format!("Cannot open index-config-path {:?}", index_config_path))?;
        let reader = std::io::BufReader::new(json_file);
        let strip_comment_reader = StripComments::new(reader);
        let builder: DefaultIndexConfigBuilder = serde_json::from_reader(strip_comment_reader)
            .with_context(|| {
                format!(
                    "index-config-path {:?} is not a valid json file",
                    index_config_path
                )
            })?;
        let default_index_config = builder.build().with_context(|| {
            format!("index-config-path file {:?} is invalid", index_config_path)
        })?;
        let index_config = Arc::new(default_index_config) as Arc<dyn IndexConfig>;

        Ok(Self {
            metastore_uri,
            index_uri,
            index_config,
            overwrite,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct IndexDataArgs {
    pub metastore_uri: String,
    pub index_id: String,
    pub input_path: Option<PathBuf>,
    pub temp_dir: Option<PathBuf>,
    pub heap_size: Byte,
    pub overwrite: bool,
}

#[derive(Debug, PartialEq, Eq, Default)]
pub struct SearchIndexArgs {
    pub metastore_uri: String,
    pub index_id: String,
    pub query: String,
    pub max_hits: usize,
    pub start_offset: usize,
    pub search_fields: Option<Vec<String>>,
    pub start_timestamp: Option<i64>,
    pub end_timestamp: Option<i64>,
    pub tags: Option<Vec<String>>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct DeleteIndexArgs {
    pub metastore_uri: String,
    pub index_id: String,
    pub dry_run: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub struct GarbageCollectIndexArgs {
    pub metastore_uri: String,
    pub index_id: String,
    pub grace_period: Duration,
    pub dry_run: bool,
}

pub async fn create_index_cli(args: CreateIndexArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "create-index");
    quickwit_telemetry::send_telemetry_event(TelemetryEvent::Create).await;
    let index_id = extract_index_id_from_index_uri(&args.index_uri)?;

    if args.overwrite {
        delete_index(&args.metastore_uri, index_id, false).await?;
    }

    let index_metadata = IndexMetadata {
        index_id: index_id.to_string(),
        index_uri: args.index_uri.to_string(),
        index_config: args.index_config,
        checkpoint: Checkpoint::default(),
    };
    create_index(&args.metastore_uri, index_metadata).await?;
    Ok(())
}

pub async fn index_data_cli(args: IndexDataArgs) -> anyhow::Result<()> {
    debug!(args=?args, "indexing");
    quickwit_telemetry::send_telemetry_event(TelemetryEvent::IndexStart).await;

    let input_path = args.input_path.clone();
    let source_config = create_source_config_from_args(input_path.clone());
    let scratch_directory = if let Some(scratch_root_path) = args.temp_dir.as_ref() {
        ScratchDirectory::new_in_path(scratch_root_path.clone())
    } else {
        ScratchDirectory::try_new_temp()
            .with_context(|| "Failed to create a tempdir for the indexer")?
    };
    let storage_uri_resolver = quickwit_storage_uri_resolver();
    let metastore_uri_resolver = MetastoreUriResolver::default();
    let metastore = metastore_uri_resolver.resolve(&args.metastore_uri).await?;

    if args.overwrite {
        reset_index(&*metastore, &args.index_id, storage_uri_resolver.clone()).await?;
    }

    let indexer_params = IndexerParams {
        scratch_directory,
        heap_size: args.heap_size,
        commit_policy: CommitPolicy::default(), //< TODO make the commit policy configurable
    };

    let indexing_pipeline_params = IndexingPipelineParams {
        index_id: args.index_id.clone(),
        source_config,
        indexer_params,
        metastore,
        storage_uri_resolver: storage_uri_resolver.clone(),
    };

    let indexing_supervisor = IndexingPipelineSupervisor::new(indexing_pipeline_params);

    let universe = Universe::new();
    let (_supervisor_mailbox, supervisor_handler) =
        universe.spawn_actor(indexing_supervisor).spawn_async();

    let is_stdin_atty = atty::is(atty::Stream::Stdin);
    if args.input_path.is_none() && is_stdin_atty {
        let eof_shortcut = match env::consts::OS {
            "windows" => "CTRL+Z",
            _ => "CTRL+D",
        };
        println!(
            "Please enter your new line delimited json documents one line at a time.\nEnd your \
             input using {}.",
            eof_shortcut
        );
    }

    let statistics =
        start_statistics_reporting_loop(supervisor_handler, args.input_path.clone()).await?;

    if statistics.num_published_splits > 0 {
        println!(
            "You can now query your index with `quickwit search --index-id {} --metastore-uri {} \
             --query \"barack obama\"`",
            args.index_id, args.metastore_uri
        );
    }
    Ok(())
}

fn create_source_config_from_args(input_path_opt: Option<PathBuf>) -> SourceConfig {
    let params = FileSourceParams {
        filepath: input_path_opt,
    };
    SourceConfig {
        id: "cli_source".to_string(),
        source_type: "file".to_string(),
        params: serde_json::to_value(params).unwrap(),
    }
}

pub async fn search_index(args: SearchIndexArgs) -> anyhow::Result<SearchResponse> {
    debug!(args = ?args, "search-index");
    let storage_uri_resolver = quickwit_storage_uri_resolver();
    let metastore_uri_resolver = MetastoreUriResolver::default();
    let metastore = metastore_uri_resolver.resolve(&args.metastore_uri).await?;
    let search_request = SearchRequest {
        index_id: args.index_id,
        query: args.query.clone(),
        search_fields: args.search_fields.unwrap_or_default(),
        start_timestamp: args.start_timestamp,
        end_timestamp: args.end_timestamp,
        max_hits: args.max_hits as u64,
        start_offset: args.start_offset as u64,
        tags: args.tags.unwrap_or_default(),
    };
    let search_response: SearchResponse =
        single_node_search(&search_request, &*metastore, storage_uri_resolver.clone()).await?;
    Ok(search_response)
}

pub async fn search_index_cli(args: SearchIndexArgs) -> anyhow::Result<()> {
    let search_response: SearchResponse = search_index(args).await?;
    let search_response_rest = SearchResponseRest::try_from(search_response)?;
    let search_response_rest_json = serde_json::to_string_pretty(&search_response_rest)?;
    println!("{}", search_response_rest_json);
    Ok(())
}

pub async fn delete_index_cli(args: DeleteIndexArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "delete-index");
    quickwit_telemetry::send_telemetry_event(TelemetryEvent::Delete).await;

    let affected_files = delete_index(&args.metastore_uri, &args.index_id, args.dry_run).await?;
    if args.dry_run {
        if affected_files.is_empty() {
            println!("Only the index will be deleted since it does not contains any data file.");
            return Ok(());
        }

        println!(
            "The following files will be removed from the index `{}`",
            args.index_id
        );
        for file_entry in affected_files {
            println!(" - {}", file_entry.file_name);
        }
        return Ok(());
    }

    println!("Index `{}` successfully deleted.", args.index_id);
    Ok(())
}

pub async fn garbage_collect_index_cli(args: GarbageCollectIndexArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "garbage-collect-index");
    quickwit_telemetry::send_telemetry_event(TelemetryEvent::GarbageCollect).await;

    let deleted_files = garbage_collect_index(
        &args.metastore_uri,
        &args.index_id,
        args.grace_period,
        args.dry_run,
    )
    .await?;
    if deleted_files.is_empty() {
        println!("No dangling files to garbage collect.");
        return Ok(());
    }

    if args.dry_run {
        println!("The following files will be garbage collected.");
        for file_entry in deleted_files {
            println!(" - {}", file_entry.file_name);
        }
        return Ok(());
    }

    let deleted_bytes: u64 = deleted_files
        .iter()
        .map(|entry| entry.file_size_in_bytes)
        .sum();
    println!(
        "{}MB of storage garbage collected.",
        deleted_bytes / 1_000_000
    );
    println!("Index `{}` successfully garbage collected.", args.index_id);
    Ok(())
}

/// Starts a tokio task that displays the indexing statistics
/// every once in awhile.
pub async fn start_statistics_reporting_loop(
    pipeline_handler: ActorHandle<IndexingPipelineSupervisor>,
    input_path_opt: Option<PathBuf>,
) -> anyhow::Result<IndexingStatistics> {
    let mut stdout_handle = stdout();
    let start_time = Instant::now();
    let is_tty = atty::is(atty::Stream::Stdout);
    let mut throughput_calculator = ThroughputCalculator::new(start_time);
    let mut report_interval = tokio::time::interval(Duration::from_secs(1));

    loop {
        // TODO fixme. The way we wait today is a bit lame: if the indexing pipeline exits, we will
        // stil wait up to an entire heartbeat...  Ideally we should  select between two
        // futures.
        report_interval.tick().await;
        // Try to receive with a timeout of 1 second.
        // 1 second is also the frequency at which we update statistic in the console
        let observation = pipeline_handler.observe().await;

        // Let's not display live statistics to allow screen to scroll.
        if observation.state.num_docs > 0 {
            display_statistics(
                &mut stdout_handle,
                &mut throughput_calculator,
                &observation.state,
                is_tty,
            )?;
        }

        if observation.obs_type == ObservationType::PostMortem {
            break;
        }
    }

    let (supervisor_exit_status, statistics) = pipeline_handler.join().await;

    match supervisor_exit_status {
        ActorExitStatus::Success => {}
        ActorExitStatus::Quit
        | ActorExitStatus::DownstreamClosed
        | ActorExitStatus::Killed
        | ActorExitStatus::Panicked => {
            bail!(supervisor_exit_status)
        }
        ActorExitStatus::Failure(err) => {
            bail!(err);
        }
    }

    // If we have received zero docs at this point,
    // there is no point in displaying report.
    if statistics.num_docs == 0 {
        return Ok(statistics);
    }

    if input_path_opt.is_none() {
        display_statistics(
            &mut stdout_handle,
            &mut throughput_calculator,
            &statistics,
            is_tty,
        )?;
    }
    // display end of task report
    println!();
    let elapsed_secs = start_time.elapsed().as_secs();
    if elapsed_secs >= 60 {
        println!(
            "Indexed {} documents in {:.2$}min",
            statistics.num_docs,
            elapsed_secs.max(1) as f64 / 60f64,
            2
        );
    } else {
        println!(
            "Indexed {} documents in {}s",
            statistics.num_docs,
            elapsed_secs.max(1)
        );
    }

    Ok(statistics)
}

fn display_statistics(
    stdout_handle: &mut Stdout,
    throughput_calculator: &mut ThroughputCalculator,
    statistics: &IndexingStatistics,
    is_tty: bool,
) -> anyhow::Result<()> {
    let elapsed_duration = chrono::Duration::from_std(throughput_calculator.elapsed_time())?;
    let elapsed_time = format!(
        "{:02}:{:02}:{:02}",
        elapsed_duration.num_hours(),
        elapsed_duration.num_minutes() % 60,
        elapsed_duration.num_seconds() % 60
    );
    let throughput_mb_s = throughput_calculator.calculate(statistics.total_bytes_processed);
    if is_tty {
        stdout_handle.queue(PrintStyledContent("Num docs: ".blue()))?;
        stdout_handle.queue(Print(format!("{:>7}", statistics.num_docs)))?;
        stdout_handle.queue(PrintStyledContent(" Parse errs: ".blue()))?;
        stdout_handle.queue(Print(format!("{:>5}", statistics.num_invalid_docs)))?;
        stdout_handle.queue(PrintStyledContent(" Staged splits: ".blue()))?;
        stdout_handle.queue(Print(format!("{:>3}", statistics.num_staged_splits)))?;
        stdout_handle.queue(PrintStyledContent(" Input size: ".blue()))?;
        stdout_handle.queue(Print(format!(
            "{:>5}MB",
            statistics.total_bytes_processed / 1_000_000
        )))?;
        stdout_handle.queue(PrintStyledContent(" Thrghput: ".blue()))?;
        stdout_handle.queue(Print(format!("{:>5.2}MB/s", throughput_mb_s)))?;
        stdout_handle.queue(PrintStyledContent(" Time: ".blue()))?;
        stdout_handle.queue(Print(format!("{}\n", elapsed_time)))?;
    } else {
        let report_line = format!(
            "Num docs: {:>7} Parse errs: {:>5} Staged splits: {:>3} Input size: {:>5}MB Thrghput: \
             {:>5.2}MB/s Time: {}\n",
            statistics.num_docs,
            statistics.num_invalid_docs,
            statistics.num_staged_splits,
            statistics.total_bytes_processed / 1_000_000,
            throughput_mb_s,
            elapsed_time,
        );
        stdout_handle.write_all(report_line.as_bytes())?;
    }
    stdout_handle.flush()?;
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
            / elapsed_time.max(1f64) as f64
    }

    pub fn elapsed_time(&self) -> Duration {
        self.start_time.elapsed()
    }
}

#[test]
fn test_extract_index_id_from_index_uri() -> anyhow::Result<()> {
    let index_uri = "file:///indexes/wikipedia-xd_1";
    let index_id = extract_index_id_from_index_uri(index_uri)?;
    assert_eq!("wikipedia-xd_1", index_id);

    let result = extract_index_id_from_index_uri("file:///indexes/_wikipedia");
    assert!(result.is_err());

    let result = extract_index_id_from_index_uri("file:///indexes/-wikipedia");
    assert!(result.is_err());

    let result = extract_index_id_from_index_uri("file:///indexes/2-wiki-pedia");
    assert!(result.is_err());

    let result = extract_index_id_from_index_uri("file:///indexes/01wikipedia");
    assert!(result.is_err());
    Ok(())
}
