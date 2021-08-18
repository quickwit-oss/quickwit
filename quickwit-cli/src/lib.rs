/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use crossterm::style::Print;
use crossterm::style::PrintStyledContent;
use crossterm::style::Stylize;
use crossterm::QueueableCommand;
use json_comments::StripComments;
use quickwit_common::extract_metastore_uri_and_index_id_from_index_uri;
use quickwit_core::DocumentSource;
use quickwit_index_config::DefaultIndexConfigBuilder;
use quickwit_index_config::IndexConfig;
use quickwit_metastore::checkpoint::Checkpoint;
use quickwit_metastore::IndexMetadata;
use quickwit_metastore::MetastoreUriResolver;
use quickwit_proto::SearchRequest;
use quickwit_proto::SearchResult;
use quickwit_search::single_node_search;
use quickwit_search::SearchResultJson;
use quickwit_storage::StorageUriResolver;
use quickwit_telemetry::payload::TelemetryEvent;
use std::collections::VecDeque;
use std::env;
use std::io;
use std::io::Stdout;
use std::io::{stdout, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::usize;
use tempfile::TempDir;
use tokio::fs::File;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::sync::watch;
use tokio::task;
use tokio::time::timeout;
use tokio::try_join;
use tracing::debug;

use quickwit_core::{
    create_index, delete_index, garbage_collect_index, index_data, IndexDataParams,
    IndexingStatistics,
};

/// Throughput calculation window size.
const THROUGHPUT_WINDOW_SIZE: usize = 5;

#[derive(Debug)]
pub struct CreateIndexArgs {
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
        index_uri: String,
        index_config_path: PathBuf,
        overwrite: bool,
    ) -> anyhow::Result<Self> {
        let json_file = std::fs::File::open(index_config_path)?;
        let reader = std::io::BufReader::new(json_file);
        let strip_comment_reader = StripComments::new(reader);
        let builder: DefaultIndexConfigBuilder = serde_json::from_reader(strip_comment_reader)?;
        let index_config = Arc::new(builder.build()?) as Arc<dyn IndexConfig>;

        Ok(Self {
            index_uri,
            index_config,
            overwrite,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct IndexDataArgs {
    pub index_uri: String,
    pub input_path: Option<PathBuf>,
    pub temp_dir: Option<PathBuf>,
    pub num_threads: usize,
    pub heap_size: u64,
    pub overwrite: bool,
}

#[derive(Debug, PartialEq, Eq, Default)]
pub struct SearchIndexArgs {
    pub index_uri: String,
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
    pub index_uri: String,
    pub dry_run: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub struct GarbageCollectIndexArgs {
    pub index_uri: String,
    pub grace_period: Duration,
    pub dry_run: bool,
}

pub async fn create_index_cli(args: CreateIndexArgs) -> anyhow::Result<()> {
    debug!(
        index_uri = %args.index_uri,
        index_config = ?args.index_config,
        overwrite = args.overwrite,
        "create-index"
    );
    quickwit_telemetry::send_telemetry_event(TelemetryEvent::Create).await;
    let (metastore_uri, index_id) =
        extract_metastore_uri_and_index_id_from_index_uri(&args.index_uri)?;
    if args.overwrite {
        delete_index(metastore_uri, index_id, false).await?;
    }

    let index_metadata = IndexMetadata {
        index_id: index_id.to_string(),
        index_uri: args.index_uri.to_string(),
        index_config: args.index_config,
        checkpoint: Checkpoint::default(),
    };
    create_index(metastore_uri, index_metadata).await?;
    Ok(())
}

fn create_index_scratch_dir(path_tempdir_opt: Option<&PathBuf>) -> anyhow::Result<Arc<TempDir>> {
    let scratch_dir = if let Some(path_tempdir) = path_tempdir_opt {
        tempfile::tempdir_in(&path_tempdir)?
    } else {
        tempfile::tempdir()?
    };
    Ok(Arc::new(scratch_dir))
}

pub async fn index_data_cli(args: IndexDataArgs) -> anyhow::Result<()> {
    debug!(
        index_uri = %args.index_uri,
        input_uri = ?args.input_path,
        temp_dir = ?args.temp_dir,
        num_threads = args.num_threads,
        heap_size = args.heap_size,
        overwrite = args.overwrite,
        "indexing"
    );
    quickwit_telemetry::send_telemetry_event(TelemetryEvent::IndexStart).await;

    let input_path = args.input_path.clone();
    let document_source = create_document_source_from_args(input_path).await?;
    let (metastore_uri, index_id) =
        extract_metastore_uri_and_index_id_from_index_uri(&args.index_uri)?;
    let temp_dir = create_index_scratch_dir(args.temp_dir.as_ref())?;
    let params = IndexDataParams {
        index_id: index_id.to_string(),
        temp_dir,
        num_threads: args.num_threads,
        heap_size: args.heap_size,
        overwrite: args.overwrite,
    };

    let is_stdin_atty = atty::is(atty::Stream::Stdin);
    if args.input_path.is_none() && is_stdin_atty {
        let eof_shortcut = match env::consts::OS {
            "windows" => "CTRL+Z",
            _ => "CTRL+D",
        };
        println!("Please enter your new line delimited json documents one line at a time.\nEnd your input using {}.", eof_shortcut);
    }

    let statistics = Arc::new(IndexingStatistics::default());
    let (task_completed_sender, task_completed_receiver) = watch::channel::<bool>(false);
    let reporting_future = start_statistics_reporting(
        statistics.clone(),
        task_completed_receiver,
        args.input_path.clone(),
    );
    let storage_uri_resolver = StorageUriResolver::default();
    let metastore_uri_resolver =
        MetastoreUriResolver::with_storage_resolver(storage_uri_resolver.clone());
    let metastore = metastore_uri_resolver.resolve(metastore_uri).await?;
    let index_future = async move {
        index_data(
            metastore,
            params,
            document_source,
            storage_uri_resolver,
            statistics.clone(),
        )
        .await?;
        task_completed_sender.send(true)?;
        anyhow::Result::<()>::Ok(())
    };

    let (_, num_published_splits) = try_join!(index_future, reporting_future)?;
    if num_published_splits > 0 {
        println!("You can now query your index with `quickwit search --index-uri {} --query \"barack obama\"`" , args.index_uri);
    }
    Ok(())
}

async fn create_document_source_from_args(
    input_path_opt: Option<PathBuf>,
) -> io::Result<Box<dyn DocumentSource>> {
    if let Some(input_path) = input_path_opt {
        let file = File::open(input_path).await?;
        let reader = BufReader::new(file);
        Ok(Box::new(reader.lines()))
    } else {
        let stdin = tokio::io::stdin();
        let reader = BufReader::new(stdin);
        Ok(Box::new(reader.lines()))
    }
}

pub async fn search_index(args: SearchIndexArgs) -> anyhow::Result<SearchResult> {
    debug!(
        index_uri = %args.index_uri,
        query = %args.query,
        max_hits = args.max_hits,
        start_offset = args.start_offset,
        search_fields = ?args.search_fields,
        start_timestamp = ?args.start_timestamp,
        end_timestamp = ?args.end_timestamp,
        tags = ?args.tags,
        "search-index"
    );
    let (metastore_uri, index_id) =
        extract_metastore_uri_and_index_id_from_index_uri(&args.index_uri)?;
    let storage_uri_resolver = StorageUriResolver::default();
    let metastore_uri_resolver =
        MetastoreUriResolver::with_storage_resolver(storage_uri_resolver.clone());
    let metastore = metastore_uri_resolver.resolve(metastore_uri).await?;
    let search_request = SearchRequest {
        index_id: index_id.to_string(),
        query: args.query.clone(),
        search_fields: args.search_fields.unwrap_or_default(),
        start_timestamp: args.start_timestamp,
        end_timestamp: args.end_timestamp,
        max_hits: args.max_hits as u64,
        start_offset: args.start_offset as u64,
        tags: args.tags.unwrap_or_default(),
    };
    let search_result: SearchResult =
        single_node_search(&search_request, &*metastore, storage_uri_resolver).await?;
    Ok(search_result)
}

pub async fn search_index_cli(args: SearchIndexArgs) -> anyhow::Result<()> {
    let search_result: SearchResult = search_index(args).await?;

    let search_result_json = SearchResultJson::from(search_result);

    let search_result_json = serde_json::to_string_pretty(&search_result_json)?;
    println!("{}", search_result_json);
    Ok(())
}

pub async fn delete_index_cli(args: DeleteIndexArgs) -> anyhow::Result<()> {
    debug!(
        index_uri = %args.index_uri,
        dry_run = args.dry_run,
        "delete-index"
    );
    quickwit_telemetry::send_telemetry_event(TelemetryEvent::Delete).await;

    let (metastore_uri, index_id) =
        extract_metastore_uri_and_index_id_from_index_uri(&args.index_uri)?;
    let affected_files = delete_index(metastore_uri, index_id, args.dry_run).await?;
    if args.dry_run {
        if affected_files.is_empty() {
            println!("Only the index will be deleted since it does not contains any data file.");
            return Ok(());
        }

        println!(
            "The following files will be removed along with the index at `{}`",
            args.index_uri
        );
        for file_entry in affected_files {
            println!(" - {}", file_entry.file_name);
        }
        return Ok(());
    }

    println!("Index successfully deleted at `{}`", args.index_uri);
    Ok(())
}

pub async fn garbage_collect_index_cli(args: GarbageCollectIndexArgs) -> anyhow::Result<()> {
    debug!(
        index_uri = %args.index_uri,
        grace_period = ?args.grace_period,
        dry_run = args.dry_run,
        "garbage-collect-index"
    );
    quickwit_telemetry::send_telemetry_event(TelemetryEvent::GarbageCollect).await;

    let (metastore_uri, index_id) =
        extract_metastore_uri_and_index_id_from_index_uri(&args.index_uri)?;
    let deleted_files =
        garbage_collect_index(metastore_uri, index_id, args.grace_period, args.dry_run).await?;
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
    println!(
        "Index successfully garbage collected at `{}`",
        args.index_uri
    );
    Ok(())
}

/// Starts a tokio task that displays the indexing statistics
/// every once in awhile.
pub async fn start_statistics_reporting(
    statistics: Arc<IndexingStatistics>,
    mut task_completed_receiver: watch::Receiver<bool>,
    input_path_opt: Option<PathBuf>,
) -> anyhow::Result<usize> {
    task::spawn(async move {
        let mut stdout_handle = stdout();
        let start_time = Instant::now();
        let is_tty = atty::is(atty::Stream::Stdout);
        let mut throughput_calculator = ThroughputCalculator::new(start_time);
        loop {
            // Try to receive with a timeout of 1 second.
            // 1 second is also the frequency at which we update statistic in the console
            let is_done = timeout(Duration::from_secs(1), task_completed_receiver.changed())
                .await
                .is_ok();

            // Let's not display live statistics to allow screen to scroll.
            if statistics.num_docs.get() > 0 {
                display_statistics(
                    &mut stdout_handle,
                    &mut throughput_calculator,
                    statistics.clone(),
                    is_tty,
                )?;
            }

            if is_done {
                break;
            }
        }

        // If we have received zero docs at this point,
        // there is no point in displaying report.
        if statistics.num_docs.get() == 0 {
            return anyhow::Result::<usize>::Ok(0);
        }

        if input_path_opt.is_none() {
            display_statistics(
                &mut stdout_handle,
                &mut throughput_calculator,
                statistics.clone(),
                is_tty,
            )?;
        }
        //display end of task report
        println!();
        let elapsed_secs = start_time.elapsed().as_secs();
        if elapsed_secs >= 60 {
            println!(
                "Indexed {} documents in {:.2$}min",
                statistics.num_docs.get(),
                elapsed_secs.max(1) as f64 / 60f64,
                2
            );
        } else {
            println!(
                "Indexed {} documents in {}s",
                statistics.num_docs.get(),
                elapsed_secs.max(1)
            );
        }

        anyhow::Result::<usize>::Ok(statistics.num_published_splits.get())
    })
    .await?
}

fn display_statistics(
    stdout_handle: &mut Stdout,
    throughput_calculator: &mut ThroughputCalculator,
    statistics: Arc<IndexingStatistics>,
    is_tty: bool,
) -> anyhow::Result<()> {
    let elapsed_duration = chrono::Duration::from_std(throughput_calculator.elapsed_time())?;
    let elapsed_time = format!(
        "{:02}:{:02}:{:02}",
        elapsed_duration.num_hours(),
        elapsed_duration.num_minutes(),
        elapsed_duration.num_seconds()
    );
    let throughput_mb_s = throughput_calculator.calculate(statistics.total_bytes_processed.get());
    if is_tty {
        stdout_handle.queue(PrintStyledContent("Num docs: ".blue()))?;
        stdout_handle.queue(Print(format!("{:>7}", statistics.num_docs.get())))?;
        stdout_handle.queue(PrintStyledContent(" Parse errs: ".blue()))?;
        stdout_handle.queue(Print(format!("{:>5}", statistics.num_parse_errors.get())))?;
        stdout_handle.queue(PrintStyledContent(" Staged splits: ".blue()))?;
        stdout_handle.queue(Print(format!("{:>3}", statistics.num_staged_splits.get())))?;
        stdout_handle.queue(PrintStyledContent(" Input size: ".blue()))?;
        stdout_handle.queue(Print(format!(
            "{:>5}MB",
            statistics.total_bytes_processed.get() / 1_000_000
        )))?;
        stdout_handle.queue(PrintStyledContent(" Thrghput: ".blue()))?;
        stdout_handle.queue(Print(format!("{:>5.2}MB/s", throughput_mb_s)))?;
        stdout_handle.queue(PrintStyledContent(" Time: ".blue()))?;
        stdout_handle.queue(Print(format!("{}\n", elapsed_time)))?;
    } else {
        let report_line = format!(
            "Num docs: {:>7} Parse errs: {:>5} Staged splits: {:>3} Input size: {:>5}MB Thrghput: {:>5.2}MB/s Time: {}\n",
            statistics.num_docs.get(),
            statistics.num_parse_errors.get(),
            statistics.num_staged_splits.get(),
            statistics.total_bytes_processed.get() / 1_000_000,
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
    processed_bytes_values: VecDeque<(Instant, usize)>,
    /// Store the time this calculator started
    start_time: Instant,
}

impl ThroughputCalculator {
    /// Creates new instance.
    pub fn new(start_time: Instant) -> Self {
        let processed_bytes_values: VecDeque<(Instant, usize)> = (0..THROUGHPUT_WINDOW_SIZE)
            .map(|_| (start_time, 0usize))
            .collect();
        Self {
            processed_bytes_values,
            start_time,
        }
    }

    /// Calculates the throughput.
    pub fn calculate(&mut self, current_processed_bytes: usize) -> f64 {
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
fn test_extract_metastore_uri_and_index_id_from_index_uri() -> anyhow::Result<()> {
    let index_uri = "file:///indexes/wikipedia-xd_1";
    let (metastore_uri, index_id) = extract_metastore_uri_and_index_id_from_index_uri(index_uri)?;
    assert_eq!("file:///indexes", metastore_uri);
    assert_eq!("wikipedia-xd_1", index_id);

    let result = extract_metastore_uri_and_index_id_from_index_uri("file:///indexes/_wikipedia");
    assert!(result.is_err());

    let result = extract_metastore_uri_and_index_id_from_index_uri("file:///indexes/-wikipedia");
    assert!(result.is_err());

    let result = extract_metastore_uri_and_index_id_from_index_uri("file:///indexes/2-wiki-pedia");
    assert!(result.is_err());

    let result = extract_metastore_uri_and_index_id_from_index_uri("file:///indexes/01wikipedia");
    assert!(result.is_err());
    Ok(())
}
