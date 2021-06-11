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

use anyhow::{bail, Context};
use byte_unit::Byte;
use clap::{load_yaml, value_t, App, AppSettings, ArgMatches};
use once_cell::sync::Lazy;
use quickwit_core::DocumentSource;
use quickwit_doc_mapping::IndexSettings;
use quickwit_doc_mapping::{
    AllFlattenDocMapper, DefaultDocMapper, DocMapper, DocMapperConfig, WikipediaMapper,
};
use quickwit_metastore::IndexMetadata;
use quickwit_metastore::MetastoreUriResolver;
use quickwit_storage::StorageUriResolver;
use regex::Regex;
use std::env;
use std::io;
use std::io::Stdout;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::fs::File;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::try_join;
use tracing::debug;

use crossterm::terminal::{Clear, ClearType};
use crossterm::{cursor, QueueableCommand};
use std::io::{stdout, Write};
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio::task;
use tokio::time::timeout;

use quickwit_core::{create_index, delete_index, index_data, IndexDataParams, IndexingStatistics};

struct CreateIndexArgs {
    index_uri: String,
    doc_mapper: Box<dyn DocMapper>,
    timestamp_field: Option<String>,
    overwrite: bool,
}

struct IndexDataArgs {
    index_uri: String,
    input_path: Option<PathBuf>,
    temp_dir: Option<PathBuf>,
    num_threads: usize,
    heap_size: u64,
    overwrite: bool,
}

struct SearchIndexArgs {
    index_uri: String,
    query: String,
    max_hits: usize,
    start_offset: usize,
    search_fields: Option<Vec<String>>,
    start_timestamp: Option<i64>,
    end_timestamp: Option<i64>,
}

struct DeleteIndexArgs {
    index_uri: String,
    dry_run: bool,
}

enum CliCommand {
    New(CreateIndexArgs),
    Index(IndexDataArgs),
    Search(SearchIndexArgs),
    Delete(DeleteIndexArgs),
}
impl CliCommand {
    fn parse_cli_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches_opt) = matches.subcommand();
        let submatches =
            submatches_opt.ok_or_else(|| anyhow::anyhow!("Unable to parse sub matches"))?;

        match subcommand {
            "new" => Self::parse_new_args(submatches),
            "index" => Self::parse_index_args(submatches),
            "search" => Self::parse_search_args(submatches),
            "delete" => Self::parse_delete_args(submatches),
            _ => bail!("Subcommand '{}' is not implemented", subcommand),
        }
    }

    fn parse_new_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_uri = matches
            .value_of("index-uri")
            .context("'index-uri' is a required arg")?
            .to_string();
        // TODO: both doc_mapper_type and doc_mapper_config_path could
        // be collapsed into one cli argument when we clarify the specs.
        let doc_mapper_type = matches
            .value_of("doc-mapper-type")
            .context("doc-mapper-type has a default value")?;
        let _doc_mapper_config_path = matches
            .value_of("doc-mapper-config-path")
            .map(PathBuf::from);
        let timestamp_field = matches
            .value_of("timestamp-field")
            .map(|field| field.to_string());
        let overwrite = matches.is_present("overwrite");

        // TODO: find better way to build doc mapper when we clarify the specs.
        let doc_mapper: Box<dyn DocMapper> = match doc_mapper_type.trim().to_lowercase().as_str() {
            "all_flatten" => Box::new(AllFlattenDocMapper::new()) as Box<dyn DocMapper>,
            "wikipedia" => Box::new(WikipediaMapper::new()) as Box<dyn DocMapper>,
            _ =>
            // TODO return an error if the type is unknown
            {
                Box::new(DefaultDocMapper::new(DocMapperConfig::default())) as Box<dyn DocMapper>
            }
        };

        Ok(CliCommand::New(CreateIndexArgs {
            index_uri,
            doc_mapper,
            timestamp_field,
            overwrite,
        }))
    }

    fn parse_index_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_uri = matches
            .value_of("index-uri")
            .context("index-uri is required")?
            .to_string();
        let input_path: Option<PathBuf> = matches.value_of("input-path").map(PathBuf::from);
        let temp_dir: Option<PathBuf> = matches.value_of("temp-dir").map(PathBuf::from);
        let num_threads = value_t!(matches, "num-threads", usize)?; // 'num-threads' has a default value
        let heap_size_str = matches
            .value_of("heap-size")
            .context("heap-size has a default value")?;
        let heap_size = Byte::from_str(heap_size_str)?.get_bytes() as u64;
        let overwrite = matches.is_present("overwrite");
        Ok(CliCommand::Index(IndexDataArgs {
            index_uri,
            input_path,
            temp_dir,
            num_threads,
            heap_size,
            overwrite,
        }))
    }

    fn parse_search_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_uri = matches
            .value_of("index-uri")
            .context("'index-uri' is a required arg")?
            .to_string();
        let query = matches
            .value_of("query")
            .context("query is a required arg")?
            .to_string();
        let max_hits = value_t!(matches, "max-hits", usize)?;
        let start_offset = value_t!(matches, "start-offset", usize)?;
        let search_fields = matches
            .values_of("search-fields")
            .map(|values| values.map(|value| value.to_string()).collect());
        let start_timestamp = if matches.is_present("start-timestamp") {
            Some(value_t!(matches, "start-timestamp", i64)?)
        } else {
            None
        };
        let end_timestamp = if matches.is_present("end-timestamp") {
            Some(value_t!(matches, "end-timestamp", i64)?)
        } else {
            None
        };

        Ok(CliCommand::Search(SearchIndexArgs {
            index_uri,
            query,
            max_hits,
            start_offset,
            search_fields,
            start_timestamp,
            end_timestamp,
        }))
    }

    fn parse_delete_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_uri = matches
            .value_of("index-uri")
            .context("'index-uri' is a required arg")?
            .to_string();
        let dry_run = matches.is_present("dry-run");
        Ok(CliCommand::Delete(DeleteIndexArgs { index_uri, dry_run }))
    }
}

/// For the moment, the only metastore available is the
/// a one file per index store, located on the same storage as the
/// index.
/// For a simpler UX, we let the user define an `index_url` instead
/// of a metastore and an index_id.
/// This function takes such a index_url and breaks it into
/// s3://my_bucket/some_path_containing_my_indices / my_index
/// \--------------------------------------------/ \------/
///        metastore_uri                           index_id
///
/// TODO force the presence of a protocol and a specific format using a regex?
fn extract_metastore_uri_and_index_id_from_index_uri(
    index_uri: &str,
) -> anyhow::Result<(&str, &str)> {
    static INDEX_ID_PATTERN: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"^[a-zA-Z][a-zA-Z0-9_\-]*$").unwrap());
    let parts: Vec<&str> = index_uri.rsplitn(2, '/').collect();
    if parts.len() != 2 {
        anyhow::bail!("Failed to parse the uri into a metastore_uri and an index_id.");
    }
    if !INDEX_ID_PATTERN.is_match(parts[0]) {
        anyhow::bail!("Invalid index_id `{}`. Only alpha-numeric, `-` and `_` characters allowed. Cannot start with `-`, `_` or digit.", parts[0]);
    }

    Ok((parts[1], parts[0]))
}

async fn create_index_cli(args: CreateIndexArgs) -> anyhow::Result<()> {
    debug!(
        index_uri = %args.index_uri,
        doc_mapper = ?args.doc_mapper,
        timestamp_field = ?args.timestamp_field,
        overwrite = args.overwrite,
        "create-index"
    );
    let (metastore_uri, index_id) =
        extract_metastore_uri_and_index_id_from_index_uri(&args.index_uri)?;
    if args.overwrite {
        delete_index(metastore_uri, index_id, false).await?;
    }

    let index_metadata = IndexMetadata {
        index_id: index_id.to_string(),
        index_uri: args.index_uri.to_string(),
        doc_mapper: args.doc_mapper,
        settings: IndexSettings {
            timestamp_field_name: args.timestamp_field,
        },
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

async fn index_data_cli(args: IndexDataArgs) -> anyhow::Result<()> {
    debug!(
        index_uri = %args.index_uri,
        input_uri = ?args.input_path,
        temp_dir = ?args.temp_dir,
        num_threads = args.num_threads,
        heap_size = args.heap_size,
        overwrite = args.overwrite,
        "indexing"
    );

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
            "macos" => "CMD+D",
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
        println!("You can now query your index with `quickwit search --index-path {} --query \"barack obama\"`" , args.index_uri);
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

async fn search_index_cli(args: SearchIndexArgs) -> anyhow::Result<()> {
    debug!(
        index_uri = %args.index_uri,
        query = %args.query,
        max_hits = args.max_hits,
        start_offset = args.start_offset,
        search_fields = ?args.search_fields,
        start_timestamp = ?args.start_timestamp,
        end_timestamp = ?args.end_timestamp,
        "search-index"
    );
    Ok(())
}

async fn delete_index_cli(args: DeleteIndexArgs) -> anyhow::Result<()> {
    debug!(
        index_uri = %args.index_uri,
        dry_run = args.dry_run,
        "delete-index"
    );

    let (metastore_uri, index_id) =
        extract_metastore_uri_and_index_id_from_index_uri(&args.index_uri)?;
    let affected_files = delete_index(metastore_uri, index_id, args.dry_run).await?;
    if args.dry_run {
        println!(
            "The following files will be removed from the index at `{}`",
            args.index_uri
        );
        for file in affected_files {
            println!(" - {}", file.display());
        }
    } else {
        println!("Index successfully deleted at `{}`", args.index_uri);
    }

    Ok(())
}

/// Starts a tokio task that displays the indexing statistics
/// every once in awhile.
pub async fn start_statistics_reporting(
    statistics: Arc<IndexingStatistics>,
    task_completed_receiver: watch::Receiver<bool>,
    input_path_opt: Option<PathBuf>,
) -> anyhow::Result<usize> {
    task::spawn(async move {
        let mut stdout_handle = stdout();
        let start_time = Instant::now();
        loop {
            // Try to receive with a timeout of 1 second.
            // 1 second is also the frequency at which we update statistic in the console
            let mut receiver = task_completed_receiver.clone();
            let is_done = timeout(Duration::from_secs(1), receiver.changed())
                .await
                .is_ok();

            // Let's not display live statistics to allow screen to scroll.
            if input_path_opt.is_some() && statistics.num_docs.get() > 0 {
                display_statistics(&mut stdout_handle, start_time, statistics.clone())?;
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
            display_statistics(&mut stdout_handle, start_time, statistics.clone())?;
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
    start_time: Instant,
    statistics: Arc<IndexingStatistics>,
) -> anyhow::Result<()> {
    let elapsed_secs = start_time.elapsed().as_secs();
    let throughput_mb_s =
        statistics.total_bytes_processed.get() as f64 / 1_000_000f64 / elapsed_secs.max(1) as f64;
    let report_line = format!(
        "Documents Read: {} Parse Errors: {}  Published Splits: {} Dataset Size: {}MB Throughput: {:.5$}MB/s",
        statistics.num_docs.get(),
        statistics.num_parse_errors.get(),
        statistics.num_published_splits.get(),
        statistics.total_bytes_processed.get() / 1_000_000,
        throughput_mb_s,
        2
    );

    stdout_handle.queue(cursor::SavePosition)?;
    stdout_handle.queue(Clear(ClearType::CurrentLine))?;
    stdout_handle.write_all(report_line.as_bytes())?;
    stdout_handle.write_all("\nPlease hold on.".as_bytes())?;
    stdout_handle.queue(cursor::RestorePosition)?;
    stdout_handle.flush()?;
    Ok(())
}

#[tracing::instrument]
#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let yaml = load_yaml!("cli.yaml");
    let app = App::from(yaml)
        .setting(AppSettings::ArgRequiredElseHelp)
        .version(env!("CARGO_PKG_VERSION"));
    let matches = app.get_matches();

    let command = match CliCommand::parse_cli_args(&matches) {
        Ok(command) => command,
        Err(err) => {
            eprintln!("Failed to parse command arguments: {:?}", err);
            std::process::exit(1);
        }
    };
    let command_res = match command {
        CliCommand::New(args) => create_index_cli(args).await,
        CliCommand::Index(args) => index_data_cli(args).await,
        CliCommand::Search(args) => search_index_cli(args).await,
        CliCommand::Delete(args) => delete_index_cli(args).await,
    };
    if let Err(err) = command_res {
        eprintln!("Command failed: {:?}", err);
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        extract_metastore_uri_and_index_id_from_index_uri, CliCommand, CreateIndexArgs,
        DeleteIndexArgs, IndexDataArgs, SearchIndexArgs,
    };
    use clap::{load_yaml, App, AppSettings};
    use std::path::{Path, PathBuf};

    #[test]
    fn test_parse_new_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches_result = app.get_matches_from_safe(vec![
            "new",
            "--index-uri",
            "file:///indexes/wikipedia",
            "--no-timestamp-field",
        ]);
        assert!(matches!(matches_result, Err(_)));

        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "new",
            "--index-uri",
            "file:///indexes/wikipedia",
            "--doc-mapper-config-path",
            "./config.json",
            "--no-timestamp-field",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::New(CreateIndexArgs {
                index_uri,
                doc_mapper: Box{..},
                timestamp_field: None,
                overwrite: false
            })) if &index_uri == "file:///indexes/wikipedia"
        ));

        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "new",
            "--index-uri",
            "file:///indexes/wikipedia",
            "--doc-mapper-type",
            "all_flatten",
            "--timestamp-field",
            "ts",
            "--overwrite",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::New(CreateIndexArgs {
                index_uri,
                doc_mapper: Box{..},
                timestamp_field: Some(field_name),
                overwrite: true
            })) if &index_uri == "file:///indexes/wikipedia" && field_name == "ts"
        ));

        Ok(())
    }

    #[test]
    fn test_parse_index_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches =
            app.get_matches_from_safe(vec!["index", "--index-uri", "file:///indexes/wikipedia"])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Index(IndexDataArgs {
                index_uri,
                input_path: None,
                temp_dir: None,
                num_threads: 2,
                heap_size: 2_000_000_000,
                overwrite: false,
            })) if &index_uri == "file:///indexes/wikipedia"
        ));

        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "index",
            "--index-uri",
            "file:///indexes/wikipedia",
            "--input-path",
            "/data/wikipedia.json",
            "--temp-dir",
            "./tmp",
            "--num-threads",
            "4",
            "--heap-size",
            "4gib",
            "--overwrite",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Index(IndexDataArgs {
                index_uri,
                input_path: Some(input_path),
                temp_dir,
                num_threads: 4,
                heap_size: 4_294_967_296,
                overwrite: true,
            })) if &index_uri == "file:///indexes/wikipedia" && input_path == Path::new("/data/wikipedia.json") && temp_dir == Some(PathBuf::from("./tmp"))
        ));

        Ok(())
    }

    #[test]
    fn test_parse_search_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "search",
            "--index-uri",
            "./wikipedia",
            "--query",
            "Barack Obama",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Search(SearchIndexArgs {
                index_uri,
                query,
                max_hits: 20,
                start_offset: 0,
                search_fields: None,
                start_timestamp: None,
                end_timestamp: None,
            })) if index_uri == "./wikipedia" && query == "Barack Obama"
        ));

        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "search",
            "--index-uri",
            "./wikipedia",
            "--query",
            "Barack Obama",
            "--max-hits",
            "50",
            "--start-offset",
            "100",
            "--search-fields",
            "title",
            "url",
            "--start-timestamp",
            "0",
            "--end-timestamp",
            "1",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Search(SearchIndexArgs {
                index_uri,
                query,
                max_hits: 50,
                start_offset: 100,
                search_fields: Some(field_names),
                start_timestamp: Some(0),
                end_timestamp: Some(1),
            })) if index_uri == "./wikipedia" && query == "Barack Obama" && field_names == vec!["title".to_string(), "url".to_string()]
        ));

        Ok(())
    }

    #[test]
    fn test_parse_delete_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches =
            app.get_matches_from_safe(vec!["delete", "--index-uri", "file:///indexes/wikipedia"])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Delete(DeleteIndexArgs {
                index_uri,
                dry_run: false
            })) if &index_uri == "file:///indexes/wikipedia"
        ));

        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "delete",
            "--index-uri",
            "file:///indexes/wikipedia",
            "--dry-run",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Delete(DeleteIndexArgs {
                index_uri,
                dry_run: true
            })) if &index_uri == "file:///indexes/wikipedia"
        ));
        Ok(())
    }

    #[test]
    fn test_extract_metastore_uri_and_index_id_from_index_uri() -> anyhow::Result<()> {
        let index_uri = "file:///indexes/wikipedia-xd_1";
        let (metastore_uri, index_id) =
            extract_metastore_uri_and_index_id_from_index_uri(index_uri)?;
        assert_eq!("file:///indexes", metastore_uri);
        assert_eq!("wikipedia-xd_1", index_id);

        let result =
            extract_metastore_uri_and_index_id_from_index_uri("file:///indexes/_wikipedia");
        assert!(result.is_err());

        let result =
            extract_metastore_uri_and_index_id_from_index_uri("file:///indexes/-wikipedia");
        assert!(result.is_err());

        let result =
            extract_metastore_uri_and_index_id_from_index_uri("file:///indexes/2-wiki-pedia");
        assert!(result.is_err());

        let result =
            extract_metastore_uri_and_index_id_from_index_uri("file:///indexes/01wikipedia");
        assert!(result.is_err());
        Ok(())
    }
}
