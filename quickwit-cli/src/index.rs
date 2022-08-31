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

use std::collections::VecDeque;
use std::io::{stdout, Stdout, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use std::{env, fmt, io};

use anyhow::{bail, Context};
use clap::{arg, ArgMatches, Command};
use colored::{ColoredString, Colorize};
use humantime::format_duration;
use itertools::Itertools;
use quickwit_actors::{ActorHandle, ObservationType, Universe};
use quickwit_common::uri::Uri;
use quickwit_common::GREEN_COLOR;
use quickwit_config::{
    IndexConfig, IndexerConfig, SourceConfig, SourceParams, CLI_INGEST_SOURCE_ID,
};
use quickwit_core::{
    clear_cache_directory, remove_indexing_directory, validate_storage_uri, IndexService,
};
use quickwit_indexing::actors::{IndexingPipeline, IndexingService};
use quickwit_indexing::models::{
    DetachPipeline, IndexingStatistics, SpawnMergePipeline, SpawnPipeline,
};
use quickwit_metastore::{quickwit_metastore_uri_resolver, IndexMetadata, SplitState};
use quickwit_proto::{SearchRequest, SearchResponse};
use quickwit_search::{single_node_search, SearchResponseRest};
use quickwit_storage::{load_file, quickwit_storage_uri_resolver};
use quickwit_telemetry::payload::TelemetryEvent;
use tabled::{Table, Tabled};
use thousands::Separable;
use tracing::{debug, warn, Level};

use crate::stats::{mean, percentile, std_deviation};
use crate::{
    load_quickwit_config, make_table, parse_duration_with_unit, prompt_confirmation,
    run_index_checklist, THROUGHPUT_WINDOW_SIZE,
};

pub fn build_index_command<'a>() -> Command<'a> {
    Command::new("index")
        .about("Create your index, ingest data, search, describe... every command you need to manage indexes.")
        .subcommand(
            Command::new("list")
                .about("List indexes.")
                .alias("ls")
                .args(&[
                    arg!(--"metastore-uri" <METASTORE_URI> "Metastore URI. Override the `metastore_uri` parameter defined in the config file. Defaults to file-backed, but could be Amazon S3 or PostgreSQL.")
                        .required(false)
                ])
            )
        .subcommand(
            Command::new("create")
                .about("Creates an index from an index config file.")
                .args(&[
                    arg!(--"index-config" <INDEX_CONFIG> "Location of the index config file."),
                    arg!(--"data-dir" <DATA_DIR> "Where data is persisted. Override data-dir defined in config file, default is `./qwdata`.")
                        .env("QW_DATA_DIR")
                        .required(false),
                    arg!(--overwrite "Overwrites pre-existing index.")
                        .required(false),
                ])
            )
        .subcommand(
            Command::new("ingest")
                .about("Indexes JSON documents read from a file or streamed from stdin.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index"),
                    arg!(--"data-dir" <DATA_DIR> "Where data is persisted. Override data-dir defined in config file, default is `./qwdata`.")
                        .env("QW_DATA_DIR")
                        .required(false),
                    arg!(--"input-path" <INPUT_PATH> "Location of the input file.")
                        .required(false),
                    arg!(--overwrite "Overwrites pre-existing index.")
                        .required(false),
                    arg!(--"keep-cache" "Does not clear local cache directory upon completion.")
                        .required(false),
                ])
            )
        .subcommand(
            Command::new("describe")
                .about("Displays descriptive statistics of an index: number of published splits, number of documents, splits min/max timestamps, size of splits.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index"),
                    arg!(--"data-dir" <DATA_DIR> "Where data is persisted. Override data-dir defined in config file, default is `./qwdata`.")
                        .env("QW_DATA_DIR")
                        .required(false),
                ])
            )
        .subcommand(
            Command::new("search")
                .about("Searches an index.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index"),
                    arg!(--"data-dir" <DATA_DIR> "Where data is persisted. Override data-dir defined in config file, default is `./qwdata`.")
                        .env("QW_DATA_DIR")
                        .required(false),
                    arg!(--query <QUERY> "Query expressed in natural query language ((barack AND obama) OR \"president of united states\"). Learn more on https://quickwit.io/docs/reference/search-language."),
                    arg!(--aggregation <AGG> "JSON serialized aggregation request in tantivy/elasticsearch format.")
                        .required(false),
                    arg!(--"max-hits" <MAX_HITS> "Maximum number of hits returned.")
                        .default_value("20")
                        .required(false),
                    arg!(--"start-offset" <OFFSET> "Offset in the global result set of the first hit returned.")
                        .default_value("0")
                        .required(false),
                    arg!(--"search-fields" <FIELD_NAME> "List of fields that Quickwit will search into if the user query does not explicitly target a field in the query. It overrides the default search fields defined in the index config. Space-separated list, e.g. \"field1 field2\". ")
                        .multiple_values(true)
                        .required(false),
                    arg!(--"snippet-fields" <FIELD_NAME> "List of fields that Quickwit will return snippet highlight on. Space-separated list, e.g. \"field1 field2\". ")
                        .multiple_values(true)
                        .required(false),
                    arg!(--"start-timestamp" <TIMESTAMP> "Filters out documents before that timestamp (time-series indexes only).")
                        .required(false),
                    arg!(--"end-timestamp" <TIMESTAMP> "Filters out documents after that timestamp (time-series indexes only).")
                        .required(false),
                    arg!(--"sort-by-score" "Setting this flag calculates and sorts documents by their BM25 score.")
                        .required(false),
                ])
            )
        .subcommand(
            Command::new("merge")
                .about("Merges an index.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index"),
                    arg!(--"data-dir" <DATA_DIR> "Where data is persisted. Override data-dir defined in config file, default is `./qwdata`.")
                        .env("QW_DATA_DIR")
                        .required(false),
                ])
            )
        .subcommand(
            Command::new("gc")
                .about("Garbage collects stale staged splits and splits marked for deletion.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index"),
                    arg!(--"data-dir" <DATA_DIR> "Where data is persisted. Override data-dir defined in config file, default is `./qwdata`.")
                        .env("QW_DATA_DIR")
                        .required(false),
                    arg!(--"grace-period" <GRACE_PERIOD> "Threshold period after which stale staged splits are garbage collected.")
                        .default_value("1h")
                        .required(false),
                    arg!(--"dry-run" "Executes the command in dry run mode and only displays the list of splits candidates for garbage collection.")
                        .required(false),
                ])
            )
        .subcommand(
            Command::new("clear")
                .alias("clr")
                .about("Clears and index. Deletes all its splits and resets its checkpoint. This operation is destructive and cannot be undone, proceed with caution.")
                .args(&[
                    arg!(--index <INDEX> "Index ID"),
                    arg!(--yes),
                ])
            )
        .subcommand(
            Command::new("delete")
            .alias("del")
                .about("Deletes an index. This operation is destructive and cannot be undone, proceed with caution.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index"),
                    arg!(--"data-dir" <DATA_DIR> "Where data is persisted. Override data-dir defined in config file, default is `./qwdata`.")
                        .env("QW_DATA_DIR")
                        .required(false),
                    arg!(--"dry-run" "Executes the command in dry run mode and only displays the list of splits candidates for deletion.")
                        .required(false),
                ])
            )
        .arg_required_else_help(true)
}

#[derive(Debug, Eq, PartialEq)]
pub struct ClearIndexArgs {
    pub config_uri: Uri,
    pub index_id: String,
    pub yes: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct CreateIndexArgs {
    pub config_uri: Uri,
    pub index_config_uri: Uri,
    pub data_dir: Option<PathBuf>,
    pub overwrite: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct DescribeIndexArgs {
    pub config_uri: Uri,
    pub data_dir: Option<PathBuf>,
    pub index_id: String,
}

#[derive(Debug, Eq, PartialEq)]
pub struct IngestDocsArgs {
    pub config_uri: Uri,
    pub index_id: String,
    pub input_path_opt: Option<PathBuf>,
    pub data_dir: Option<PathBuf>,
    pub overwrite: bool,
    pub clear_cache: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct SearchIndexArgs {
    pub config_uri: Uri,
    pub index_id: String,
    pub query: String,
    pub aggregation: Option<String>,
    pub max_hits: usize,
    pub start_offset: usize,
    pub search_fields: Option<Vec<String>>,
    pub snippet_fields: Option<Vec<String>>,
    pub start_timestamp: Option<i64>,
    pub end_timestamp: Option<i64>,
    pub data_dir: Option<PathBuf>,
    pub sort_by_score: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct DeleteIndexArgs {
    pub config_uri: Uri,
    pub index_id: String,
    pub dry_run: bool,
    pub data_dir: Option<PathBuf>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct GarbageCollectIndexArgs {
    pub config_uri: Uri,
    pub index_id: String,
    pub grace_period: Duration,
    pub dry_run: bool,
    pub data_dir: Option<PathBuf>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct MergeArgs {
    pub config_uri: Uri,
    pub index_id: String,
    pub data_dir: Option<PathBuf>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ListIndexesArgs {
    pub config_uri: Uri,
    pub metastore_uri: Option<Uri>,
}

#[derive(Debug, Eq, PartialEq)]
pub enum IndexCliCommand {
    Clear(ClearIndexArgs),
    Create(CreateIndexArgs),
    Delete(DeleteIndexArgs),
    Describe(DescribeIndexArgs),
    GarbageCollect(GarbageCollectIndexArgs),
    Ingest(IngestDocsArgs),
    List(ListIndexesArgs),
    Merge(MergeArgs),
    Search(SearchIndexArgs),
}

impl IndexCliCommand {
    pub fn default_log_level(&self) -> Level {
        match self {
            Self::Search(_) => Level::ERROR,
            _ => Level::INFO,
        }
    }

    pub fn parse_cli_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches) = matches
            .subcommand()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse sub-matches."))?;
        match subcommand {
            "clear" => Self::parse_clear_args(submatches),
            "create" => Self::parse_create_args(submatches),
            "delete" => Self::parse_delete_args(submatches),
            "describe" => Self::parse_describe_args(submatches),
            "gc" => Self::parse_garbage_collect_args(submatches),
            "ingest" => Self::parse_ingest_args(submatches),
            "list" => Self::parse_list_args(submatches),
            "merge" => Self::parse_merge_args(submatches),
            "search" => Self::parse_search_args(submatches),
            _ => bail!("Index subcommand `{}` is not implemented.", subcommand),
        }
    }

    fn parse_clear_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let config_uri = matches
            .value_of("config")
            .map(Uri::try_new)
            .expect("`config` is a required arg.")?;
        let index_id = matches
            .value_of("index")
            .expect("`index` is a required arg.")
            .to_string();
        let yes = matches.is_present("yes");
        Ok(Self::Clear(ClearIndexArgs {
            config_uri,
            index_id,
            yes,
        }))
    }

    fn parse_create_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let config_uri = matches
            .value_of("config")
            .map(Uri::try_new)
            .expect("`config` is a required arg.")?;
        let index_config_uri = matches
            .value_of("index-config")
            .map(Uri::try_new)
            .expect("`index-config` is a required arg.")?;
        let data_dir = matches.value_of("data-dir").map(PathBuf::from);
        let overwrite = matches.is_present("overwrite");

        Ok(Self::Create(CreateIndexArgs {
            config_uri,
            data_dir,
            index_config_uri,
            overwrite,
        }))
    }

    fn parse_describe_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let config_uri = matches
            .value_of("config")
            .map(Uri::try_new)
            .expect("`config` is a required arg.")?;
        let index_id = matches
            .value_of("index")
            .expect("`index` is a required arg.")
            .to_string();
        let data_dir = matches.value_of("data-dir").map(PathBuf::from);
        Ok(Self::Describe(DescribeIndexArgs {
            config_uri,
            index_id,
            data_dir,
        }))
    }

    fn parse_list_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let config_uri = matches
            .value_of("config")
            .map(Uri::try_new)
            .expect("`config` is a required arg.")?;

        let metastore_uri = matches
            .value_of("metastore-uri")
            .map(Uri::try_new)
            .transpose()?;

        Ok(Self::List(ListIndexesArgs {
            config_uri,
            metastore_uri,
        }))
    }

    fn parse_ingest_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_id = matches
            .value_of("index")
            .expect("`index` is a required arg.")
            .to_string();
        let input_path_opt = if let Some(input_path) = matches.value_of("input-path") {
            Uri::try_new(input_path)?
                .filepath()
                .map(|path| path.to_path_buf())
        } else {
            None
        };
        let config_uri = matches
            .value_of("config")
            .map(Uri::try_new)
            .expect("`config` is a required arg.")?;
        let data_dir = matches.value_of("data-dir").map(PathBuf::from);
        let overwrite = matches.is_present("overwrite");
        let clear_cache = !matches.is_present("keep-cache");

        Ok(Self::Ingest(IngestDocsArgs {
            index_id,
            input_path_opt,
            overwrite,
            config_uri,
            data_dir,
            clear_cache,
        }))
    }

    fn parse_search_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_id = matches
            .value_of("index")
            .expect("`index` is a required arg.")
            .to_string();
        let query = matches
            .value_of("query")
            .context("`query` is a required arg.")?
            .to_string();
        let aggregation = matches.value_of("aggregation").map(|el| el.to_string());

        let max_hits = matches.value_of_t::<usize>("max-hits")?;
        let start_offset = matches.value_of_t::<usize>("start-offset")?;
        let search_fields = matches
            .values_of("search-fields")
            .map(|values| values.map(|value| value.to_string()).collect());
        let snippet_fields = matches
            .values_of("snippet-fields")
            .map(|values| values.map(|value| value.to_string()).collect());
        let sort_by_score = matches.is_present("sort-by-score");
        let start_timestamp = if matches.is_present("start-timestamp") {
            Some(matches.value_of_t::<i64>("start-timestamp")?)
        } else {
            None
        };
        let end_timestamp = if matches.is_present("end-timestamp") {
            Some(matches.value_of_t::<i64>("end-timestamp")?)
        } else {
            None
        };
        let config_uri = matches
            .value_of("config")
            .map(Uri::try_new)
            .expect("`config` is a required arg.")?;
        let data_dir = matches.value_of("data-dir").map(PathBuf::from);
        Ok(Self::Search(SearchIndexArgs {
            index_id,
            query,
            aggregation,
            max_hits,
            start_offset,
            search_fields,
            snippet_fields,
            start_timestamp,
            end_timestamp,
            config_uri,
            data_dir,
            sort_by_score,
        }))
    }

    fn parse_merge_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_id = matches
            .value_of("index")
            .context("'index-id' is a required arg.")?
            .to_string();
        let config_uri = matches
            .value_of("config")
            .map(Uri::try_new)
            .expect("`config` is a required arg.")?;
        let data_dir = matches.value_of("data-dir").map(PathBuf::from);
        Ok(Self::Merge(MergeArgs {
            index_id,
            config_uri,
            data_dir,
        }))
    }

    fn parse_garbage_collect_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_id = matches
            .value_of("index")
            .expect("`index` is a required arg.")
            .to_string();
        let grace_period = matches
            .value_of("grace-period")
            .map(parse_duration_with_unit)
            .expect("`grace-period` should have a default value.")?;
        let dry_run = matches.is_present("dry-run");
        let config_uri = matches
            .value_of("config")
            .map(Uri::try_new)
            .expect("`config` is a required arg.")?;
        let data_dir = matches.value_of("data-dir").map(PathBuf::from);
        Ok(Self::GarbageCollect(GarbageCollectIndexArgs {
            index_id,
            grace_period,
            dry_run,
            config_uri,
            data_dir,
        }))
    }

    fn parse_delete_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_id = matches
            .value_of("index")
            .expect("`index` is a required arg.")
            .to_string();
        let dry_run = matches.is_present("dry-run");
        let config_uri = matches
            .value_of("config")
            .map(Uri::try_new)
            .expect("`config` is a required arg.")?;
        let data_dir = matches.value_of("data-dir").map(PathBuf::from);
        Ok(Self::Delete(DeleteIndexArgs {
            index_id,
            dry_run,
            config_uri,
            data_dir,
        }))
    }

    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            Self::Clear(args) => clear_index_cli(args).await,
            Self::Create(args) => create_index_cli(args).await,
            Self::Delete(args) => delete_index_cli(args).await,
            Self::Describe(args) => describe_index_cli(args).await,
            Self::GarbageCollect(args) => garbage_collect_index_cli(args).await,
            Self::Ingest(args) => ingest_docs_cli(args).await,
            Self::List(args) => list_index_cli(args).await,
            Self::Merge(args) => merge_cli(args, true).await,
            Self::Search(args) => search_index_cli(args).await,
        }
    }
}

pub async fn clear_index_cli(args: ClearIndexArgs) -> anyhow::Result<()> {
    debug!(args=?args, "clear-index");
    if !args.yes {
        let prompt = format!(
            "This operation will delete all the splits of the index `{}` and reset its \
             checkpoint. Do you want to proceed?",
            args.index_id
        );
        if !prompt_confirmation(&prompt, false) {
            return Ok(());
        }
    }
    let config = load_quickwit_config(&args.config_uri, None).await?;
    let index_service = IndexService::from_config(config).await?;
    index_service.clear_index(&args.index_id).await?;
    println!("Index `{}` successfully cleared.", args.index_id);
    Ok(())
}

pub async fn create_index_cli(args: CreateIndexArgs) -> anyhow::Result<()> {
    debug!(args=?args, "create-index");
    quickwit_telemetry::send_telemetry_event(TelemetryEvent::Create).await;

    let quickwit_config = load_quickwit_config(&args.config_uri, args.data_dir).await?;
    let file_content = load_file(&args.index_config_uri).await?;
    let index_config = IndexConfig::load(&args.index_config_uri, file_content.as_slice()).await?;
    let index_id = index_config.index_id.clone();
    let metastore_uri_resolver = quickwit_metastore_uri_resolver();
    let metastore = metastore_uri_resolver
        .resolve(&quickwit_config.metastore_uri)
        .await?;

    validate_storage_uri(metastore_uri_resolver, &quickwit_config, &index_config).await?;

    let index_service = IndexService::new(
        metastore,
        quickwit_storage_uri_resolver().clone(),
        quickwit_config.default_index_root_uri,
    );
    index_service
        .create_index(index_config, args.overwrite)
        .await?;
    println!("Index `{}` successfully created.", index_id);
    Ok(())
}

pub async fn list_index_cli(args: ListIndexesArgs) -> anyhow::Result<()> {
    debug!(args=?args, "list");
    let metastore_uri_resolver = quickwit_metastore_uri_resolver();
    let quickwit_config = load_quickwit_config(&args.config_uri, None).await?;
    let metastore_uri = args.metastore_uri.unwrap_or(quickwit_config.metastore_uri);
    let metastore = metastore_uri_resolver.resolve(&metastore_uri).await?;
    let indexes = metastore.list_indexes_metadatas().await?;
    let index_table = make_list_indexes_table(indexes);

    println!();
    println!("{}", index_table);
    println!();
    Ok(())
}

fn make_list_indexes_table<I>(indexes: I) -> Table
where I: IntoIterator<Item = IndexMetadata> {
    let rows = indexes
        .into_iter()
        .map(|index| IndexRow {
            index_id: index.index_id,
            index_uri: index.index_uri,
        })
        .sorted_by(|left, right| left.index_id.cmp(&right.index_id));
    make_table("Indexes", rows, false)
}

#[derive(Tabled)]
struct IndexRow {
    #[tabled(rename = "Index ID")]
    index_id: String,
    #[tabled(rename = "Index URI")]
    index_uri: Uri,
}

pub async fn describe_index_cli(args: DescribeIndexArgs) -> anyhow::Result<()> {
    debug!(args=?args, "describe");
    let metastore_uri_resolver = quickwit_metastore_uri_resolver();
    let quickwit_config = load_quickwit_config(&args.config_uri, args.data_dir).await?;
    let metastore = metastore_uri_resolver
        .resolve(&quickwit_config.metastore_uri)
        .await?;
    let index_metadata = metastore.index_metadata(&args.index_id).await?;
    let splits = metastore
        .list_splits(&args.index_id, SplitState::Published, None, None)
        .await?;

    let splits_num_docs = splits
        .iter()
        .map(|split| split.split_metadata.num_docs)
        .sorted()
        .collect_vec();
    let total_num_docs = splits_num_docs.iter().sum::<usize>();
    let splits_bytes = splits
        .iter()
        .map(|split| (split.split_metadata.footer_offsets.end / 1_000_000) as usize)
        .sorted()
        .collect_vec();
    let total_bytes = splits_bytes.iter().sum::<usize>();

    println!();
    println!("1. General information");
    println!("===============================================================================");
    println!(
        "{:<35} {}",
        "Index ID:".color(GREEN_COLOR),
        index_metadata.index_id
    );
    println!(
        "{:<35} {}",
        "Index URI:".color(GREEN_COLOR),
        index_metadata.index_uri
    );
    println!(
        "{:<35} {}",
        "Number of published splits:".color(GREEN_COLOR),
        splits.len()
    );
    println!(
        "{:<35} {}",
        "Number of published documents:".color(GREEN_COLOR),
        total_num_docs
    );
    println!(
        "{:<35} {} MB",
        "Size of published splits:".color(GREEN_COLOR),
        total_bytes
    );
    if let Some(timestamp_field_name) = &index_metadata.indexing_settings.timestamp_field {
        println!(
            "{:<35} {}",
            "Timestamp field:".color(GREEN_COLOR),
            timestamp_field_name
        );
        let time_min = splits
            .iter()
            .map(|split| split.split_metadata.time_range.clone())
            .filter(|time_range| time_range.is_some())
            .map(|time_range| *time_range.unwrap().start())
            .min();
        let time_max = splits
            .iter()
            .map(|split| split.split_metadata.time_range.clone())
            .filter(|time_range| time_range.is_some())
            .map(|time_range| *time_range.unwrap().start())
            .max();
        println!(
            "{:<35} {:?} -> {:?}",
            "Timestamp range:".color(GREEN_COLOR),
            time_min,
            time_max
        );
    }

    if splits.is_empty() {
        return Ok(());
    }

    println!();
    println!("2. Split statistics");
    println!("===============================================================================");
    println!("Document count stats:");
    print_descriptive_stats(&splits_num_docs);
    println!();
    println!("Size in MB stats:");
    print_descriptive_stats(&splits_bytes);

    println!();
    Ok(())
}

fn print_descriptive_stats(values: &[usize]) {
    let mean_val = mean(values);
    let std_val = std_deviation(values);
    let min_val = values.iter().min().unwrap();
    let max_val = values.iter().max().unwrap();
    println!(
        "{:<35} {:>2} ± {} in [{} … {}]",
        "Mean ± σ in [min … max]:".color(GREEN_COLOR),
        mean_val,
        std_val,
        min_val,
        max_val,
    );
    let q1 = percentile(values, 1);
    let q25 = percentile(values, 50);
    let q50 = percentile(values, 50);
    let q75 = percentile(values, 75);
    let q99 = percentile(values, 75);
    println!(
        "{:<35} [{}, {}, {}, {}, {}]",
        "Quantiles [1%, 25%, 50%, 75%, 99%]:".color(GREEN_COLOR),
        q1,
        q25,
        q50,
        q75,
        q99,
    );
}

pub async fn ingest_docs_cli(args: IngestDocsArgs) -> anyhow::Result<()> {
    debug!(args=?args, "ingest-docs");
    quickwit_telemetry::send_telemetry_event(TelemetryEvent::Ingest).await;

    let config = load_quickwit_config(&args.config_uri, args.data_dir).await?;

    let source_params = if let Some(filepath) = args.input_path_opt.as_ref() {
        SourceParams::file(filepath)
    } else {
        SourceParams::stdin()
    };
    let source_config = SourceConfig {
        source_id: CLI_INGEST_SOURCE_ID.to_string(),
        num_pipelines: 1,
        source_params,
    };
    run_index_checklist(&config.metastore_uri, &args.index_id, Some(&source_config)).await?;
    let metastore_uri_resolver = quickwit_metastore_uri_resolver();
    let metastore = metastore_uri_resolver
        .resolve(&config.metastore_uri)
        .await?;

    if args.overwrite {
        let index_service = IndexService::new(
            metastore.clone(),
            quickwit_storage_uri_resolver().clone(),
            config.default_index_root_uri.clone(),
        );
        index_service.clear_index(&args.index_id).await?;
    }
    let indexer_config = IndexerConfig {
        ..Default::default()
    };
    let universe = Universe::new();
    let enable_ingest_api = false;
    let indexing_server = IndexingService::new(
        config.node_id.clone(),
        config.data_dir_path.clone(),
        indexer_config,
        metastore,
        quickwit_storage_uri_resolver().clone(),
        enable_ingest_api,
    );
    let (indexing_server_mailbox, _) = universe.spawn_actor(indexing_server).spawn();
    let pipeline_id = indexing_server_mailbox
        .ask_for_res(SpawnPipeline {
            index_id: args.index_id.clone(),
            source_config,
            pipeline_ord: 0,
        })
        .await?;
    let pipeline_handle = indexing_server_mailbox
        .ask_for_res(DetachPipeline { pipeline_id })
        .await?;

    let is_stdin_atty = atty::is(atty::Stream::Stdin);
    if args.input_path_opt.is_none() && is_stdin_atty {
        let eof_shortcut = match env::consts::OS {
            "windows" => "CTRL+Z",
            _ => "CTRL+D",
        };
        println!(
            "Please, enter JSON documents one line at a time.\nEnd your input using {}.",
            eof_shortcut
        );
    }
    let statistics =
        start_statistics_reporting_loop(pipeline_handle, args.input_path_opt.is_none()).await?;
    if statistics.num_published_splits > 0 {
        println!(
            "Now, you can query the index with the following command:\nquickwit index search \
             --index {} --config ./config/quickwit.yaml --query \"my query\"",
            args.index_id
        );
    }

    if args.clear_cache {
        println!("Clearing local cache directory...");
        clear_cache_directory(
            &config.data_dir_path,
            args.index_id.clone(),
            CLI_INGEST_SOURCE_ID.to_string(),
        )
        .await?;
    }

    match statistics.num_invalid_docs {
        0 => Ok(()),
        _ => bail!("Failed to ingest all the documents."),
    }
}

pub async fn search_index(args: SearchIndexArgs) -> anyhow::Result<SearchResponse> {
    debug!(args=?args, "search-index");
    let quickwit_config = load_quickwit_config(&args.config_uri, args.data_dir).await?;
    let storage_uri_resolver = quickwit_storage_uri_resolver();
    let metastore_uri_resolver = quickwit_metastore_uri_resolver();
    let metastore = metastore_uri_resolver
        .resolve(&quickwit_config.metastore_uri)
        .await?;
    let search_request = SearchRequest {
        index_id: args.index_id,
        query: args.query.clone(),
        search_fields: args.search_fields.unwrap_or_default(),
        snippet_fields: args.snippet_fields.unwrap_or_default(),
        start_timestamp: args.start_timestamp,
        end_timestamp: args.end_timestamp,
        max_hits: args.max_hits as u64,
        start_offset: args.start_offset as u64,
        sort_order: None,
        sort_by_field: args.sort_by_score.then_some("_score".to_string()),
        aggregation_request: args.aggregation,
    };
    let search_response: SearchResponse =
        single_node_search(&search_request, &*metastore, storage_uri_resolver.clone()).await?;
    Ok(search_response)
}

pub async fn search_index_cli(args: SearchIndexArgs) -> anyhow::Result<()> {
    let search_response: SearchResponse = search_index(args).await?;
    let search_response_rest = SearchResponseRest::try_from(search_response)?;
    let search_response_json = serde_json::to_string_pretty(&search_response_rest)?;
    println!("{}", search_response_json);
    Ok(())
}

pub async fn merge_cli(args: MergeArgs, merge_enabled: bool) -> anyhow::Result<()> {
    debug!(args=?args, merge_enabled = merge_enabled, "run-merge-operations");
    let config = load_quickwit_config(&args.config_uri, args.data_dir).await?;
    run_index_checklist(&config.metastore_uri, &args.index_id, None).await?;
    let indexer_config = IndexerConfig {
        ..Default::default()
    };
    let metastore_uri_resolver = quickwit_metastore_uri_resolver();
    let metastore = metastore_uri_resolver
        .resolve(&config.metastore_uri)
        .await?;
    let storage_resolver = quickwit_storage_uri_resolver().clone();
    let enable_ingest_api = false;
    let indexing_server = IndexingService::new(
        config.node_id,
        config.data_dir_path,
        indexer_config,
        metastore,
        storage_resolver,
        enable_ingest_api,
    );
    let universe = Universe::new();
    let (indexing_server_mailbox, _) = universe.spawn_actor(indexing_server).spawn();
    let pipeline_id = indexing_server_mailbox
        .ask_for_res(SpawnMergePipeline {
            index_id: args.index_id.clone(),
            merge_enabled,
        })
        .await?;
    let pipeline_handle = indexing_server_mailbox
        .ask_for_res(DetachPipeline { pipeline_id })
        .await?;
    let (pipeline_exit_status, _pipeline_statistics) = pipeline_handle.join().await;
    if !pipeline_exit_status.is_success() {
        bail!(pipeline_exit_status);
    }
    Ok(())
}

pub async fn delete_index_cli(args: DeleteIndexArgs) -> anyhow::Result<()> {
    debug!(args=?args, "delete-index");
    quickwit_telemetry::send_telemetry_event(TelemetryEvent::Delete).await;

    let quickwit_config = load_quickwit_config(&args.config_uri, args.data_dir).await?;
    let metastore = quickwit_metastore_uri_resolver()
        .resolve(&quickwit_config.metastore_uri)
        .await?;
    let index_service = IndexService::new(
        metastore,
        quickwit_storage_uri_resolver().clone(),
        quickwit_config.default_index_root_uri,
    );
    let affected_files = index_service
        .delete_index(&args.index_id, args.dry_run)
        .await?;
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
    if let Err(error) =
        remove_indexing_directory(&quickwit_config.data_dir_path, args.index_id.clone()).await
    {
        warn!(error= ?error, "Failed to remove indexing directory.");
    }
    println!("Index `{}` successfully deleted.", args.index_id);
    Ok(())
}

pub async fn garbage_collect_index_cli(args: GarbageCollectIndexArgs) -> anyhow::Result<()> {
    debug!(args=?args, "garbage-collect-index");
    quickwit_telemetry::send_telemetry_event(TelemetryEvent::GarbageCollect).await;

    let quickwit_config = load_quickwit_config(&args.config_uri, args.data_dir).await?;
    let metastore = quickwit_metastore_uri_resolver()
        .resolve(&quickwit_config.metastore_uri)
        .await?;
    let index_service = IndexService::new(
        metastore,
        quickwit_storage_uri_resolver().clone(),
        quickwit_config.default_index_root_uri,
    );
    let deleted_files = index_service
        .garbage_collect_index(&args.index_id, args.grace_period, args.dry_run)
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
    pipeline_handle: ActorHandle<IndexingPipeline>,
    is_stdin: bool,
) -> anyhow::Result<IndexingStatistics> {
    let mut stdout_handle = stdout();
    let start_time = Instant::now();
    let mut throughput_calculator = ThroughputCalculator::new(start_time);
    let mut report_interval = tokio::time::interval(Duration::from_secs(1));

    loop {
        // TODO fixme. The way we wait today is a bit lame: if the indexing pipeline exits, we will
        // stil wait up to an entire heartbeat...  Ideally we should  select between two
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
    let error_rate_message = format!("({:.1}% error rate)", error_rate);
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
        write!(&mut self.stdout, " {}", fmt_args)
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
    printer.print_value(format_args!("{:>5.2}MB/s", throughput_mb_s))?;
    printer.print_header("Time")?;
    printer.print_value(format_args!("{}\n", elapsed_time))?;
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
            / elapsed_time.max(1f64) as f64
    }

    pub fn elapsed_time(&self) -> Duration {
        self.start_time.elapsed()
    }
}
