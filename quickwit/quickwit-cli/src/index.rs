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

use std::collections::VecDeque;
use std::fmt::Display;
use std::io::{stdout, Stdout, Write};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, Instant};
use std::{fmt, io};

use anyhow::{bail, Context};
use byte_unit::Byte;
use bytes::Bytes;
use clap::{arg, ArgMatches, Command};
use colored::{ColoredString, Colorize};
use humantime::format_duration;
use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools;
use quickwit_actors::{ActorHandle, ObservationType};
use quickwit_common::uri::Uri;
use quickwit_common::GREEN_COLOR;
use quickwit_config::{ConfigFormat, IndexConfig};
use quickwit_indexing::models::IndexingStatistics;
use quickwit_indexing::IndexingPipeline;
use quickwit_metastore::{IndexMetadata, Split, SplitState};
use quickwit_proto::SortOrder;
use quickwit_rest_client::models::IngestSource;
use quickwit_rest_client::rest_client::{IngestEvent, QuickwitClient, Transport};
use quickwit_search::SearchResponseRest;
use quickwit_serve::{ListSplitsQueryParams, SearchRequestQueryString, SortByField};
use quickwit_storage::load_file;
use quickwit_telemetry::payload::TelemetryEvent;
use reqwest::Url;
use tabled::object::{Columns, Segment};
use tabled::{Alignment, Concat, Format, Modify, Panel, Rotate, Style, Table, Tabled};
use thousands::Separable;
use tracing::{debug, Level};

use crate::stats::{mean, percentile, std_deviation};
use crate::{cluster_endpoint_arg, make_table, prompt_confirmation, THROUGHPUT_WINDOW_SIZE};

pub fn build_index_command<'a>() -> Command<'a> {
    Command::new("index")
        .about("Manages indexes: creates, deletes, ingests, searches, describes...")
        .arg(cluster_endpoint_arg())
        .subcommand(
            Command::new("create")
                .display_order(1)
                .about("Creates an index from an index config file.")
                .args(&[
                    arg!(--"index-config" <INDEX_CONFIG> "Location of the index config file."),
                    arg!(--overwrite "Overwrites pre-existing index. This will delete all existing data stored at `index-uri` before creating a new index.")
                        .required(false),
                ])
            )
        .subcommand(
            Command::new("clear")
                .display_order(2)
                .alias("clr")
                .about("Clears an index: deletes all splits and resets checkpoint.")
                .long_about("Deletes all its splits and resets its checkpoint. This operation is destructive and cannot be undone, proceed with caution.")
                .args(&[
                    arg!(--index <INDEX> "Index ID")
                        .display_order(1),
                ])
            )
        .subcommand(
            Command::new("delete")
                .display_order(3)
                .alias("del")
                .about("Deletes an index.")
                .long_about("Deletes an index. This operation is destructive and cannot be undone, proceed with caution.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index")
                        .display_order(1),
                    arg!(--"dry-run" "Executes the command in dry run mode and only displays the list of splits candidates for deletion.")
                        .required(false),
                ])
            )
        .subcommand(
            Command::new("describe")
                .display_order(4)
                .about("Displays descriptive statistics of an index.")
                .long_about("Displays descriptive statistics of an index. Displayed statistics are: number of published splits, number of documents, splits min/max timestamps, size of splits.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index")
                        .display_order(1),
                ])
            )
        .subcommand(
            Command::new("list")
                .alias("ls")
                .display_order(5)
                .about("List indexes.")
                .arg(cluster_endpoint_arg())
            )
        .subcommand(
            Command::new("ingest")
                .display_order(6)
                .about("Ingest NDJSON documents with the ingest API.")
                .long_about("Reads NDJSON documents from a file or streamed from stdin and sends them into ingest API.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index")
                        .display_order(1),
                    arg!(--"input-path" <INPUT_PATH> "Location of the input file.")
                        .required(false),
                ])
            )
        .subcommand(
            Command::new("search")
                .display_order(7)
                .about("Searches an index.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index")
                        .display_order(1),
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
        .arg_required_else_help(true)
}

#[derive(Debug, Eq, PartialEq)]
pub struct ClearIndexArgs {
    pub cluster_endpoint: Url,
    pub index_id: String,
    pub assume_yes: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct CreateIndexArgs {
    pub cluster_endpoint: Url,
    pub index_config_uri: Uri,
    pub overwrite: bool,
    pub assume_yes: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct DescribeIndexArgs {
    pub cluster_endpoint: Url,
    pub index_id: String,
}

#[derive(Debug, Eq, PartialEq)]
pub struct IngestDocsArgs {
    pub cluster_endpoint: Url,
    pub index_id: String,
    pub input_path_opt: Option<PathBuf>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct SearchIndexArgs {
    pub cluster_endpoint: Url,
    pub index_id: String,
    pub query: String,
    pub aggregation: Option<String>,
    pub max_hits: usize,
    pub start_offset: usize,
    pub search_fields: Option<Vec<String>>,
    pub snippet_fields: Option<Vec<String>>,
    pub start_timestamp: Option<i64>,
    pub end_timestamp: Option<i64>,
    pub sort_by_score: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct DeleteIndexArgs {
    pub cluster_endpoint: Url,
    pub index_id: String,
    pub dry_run: bool,
    pub assume_yes: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ListIndexesArgs {
    pub cluster_endpoint: Url,
}

#[derive(Debug, Eq, PartialEq)]
pub enum IndexCliCommand {
    Clear(ClearIndexArgs),
    Create(CreateIndexArgs),
    Delete(DeleteIndexArgs),
    Describe(DescribeIndexArgs),
    Ingest(IngestDocsArgs),
    List(ListIndexesArgs),
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
            "ingest" => Self::parse_ingest_args(submatches),
            "list" => Self::parse_list_args(submatches),
            "search" => Self::parse_search_args(submatches),
            _ => bail!("Index subcommand `{}` is not implemented.", subcommand),
        }
    }

    fn parse_clear_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let cluster_endpoint = matches
            .value_of("endpoint")
            .map(Url::from_str)
            .expect("`endpoint` is a required arg.")?;
        let index_id = matches
            .value_of("index")
            .expect("`index` is a required arg.")
            .to_string();
        let assume_yes = matches.is_present("yes");
        Ok(Self::Clear(ClearIndexArgs {
            cluster_endpoint,
            index_id,
            assume_yes,
        }))
    }

    fn parse_create_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let cluster_endpoint = matches
            .value_of("endpoint")
            .map(Url::from_str)
            .expect("`endpoint` is a required arg.")?;
        let index_config_uri = matches
            .value_of("index-config")
            .map(Uri::from_str)
            .expect("`index-config` is a required arg.")?;
        let overwrite = matches.is_present("overwrite");
        let assume_yes = matches.is_present("yes");

        Ok(Self::Create(CreateIndexArgs {
            cluster_endpoint,
            index_config_uri,
            overwrite,
            assume_yes,
        }))
    }

    fn parse_describe_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let cluster_endpoint = matches
            .value_of("endpoint")
            .map(Url::from_str)
            .expect("`endpoint` is a required arg.")?;
        let index_id = matches
            .value_of("index")
            .expect("`index` is a required arg.")
            .to_string();
        Ok(Self::Describe(DescribeIndexArgs {
            cluster_endpoint,
            index_id,
        }))
    }

    fn parse_list_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let cluster_endpoint = matches
            .value_of("endpoint")
            .map(Url::from_str)
            .expect("`endpoint` is a required arg.")?;
        Ok(Self::List(ListIndexesArgs { cluster_endpoint }))
    }

    fn parse_ingest_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let cluster_endpoint = matches
            .value_of("endpoint")
            .map(Url::from_str)
            .expect("`endpoint` is a required arg.")?;
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

        Ok(Self::Ingest(IngestDocsArgs {
            cluster_endpoint,
            index_id,
            input_path_opt,
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
        let cluster_endpoint = matches
            .value_of("endpoint")
            .map(Url::from_str)
            .expect("`endpoint` is a required arg.")?;
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
            cluster_endpoint,
            sort_by_score,
        }))
    }

    fn parse_delete_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let cluster_endpoint = matches
            .value_of("endpoint")
            .map(Url::from_str)
            .expect("`endpoint` is a required arg.")?;
        let index_id = matches
            .value_of("index")
            .expect("`index` is a required arg.")
            .to_string();
        let dry_run = matches.is_present("dry-run");
        let assume_yes = matches.is_present("yes");
        Ok(Self::Delete(DeleteIndexArgs {
            index_id,
            dry_run,
            cluster_endpoint,
            assume_yes,
        }))
    }

    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            Self::Clear(args) => clear_index_cli(args).await,
            Self::Create(args) => create_index_cli(args).await,
            Self::Delete(args) => delete_index_cli(args).await,
            Self::Describe(args) => describe_index_cli(args).await,
            Self::Ingest(args) => ingest_docs_cli(args).await,
            Self::List(args) => list_index_cli(args).await,
            Self::Search(args) => search_index_cli(args).await,
        }
    }
}

pub async fn clear_index_cli(args: ClearIndexArgs) -> anyhow::Result<()> {
    debug!(args=?args, "clear-index");
    if !args.assume_yes {
        let prompt = format!(
            "This operation will delete all the splits of the index `{}` and reset its \
             checkpoint. Do you want to proceed?",
            args.index_id
        );
        if !prompt_confirmation(&prompt, false) {
            return Ok(());
        }
    }
    let endpoint = args.cluster_endpoint;
    let transport = Transport::new(endpoint);
    let qw_client = QuickwitClient::new(transport);
    qw_client.indexes().clear(&args.index_id).await?;
    println!("{} Index successfully cleared.", "✔".color(GREEN_COLOR),);
    Ok(())
}

pub async fn create_index_cli(args: CreateIndexArgs) -> anyhow::Result<()> {
    debug!(args=?args, "create-index");
    println!("❯ Creating index...");
    quickwit_telemetry::send_telemetry_event(TelemetryEvent::Create).await;
    let file_content = load_file(&args.index_config_uri).await?;
    let config_format = ConfigFormat::sniff_from_uri(&args.index_config_uri)?;
    let transport = Transport::new(args.cluster_endpoint);
    let qw_client = QuickwitClient::new(transport);
    // TODO: nice to have: check first if the index exists by send a GET request, if we get a 404,
    // the index does not exist. If it exists, we can display the prompt.
    if args.overwrite && !args.assume_yes {
        // Stop if user answers no.
        let prompt = "This operation will overwrite the index and delete all its data. Do you \
                      want to proceed?"
            .to_string();
        if !prompt_confirmation(&prompt, false) {
            return Ok(());
        }
    }
    let bytes = Bytes::from(file_content.to_vec());
    qw_client
        .indexes()
        .create(bytes, config_format, args.overwrite)
        .await?;
    println!("{} Index successfully created.", "✔".color(GREEN_COLOR));
    Ok(())
}

pub async fn list_index_cli(args: ListIndexesArgs) -> anyhow::Result<()> {
    debug!(args=?args, "list-index");
    let transport = Transport::new(args.cluster_endpoint);
    let qw_client = QuickwitClient::new(transport);
    let indexes_metadatas = qw_client.indexes().list().await?;
    let index_table = make_list_indexes_table(
        indexes_metadatas
            .into_iter()
            .map(IndexMetadata::into_index_config),
    );
    println!("\n{index_table}\n");
    Ok(())
}

fn make_list_indexes_table<I>(indexes: I) -> Table
where I: IntoIterator<Item = IndexConfig> {
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
    debug!(args=?args, "describe-index");
    let endpoint =
        Url::parse(args.cluster_endpoint.as_str()).context("Failed to parse cluster endpoint.")?;
    let transport = Transport::new(endpoint);
    let qw_client = QuickwitClient::new(transport);
    let index_metadata = qw_client.indexes().get(&args.index_id).await?;
    let list_splits_query_params = ListSplitsQueryParams::default();
    let splits = qw_client
        .splits(&args.index_id)
        .list(list_splits_query_params)
        .await?;
    let index_stats = IndexStats::from_metadata(index_metadata, splits)?;
    println!("{}", index_stats.display_as_table());
    Ok(())
}

pub struct IndexStats {
    pub index_id: String,
    pub index_uri: Uri,
    pub num_published_splits: usize,
    pub num_published_docs: u64,
    pub size_published_docs: Byte,
    pub timestamp_field_name: Option<String>,
    pub timestamp_range: Option<(i64, i64)>,
    pub num_docs_descriptive: Option<DescriptiveStats>,
    pub num_bytes_descriptive: Option<DescriptiveStats>,
}

impl Tabled for IndexStats {
    const LENGTH: usize = 7;

    fn fields(&self) -> Vec<String> {
        vec![
            self.index_id.clone(),
            self.index_uri.to_string(),
            self.num_published_splits.to_string(),
            self.num_published_docs.to_string(),
            self.size_published_docs
                .get_appropriate_unit(false)
                .to_string(),
            display_option_in_table(&self.timestamp_field_name),
            display_timestamp_range(&self.timestamp_range),
        ]
    }

    fn headers() -> Vec<String> {
        vec![
            "Index ID: ".to_string(),
            "Index URI: ".to_string(),
            "Number of published splits: ".to_string(),
            "Number of published documents: ".to_string(),
            "Size of published splits: ".to_string(),
            "Timestamp field: ".to_string(),
            "Timestamp range: ".to_string(),
        ]
    }
}

fn display_option_in_table(opt: &Option<impl Display>) -> String {
    match opt {
        Some(opt_val) => format!("{opt_val}"),
        None => "Field does not exist for the index.".to_string(),
    }
}

fn display_timestamp_range(range: &Option<(i64, i64)>) -> String {
    match range {
        Some((timestamp_min, timestamp_max)) => {
            format!("{timestamp_min} -> {timestamp_max}")
        }
        _ => "Range does not exist for the index.".to_string(),
    }
}

impl IndexStats {
    pub fn from_metadata(
        index_metadata: IndexMetadata,
        splits: Vec<Split>,
    ) -> anyhow::Result<Self> {
        let published_splits: Vec<Split> = splits
            .into_iter()
            .filter(|split| split.split_state == SplitState::Published)
            .collect();
        let splits_num_docs = published_splits
            .iter()
            .map(|split| split.split_metadata.num_docs as u64)
            .sorted()
            .collect_vec();

        let total_num_docs = splits_num_docs.iter().sum::<u64>();

        let splits_bytes = published_splits
            .iter()
            .filter(|split| split.split_state == SplitState::Published)
            .map(|split| split.split_metadata.footer_offsets.end)
            .sorted()
            .collect_vec();
        let total_bytes = splits_bytes.iter().sum::<u64>();

        let timestamp_range = if index_metadata
            .index_config()
            .doc_mapping
            .timestamp_field
            .is_some()
        {
            let time_min = published_splits
                .iter()
                .flat_map(|split| split.split_metadata.time_range.clone())
                .map(|time_range| *time_range.start())
                .min();
            let time_max = published_splits
                .iter()
                .flat_map(|split| split.split_metadata.time_range.clone())
                .map(|time_range| *time_range.end())
                .max();
            if let (Some(time_min), Some(time_max)) = (time_min, time_max) {
                Some((time_min, time_max))
            } else {
                None
            }
        } else {
            None
        };

        let (num_docs_descriptive, num_bytes_descriptive) = if !published_splits.is_empty() {
            (
                DescriptiveStats::maybe_new(&splits_num_docs),
                DescriptiveStats::maybe_new(&splits_bytes),
            )
        } else {
            (None, None)
        };
        let index_config = index_metadata.into_index_config();

        Ok(Self {
            index_id: index_config.index_id.clone(),
            index_uri: index_config.index_uri.clone(),
            num_published_splits: published_splits.len(),
            num_published_docs: total_num_docs,
            size_published_docs: Byte::from(total_bytes),
            timestamp_field_name: index_config.doc_mapping.timestamp_field,
            timestamp_range,
            num_docs_descriptive,
            num_bytes_descriptive,
        })
    }

    pub fn display_as_table(&self) -> String {
        let index_stats_table = create_table(self, "General Information");

        let index_stats_table = if let Some(docs_stats) = &self.num_docs_descriptive {
            let doc_stats_table = create_table(docs_stats, "Document count stats (published)");
            index_stats_table.with(Concat::vertical(doc_stats_table))
        } else {
            index_stats_table
        };

        let index_stats_table = if let Some(size_stats) = &self.num_bytes_descriptive {
            // size_stats is in byte, we have to divide all stats by 1_000_000 to be in MB.
            let size_stats_in_mb = DescriptiveStats {
                max_val: size_stats.max_val / 1_000_000,
                min_val: size_stats.min_val / 1_000_000,
                mean_val: size_stats.mean_val / 1_000_000.0,
                q1: size_stats.q1 / 1_000_000.0,
                q25: size_stats.q25 / 1_000_000.0,
                q50: size_stats.q50 / 1_000_000.0,
                q75: size_stats.q75 / 1_000_000.0,
                q99: size_stats.q99 / 1_000_000.0,
                std_val: size_stats.std_val / 1_000_000.0,
            };
            let size_stats_table = create_table(size_stats_in_mb, "Size in MB stats (published)");
            index_stats_table.with(Concat::vertical(size_stats_table))
        } else {
            index_stats_table
        };

        index_stats_table.to_string()
    }
}

fn create_table(table: impl Tabled, header: &str) -> Table {
    Table::new(vec![table])
        .with(Rotate::Left)
        .with(Rotate::Bottom)
        .with(
            Modify::new(Columns::first())
                .with(Format::new(|column| column.color(GREEN_COLOR).to_string())),
        )
        .with(
            Modify::new(Segment::all())
                .with(Alignment::left())
                .with(Alignment::top()),
        )
        .with(Panel(header, 0))
        .with(Style::psql())
        .with(Panel("\n", 0))
}

#[derive(Debug)]
pub struct DescriptiveStats {
    mean_val: f32,
    std_val: f32,
    min_val: u64,
    max_val: u64,
    q1: f32,
    q25: f32,
    q50: f32,
    q75: f32,
    q99: f32,
}

impl Tabled for DescriptiveStats {
    const LENGTH: usize = 2;

    fn fields(&self) -> Vec<String> {
        vec![
            format!(
                "{} ± {} in [{} … {}]",
                self.mean_val, self.std_val, self.min_val, self.max_val
            ),
            format!(
                "[{}, {}, {}, {}, {}]",
                self.q1, self.q25, self.q50, self.q75, self.q99,
            ),
        ]
    }

    fn headers() -> Vec<String> {
        vec![
            "Mean ± σ in [min … max]:".to_string(),
            "Quantiles [1%, 25%, 50%, 75%, 99%]:".to_string(),
        ]
    }
}

impl DescriptiveStats {
    pub fn maybe_new(values: &[u64]) -> Option<DescriptiveStats> {
        if values.is_empty() {
            return None;
        }
        Some(DescriptiveStats {
            mean_val: mean(values),
            std_val: std_deviation(values),
            min_val: *values.iter().min().expect("Values should not be empty."),
            max_val: *values.iter().max().expect("Values should not be empty."),
            q1: percentile(values, 1),
            q25: percentile(values, 50),
            q50: percentile(values, 50),
            q75: percentile(values, 75),
            q99: percentile(values, 75),
        })
    }
}

pub async fn ingest_docs_cli(args: IngestDocsArgs) -> anyhow::Result<()> {
    debug!(args=?args, "ingest-docs");
    if let Some(input_path) = &args.input_path_opt {
        println!("❯ Ingesting documents from {}.", input_path.display());
    } else {
        println!("❯ Ingesting documents from stdin.");
    }
    quickwit_telemetry::send_telemetry_event(TelemetryEvent::Ingest).await;
    let progress_bar = match &args.input_path_opt {
        Some(filepath) => {
            let file_len = std::fs::metadata(filepath).context("File not found")?.len();
            ProgressBar::new(file_len)
        }
        None => ProgressBar::new_spinner(),
    };
    progress_bar.enable_steady_tick(Duration::from_millis(100));
    progress_bar.set_style(progress_bar_style());
    progress_bar.set_message("0MiB/s");
    let update_progress_bar = |ingest_event: IngestEvent| {
        match ingest_event {
            IngestEvent::IngestedDocBatch(num_bytes) => progress_bar.inc(num_bytes as u64),
            IngestEvent::Sleep => {} // To
        };
        let throughput =
            progress_bar.position() as f64 / progress_bar.elapsed().as_secs_f64() / 1024.0 / 1024.0;
        progress_bar.set_message(format!("{throughput:.1} MiB/s"));
    };

    let transport = Transport::new(args.cluster_endpoint);
    let qw_client = QuickwitClient::new(transport);
    let ingest_source = match args.input_path_opt {
        Some(filepath) => IngestSource::File(filepath),
        None => IngestSource::Stdin,
    };
    qw_client
        .ingest(&args.index_id, ingest_source, Some(&update_progress_bar))
        .await?;
    progress_bar.finish();
    println!(
        "Ingested {} documents successfully.",
        "✔".color(GREEN_COLOR)
    );
    Ok(())
}

fn progress_bar_style() -> ProgressStyle {
    ProgressStyle::with_template(
        "{spinner:.blue} [{elapsed_precise}] {bytes}/{total_bytes} ({msg})",
    )
    .expect("Progress style should always be valid.")
    .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"])
}

pub async fn search_index(args: SearchIndexArgs) -> anyhow::Result<SearchResponseRest> {
    let aggs: Option<serde_json::Value> = args
        .aggregation
        .map(|aggs_string| {
            serde_json::from_str(&aggs_string).context("Failed to deserialize aggregations.")
        })
        .transpose()?;
    let sort_by_field = args.sort_by_score.then_some(SortByField {
        field_name: "_score".to_string(),
        order: SortOrder::Desc,
    });
    let search_request = SearchRequestQueryString {
        query: args.query,
        aggs,
        search_fields: args.search_fields.clone(),
        snippet_fields: args.snippet_fields.clone(),
        start_timestamp: args.start_timestamp,
        end_timestamp: args.end_timestamp,
        max_hits: args.max_hits as u64,
        start_offset: args.start_offset as u64,
        sort_by_field,
        ..Default::default()
    };
    let transport = Transport::new(args.cluster_endpoint);
    let qw_client = QuickwitClient::new(transport);
    let search_response = qw_client.search(&args.index_id, search_request).await?;
    Ok(search_response)
}

pub async fn search_index_cli(args: SearchIndexArgs) -> anyhow::Result<()> {
    debug!(args=?args, "search-index");
    let search_response_rest = search_index(args).await?;
    let search_response_json = serde_json::to_string_pretty(&search_response_rest)?;
    println!("{search_response_json}");
    Ok(())
}

pub async fn delete_index_cli(args: DeleteIndexArgs) -> anyhow::Result<()> {
    debug!(args=?args, "delete-index");
    println!("❯ Deleting index...");
    quickwit_telemetry::send_telemetry_event(TelemetryEvent::Delete).await;
    let endpoint =
        Url::parse(args.cluster_endpoint.as_str()).context("Failed to parse cluster endpoint.")?;
    let transport = Transport::new(endpoint);
    let qw_client = QuickwitClient::new(transport);
    let affected_files = qw_client
        .indexes()
        .delete(&args.index_id, args.dry_run)
        .await?;
    if !args.dry_run && !args.assume_yes {
        let prompt = "This operation will delete the index. Do you want to proceed?".to_string();
        if !prompt_confirmation(&prompt, false) {
            return Ok(());
        }
    }
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
    println!("{} Index successfully deleted.", "✔".color(GREEN_COLOR));
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

#[cfg(test)]
mod test {

    use std::ops::RangeInclusive;

    use quickwit_metastore::SplitMetadata;

    use super::*;

    pub fn split_metadata_for_test(
        split_id: &str,
        num_docs: usize,
        time_range: RangeInclusive<i64>,
        size: u64,
    ) -> SplitMetadata {
        let mut split_metadata = SplitMetadata::for_test(split_id.to_string());
        split_metadata.num_docs = num_docs;
        split_metadata.time_range = Some(time_range);
        split_metadata.footer_offsets = (size - 10)..size;
        split_metadata
    }

    #[test]
    fn test_index_stats() -> anyhow::Result<()> {
        let index_id = "index-stats-env".to_string();
        let split_id_1 = "test_split_id_1".to_string();
        let split_id_2 = "test_split_id_2".to_string();
        let index_uri = "s3://some-test-bucket";

        let index_metadata = IndexMetadata::for_test(&index_id, index_uri);
        let split_metadata_1 =
            split_metadata_for_test(&split_id_1, 100_000, 1111..=2222, 15_000_000);
        let split_metadata_2 =
            split_metadata_for_test(&split_id_2, 100_000, 1000..=3000, 30_000_000);

        let split_data_1 = Split {
            split_metadata: split_metadata_1,
            split_state: quickwit_metastore::SplitState::Published,
            update_timestamp: 0,
            publish_timestamp: Some(0),
        };
        let split_data_2 = Split {
            split_metadata: split_metadata_2,
            split_state: quickwit_metastore::SplitState::MarkedForDeletion,
            update_timestamp: 0,
            publish_timestamp: Some(0),
        };

        let index_stats =
            IndexStats::from_metadata(index_metadata, vec![split_data_1, split_data_2])?;

        assert_eq!(index_stats.index_id, index_id);
        assert_eq!(index_stats.index_uri.as_str(), index_uri);
        assert_eq!(index_stats.num_published_splits, 1);
        assert_eq!(index_stats.num_published_docs, 100_000);
        assert_eq!(index_stats.size_published_docs, Byte::from(15_000_000usize));
        assert_eq!(
            index_stats.timestamp_field_name,
            Some("timestamp".to_string())
        );
        assert_eq!(index_stats.timestamp_range, Some((1111, 2222)));

        Ok(())
    }

    #[test]
    fn test_descriptive_stats() -> anyhow::Result<()> {
        let split_id = "stat-test-split".to_string();
        let template_split = Split {
            split_state: quickwit_metastore::SplitState::Published,
            update_timestamp: 0,
            publish_timestamp: Some(0),
            split_metadata: SplitMetadata::default(),
        };

        let split_metadata_1 = split_metadata_for_test(&split_id, 70_000, 10..=12, 60_000_000);
        let split_metadata_2 = split_metadata_for_test(&split_id, 120_000, 11..=15, 145_000_000);
        let split_metadata_3 = split_metadata_for_test(&split_id, 90_000, 15..=22, 115_000_000);
        let split_metadata_4 = split_metadata_for_test(&split_id, 40_000, 22..=22, 55_000_000);

        let mut split_1 = template_split.clone();
        split_1.split_metadata = split_metadata_1;
        let mut split_2 = template_split.clone();
        split_2.split_metadata = split_metadata_2;
        let mut split_3 = template_split.clone();
        split_3.split_metadata = split_metadata_3;
        let mut split_4 = template_split;
        split_4.split_metadata = split_metadata_4;

        let splits = vec![split_1, split_2, split_3, split_4];

        let splits_num_docs = splits
            .iter()
            .map(|split| split.split_metadata.num_docs as u64)
            .sorted()
            .collect_vec();

        let splits_bytes = splits
            .iter()
            .map(|split| split.split_metadata.footer_offsets.end)
            .sorted()
            .collect_vec();

        let num_docs_descriptive = DescriptiveStats::maybe_new(&splits_num_docs);
        let num_bytes_descriptive = DescriptiveStats::maybe_new(&splits_bytes);
        let desciptive_stats_none = DescriptiveStats::maybe_new(&[]);

        assert!(num_docs_descriptive.is_some());
        assert!(num_bytes_descriptive.is_some());

        assert!(desciptive_stats_none.is_none());

        Ok(())
    }
}
