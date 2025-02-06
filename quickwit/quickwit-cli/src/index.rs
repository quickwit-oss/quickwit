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

use std::borrow::Cow;
use std::fmt::Display;
use std::num::NonZeroUsize;
use std::ops::Div;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context};
use bytesize::ByteSize;
use clap::{arg, Arg, ArgAction, ArgMatches, Command};
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools;
use numfmt::{Formatter, Scales};
use quickwit_common::tower::{Rate, RateEstimator, SmaRateEstimator};
use quickwit_common::uri::Uri;
use quickwit_config::{ConfigFormat, IndexConfig};
use quickwit_metastore::{IndexMetadata, Split, SplitState};
use quickwit_proto::search::{CountHits, SortField, SortOrder};
use quickwit_proto::types::IndexId;
use quickwit_rest_client::models::{IngestSource, SearchResponseRestClient};
use quickwit_rest_client::rest_client::{CommitType, IngestEvent};
use quickwit_serve::{ListSplitsQueryParams, SearchRequestQueryString, SortBy};
use quickwit_storage::{load_file, StorageResolver};
use tabled::settings::object::{FirstRow, Rows, Segment};
use tabled::settings::panel::Footer;
use tabled::settings::{Alignment, Disable, Format, Modify, Panel, Rotate, Style};
use tabled::{Table, Tabled};
use tracing::{debug, Level};

use crate::checklist::{GREEN_COLOR, RED_COLOR};
use crate::stats::{mean, percentile, std_deviation};
use crate::{client_args, make_table, prompt_confirmation, ClientArgs};

pub fn build_index_command() -> Command {
    Command::new("index")
        .about("Manages indexes: creates, updates, deletes, ingests, searches, describes...")
        .args(client_args())
        .subcommand(
            Command::new("create")
                .display_order(1)
                .about("Creates an index from an index config file.")
                .args(&[
                    arg!(--"index-config" <INDEX_CONFIG> "Location of the index config file.")
                        .display_order(1)
                        .required(true),
                    arg!(--overwrite "Overwrites pre-existing index. This will delete all existing data stored at `index-uri` before creating a new index.")
                        .required(false),
                ])
            )
        .subcommand(
            Command::new("update")
            .display_order(1)
            .about("Updates an index using an index config file.")
            .long_about("This command follows PUT semantics, which means that all the fields of the current configuration are replaced by the values specified in this request or the associated defaults. In particular, if the field is optional (e.g. `retention_policy`), omitting it will delete the associated configuration. If the new configuration file contains updates that cannot be applied, the request fails, and none of the updates are applied.")
            .args(&[
                arg!(--index <INDEX> "ID of the target index")
                    .display_order(1)
                    .required(true),
                arg!(--"index-config" <INDEX_CONFIG> "Location of the index config file.")
                    .display_order(2)
                    .required(true),
            ])
        )
        .subcommand(
            Command::new("clear")
                .display_order(3)
                .alias("clr")
                .about("Clears an index: deletes all splits and resets checkpoint.")
                .long_about("Deletes all its splits and resets its checkpoint. This operation is destructive and cannot be undone, proceed with caution.")
                .args(&[
                    arg!(--index <INDEX> "Index ID")
                        .display_order(1)
                        .required(true),
                ])
            )
        .subcommand(
            Command::new("delete")
                .display_order(4)
                .alias("del")
                .about("Deletes an index.")
                .long_about("Deletes an index. This operation is destructive and cannot be undone, proceed with caution.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index")
                        .display_order(1)
                        .required(true),
                    arg!(--"dry-run" "Executes the command in dry run mode and only displays the list of splits candidates for deletion.")
                        .required(false),
                ])
            )
        .subcommand(
            Command::new("describe")
                .display_order(5)
                .about("Displays descriptive statistics of an index.")
                .long_about("Displays descriptive statistics of an index. Displayed statistics are: number of published splits, number of documents, splits min/max timestamps, size of splits.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index")
                        .required(true),
                ])
            )
        .subcommand(
            Command::new("list")
                .alias("ls")
                .display_order(6)
                .about("List indexes.")
            )
        .subcommand(
            Command::new("ingest")
                .display_order(7)
                .about("Ingest NDJSON documents with the ingest API.")
                .long_about("Reads NDJSON documents from a file or streamed from stdin and sends them into ingest API.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index")
                        .display_order(1)
                        .required(true),
                    arg!(--"input-path" <INPUT_PATH> "Location of the input file.")
                        .required(false),
                    arg!(--"batch-size-limit" <BATCH_SIZE_LIMIT> "Size limit of each submitted document batch.")
                        .required(false),
                    Arg::new("wait")
                        .long("wait")
                        .short('w')
                        .help("Wait for all documents to be committed and available for search before exiting. Applies only to the last batch, see [#5417](https://github.com/quickwit-oss/quickwit/issues/5417).")
                        .action(ArgAction::SetTrue),
                    Arg::new("detailed-response")
                        .long("detailed-response")
                        .help("Print detailed errors. Enabling might impact performance negatively.")
                        .action(ArgAction::SetTrue),
                    Arg::new("force")
                        .long("force")
                        .short('f')
                        .help("Force a commit after the last document is sent, and wait for all documents to be committed and available for search before exiting. Applies only to the last batch, see [#5417](https://github.com/quickwit-oss/quickwit/issues/5417).")
                        .action(ArgAction::SetTrue)
                        .conflicts_with("wait"),
                    Arg::new("commit-timeout")
                        .long("commit-timeout")
                        .help("Timeout for ingest operations that require waiting for the final commit (`--wait` or `--force`). This is different from the `commit_timeout_secs` indexing setting, which sets the maximum time before committing splits after their creation.")
                        .required(false)
                        .global(true),
                ])
            )
        .subcommand(
            Command::new("search")
                .display_order(8)
                .about("Searches an index.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index")
                        .display_order(1)
                        .required(true),
                    arg!(--query <QUERY> "Query expressed in natural query language ((barack AND obama) OR \"president of united states\"). Learn more on https://quickwit.io/docs/reference/search-language.")
                        .display_order(2)
                        .required(true),
                    arg!(--aggregation <AGG> "JSON serialized aggregation request in tantivy/elasticsearch format.")
                        .required(false),
                    arg!(--"max-hits" <MAX_HITS> "Maximum number of hits returned.")
                        .default_value("20")
                        .required(false),
                    arg!(--"start-offset" <OFFSET> "Offset in the global result set of the first hit returned.")
                        .default_value("0")
                        .required(false),
                    arg!(--"search-fields" <FIELD_NAME> "List of fields that Quickwit will search into if the user query does not explicitly target a field in the query. It overrides the default search fields defined in the index config. Space-separated list, e.g. \"field1 field2\". ")
                        .num_args(1..)
                        .required(false),
                    arg!(--"snippet-fields" <FIELD_NAME> "List of fields that Quickwit will return snippet highlight on. Space-separated list, e.g. \"field1 field2\". ")
                        .num_args(1..)
                        .required(false),
                    arg!(--"start-timestamp" <TIMESTAMP> "Filters out documents before that timestamp (time-series indexes only).")
                        .required(false),
                    arg!(--"end-timestamp" <TIMESTAMP> "Filters out documents after that timestamp (time-series indexes only).")
                        .required(false),
                    arg!(--"sort-by-score" "Sorts documents by their BM25 score.")
                        .required(false),
                ])
            )
        .arg_required_else_help(true)
}

#[derive(Debug, Eq, PartialEq)]
pub struct ClearIndexArgs {
    pub client_args: ClientArgs,
    pub index_id: IndexId,
    pub assume_yes: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct CreateIndexArgs {
    pub client_args: ClientArgs,
    pub index_config_uri: Uri,
    pub overwrite: bool,
    pub assume_yes: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct UpdateIndexArgs {
    pub client_args: ClientArgs,
    pub index_id: IndexId,
    pub index_config_uri: Uri,
    pub assume_yes: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct DescribeIndexArgs {
    pub client_args: ClientArgs,
    pub index_id: IndexId,
}

#[derive(Debug, Eq, PartialEq)]
pub struct IngestDocsArgs {
    pub client_args: ClientArgs,
    pub index_id: IndexId,
    pub input_path_opt: Option<PathBuf>,
    pub batch_size_limit_opt: Option<ByteSize>,
    pub commit_type: CommitType,
    pub detailed_response: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct SearchIndexArgs {
    pub client_args: ClientArgs,
    pub index_id: IndexId,
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
    pub client_args: ClientArgs,
    pub index_id: IndexId,
    pub dry_run: bool,
    pub assume_yes: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ListIndexesArgs {
    pub client_args: ClientArgs,
}

#[derive(Debug, Eq, PartialEq)]
pub enum IndexCliCommand {
    Clear(ClearIndexArgs),
    Create(CreateIndexArgs),
    Update(UpdateIndexArgs),
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

    pub fn parse_cli_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches) = matches
            .remove_subcommand()
            .context("failed to parse index subcommand")?;
        match subcommand.as_str() {
            "clear" => Self::parse_clear_args(submatches),
            "create" => Self::parse_create_args(submatches),
            "delete" => Self::parse_delete_args(submatches),
            "describe" => Self::parse_describe_args(submatches),
            "ingest" => Self::parse_ingest_args(submatches),
            "list" => Self::parse_list_args(submatches),
            "search" => Self::parse_search_args(submatches),
            "update" => Self::parse_update_args(submatches),
            _ => bail!("unknown index subcommand `{subcommand}`"),
        }
    }

    fn parse_clear_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let client_args = ClientArgs::parse(&mut matches)?;
        let index_id = matches
            .remove_one::<String>("index")
            .expect("`index` should be a required arg.");
        let assume_yes = matches.get_flag("yes");
        Ok(Self::Clear(ClearIndexArgs {
            client_args,
            index_id,
            assume_yes,
        }))
    }

    fn parse_create_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let client_args = ClientArgs::parse(&mut matches)?;
        let index_config_uri = matches
            .remove_one::<String>("index-config")
            .map(|uri| Uri::from_str(&uri))
            .expect("`index-config` should be a required arg.")?;
        let overwrite = matches.get_flag("overwrite");
        let assume_yes = matches.get_flag("yes");

        Ok(Self::Create(CreateIndexArgs {
            client_args,
            index_config_uri,
            overwrite,
            assume_yes,
        }))
    }

    fn parse_update_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let client_args = ClientArgs::parse(&mut matches)?;
        let index_id = matches
            .remove_one::<String>("index")
            .expect("`index` should be a required arg.");
        let index_config_uri = matches
            .remove_one::<String>("index-config")
            .map(|uri| Uri::from_str(&uri))
            .expect("`index-config` should be a required arg.")?;
        let assume_yes = matches.get_flag("yes");

        Ok(Self::Update(UpdateIndexArgs {
            index_id,
            client_args,
            index_config_uri,
            assume_yes,
        }))
    }

    fn parse_describe_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let client_args = ClientArgs::parse(&mut matches)?;
        let index_id = matches
            .remove_one::<String>("index")
            .expect("`index` should be a required arg.");
        Ok(Self::Describe(DescribeIndexArgs {
            client_args,
            index_id,
        }))
    }

    fn parse_list_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let client_args = ClientArgs::parse(&mut matches)?;
        Ok(Self::List(ListIndexesArgs { client_args }))
    }

    fn parse_ingest_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let client_args = ClientArgs::parse_for_ingest(&mut matches)?;
        let index_id = matches
            .remove_one::<String>("index")
            .expect("`index` should be a required arg.");
        let input_path_opt = if let Some(input_path) = matches.remove_one::<String>("input-path") {
            Uri::from_str(&input_path)?
                .filepath()
                .map(|path| path.to_path_buf())
        } else {
            None
        };
        let detailed_response: bool = matches.get_flag("detailed-response");
        let batch_size_limit_opt = matches
            .remove_one::<String>("batch-size-limit")
            .map(|limit| limit.parse::<ByteSize>())
            .transpose()
            .map_err(|error| anyhow!(error))?;
        let commit_type = match (matches.get_flag("wait"), matches.get_flag("force")) {
            (false, false) => CommitType::Auto,
            (false, true) => CommitType::Force,
            (true, false) => CommitType::WaitFor,
            (true, true) => bail!("`--wait` and `--force` are mutually exclusive options"),
        };

        if commit_type == CommitType::Auto && client_args.commit_timeout.is_some() {
            bail!("`--commit-timeout` can only be used with --wait or --force options");
        }

        Ok(Self::Ingest(IngestDocsArgs {
            client_args,
            index_id,
            input_path_opt,
            batch_size_limit_opt,
            commit_type,
            detailed_response,
        }))
    }

    fn parse_search_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let index_id = matches
            .remove_one::<String>("index")
            .expect("`index` should be a required arg.");
        let query = matches
            .remove_one::<String>("query")
            .context("`query` should be a required arg")?;
        let aggregation = matches.remove_one::<String>("aggregation");

        let max_hits = matches
            .remove_one::<String>("max-hits")
            .expect("`max-hits` should have a default value.")
            .parse()?;
        let start_offset = matches
            .remove_one::<String>("start-offset")
            .expect("`start-offset` should have a default value.")
            .parse()?;
        let search_fields = matches
            .remove_many::<String>("search-fields")
            .map(|values| values.collect());
        let snippet_fields = matches
            .remove_many::<String>("snippet-fields")
            .map(|values| values.collect());
        let sort_by_score = matches.get_flag("sort-by-score");
        let start_timestamp = matches
            .remove_one::<String>("start-timestamp")
            .map(|ts| ts.parse())
            .transpose()?;
        let end_timestamp = matches
            .remove_one::<String>("end-timestamp")
            .map(|ts| ts.parse())
            .transpose()?;
        let client_args = ClientArgs::parse(&mut matches)?;
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
            client_args,
            sort_by_score,
        }))
    }

    fn parse_delete_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let client_args = ClientArgs::parse(&mut matches)?;
        let index_id = matches
            .remove_one::<String>("index")
            .expect("`index` should be a required arg.");
        let dry_run = matches.get_flag("dry-run");
        let assume_yes = matches.get_flag("yes");
        Ok(Self::Delete(DeleteIndexArgs {
            index_id,
            dry_run,
            client_args,
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
            Self::Update(args) => update_index_cli(args).await,
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
    let qw_client = args.client_args.client();
    qw_client.indexes().clear(&args.index_id).await?;
    println!("{} Index successfully cleared.", "✔".color(GREEN_COLOR),);
    Ok(())
}

pub async fn create_index_cli(args: CreateIndexArgs) -> anyhow::Result<()> {
    debug!(args=?args, "create-index");
    println!("❯ Creating index...");
    let storage_resolver = StorageResolver::unconfigured();
    let file_content = load_file(&storage_resolver, &args.index_config_uri).await?;
    let index_config_str: String = std::str::from_utf8(&file_content)
        .with_context(|| format!("Invalid utf8: `{}`", args.index_config_uri))?
        .to_string();
    let config_format = ConfigFormat::sniff_from_uri(&args.index_config_uri)?;
    let qw_client = args.client_args.client();
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
    qw_client
        .indexes()
        .create(&index_config_str, config_format, args.overwrite)
        .await?;
    println!("{} Index successfully created.", "✔".color(GREEN_COLOR));
    Ok(())
}

pub async fn update_index_cli(args: UpdateIndexArgs) -> anyhow::Result<()> {
    debug!(args=?args, "update-index");
    println!("❯ Updating index...");
    let storage_resolver = StorageResolver::unconfigured();
    let file_content = load_file(&storage_resolver, &args.index_config_uri).await?;
    let index_config_str = std::str::from_utf8(&file_content)
        .with_context(|| {
            format!(
                "index config file `{}` contains some invalid UTF-8 characters",
                args.index_config_uri
            )
        })?
        .to_string();
    let config_format = ConfigFormat::sniff_from_uri(&args.index_config_uri)?;
    let qw_client = args.client_args.client();
    if !args.assume_yes {
        let prompt = "This operation will update the index configuration. Do you want to proceed?";
        if !prompt_confirmation(prompt, false) {
            return Ok(());
        }
    }
    qw_client
        .indexes()
        .update(&args.index_id, &index_config_str, config_format)
        .await?;
    println!("{} Index successfully updated.", "✔".color(GREEN_COLOR));
    Ok(())
}

pub async fn list_index_cli(args: ListIndexesArgs) -> anyhow::Result<()> {
    debug!(args=?args, "list-index");
    let qw_client = args.client_args.client();
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
    index_id: IndexId,
    #[tabled(rename = "Index URI")]
    index_uri: Uri,
}

pub async fn describe_index_cli(args: DescribeIndexArgs) -> anyhow::Result<()> {
    debug!(args=?args, "describe-index");
    let qw_client = args.client_args.client();
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
    pub index_id: IndexId,
    pub index_uri: Uri,
    pub num_published_splits: usize,
    pub size_published_splits: ByteSize,
    pub num_published_docs: u64,
    pub size_published_docs_uncompressed: ByteSize,
    pub timestamp_field_name: Option<String>,
    pub timestamp_range: Option<(i64, i64)>,
    pub num_docs_descriptive: Option<DescriptiveStats>,
    pub num_bytes_descriptive: Option<DescriptiveStats>,
}

impl Tabled for IndexStats {
    const LENGTH: usize = 9;

    fn fields(&self) -> Vec<Cow<'_, str>> {
        let num_published_docs = format!(
            "{} ({})",
            format_to_si_scale(self.num_published_docs),
            separate_thousands(self.num_published_docs)
        );

        [
            self.index_id.to_string(),
            self.index_uri.to_string(),
            num_published_docs,
            self.size_published_docs_uncompressed.to_string(),
            separate_thousands(self.num_published_splits),
            self.size_published_splits.to_string(),
            display_option_in_table(&self.timestamp_field_name),
            display_timestamp(&self.timestamp_range.map(|(start, _end)| start)),
            display_timestamp(&self.timestamp_range.map(|(_start, end)| end)),
        ]
        .into_iter()
        .map(|field| field.into())
        .collect()
    }

    fn headers() -> Vec<Cow<'static, str>> {
        [
            "Index ID",
            "Index URI",
            "Number of published documents",
            "Size of published documents (uncompressed)",
            "Number of published splits",
            "Size of published splits",
            "Timestamp field",
            "Timestamp range start",
            "Timestamp range end",
        ]
        .into_iter()
        .map(|header| header.into())
        .collect()
    }
}

fn format_to_si_scale(num: impl numfmt::Numeric) -> String {
    let mut si_scale_formatter = Formatter::new().scales(Scales::metric());
    si_scale_formatter.fmt2(num).to_string()
}

fn separate_thousands(num: impl numfmt::Numeric) -> String {
    let mut thousands_separator_formatter = Formatter::new()
        .separator(',')
        // NOTE: .separator(sep) only panics if sep.len_utf8() != 1
        .expect("`,` separator should be valid")
        .precision(numfmt::Precision::Significance(3));

    thousands_separator_formatter.fmt2(num).to_string()
}

fn display_option_in_table(opt: &Option<impl Display>) -> String {
    match opt {
        Some(opt_val) => format!("\"{opt_val}\""),
        None => "Field does not exist for the index.".to_string(),
    }
}

fn display_timestamp(timestamp: &Option<i64>) -> String {
    match timestamp {
        Some(timestamp) => {
            let datetime = chrono::DateTime::from_timestamp_millis(*timestamp * 1000)
                .map(|datetime| datetime.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| "Invalid timestamp!".to_string());
            format!("{} (Timestamp: {})", datetime, timestamp)
        }
        _ => "Timestamp does not exist for the index.".to_string(),
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
            .map(|split| split.split_metadata.footer_offsets.end)
            .sorted()
            .collect_vec();
        let total_num_bytes = splits_bytes.iter().sum::<u64>();
        let total_uncompressed_num_bytes = published_splits
            .iter()
            .map(|split| split.split_metadata.uncompressed_docs_size_in_bytes)
            .sum::<u64>();

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
            size_published_splits: ByteSize(total_num_bytes),
            num_published_docs: total_num_docs,
            size_published_docs_uncompressed: ByteSize(total_uncompressed_num_bytes),
            timestamp_field_name: index_config.doc_mapping.timestamp_field,
            timestamp_range,
            num_docs_descriptive,
            num_bytes_descriptive,
        })
    }

    pub fn display_as_table(&self) -> String {
        let mut tables = Vec::new();
        let index_stats_table = create_table(self, "General Information", true);
        tables.push(index_stats_table);

        if let Some(docs_stats) = &self.num_docs_descriptive {
            let doc_stats_table = docs_stats.into_table("Published documents count stats");
            tables.push(doc_stats_table);
        }

        if let Some(size_stats) = &self.num_bytes_descriptive {
            let size_stats_in_mb = size_stats / 1_000_000.0;
            let size_stats_table = size_stats_in_mb.into_table("Published splits size stats (MB)");
            tables.push(size_stats_table);
        }

        let table = Table::builder(tables.into_iter().map(|table| table.to_string()))
            .build()
            .with(Modify::new(Segment::all()).with(Alignment::center_vertical()))
            .with(Disable::row(FirstRow))
            .with(Style::empty())
            .to_string();

        table
    }
}

fn create_table(table: impl Tabled, header: &str, is_vertical: bool) -> Table {
    let mut table = Table::new(vec![table]);

    // Make the field names GREEN :D
    table.with(Modify::new(Rows::first()).with(Format::content(|column| {
        column.color(GREEN_COLOR).to_string()
    })));

    if is_vertical {
        table.with(Rotate::Left).with(Rotate::Bottom);
    }

    table
        .with(Panel::header(header))
        // Makes the table header bright green and bold.
        .with(Modify::new(Rows::first()).with(Format::content(|header| {
            header.bright_green().bold().to_string()
        })))
        .with(
            Modify::new(Segment::all())
                .with(Alignment::left())
                .with(Alignment::top()),
        )
        .with(Footer::new("\n"))
        .with(Style::psql());

    table
}

#[derive(Debug, Clone, Copy)]
pub struct DescriptiveStats {
    summary_stats: SummaryStats,
    quantiles: Quantiles,
}

impl DescriptiveStats {
    pub fn into_table(self, header: &str) -> Table {
        let summary_stats_table = create_table(self.summary_stats, header, true);
        let quantiles_table = create_table(self.quantiles, "Quantiles", false);
        let mut table =
            Table::builder([summary_stats_table.to_string(), quantiles_table.to_string()]).build();

        table
            .with(Style::empty())
            .with(Disable::row(FirstRow))
            // We separate tables with a newline already, this is to separate quantile part of the
            // table further away from the next table.
            .with(Footer::new("\n"));

        table
    }
}

impl Div<f32> for &DescriptiveStats {
    type Output = DescriptiveStats;

    fn div(self, rhs: f32) -> Self::Output {
        DescriptiveStats {
            summary_stats: self.summary_stats / rhs,
            quantiles: self.quantiles / rhs,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SummaryStats {
    mean_val: f32,
    std_val: f32,
    min_val: u64,
    max_val: u64,
}

impl Div<f32> for SummaryStats {
    type Output = Self;

    fn div(self, rhs: f32) -> Self::Output {
        Self {
            mean_val: self.mean_val / rhs,
            std_val: self.std_val / rhs,
            min_val: self.min_val / rhs as u64,
            max_val: self.max_val / rhs as u64,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Quantiles {
    q1: f32,
    q25: f32,
    q50: f32,
    q75: f32,
    q99: f32,
}

impl Div<f32> for Quantiles {
    type Output = Self;

    fn div(self, rhs: f32) -> Self::Output {
        Self {
            q1: self.q1 / rhs,
            q25: self.q25 / rhs,
            q50: self.q50 / rhs,
            q75: self.q75 / rhs,
            q99: self.q99 / rhs,
        }
    }
}

impl DescriptiveStats {
    pub fn maybe_new(values: &[u64]) -> Option<DescriptiveStats> {
        if values.is_empty() {
            return None;
        }

        Some(DescriptiveStats {
            summary_stats: SummaryStats {
                mean_val: mean(values),
                std_val: std_deviation(values),
                min_val: *values.iter().min().expect("Values should not be empty."),
                max_val: *values.iter().max().expect("Values should not be empty."),
            },
            quantiles: Quantiles {
                q1: percentile(values, 1),
                q25: percentile(values, 25),
                q50: percentile(values, 50),
                q75: percentile(values, 75),
                q99: percentile(values, 99),
            },
        })
    }
}

impl Tabled for SummaryStats {
    const LENGTH: usize = 4;

    fn fields(&self) -> Vec<Cow<'_, str>> {
        [
            separate_thousands(self.mean_val),
            separate_thousands(self.min_val),
            separate_thousands(self.max_val),
            separate_thousands(self.std_val),
        ]
        .into_iter()
        .map(|field| field.into())
        .collect()
    }

    fn headers() -> Vec<Cow<'static, str>> {
        [
            "Mean".to_string(),
            "Min".to_string(),
            "Max".to_string(),
            "Standard deviation".to_string(),
        ]
        .into_iter()
        .map(|header| header.into())
        .collect()
    }
}

impl Tabled for Quantiles {
    const LENGTH: usize = 5;

    fn fields(&self) -> Vec<Cow<'_, str>> {
        [
            separate_thousands(self.q1),
            separate_thousands(self.q25),
            separate_thousands(self.q50),
            separate_thousands(self.q75),
            separate_thousands(self.q99),
        ]
        .into_iter()
        .map(|field| field.into())
        .collect()
    }

    fn headers() -> Vec<Cow<'static, str>> {
        [
            "1%".to_string(),
            "25%".to_string(),
            "50%".to_string(),
            "75%".to_string(),
            "99%".to_string(),
        ]
        .into_iter()
        .map(|header| header.into())
        .collect()
    }
}

pub async fn ingest_docs_cli(args: IngestDocsArgs) -> anyhow::Result<()> {
    debug!(args=?args, "ingest-docs");
    let mut rate_estimator = SmaRateEstimator::new(
        NonZeroUsize::new(8).unwrap(),
        Duration::from_millis(250),
        Duration::from_secs(1),
    );
    if let Some(input_path) = &args.input_path_opt {
        println!("❯ Ingesting documents from {}.", input_path.display());
    } else {
        println!("❯ Ingesting documents from stdin.");
    }
    let progress_bar = match &args.input_path_opt {
        Some(filepath) => {
            let file_len = std::fs::metadata(filepath).context("file not found")?.len();
            ProgressBar::new(file_len)
        }
        None => ProgressBar::new_spinner(),
    };
    progress_bar.enable_steady_tick(Duration::from_millis(100));
    progress_bar.set_style(progress_bar_style());
    progress_bar.set_message("0MiB/s");
    // It is not used by the rate estimator anyway.
    let useless_start_time = Instant::now();
    let mut update_progress_bar = |ingest_event: IngestEvent| {
        match ingest_event {
            IngestEvent::IngestedDocBatch(num_bytes) => {
                rate_estimator.update(useless_start_time, Instant::now(), num_bytes as u64);
                progress_bar.inc(num_bytes as u64)
            }
            IngestEvent::Sleep => {} // To
        };
        let throughput = rate_estimator.work() as f64 / (1024 * 1024) as f64;
        progress_bar.set_message(format!("{throughput:.1} MiB/s"));
    };

    let mut qw_client_builder = args.client_args.client_builder();
    if args.detailed_response {
        qw_client_builder = qw_client_builder.detailed_response(args.detailed_response);
    }
    let qw_client = qw_client_builder.build();
    let ingest_source = match args.input_path_opt {
        Some(filepath) => IngestSource::File(filepath),
        None => IngestSource::Stdin,
    };
    let batch_size_limit_opt = args
        .batch_size_limit_opt
        .map(|batch_size_limit| batch_size_limit.as_u64() as usize);
    let response = qw_client
        .ingest(
            &args.index_id,
            ingest_source,
            batch_size_limit_opt,
            Some(&mut update_progress_bar),
            args.commit_type,
        )
        .await?;
    progress_bar.finish();
    println!(
        "{} Ingested {} document(s) successfully.",
        "✔".color(GREEN_COLOR),
        response
            .num_ingested_docs
            // TODO(#5604) remove unwrap
            .unwrap_or(response.num_docs_for_processing),
    );
    if let Some(rejected) = response.num_rejected_docs {
        if rejected > 0 {
            println!(
                "{} Rejected {} document(s).",
                "✖".color(RED_COLOR),
                rejected
            );
        }
    }
    if let Some(parse_failures) = response.parse_failures {
        if !parse_failures.is_empty() {
            println!("Detailed parse failures:");
        }
        for (idx, failure) in parse_failures.iter().enumerate() {
            let reason_value = serde_json::to_value(failure.reason).unwrap();
            println!();
            println!("┌ error {}", idx + 1);
            println!("├ reason: {}", reason_value.as_str().unwrap());
            println!("├ message: {}", failure.message);
            println!("└ document: {}", failure.document);
        }
    }
    if response.num_too_many_requests > 0 {
        println!("Retried request counts:");
        println!(
            "  429 (too many requests)   = {}",
            response.num_too_many_requests
        );
    }
    Ok(())
}

fn progress_bar_style() -> ProgressStyle {
    ProgressStyle::with_template(
        "{spinner:.blue} [{elapsed_precise}] {bytes}/{total_bytes} ({msg})",
    )
    .expect("Progress style should always be valid.")
    .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"])
}

pub async fn search_index(args: SearchIndexArgs) -> anyhow::Result<SearchResponseRestClient> {
    let aggs: Option<serde_json::Value> = args
        .aggregation
        .map(|aggs_string| {
            serde_json::from_str(&aggs_string).context("failed to deserialize aggregations")
        })
        .transpose()?;
    let sort_by = args
        .sort_by_score
        .then_some(SortBy {
            sort_fields: vec![SortField {
                field_name: "_score".to_string(),
                sort_order: SortOrder::Desc as i32,
                sort_datetime_format: None,
            }],
        })
        .unwrap_or_default();
    let search_request = SearchRequestQueryString {
        query: args.query,
        aggs,
        search_fields: args.search_fields.clone(),
        snippet_fields: args.snippet_fields.clone(),
        start_timestamp: args.start_timestamp,
        end_timestamp: args.end_timestamp,
        max_hits: args.max_hits as u64,
        start_offset: args.start_offset as u64,
        sort_by,
        count_all: CountHits::CountAll,
        ..Default::default()
    };
    let qw_client = args.client_args.client();
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
    if !args.dry_run && !args.assume_yes {
        let prompt = "This operation will delete the index. Do you want to proceed?".to_string();
        if !prompt_confirmation(&prompt, false) {
            return Ok(());
        }
    }

    println!("❯ Deleting index...");
    let qw_client = args.client_args.client();
    let affected_files = qw_client
        .indexes()
        .delete(&args.index_id, args.dry_run)
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
        for split_info in affected_files {
            println!(" - {}", split_info.file_name.display());
        }
        return Ok(());
    }
    println!("{} Index successfully deleted.", "✔".color(GREEN_COLOR));
    Ok(())
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
        let mut split_metadata_1 =
            split_metadata_for_test(&split_id_1, 100_000, 1111..=2222, 15_000_000);
        split_metadata_1.uncompressed_docs_size_in_bytes = 19_000_000;
        let mut split_metadata_2 =
            split_metadata_for_test(&split_id_2, 100_000, 1000..=3000, 30_000_000);
        split_metadata_2.uncompressed_docs_size_in_bytes = 36_000_000;

        let split_data_1 = Split {
            split_metadata: split_metadata_1,
            split_state: SplitState::Published,
            update_timestamp: 0,
            publish_timestamp: Some(10),
        };
        let split_data_2 = Split {
            split_metadata: split_metadata_2,
            split_state: SplitState::MarkedForDeletion,
            update_timestamp: 0,
            publish_timestamp: Some(10),
        };

        let index_stats =
            IndexStats::from_metadata(index_metadata, vec![split_data_1, split_data_2])?;

        assert_eq!(index_stats.index_id, index_id);
        assert_eq!(index_stats.index_uri.as_str(), index_uri);
        assert_eq!(index_stats.num_published_splits, 1);
        assert_eq!(index_stats.size_published_splits, ByteSize::mb(15));
        assert_eq!(index_stats.num_published_docs, 100_000);
        assert_eq!(
            index_stats.size_published_docs_uncompressed,
            ByteSize::mb(19)
        );
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
            split_state: SplitState::Published,
            update_timestamp: 10,
            publish_timestamp: Some(10),
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

        assert!(num_docs_descriptive.is_some());
        assert!(num_bytes_descriptive.is_some());

        let num_docs_descriptive = num_docs_descriptive.unwrap();
        let num_bytes_descriptive = num_bytes_descriptive.unwrap();

        assert_eq!(num_docs_descriptive.quantiles.q1, 40900.0);
        assert_eq!(num_docs_descriptive.quantiles.q25, 62500.0);
        assert_eq!(num_docs_descriptive.quantiles.q50, 80000.0);
        assert_eq!(num_docs_descriptive.quantiles.q75, 97500.0);
        assert_eq!(num_docs_descriptive.quantiles.q99, 119100.0);

        assert_eq!(num_bytes_descriptive.quantiles.q1, 55150000.0);
        assert_eq!(num_bytes_descriptive.quantiles.q25, 58750000.0);
        assert_eq!(num_bytes_descriptive.quantiles.q50, 87500000.0);
        assert_eq!(num_bytes_descriptive.quantiles.q75, 122500000.0);
        assert_eq!(num_bytes_descriptive.quantiles.q99, 144100000.0);

        let descriptive_stats_none = DescriptiveStats::maybe_new(&[]);
        assert!(descriptive_stats_none.is_none());

        Ok(())
    }
}
