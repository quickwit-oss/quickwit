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

use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{bail, Context};
use clap::{arg, ArgMatches, Command};
use humansize::{format_size, DECIMAL};
use itertools::Itertools;
use quickwit_common::uri::Uri;
use quickwit_directories::{
    get_hotcache_from_split, read_split_footer, BundleDirectory, HotDirectory,
};
use quickwit_doc_mapper::tag_pruning::TagFilterAst;
use quickwit_metastore::{quickwit_metastore_uri_resolver, ListSplitsQuery, Split, SplitState};
use quickwit_storage::{quickwit_storage_uri_resolver, BundleStorage, Storage};
use tabled::{Table, Tabled};
use time::{format_description, Date, OffsetDateTime, PrimitiveDateTime};
use tracing::debug;

use crate::{load_quickwit_config, make_table};

pub fn build_split_command<'a>() -> Command<'a> {
    Command::new("split")
        .about("Performs operations on splits (list, describe, mark for deletion, extract).")
        .subcommand(
            Command::new("list")
                .about("Lists the splits of an index.")
                .alias("ls")
                .args(&[
                    arg!(--index <INDEX> "Target index ID")
                        .display_order(1)
                        .required(true),
                    arg!(--states <SPLIT_STATES> "Selects the splits whose states are included in this comma-separated list of states. Possible values are `staged`, `published`, and `marked`.")
                        .display_order(2)
                        .required(false)
                        .use_value_delimiter(true),
                    arg!(--"create-date" <CREATE_DATE> "Selects the splits whose creation dates are before this date.")
                        .display_order(3)
                        .required(false),
                    arg!(--"start-date" <START_DATE> "Selects the splits that contain documents after this date (time-series indexes only).")
                        .display_order(4)
                        .required(false),
                    arg!(--"end-date" <END_DATE> "Selects the splits that contain documents before this date (time-series indexes only).")
                        .display_order(5)
                        .required(false),
                    arg!(--tags <TAGS> "Selects the splits whose tags are all included in this comma-separated list of tags.")
                        .display_order(6)
                        .required(false)
                        .use_value_delimiter(true),
                    arg!(--"output-format" <OUTPUT_FORMAT> "Output format. Possible values are `table`, `json`, and `prettyjson`.")
                        .alias("format")
                        .display_order(7)
                        .required(false)
                ])
            )
        .subcommand(
            Command::new("extract")
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
            Command::new("describe")
                .about("Displays metadata about a split.")
                .alias("desc")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index")
                        .display_order(1),
                    arg!(--split <SPLIT> "ID of the target split")
                        .display_order(2),
                    arg!(--verbose "Displays additional metadata about the hotcache."),
                ])
            )
        .subcommand(
            Command::new("mark-for-deletion")
                .about("Marks one or multiple splits of an index for deletion.")
                .alias("mark")
                .args(&[
                    arg!(--index <INDEX_ID> "Target index ID")
                        .display_order(1)
                        .required(true),
                    arg!(--splits <SPLIT_IDS> "Comma-separated list of split IDs")
                        .display_order(2)
                        .required(true)
                        .use_value_delimiter(true),
                ])
            )
        .arg_required_else_help(true)
}

#[derive(Debug, Eq, PartialEq)]
enum OutputFormat {
    Table, // Default
    Json,
    PrettyJson,
}

impl FromStr for OutputFormat {
    type Err = anyhow::Error;

    fn from_str(output_format_str: &str) -> anyhow::Result<Self> {
        match output_format_str {
            "table" => Ok(OutputFormat::Table),
            "json" => Ok(OutputFormat::Json),
            "prettyjson" => Ok(OutputFormat::PrettyJson),
            _ => bail!(
                "Failed to parse output format `{output_format_str}`. Supported formats are: \
                 `table`, `json`, and `prettyjson`."
            ),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ListSplitArgs {
    pub config_uri: Uri,
    pub index_id: String,
    pub split_states: Option<Vec<SplitState>>,
    pub create_date: Option<OffsetDateTime>,
    pub start_date: Option<OffsetDateTime>,
    pub end_date: Option<OffsetDateTime>,
    pub tags: Option<TagFilterAst>,
    output_format: OutputFormat,
}

#[derive(Debug, Eq, PartialEq)]
pub struct MarkForDeletionArgs {
    pub config_uri: Uri,
    pub index_id: String,
    pub split_ids: Vec<String>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct DescribeSplitArgs {
    pub config_uri: Uri,
    pub index_id: String,
    pub split_id: String,
    pub verbose: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ExtractSplitArgs {
    pub config_uri: Uri,
    pub index_id: String,
    pub split_id: String,
    pub target_dir: PathBuf,
}

#[derive(Debug, PartialEq)]
pub enum SplitCliCommand {
    List(ListSplitArgs),
    MarkForDeletion(MarkForDeletionArgs),
    Describe(DescribeSplitArgs),
    Extract(ExtractSplitArgs),
}

impl SplitCliCommand {
    pub fn parse_cli_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches) = matches
            .subcommand()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse sub-matches."))?;
        match subcommand {
            "describe" => Self::parse_describe_args(submatches),
            "extract" => Self::parse_extract_split_args(submatches),
            "list" => Self::parse_list_args(submatches),
            "mark-for-deletion" => Self::parse_mark_for_deletion_args(submatches),
            _ => bail!("Subcommand `{}` is not implemented.", subcommand),
        }
    }

    fn parse_list_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let config_uri = matches
            .value_of("config")
            .map(Uri::from_str)
            .expect("`config` is a required arg.")?;
        let index_id = matches
            .value_of("index")
            .map(String::from)
            .expect("`index` is a required arg.");
        let split_states = matches
            .values_of("states")
            .map(|values| {
                values
                    .into_iter()
                    .map(parse_split_state)
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?;
        let create_date = matches
            .value_of("create-date")
            .map(|arg| parse_date(arg, "create"))
            .transpose()?;
        let start_date = matches
            .value_of("start-date")
            .map(|arg| parse_date(arg, "start"))
            .transpose()?;
        let end_date = matches
            .value_of("end-date")
            .map(|arg| parse_date(arg, "end"))
            .transpose()?;
        let tags = matches.values_of("tags").map(|values| {
            TagFilterAst::And(
                values
                    .into_iter()
                    .map(|value| TagFilterAst::Tag {
                        is_present: true,
                        tag: value.to_string(),
                    })
                    .collect(),
            )
        });
        let output_format = matches
            .value_of("output-format")
            .map(OutputFormat::from_str)
            .transpose()?
            .unwrap_or(OutputFormat::Table);

        Ok(Self::List(ListSplitArgs {
            config_uri,
            index_id,
            split_states,
            start_date,
            end_date,
            create_date,
            tags,
            output_format,
        }))
    }

    fn parse_mark_for_deletion_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let config_uri = matches
            .value_of("config")
            .map(Uri::from_str)
            .expect("`config` is a required arg.")?;
        let index_id = matches
            .value_of("index")
            .map(String::from)
            .expect("`index` is a required arg.");
        let split_ids = matches
            .values_of("splits")
            .expect("`splits` is a required arg.")
            .into_iter()
            .map(String::from)
            .collect();

        Ok(Self::MarkForDeletion(MarkForDeletionArgs {
            config_uri,
            index_id,
            split_ids,
        }))
    }

    fn parse_describe_args(matches: &ArgMatches) -> anyhow::Result<Self> {
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
        let verbose = matches.is_present("verbose");

        Ok(Self::Describe(DescribeSplitArgs {
            config_uri,
            index_id,
            split_id,
            verbose,
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
        Ok(Self::Extract(ExtractSplitArgs {
            config_uri,
            index_id,
            split_id,
            target_dir,
        }))
    }

    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            Self::List(args) => list_split_cli(args).await,
            Self::MarkForDeletion(args) => mark_splits_for_deletion_cli(args).await,
            Self::Describe(args) => describe_split_cli(args).await,
            Self::Extract(args) => extract_split_cli(args).await,
        }
    }
}

async fn list_split_cli(args: ListSplitArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "list-split");

    let quickwit_config = load_quickwit_config(&args.config_uri).await?;
    let metastore_uri_resolver = quickwit_metastore_uri_resolver();
    let metastore = metastore_uri_resolver
        .resolve(&quickwit_config.metastore_uri)
        .await?;

    let mut query = ListSplitsQuery::for_index(&args.index_id)
        .with_split_states(args.split_states.unwrap_or_default());

    if let Some(start_date) = args.start_date {
        query = query.with_time_range_start_gte(start_date.unix_timestamp());
    }
    if let Some(end_date) = args.end_date {
        query = query.with_time_range_end_lte(end_date.unix_timestamp());
    }
    if let Some(create_date) = args.create_date {
        query = query.with_create_timestamp_lte(create_date.unix_timestamp());
    }
    if let Some(tags) = args.tags {
        query = query.with_tags_filter(tags);
    }

    let splits = metastore.list_splits(query).await?;

    let output = match args.output_format {
        OutputFormat::Json => serde_json::to_string(&splits)?,
        OutputFormat::PrettyJson => serde_json::to_string_pretty(&splits)?,
        OutputFormat::Table => make_split_table(&splits, "Splits").to_string(),
    };
    println!("{output}");
    Ok(())
}

async fn mark_splits_for_deletion_cli(args: MarkForDeletionArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "mark-splits-for-deletion");

    let quickwit_config = load_quickwit_config(&args.config_uri).await?;
    let metastore_uri_resolver = quickwit_metastore_uri_resolver();
    let metastore = metastore_uri_resolver
        .resolve(&quickwit_config.metastore_uri)
        .await?;
    let split_ids: Vec<&str> = args
        .split_ids
        .iter()
        .map(|split_id| split_id.as_ref())
        .collect();
    metastore
        .mark_splits_for_deletion(&args.index_id, &split_ids)
        .await?;
    Ok(())
}

#[derive(Tabled)]
struct FileRow {
    #[tabled(rename = "File Name")]
    file_name: String,
    #[tabled(rename = "Size")]
    size: String,
}

async fn describe_split_cli(args: DescribeSplitArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "describe-split");

    let quickwit_config = load_quickwit_config(&args.config_uri).await?;
    let storage_uri_resolver = quickwit_storage_uri_resolver();
    let metastore_uri_resolver = quickwit_metastore_uri_resolver();
    let metastore = metastore_uri_resolver
        .resolve(&quickwit_config.metastore_uri)
        .await?;
    let index_metadata = metastore.index_metadata(&args.index_id).await?;
    let index_storage = storage_uri_resolver.resolve(index_metadata.index_uri())?;

    let split_metadata = metastore
        .list_all_splits(&args.index_id)
        .await?
        .into_iter()
        .find(|split| split.split_id() == args.split_id)
        .with_context(|| {
            format!(
                "Could not find split metadata in metastore {}",
                args.split_id
            )
        })?;

    println!("{}", make_split_table(&[split_metadata], "Split"));

    let split_file = PathBuf::from(format!("{}.split", args.split_id));
    let (split_footer, _) = read_split_footer(index_storage, &split_file).await?;
    let stats = BundleDirectory::get_stats_split(split_footer.clone())?;
    let hotcache_bytes = get_hotcache_from_split(split_footer)?;

    let mut file_rows = Vec::new();

    for (path, size) in stats {
        file_rows.push(FileRow {
            file_name: path.to_str().unwrap().to_string(),
            size: format_size(size, DECIMAL),
        });
    }
    println!(
        "{}",
        make_table("Files in Split", file_rows.into_iter(), false)
    );
    if args.verbose {
        let mut hotcache_files = Vec::new();
        let hotcache_stats = HotDirectory::get_stats_per_file(hotcache_bytes)?;
        for (path, size) in hotcache_stats {
            hotcache_files.push(FileRow {
                file_name: path.to_str().unwrap().to_string(),
                size: format_size(size, DECIMAL),
            });
        }
        let hotcache_table = make_table("Files in Hotcache", hotcache_files.into_iter(), false);
        println!("{hotcache_table}");
    }
    Ok(())
}

async fn extract_split_cli(args: ExtractSplitArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "extract-split");

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
        println!("Copying {:?}", out_path);
        bundle_storage.copy_to_file(path, &out_path).await?;
    }

    Ok(())
}

fn make_split_table(splits: &[Split], title: &str) -> Table {
    let rows = splits
        .iter()
        .map(|split| {
            let time_range = if let Some(time_range) = &split.split_metadata.time_range {
                format!("[{:?}]", time_range)
            } else {
                "[*]".to_string()
            };
            let created_at =
                OffsetDateTime::from_unix_timestamp(split.split_metadata.create_timestamp)
                    .expect("Failed to create `OffsetDateTime` from split create timestamp.");
            let updated_at = OffsetDateTime::from_unix_timestamp(split.update_timestamp)
                .expect("Failed to create `OffsetDateTime` from split update timestamp.");

            SplitRow {
                split_id: split.split_metadata.split_id.clone(),
                split_state: split.split_state,
                num_docs: split.split_metadata.num_docs,
                size_mega_bytes: split.split_metadata.uncompressed_docs_size_in_bytes / 1_000_000,
                created_at,
                updated_at,
                time_range,
            }
        })
        .sorted_by(|left, right| left.created_at.cmp(&right.created_at));
    make_table(title, rows, false)
}

fn parse_date(date_arg: &str, option_name: &str) -> anyhow::Result<OffsetDateTime> {
    let description = format_description::parse("[year]-[month]-[day]")?;
    if let Ok(date) = Date::parse(date_arg, &description) {
        return Ok(date.with_hms(0, 0, 0)?.assume_utc());
    }

    for datetime_format in [
        "[year]-[month]-[day] [hour]:[minute]",
        "[year]-[month]-[day] [hour]:[minute]:[second]",
        "[year]-[month]-[day]T[hour]:[minute]",
        "[year]-[month]-[day]T[hour]:[minute]:[second]",
    ] {
        let description = format_description::parse(datetime_format)?;
        if let Ok(datetime) = PrimitiveDateTime::parse(date_arg, &description) {
            return Ok(datetime.assume_utc());
        }
    }
    bail!(
        "Failed to parse --{}-date option parameter `{}`. Supported format is `YYYY-MM-DD[ \
         HH:DD[:SS]]`.",
        option_name,
        date_arg
    );
}

fn parse_split_state(split_state_arg: &str) -> anyhow::Result<SplitState> {
    let split_state = match split_state_arg.to_lowercase().as_ref() {
        "staged" => SplitState::Staged,
        "published" => SplitState::Published,
        "marked" => SplitState::MarkedForDeletion,
        _ => bail!(format!(
            "Failed to parse split state `{}`. Possible values are `staged`, `published`, and \
             `marked`.",
            split_state_arg
        )),
    };
    Ok(split_state)
}

#[derive(Tabled)]
struct SplitRow {
    #[tabled(rename = "ID")]
    split_id: String,
    #[tabled(rename = "State")]
    split_state: SplitState,
    #[tabled(rename = "Num docs")]
    num_docs: usize,
    #[tabled(rename = "Size (MB)")]
    size_mega_bytes: u64,
    #[tabled(rename = "Created at")]
    created_at: OffsetDateTime,
    #[tabled(rename = "Updated at")]
    updated_at: OffsetDateTime,
    #[tabled(rename = "Time range")]
    time_range: String,
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::str::FromStr;

    use time::macros::datetime;

    use super::*;
    use crate::cli::{build_cli, CliCommand};

    #[test]
    fn test_parse_list_split_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from(vec![
            "split",
            "list",
            "--config",
            "config.yaml",
            "--index",
            "hdfs",
            "--states",
            "staged,published",
            "--create-date",
            "2020-12-24",
            "--start-date",
            "2020-12-24",
            "--end-date",
            "2020-12-25T12:42",
            "--tags",
            "tenant:a,service:zk",
            "--format",
            "json",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;

        let expected_split_states = Some(vec![SplitState::Staged, SplitState::Published]);
        let expected_create_date = Some(datetime!(2020-12-24 00:00 UTC));
        let expected_start_date = Some(datetime!(2020-12-24 00:00 UTC));
        let expected_end_date = Some(datetime!(2020-12-25 12:42 UTC));
        let expected_tags = Some(TagFilterAst::And(vec![
            TagFilterAst::Tag {
                is_present: true,
                tag: "tenant:a".to_string(),
            },
            TagFilterAst::Tag {
                is_present: true,
                tag: "service:zk".to_string(),
            },
        ]));
        let expected_output_format = OutputFormat::Json;
        assert!(matches!(
            command,
            CliCommand::Split(SplitCliCommand::List(ListSplitArgs {
                index_id,
                split_states,
                create_date,
                start_date,
                end_date,
                tags,
                output_format,
                ..
            })) if index_id == "hdfs"
                   && split_states == expected_split_states
                   && create_date == expected_create_date
                   && start_date == expected_start_date
                   && end_date == expected_end_date
                   && tags == expected_tags
                   && output_format == expected_output_format
        ));
        Ok(())
    }

    #[test]
    fn test_parse_split_mark_for_deletion_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from(vec![
            "split",
            "mark",
            "--config",
            "file:///config.yaml",
            "--index",
            "wikipedia",
            "--splits",
            "split1,split2",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Split(SplitCliCommand::MarkForDeletion(MarkForDeletionArgs {
                config_uri,
                index_id,
                split_ids,
            })) if config_uri == Uri::from_str("file:///config.yaml").unwrap()
                && index_id == "wikipedia"
                && split_ids == vec!["split1".to_string(), "split2".to_string()]
        ));
        Ok(())
    }

    #[test]
    fn test_parse_split_describe_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from(vec![
            "split",
            "describe",
            "--index",
            "wikipedia",
            "--split",
            "ABC",
            "--config",
            "file:///config.yaml",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Split(SplitCliCommand::Describe(DescribeSplitArgs {
                index_id,
                split_id,
                verbose: false,
                ..
            })) if &index_id == "wikipedia" && &split_id == "ABC"
        ));
        Ok(())
    }

    #[test]
    fn test_parse_split_extract_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from(vec![
            "split",
            "extract",
            "--index",
            "wikipedia",
            "--split",
            "ABC",
            "--target-dir",
            "/datadir",
            "--config",
            "file:///config.yaml",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Split(SplitCliCommand::Extract(ExtractSplitArgs {
                index_id,
                split_id,
                target_dir,
                ..
            })) if &index_id == "wikipedia" && &split_id == "ABC" && target_dir == PathBuf::from("/datadir")
        ));
        Ok(())
    }

    #[test]
    fn test_parse_date() {
        assert_eq!(
            parse_date("2020-12-24", "create").unwrap(),
            datetime!(2020-12-24 00:00 UTC)
        );
        assert_eq!(
            parse_date("2020-12-24 10:20", "create").unwrap(),
            datetime!(2020-12-24 10:20 UTC)
        );
        assert_eq!(
            parse_date("2020-12-24T10:20", "create").unwrap(),
            datetime!(2020-12-24 10:20 UTC)
        );
        assert_eq!(
            parse_date("2020-12-24 10:20:30", "create").unwrap(),
            datetime!(2020-12-24 10:20:30 UTC)
        );
        assert_eq!(
            parse_date("2020-12-24T10:20:30", "create").unwrap(),
            datetime!(2020-12-24 10:20:30 UTC)
        );
    }

    #[test]
    fn test_parse_split_state() {
        assert_eq!(parse_split_state("Staged").unwrap(), SplitState::Staged);
        assert_eq!(
            parse_split_state("Published").unwrap(),
            SplitState::Published
        );
        assert_eq!(
            parse_split_state("Marked").unwrap(),
            SplitState::MarkedForDeletion
        );
    }
}
