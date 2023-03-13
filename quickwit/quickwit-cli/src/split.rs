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

use std::str::FromStr;

use anyhow::{bail, Context};
use clap::{arg, ArgMatches, Command};
use colored::Colorize;
use itertools::Itertools;
use quickwit_common::GREEN_COLOR;
use quickwit_metastore::{Split, SplitState};
use quickwit_rest_client::rest_client::{QuickwitClient, Transport};
use quickwit_serve::ListSplitsQueryParams;
use reqwest::Url;
use tabled::{Table, Tabled};
use time::{format_description, Date, OffsetDateTime, PrimitiveDateTime};
use tracing::debug;

use crate::{cluster_endpoint_arg, make_table, prompt_confirmation};

pub fn build_split_command<'a>() -> Command<'a> {
    Command::new("split")
        .about("Manages splits: lists, describes, marks for deletion...")
        .arg(cluster_endpoint_arg())
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
                    // arg!(--tags <TAGS> "Selects the splits whose tags are all included in this comma-separated list of tags.")
                    //     .display_order(6)
                    //     .required(false)
                    //     .use_value_delimiter(true),
                    arg!(--"output-format" <OUTPUT_FORMAT> "Output format. Possible values are `table`, `json`, and `pretty_json`.")
                        .alias("format")
                        .display_order(7)
                        .required(false)
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
                    arg!(-y --"yes" "Assume \"yes\" as an answer to all prompts and run non-interactively.")
                        .required(false),
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
            "pretty_json" => Ok(OutputFormat::PrettyJson),
            _ => bail!(
                "Failed to parse output format `{output_format_str}`. Supported formats are: \
                 `table`, `json`, and `pretty_json`."
            ),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ListSplitArgs {
    pub cluster_endpoint: Url,
    pub index_id: String,
    pub split_states: Option<Vec<SplitState>>,
    pub create_date: Option<OffsetDateTime>,
    pub start_date: Option<OffsetDateTime>,
    pub end_date: Option<OffsetDateTime>,
    // pub tags: Option<TagFilterAst>,
    output_format: OutputFormat,
}

#[derive(Debug, Eq, PartialEq)]
pub struct MarkForDeletionArgs {
    pub cluster_endpoint: Url,
    pub index_id: String,
    pub split_ids: Vec<String>,
    pub assume_yes: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct DescribeSplitArgs {
    pub cluster_endpoint: Url,
    pub index_id: String,
    pub split_id: String,
    pub verbose: bool,
}

#[derive(Debug, PartialEq)]
pub enum SplitCliCommand {
    List(ListSplitArgs),
    MarkForDeletion(MarkForDeletionArgs),
    Describe(DescribeSplitArgs),
}

impl SplitCliCommand {
    pub fn parse_cli_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches) = matches
            .subcommand()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse sub-matches."))?;
        match subcommand {
            "describe" => Self::parse_describe_args(submatches),
            "list" => Self::parse_list_args(submatches),
            "mark-for-deletion" => Self::parse_mark_for_deletion_args(submatches),
            _ => bail!("Subcommand `{}` is not implemented.", subcommand),
        }
    }

    fn parse_list_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let cluster_endpoint = matches
            .value_of("endpoint")
            .map(Url::from_str)
            .expect("`endpoint` is a required arg.")?;
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
        // let tags = matches.values_of("tags").map(|values| {
        //     TagFilterAst::And(
        //         values
        //             .into_iter()
        //             .map(|value| TagFilterAst::Tag {
        //                 is_present: true,
        //                 tag: value.to_string(),
        //             })
        //             .collect(),
        //     )
        // });
        let output_format = matches
            .value_of("output-format")
            .map(OutputFormat::from_str)
            .transpose()?
            .unwrap_or(OutputFormat::Table);

        Ok(Self::List(ListSplitArgs {
            cluster_endpoint,
            index_id,
            split_states,
            start_date,
            end_date,
            create_date,
            // tags,
            output_format,
        }))
    }

    fn parse_mark_for_deletion_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let cluster_endpoint = matches
            .value_of("endpoint")
            .map(Url::from_str)
            .expect("`endpoint` is a required arg.")?;
        let index_id = matches
            .value_of("index")
            .map(String::from)
            .expect("`index` is a required arg.");
        let split_ids = matches
            .values_of("splits")
            .expect("`splits` is a required arg.")
            .map(String::from)
            .collect();
        let assume_yes = matches.is_present("yes");
        Ok(Self::MarkForDeletion(MarkForDeletionArgs {
            cluster_endpoint,
            index_id,
            split_ids,
            assume_yes,
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
        let cluster_endpoint = matches
            .value_of("endpoint")
            .map(Url::from_str)
            .expect("`endpoint` is a required arg.")?;
        let verbose = matches.is_present("verbose");

        Ok(Self::Describe(DescribeSplitArgs {
            cluster_endpoint,
            index_id,
            split_id,
            verbose,
        }))
    }

    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            Self::List(args) => list_split_cli(args).await,
            Self::MarkForDeletion(args) => mark_splits_for_deletion_cli(args).await,
            Self::Describe(args) => describe_split_cli(args).await,
        }
    }
}

async fn list_split_cli(args: ListSplitArgs) -> anyhow::Result<()> {
    debug!(args=?args, "list-split");
    let transport = Transport::new(args.cluster_endpoint);
    let qw_client = QuickwitClient::new(transport);
    let list_splits_query_params = ListSplitsQueryParams {
        split_states: args.split_states,
        start_timestamp: args.start_date.map(OffsetDateTime::unix_timestamp),
        end_timestamp: args.end_date.map(OffsetDateTime::unix_timestamp),
        end_create_timestamp: args.create_date.map(OffsetDateTime::unix_timestamp),
    };
    // TODO: plug tags.
    // if let Some(tags) = args.tags {
    //     query = query.with_tags_filter(tags);
    // }
    let splits = qw_client
        .splits(&args.index_id)
        .list(list_splits_query_params)
        .await
        .expect("Failed to fetch splits.");
    let output = match args.output_format {
        OutputFormat::Json => serde_json::to_string(&splits)?,
        OutputFormat::PrettyJson => serde_json::to_string_pretty(&splits)?,
        OutputFormat::Table => make_split_table(&splits, "Splits").to_string(),
    };
    println!("{output}");
    Ok(())
}

async fn mark_splits_for_deletion_cli(args: MarkForDeletionArgs) -> anyhow::Result<()> {
    debug!(args=?args, "mark-splits-for-deletion");
    println!("❯ Marking splits for deletion...");
    if !args.assume_yes {
        let prompt = "This operation will mark splits for deletion, those splits will be deleted \
                      after the next garbage collection. Do you want to proceed?"
            .to_string();
        if !prompt_confirmation(&prompt, false) {
            return Ok(());
        }
    }
    let transport = Transport::new(args.cluster_endpoint);
    let qw_client = QuickwitClient::new(transport);
    qw_client
        .splits(&args.index_id)
        .mark_for_deletion(args.split_ids)
        .await?;
    println!(
        "{} Splits successfully marked for deletion.",
        "✔".color(GREEN_COLOR)
    );
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
    debug!(args=?args, "describe-split");
    let transport = Transport::new(args.cluster_endpoint);
    let qw_client = QuickwitClient::new(transport);
    let list_splits_query_params = ListSplitsQueryParams::default();
    let split = qw_client
        .splits(&args.index_id)
        .list(list_splits_query_params)
        .await
        .expect("Failed to fetch splits.")
        .into_iter()
        .find(|split| split.split_id() == args.split_id)
        .with_context(|| {
            format!(
                "Could not find split metadata in metastore {}",
                args.split_id
            )
        })?;

    println!("{}", make_split_table(&[split], "Split"));

    // TODO: if we have access to the storage, we could fetch that.
    // let split_file = PathBuf::from(format!("{}.split", args.split_id));
    // let (split_footer, _) = read_split_footer(index_storage, &split_file).await?;
    // let stats = BundleDirectory::get_stats_split(split_footer.clone())?;
    // let hotcache_bytes = get_hotcache_from_split(split_footer)?;

    // let mut file_rows = Vec::new();

    // for (path, size) in stats {
    //     file_rows.push(FileRow {
    //         file_name: path.to_str().unwrap().to_string(),
    //         size: format_size(size, DECIMAL),
    //     });
    // }
    // println!(
    //     "{}",
    //     make_table("Files in Split", file_rows.into_iter(), false)
    // );
    // if args.verbose {
    //     let mut hotcache_files = Vec::new();
    //     let hotcache_stats = HotDirectory::get_stats_per_file(hotcache_bytes)?;
    //     for (path, size) in hotcache_stats {
    //         hotcache_files.push(FileRow {
    //             file_name: path.to_str().unwrap().to_string(),
    //             size: format_size(size, DECIMAL),
    //         });
    //     }
    //     let hotcache_table = make_table("Files in Hotcache", hotcache_files.into_iter(), false);
    //     println!("{hotcache_table}");
    // }
    Ok(())
}

fn make_split_table(splits: &[Split], title: &str) -> Table {
    let rows = splits
        .iter()
        .map(|split| {
            let time_range = if let Some(time_range) = &split.split_metadata.time_range {
                format!("[{time_range:?}]")
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
            "Failed to parse split state `{split_state_arg}`. Possible values are `staged`, \
             `published`, and `marked`."
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
    use time::macros::datetime;

    use super::*;
    use crate::cli::{build_cli, CliCommand};

    #[test]
    fn test_parse_list_split_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from(vec![
            "split",
            "list",
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
            // "--tags",
            // "tenant:a,service:zk",
            "--format",
            "json",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;

        let expected_split_states = Some(vec![SplitState::Staged, SplitState::Published]);
        let expected_create_date = Some(datetime!(2020-12-24 00:00 UTC));
        let expected_start_date = Some(datetime!(2020-12-24 00:00 UTC));
        let expected_end_date = Some(datetime!(2020-12-25 12:42 UTC));
        // let expected_tags = Some(TagFilterAst::And(vec![
        //     TagFilterAst::Tag {
        //         is_present: true,
        //         tag: "tenant:a".to_string(),
        //     },
        //     TagFilterAst::Tag {
        //         is_present: true,
        //         tag: "service:zk".to_string(),
        //     },
        // ]));
        let expected_output_format = OutputFormat::Json;
        assert!(matches!(
            command,
            CliCommand::Split(SplitCliCommand::List(ListSplitArgs {
                index_id,
                split_states,
                create_date,
                start_date,
                end_date,
                // tags,
                output_format,
                ..
            })) if index_id == "hdfs"
                   && split_states == expected_split_states
                   && create_date == expected_create_date
                   && start_date == expected_start_date
                   && end_date == expected_end_date
                   // && tags == expected_tags
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
            "--endpoint",
            "https://quickwit-cluster.io",
            "--index",
            "wikipedia",
            "--splits",
            "split1,split2",
            "--yes",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Split(SplitCliCommand::MarkForDeletion(MarkForDeletionArgs {
                cluster_endpoint,
                index_id,
                split_ids,
                assume_yes,
            })) if cluster_endpoint == Url::from_str("https://quickwit-cluster.io").unwrap()
                && index_id == "wikipedia"
                && split_ids == vec!["split1".to_string(), "split2".to_string()]
                && assume_yes
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
