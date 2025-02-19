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

use std::str::FromStr;

use anyhow::{bail, Context};
use clap::{arg, ArgMatches, Command};
use colored::Colorize;
use itertools::Itertools;
use quickwit_metastore::{Split, SplitState};
use quickwit_proto::types::{IndexId, SplitId};
use quickwit_serve::ListSplitsQueryParams;
use tabled::{Table, Tabled};
use time::{format_description, Date, OffsetDateTime, PrimitiveDateTime};
use tracing::debug;

use crate::checklist::GREEN_COLOR;
use crate::{client_args, make_table, prompt_confirmation, ClientArgs};

pub fn build_split_command() -> Command {
    Command::new("split")
        .about("Manages splits: lists, describes, marks for deletion...")
        .args(client_args())
        .subcommand(
            Command::new("list")
                .about("Lists the splits of an index.")
                .alias("ls")
                .args(&[
                    arg!(--index <INDEX> "Target index ID")
                        .display_order(1)
                        .required(true),
                    arg!(--"offset" <OFFSET> "Number of splits to skip.")
                        .display_order(2)
                        .required(false),
                    arg!(--"limit" <LIMIT> "Maximum number of splits to retrieve.")
                        .display_order(3)
                        .required(false),
                    arg!(--states <SPLIT_STATES> "Selects the splits whose states are included in this comma-separated list of states. Possible values are `staged`, `published`, and `marked`.")
                        .display_order(4)
                        .required(false)
                        .value_delimiter(','),
                    arg!(--"create-date" <CREATE_DATE> "Selects the splits whose creation dates are before this date.")
                        .display_order(5)
                        .required(false),
                    arg!(--"start-date" <START_DATE> "Selects the splits that contain documents after this date (time-series indexes only).")
                        .display_order(6)
                        .required(false),
                    arg!(--"end-date" <END_DATE> "Selects the splits that contain documents before this date (time-series indexes only).")
                        .display_order(7)
                        .required(false),
                    // See #2762:
                    // arg!(--tags <TAGS> "Selects the splits whose tags are all included in this comma-separated list of tags.")
                    //     .display_order(6)
                    //     .required(false)
                    //     .use_value_delimiter(true),
                    arg!(--"output-format" <OUTPUT_FORMAT> "Output format. Possible values are `table`, `json`, and `pretty-json`.")
                        .alias("format")
                        .display_order(8)
                        .required(false)
                ])
            )
        .subcommand(
            Command::new("describe")
                .about("Displays metadata about a split.")
                .alias("desc")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index")
                        .display_order(1)
                        .required(true),
                    arg!(--split <SPLIT> "ID of the target split")
                        .display_order(2)
                        .required(true),
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
                        .value_delimiter(','),
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
            "json" => Ok(OutputFormat::Json),
            "pretty-json" | "pretty_json" => Ok(OutputFormat::PrettyJson),
            "table" => Ok(OutputFormat::Table),
            _ => bail!(
                "unknown output format `{output_format_str}`. supported formats are: `table`, \
                 `json`, and `pretty-json`"
            ),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ListSplitArgs {
    pub client_args: ClientArgs,
    pub index_id: IndexId,
    pub offset: Option<usize>,
    pub limit: Option<usize>,
    pub split_states: Option<Vec<SplitState>>,
    pub create_date: Option<OffsetDateTime>,
    pub start_date: Option<OffsetDateTime>,
    pub end_date: Option<OffsetDateTime>,
    // pub tags: Option<TagFilterAst>,
    output_format: OutputFormat,
}

#[derive(Debug, Eq, PartialEq)]
pub struct MarkForDeletionArgs {
    pub client_args: ClientArgs,
    pub index_id: IndexId,
    pub split_ids: Vec<String>,
    pub assume_yes: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct DescribeSplitArgs {
    pub client_args: ClientArgs,
    pub index_id: IndexId,
    pub split_id: SplitId,
    pub verbose: bool,
}

#[derive(Debug, PartialEq)]
pub enum SplitCliCommand {
    List(ListSplitArgs),
    MarkForDeletion(MarkForDeletionArgs),
    Describe(DescribeSplitArgs),
}

impl SplitCliCommand {
    pub fn parse_cli_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches) = matches
            .remove_subcommand()
            .context("failed to split subcommand")?;
        match subcommand.as_str() {
            "describe" => Self::parse_describe_args(submatches),
            "list" => Self::parse_list_args(submatches),
            "mark-for-deletion" => Self::parse_mark_for_deletion_args(submatches),
            _ => bail!("unknown split subcommand `{subcommand}`"),
        }
    }

    fn parse_list_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let client_args = ClientArgs::parse(&mut matches)?;
        let index_id = matches
            .remove_one::<String>("index")
            .expect("`index` should be a required arg.");
        let offset = matches
            .remove_one::<String>("offset")
            .and_then(|s| s.parse::<usize>().ok());
        let limit = matches
            .remove_one::<String>("limit")
            .and_then(|s| s.parse::<usize>().ok());
        let split_states = matches
            .remove_many::<String>("states")
            .map(|values| {
                values
                    .into_iter()
                    .dedup()
                    .map(|split_state_str| parse_split_state(&split_state_str))
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?;
        let create_date = matches
            .remove_one::<String>("create-date")
            .map(|date_str| parse_date(&date_str, "create"))
            .transpose()?;
        let start_date = matches
            .remove_one::<String>("start-date")
            .map(|date_str| parse_date(&date_str, "start"))
            .transpose()?;
        let end_date = matches
            .remove_one::<String>("end-date")
            .map(|date_str| parse_date(&date_str, "end"))
            .transpose()?;
        // let tags = matches.values_of("tags").map(|values| {
        //     TagFilterAst::And(
        //         values
        //             .into_iter()
        //             .map(|value| TagFilterAst::Tag {
        //                 get_flag: true,
        //                 tag: value.to_string(),
        //             })
        //             .collect(),
        //     )
        // });
        let output_format = matches
            .remove_one::<String>("output-format")
            .map(|s| OutputFormat::from_str(s.as_str()))
            .transpose()?
            .unwrap_or(OutputFormat::Table);
        Ok(Self::List(ListSplitArgs {
            client_args,
            index_id,
            offset,
            limit,
            split_states,
            start_date,
            end_date,
            create_date,
            // tags,
            output_format,
        }))
    }

    fn parse_mark_for_deletion_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let client_args = ClientArgs::parse(&mut matches)?;
        let index_id = matches
            .remove_one::<String>("index")
            .expect("`index` should be a required arg.");
        let split_ids = matches
            .remove_many::<String>("splits")
            .expect("`splits` should be a required arg.")
            .collect();
        let assume_yes = matches.get_flag("yes");
        Ok(Self::MarkForDeletion(MarkForDeletionArgs {
            client_args,
            index_id,
            split_ids,
            assume_yes,
        }))
    }

    fn parse_describe_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let index_id = matches
            .remove_one::<String>("index")
            .expect("`index` should be a required arg.");
        let split_id = matches
            .remove_one::<String>("split")
            .expect("`split` should be a required arg.");
        let client_args = ClientArgs::parse(&mut matches)?;
        let verbose = matches.get_flag("verbose");

        Ok(Self::Describe(DescribeSplitArgs {
            client_args,
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
    let qw_client = args.client_args.client();
    let list_splits_query_params = ListSplitsQueryParams {
        offset: args.offset,
        limit: args.limit,
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
        .context("failed to list splits")?;
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
                      after the next garbage collection. Do you want to proceed?";
        if !prompt_confirmation(prompt, false) {
            return Ok(());
        }
    }
    let qw_client = args.client_args.client();
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

async fn describe_split_cli(args: DescribeSplitArgs) -> anyhow::Result<()> {
    debug!(args=?args, "describe-split");
    let qw_client = args.client_args.client();
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
                "could not find split metadata in metastore {}",
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
        "failed to parse --{}-date option parameter `{}`. supported format is `YYYY-MM-DD[ \
         HH:DD[:SS]]`",
        option_name,
        date_arg
    );
}

fn parse_split_state(split_state_arg: &str) -> anyhow::Result<SplitState> {
    let split_state = match split_state_arg.to_lowercase().as_str() {
        "staged" => SplitState::Staged,
        "published" => SplitState::Published,
        "marked" => SplitState::MarkedForDeletion,
        _ => bail!(format!(
            "unknown split state `{split_state_arg}`. possible values are `staged`, `published`, \
             and `marked`"
        )),
    };
    Ok(split_state)
}

#[derive(Tabled)]
struct SplitRow {
    #[tabled(rename = "ID")]
    split_id: SplitId,
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
    use reqwest::Url;
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
        let command = CliCommand::parse_cli_args(matches)?;

        let expected_split_states = Some(vec![SplitState::Staged, SplitState::Published]);
        let expected_create_date = Some(datetime!(2020-12-24 00:00 UTC));
        let expected_start_date = Some(datetime!(2020-12-24 00:00 UTC));
        let expected_end_date = Some(datetime!(2020-12-25 12:42 UTC));
        // let expected_tags = Some(TagFilterAst::And(vec![
        //     TagFilterAst::Tag {
        //         get_flag: true,
        //         tag: "tenant:a".to_string(),
        //     },
        //     TagFilterAst::Tag {
        //         get_flag: true,
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
        let command = CliCommand::parse_cli_args(matches)?;
        assert!(matches!(
            command,
            CliCommand::Split(SplitCliCommand::MarkForDeletion(MarkForDeletionArgs {
                client_args,
                index_id,
                split_ids,
                assume_yes,
            })) if client_args.cluster_endpoint == Url::from_str("https://quickwit-cluster.io").unwrap()
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
        let command = CliCommand::parse_cli_args(matches)?;
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
