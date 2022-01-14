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

use std::collections::BTreeSet;
use std::ops::{Range, RangeInclusive};
use std::path::PathBuf;

use anyhow::{bail, Context};
use chrono::{NaiveDate, NaiveDateTime};
use clap::ArgMatches;
use humansize::{file_size_opts, FileSize};
use itertools::Itertools;
use quickwit_common::uri::Uri;
use quickwit_directories::{
    get_hotcache_from_split, read_split_footer, BundleDirectory, HotDirectory,
};
use quickwit_metastore::{quickwit_metastore_uri_resolver, Split, SplitState};
use quickwit_storage::{quickwit_storage_uri_resolver, BundleStorage, Storage};
use tabled::{Table, Tabled};
use tracing::debug;

use crate::{load_quickwit_config, make_table};

#[derive(Debug, Eq, PartialEq)]
pub struct ListSplitArgs {
    pub config_uri: Uri,
    pub data_dir: Option<PathBuf>,
    pub index_id: String,
    pub states: Vec<SplitState>,
    pub start_date: Option<i64>,
    pub end_date: Option<i64>,
    pub tags: BTreeSet<String>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct DescribeSplitArgs {
    pub config_uri: Uri,
    pub data_dir: Option<PathBuf>,
    pub index_id: String,
    pub split_id: String,
    pub verbose: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ExtractSplitArgs {
    pub config_uri: Uri,
    pub data_dir: Option<PathBuf>,
    pub index_id: String,
    pub split_id: String,
    pub target_dir: PathBuf,
}

#[derive(Debug, PartialEq)]
pub enum SplitCliCommand {
    List(ListSplitArgs),
    Describe(DescribeSplitArgs),
    Extract(ExtractSplitArgs),
}

impl SplitCliCommand {
    pub fn parse_cli_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches) = matches
            .subcommand()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse sub-matches."))?;
        match subcommand {
            "list" => Self::parse_list_args(submatches),
            "describe" => Self::parse_describe_args(submatches),
            "extract" => Self::parse_extract_split_args(submatches),
            _ => bail!("Subcommand `{}` is not implemented.", subcommand),
        }
    }

    fn parse_list_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let config_uri = matches
            .value_of("config")
            .map(Uri::try_new)
            .expect("`config` is a required arg.")?;
        let data_dir = matches.value_of("data-dir").map(PathBuf::from);
        let index_id = matches
            .value_of("index")
            .map(String::from)
            .expect("`index` is a required arg.");
        let states = matches
            .values_of("states")
            .map_or(vec![], |values| {
                values.into_iter().map(split_state_from_input_str).collect()
            })
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err_str| anyhow::anyhow!(err_str))?;
        let start_date = if let Some(date_str) = matches.value_of("start-date") {
            let from_date_time = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
                .map(|date| date.and_hms(0, 0, 0))
                .or_else(|_err| NaiveDateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M:%S"))
                .context(format!(
                    "'start-date' `{}` should be of the format `2020-10-31` or \
                     `2020-10-31T02:00:00`",
                    date_str
                ))?;
            Some(from_date_time.timestamp())
        } else {
            None
        };
        let end_date = if let Some(date_str) = matches.value_of("end-date") {
            let to_date_time = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
                .map(|date| date.and_hms(0, 0, 0))
                .or_else(|_err| NaiveDateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M:%S"))
                .context(format!(
                    "'end-date' `{}` should be of the format `2020-10-31` or `2020-10-31T02:00:00`",
                    date_str
                ))?;
            Some(to_date_time.timestamp())
        } else {
            None
        };
        let tags = matches
            .values_of("tags")
            .map_or(BTreeSet::default(), |values| {
                values
                    .into_iter()
                    .map(str::to_string)
                    .collect::<BTreeSet<_>>()
            });
        Ok(Self::List(ListSplitArgs {
            index_id,
            states,
            start_date,
            end_date,
            tags,
            config_uri,
            data_dir,
        }))
    }

    fn parse_describe_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_id = matches
            .value_of("index")
            .map(String::from)
            .expect("'index-id' is a required arg.");
        let split_id = matches
            .value_of("split")
            .map(String::from)
            .expect("'split-id' is a required arg.");
        let config_uri = matches
            .value_of("config")
            .map(Uri::try_new)
            .expect("`config` is a required arg.")?;
        let data_dir = matches.value_of("data-dir").map(PathBuf::from);
        let verbose = matches.is_present("verbose");

        Ok(Self::Describe(DescribeSplitArgs {
            config_uri,
            index_id,
            split_id,
            verbose,
            data_dir,
        }))
    }

    fn parse_extract_split_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_id = matches
            .value_of("index")
            .map(String::from)
            .expect("'index-id' is a required arg.");
        let split_id = matches
            .value_of("split")
            .map(String::from)
            .expect("'split-id' is a required arg.");
        let config_uri = matches
            .value_of("config")
            .map(Uri::try_new)
            .expect("`config` is a required arg.")?;
        let target_dir = matches
            .value_of("target-dir")
            .map(PathBuf::from)
            .expect("`target-dir` is a required arg.");
        let data_dir = matches.value_of("data-dir").map(PathBuf::from);
        Ok(Self::Extract(ExtractSplitArgs {
            config_uri,
            index_id,
            split_id,
            target_dir,
            data_dir,
        }))
    }

    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            Self::List(args) => list_split_cli(args).await,
            Self::Describe(args) => describe_split_cli(args).await,
            Self::Extract(args) => extract_split_cli(args).await,
        }
    }
}

async fn list_split_cli(args: ListSplitArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "list-split");

    let quickwit_config = load_quickwit_config(args.config_uri, args.data_dir).await?;
    let metastore_uri_resolver = quickwit_metastore_uri_resolver();
    let metastore = metastore_uri_resolver
        .resolve(&quickwit_config.metastore_uri)
        .await?;
    let splits = metastore.list_all_splits(&args.index_id).await?;

    let filtered_splits = filter_splits(
        splits,
        args.states,
        args.start_date,
        args.end_date,
        args.tags,
    )?;
    let filtered_splits_table = make_list_splits_table(filtered_splits);

    println!("{filtered_splits_table}");

    Ok(())
}

async fn describe_split_cli(args: DescribeSplitArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "describe-split");

    let quickwit_config = load_quickwit_config(args.config_uri, args.data_dir).await?;
    let storage_uri_resolver = quickwit_storage_uri_resolver();
    let metastore_uri_resolver = quickwit_metastore_uri_resolver();
    let metastore = metastore_uri_resolver
        .resolve(&quickwit_config.metastore_uri)
        .await?;
    let index_metadata = metastore.index_metadata(&args.index_id).await?;
    let index_storage = storage_uri_resolver.resolve(&index_metadata.index_uri)?;

    let split_file = PathBuf::from(format!("{}.split", args.split_id));
    let (split_footer, _) = read_split_footer(index_storage, &split_file).await?;
    let stats = BundleDirectory::get_stats_split(split_footer.clone())?;
    let hotcache_bytes = get_hotcache_from_split(split_footer)?;
    for (path, size) in stats {
        let readable_size = size.file_size(file_size_opts::DECIMAL).unwrap();
        println!("{:?} {}", path, readable_size);
    }
    if args.verbose {
        let hotcache_stats = HotDirectory::get_stats_per_file(hotcache_bytes)?;
        for (path, size) in hotcache_stats {
            let readable_size = size.file_size(file_size_opts::DECIMAL).unwrap();
            println!("HotCache {:?} {}", path, readable_size);
        }
    }
    Ok(())
}

async fn extract_split_cli(args: ExtractSplitArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "extract-split");

    let quickwit_config = load_quickwit_config(args.config_uri, args.data_dir).await?;
    let storage_uri_resolver = quickwit_storage_uri_resolver();
    let metastore_uri_resolver = quickwit_metastore_uri_resolver();
    let metastore = metastore_uri_resolver
        .resolve(&quickwit_config.metastore_uri)
        .await?;
    let index_metadata = metastore.index_metadata(&args.index_id).await?;
    let index_storage = storage_uri_resolver.resolve(&index_metadata.index_uri)?;
    let split_file = PathBuf::from(format!("{}.split", args.split_id));
    let split_data = index_storage.get_all(split_file.as_path()).await?;
    let (_hotcache_bytes, bundle_storage) = BundleStorage::open_from_split_data_with_owned_bytes(
        index_storage,
        split_file,
        split_data,
    )?;
    std::fs::create_dir_all(args.target_dir.to_owned())?;
    for path in bundle_storage.iter_files() {
        let mut out_path = args.target_dir.to_owned();
        out_path.push(path.to_owned());
        println!("Copying {:?}", out_path);
        bundle_storage.copy_to_file(path, &out_path).await?;
    }

    Ok(())
}

fn filter_splits(
    splits: Vec<Split>,
    states: Vec<SplitState>,
    start_date: Option<i64>,
    end_date: Option<i64>,
    tags: BTreeSet<String>,
) -> anyhow::Result<Vec<Split>> {
    let time_range_opt = match (start_date, end_date) {
        (None, None) => None,
        (None, Some(end_date)) => Some(Range {
            start: i64::MIN,
            end: end_date,
        }),
        (Some(start_date), None) => Some(Range {
            start: start_date,
            end: i64::MAX,
        }),
        (Some(start_date), Some(end_date)) => Some(Range {
            start: start_date,
            end: end_date,
        }),
    };
    let is_disjoint_time_range = |left: &Range<i64>, right: &RangeInclusive<i64>| {
        left.end <= *right.start() || *right.end() < left.start
    };

    let mut filtered_splits = vec![];

    // apply tags & time range filter.
    for split in splits {
        let is_any_tag_not_in_split = tags.iter().any(|tag| {
            let has_many_tags_for_field = tag
                .split_once(":")
                .map(|(field_name, _)| {
                    split
                        .split_metadata
                        .tags
                        .contains(&format!("{}:*", field_name))
                })
                .unwrap_or(false);
            !(split.split_metadata.tags.contains(tag) || has_many_tags_for_field)
        });
        if is_any_tag_not_in_split {
            continue;
        }

        if let (Some(filter_time_range), Some(split_time_range)) =
            (&time_range_opt, &split.split_metadata.time_range)
        {
            if is_disjoint_time_range(filter_time_range, split_time_range) {
                continue;
            }
        }
        filtered_splits.push(split);
    }

    // apply SplitState filter.
    if !states.is_empty() {
        filtered_splits = filtered_splits
            .into_iter()
            .filter(|split| states.contains(&split.split_state))
            .collect::<Vec<_>>();
    }

    Ok(filtered_splits)
}

fn make_list_splits_table(splits: Vec<Split>) -> Table {
    let rows = splits
        .into_iter()
        .map(|split| {
            let time_range = if let Some(time_range) = split.split_metadata.time_range {
                format!("[{:?}]", time_range)
            } else {
                "[*]".to_string()
            };
            SplitRow {
                id: split.split_metadata.split_id,
                num_docs: split.split_metadata.num_docs,
                size_mega_bytes: split.split_metadata.original_size_in_bytes / 1_000_000,
                create_at: NaiveDateTime::from_timestamp(split.split_metadata.create_timestamp, 0),
                updated_at: NaiveDateTime::from_timestamp(split.update_timestamp, 0),
                time_range,
            }
        })
        .sorted_by(|left, right| left.id.cmp(&right.id));
    make_table("Splits", rows)
}

fn split_state_from_input_str(input: &str) -> anyhow::Result<SplitState> {
    match input.to_lowercase().as_str() {
        "staged" => Ok(SplitState::Staged),
        "published" => Ok(SplitState::Published),
        "marked" => Ok(SplitState::MarkedForDeletion),
        _ => bail!(format!(
            "Unknown split state `{}`. Possible values are `staged`, `published`, and `marked`.",
            input
        )),
    }
}

#[derive(Tabled)]
struct SplitRow {
    #[header("Id")]
    id: String,
    #[header("Num Docs")]
    num_docs: usize,
    #[header("Size (MB)")]
    size_mega_bytes: u64,
    #[header("Created At")]
    create_at: NaiveDateTime,
    #[header("Updated At")]
    updated_at: NaiveDateTime,
    #[header("Time Range")]
    time_range: String,
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use chrono::NaiveDateTime;
    use clap::{load_yaml, App, AppSettings};
    use quickwit_metastore::SplitMetadata;

    use super::*;
    use crate::cli::CliCommand;

    #[test]
    fn test_parse_list_split_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.try_get_matches_from(vec![
            "split",
            "list",
            "--index",
            "wikipedia",
            "--states",
            "published,staged",
            "--start-date",
            "2021-12-03",
            "--end-date",
            "2021-12-05T00:30:25",
            "--tags",
            "foo:bar,bar:baz",
            "--config",
            "file:///config.yaml",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Split(SplitCliCommand::List(ListSplitArgs {
                index_id, states, start_date, end_date, tags, ..
            })) if &index_id == "wikipedia"
            && states == vec![SplitState::Published, SplitState::Staged]
            && start_date == Some(NaiveDateTime::parse_from_str("2021-12-03T00:00:00", "%Y-%m-%dT%H:%M:%S").unwrap().timestamp())
            && end_date == Some(NaiveDateTime::parse_from_str("2021-12-05T00:30:25", "%Y-%m-%dT%H:%M:%S").unwrap().timestamp())
            && tags == BTreeSet::from(["foo:bar".to_string(), "bar:baz".to_string()])
        ));

        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.try_get_matches_from(vec![
            "split",
            "list",
            "--index",
            "wikipedia",
            "--states",
            "published",
            "--start-date",
            "2021-12-03T", // <- expect time
            "--config",
            "file:///config.yaml",
        ])?;
        assert!(matches!(CliCommand::parse_cli_args(&matches), Err { .. }));

        Ok(())
    }

    #[test]
    fn test_parse_split_describe_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
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
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
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

    fn make_split(
        split_id: &str,
        split_state: SplitState,
        time_range: Option<RangeInclusive<i64>>,
        tags: Vec<&str>,
    ) -> Split {
        Split {
            split_metadata: SplitMetadata {
                split_id: split_id.to_string(),
                footer_offsets: 10..30,
                time_range,
                tags: tags.into_iter().map(|tag| tag.to_string()).collect(),
                create_timestamp: 1639997967,
                ..Default::default()
            },
            split_state,
            update_timestamp: 1639997968,
        }
    }

    #[test]
    fn test_filter_splits() -> anyhow::Result<()> {
        let splits = vec![
            make_split("one", SplitState::MarkedForDeletion, Some(5..=10), vec![]),
            make_split(
                "two",
                SplitState::Published,
                None,
                vec!["tenant:a", "foo:bar"],
            ),
            make_split(
                "three",
                SplitState::Staged,
                Some(15..=20),
                vec!["tenant:a", "foo:*"],
            ),
            make_split(
                "four",
                SplitState::Published,
                None,
                vec!["tenant:b", "foo:bar"],
            ),
            make_split("five", SplitState::Staged, Some(8..=12), vec!["tenant:b"]),
        ];

        // select by SplitState
        let filtered_splits = filter_splits(
            splits.clone(),
            vec![SplitState::Published, SplitState::MarkedForDeletion],
            None,
            None,
            BTreeSet::default(),
        )?;
        assert_eq!(filtered_splits.len(), 3);
        assert_eq!(
            filtered_splits
                .iter()
                .map(|split| split.split_id())
                .collect::<Vec<_>>(),
            ["one", "two", "four"]
        );

        // select by tags
        let filtered_splits = filter_splits(
            splits.clone(),
            vec![],
            None,
            None,
            BTreeSet::from(["tenant:a".to_string(), "foo:bar".to_string()]),
        )?;
        assert_eq!(filtered_splits.len(), 2);
        assert_eq!(
            filtered_splits
                .iter()
                .map(|split| split.split_id())
                .collect::<Vec<_>>(),
            ["two", "three"]
        );

        // select by time range
        let filtered_splits =
            filter_splits(splits, vec![], Some(7), Some(15), BTreeSet::default())?;
        assert_eq!(filtered_splits.len(), 4);
        assert_eq!(
            filtered_splits
                .iter()
                .map(|split| split.split_id())
                .collect::<Vec<_>>(),
            ["one", "two", "four", "five"]
        );

        Ok(())
    }
}
