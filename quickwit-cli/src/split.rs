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
use std::io::stdout;
use std::ops::{Range, RangeInclusive};
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{bail, Context};
use chrono::{NaiveDate, NaiveDateTime};
use clap::ArgMatches;
use humansize::{file_size_opts, FileSize};
use quickwit_common::uri::normalize_uri;
use quickwit_directories::{
    get_hotcache_from_split, read_split_footer, BundleDirectory, HotDirectory,
};
use quickwit_metastore::{MetastoreUriResolver, SplitState};
use quickwit_storage::{quickwit_storage_uri_resolver, BundleStorage, Storage};
use tracing::debug;

use crate::Printer;

#[derive(Debug, Eq, PartialEq)]
pub struct ListSplitArgs {
    pub metastore_uri: String,
    pub index_id: String,
    pub states: Vec<SplitState>,
    pub from: Option<i64>,
    pub to: Option<i64>,
    pub tags: Vec<String>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct DescribeSplitArgs {
    pub metastore_uri: String,
    pub index_id: String,
    pub split_id: String,
    pub verbose: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ExtractSplitArgs {
    pub metastore_uri: String,
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
        let metastore_uri = matches
            .value_of("metastore-uri")
            .context("'metastore-uri' is a required arg")
            .map(normalize_uri)??;
        let index_id = matches
            .value_of("index-id")
            .context("'index-id' is a required arg")?
            .to_string();

        let states = matches
            .values_of("states")
            .map_or(vec![], |values| {
                values.into_iter().map(SplitState::from_str).collect()
            })
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err_str| anyhow::anyhow!(err_str))?;

        let from = if let Some(date_str) = matches.value_of("from") {
            let from_date_time = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
                .map(|date| date.and_hms(0, 0, 0))
                .or_else(|_err| NaiveDateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M:%S"))
                .context("'from' should be of the format `2020-10-31` or `2020-10-31T02:00:00`")?;
            Some(from_date_time.timestamp())
        } else {
            None
        };

        let to = if let Some(date_str) = matches.value_of("to") {
            let to_date_time = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
                .map(|date| date.and_hms(0, 0, 0))
                .or_else(|_err| NaiveDateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M:%S"))
                .context("'to' should be of the format `2020-10-31` or `2020-10-31T02:00:00`")?;
            Some(to_date_time.timestamp())
        } else {
            None
        };

        let tags = matches.values_of("tags").map_or(vec![], |values| {
            values.into_iter().map(str::to_string).collect::<Vec<_>>()
        });

        Ok(Self::List(ListSplitArgs {
            metastore_uri,
            index_id,
            states,
            from,
            to,
            tags,
        }))
    }

    fn parse_describe_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_id = matches
            .value_of("index-id")
            .context("'index-id' is a required arg.")?
            .to_string();
        let split_id = matches
            .value_of("split-id")
            .context("'split-id' is a required arg.")?
            .to_string();
        let metastore_uri = matches
            .value_of("metastore-uri")
            .context("'metastore-uri' is a required arg.")
            .map(normalize_uri)??;
        let verbose = matches.is_present("verbose");

        Ok(Self::Describe(DescribeSplitArgs {
            metastore_uri,
            index_id,
            split_id,
            verbose,
        }))
    }

    fn parse_extract_split_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_id = matches
            .value_of("index-id")
            .context("'index-id' is a required arg.")?
            .to_string();
        let split_id = matches
            .value_of("split-id")
            .context("'split-id' is a required arg.")?
            .to_string();
        let metastore_uri = matches
            .value_of("metastore-uri")
            .context("'metastore-uri' is a required arg.")
            .map(normalize_uri)??;
        let target_dir = matches
            .value_of("target-dir")
            .map(PathBuf::from)
            .context("'target-dir' is a required arg.")?;

        Ok(Self::Extract(ExtractSplitArgs {
            metastore_uri,
            index_id,
            split_id,
            target_dir,
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

pub async fn list_split_cli(args: ListSplitArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "list-split");

    let metastore_uri_resolver = MetastoreUriResolver::default();
    let metastore = metastore_uri_resolver.resolve(&args.metastore_uri).await?;
    let time_range_opt = match (args.from, args.to) {
        (None, None) => None,
        (None, Some(to)) => Some(Range {
            start: i64::MIN,
            end: to,
        }),
        (Some(from), None) => Some(Range {
            start: from,
            end: i64::MAX,
        }),
        (Some(from), Some(to)) => Some(Range {
            start: from,
            end: to,
        }),
    };
    let is_disjoint_time_range = |left: &Range<i64>, right: &RangeInclusive<i64>| {
        left.end <= *right.start() || *right.end() < left.start
    };
    let filter_tag_set = args.tags.iter().cloned().collect::<BTreeSet<_>>();

    let mut splits = vec![];
    // apply tags & time range filter.
    for split in metastore.list_all_splits(&args.index_id).await? {
        let is_any_tag_not_in_split = filter_tag_set.iter().any(|tag| {
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
        splits.push(split);
    }

    // apply SplitState filter.
    if !args.states.is_empty() {
        splits = splits
            .into_iter()
            .filter(|split| args.states.contains(&split.split_state))
            .collect::<Vec<_>>();
    }

    let mut stdout_handle = stdout();
    let mut printer = Printer {
        stdout: &mut stdout_handle,
    };
    for split in splits {
        printer.print_header("Id")?;
        printer.print_value(format_args!("{:>7}", split.split_metadata.split_id))?;
        printer.print_header("Created at")?;
        printer.print_value(format_args!(
            "{:>5}",
            NaiveDateTime::from_timestamp(split.split_metadata.create_timestamp, 0)
        ))?;
        printer.print_header("Updated at")?;
        printer.print_value(format_args!(
            "{:>3}",
            NaiveDateTime::from_timestamp(split.update_timestamp, 0)
        ))?;
        printer.print_header("Num docs")?;
        printer.print_value(format_args!("{:>7}", split.split_metadata.num_docs))?;
        printer.print_header("Size")?;
        printer.print_value(format_args!(
            "{:>5}MB",
            split.split_metadata.original_size_in_bytes / 1_000_000
        ))?;
        printer.print_header("Demux ops")?;
        printer.print_value(format_args!("{:>7}", split.split_metadata.demux_num_ops))?;
        printer.print_header("Time range")?;
        if let Some(time_range) = split.split_metadata.time_range {
            printer.print_value(format_args!("[{:?}]\n", time_range))?;
        } else {
            printer.print_value(format_args!("[*]\n"))?;
        }
        printer.flush()?;
    }

    Ok(())
}

pub async fn describe_split_cli(args: DescribeSplitArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "describe-split");

    let storage_uri_resolver = quickwit_storage_uri_resolver();
    let metastore_uri_resolver = MetastoreUriResolver::default();
    let metastore = metastore_uri_resolver.resolve(&args.metastore_uri).await?;
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

pub async fn extract_split_cli(args: ExtractSplitArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "extract-split");

    let storage_uri_resolver = quickwit_storage_uri_resolver();
    let metastore_uri_resolver = MetastoreUriResolver::default();
    let metastore = metastore_uri_resolver.resolve(&args.metastore_uri).await?;
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
