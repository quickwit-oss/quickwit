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

use std::collections::HashSet;

use anyhow::{bail, Context};
use clap::ArgMatches;
use colored::*;
use itertools::Itertools;
use quickwit_index_config::match_tag_field_name;
use quickwit_metastore::{MetastoreUriResolver, SplitState};
use tracing::debug;

#[derive(Debug, Eq, PartialEq)]
pub struct DemuxStatsArgs {
    metastore_uri: String,
    index_id: String,
}

impl DemuxStatsArgs {
    pub fn new(metastore_uri: String, index_id: String) -> anyhow::Result<Self> {
        Ok(Self {
            metastore_uri,
            index_id,
        })
    }
}

#[derive(Debug, PartialEq)]
pub enum StatsCliSubCommand {
    DemuxStats(DemuxStatsArgs),
}

impl StatsCliSubCommand {
    pub fn parse_cli_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches_opt) = matches.subcommand();
        let submatches =
            submatches_opt.ok_or_else(|| anyhow::anyhow!("Failed to parse sub-matches."))?;
        match subcommand {
            "demux" => Self::parse_demux_stats_args(submatches),
            _ => bail!("Stats subcommand '{}' is not implemented", subcommand),
        }
    }

    fn parse_demux_stats_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_id = matches
            .value_of("index-id")
            .context("'index-id' is a required arg")?
            .to_string();
        let metastore_uri = matches
            .value_of("metastore-uri")
            .map(|metastore_uri_str| metastore_uri_str.to_string())
            .context("'metastore-uri' is a required arg")?;

        Ok(Self::DemuxStats(DemuxStatsArgs::new(
            metastore_uri,
            index_id,
        )?))
    }

    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            Self::DemuxStats(args) => demux_stats_cli(args).await,
        }
    }
}

pub async fn demux_stats_cli(args: DemuxStatsArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "demux-stats");
    let metastore_uri_resolver = MetastoreUriResolver::default();
    let metastore = metastore_uri_resolver.resolve(&args.metastore_uri).await?;
    let index_metadata = metastore.index_metadata(&args.index_id).await?;
    let demux_field_name = index_metadata
        .index_config
        .demux_field_name()
        .ok_or_else(|| anyhow::anyhow!("Index must have a demux field to get demux stats."))?;
    let split_infos = metastore
        .list_splits(&args.index_id, SplitState::Published, None, &[])
        .await?
        .into_iter()
        .map(|metadata| metadata.split_metadata)
        .collect::<Vec<_>>();
    let demux_uniq_values: HashSet<String> = split_infos
        .iter()
        .map(|metadata| {
            metadata
                .tags
                .iter()
                .filter(|tag| match_tag_field_name(&demux_field_name, tag))
                .cloned()
        })
        .flatten()
        .collect();
    println!("{}", "Statistic reports on demux:".bold());
    println!(
        "{}",
        format!(
            "- Found {} `{}` unique values in {} splits.",
            demux_uniq_values.len(),
            demux_field_name,
            split_infos.len(),
        )
    );
    // Compute split count per demux value.
    println!(
        "{}",
        format!("- Stats on split count per `{}` value.", demux_field_name)
    );
    let mut split_counts_per_demux_values = Vec::new();
    for demux_value in demux_uniq_values {
        let split_count = split_infos
            .iter()
            .filter(|split_meta| split_meta.tags.contains(&demux_value))
            .count();
        split_counts_per_demux_values.push(split_count);
    }
    print_demux_stats(&split_counts_per_demux_values);

    // Compute demux unique values count per split.
    let (non_demuxed_splits, demuxed_splits): (Vec<_>, Vec<_>) = split_infos
        .iter()
        .cloned()
        .partition(|split| split.demux_num_ops == 0);
    let non_demuxed_split_demux_values_counts = non_demuxed_splits
        .iter()
        .map(|split| {
            split
                .tags
                .iter()
                .filter(|tag| match_tag_field_name(&demux_field_name, tag))
                .count()
        })
        .sorted()
        .collect_vec();
    let demuxed_split_demux_values_counts = demuxed_splits
        .iter()
        .map(|split| {
            split
                .tags
                .iter()
                .filter(|tag| match_tag_field_name(&demux_field_name, tag))
                .count()
        })
        .sorted()
        .collect_vec();
    println!(
        "{}",
        format!(
            "- Stats on `{}` unique values count per split.",
            demux_field_name
        )
    );
    println!("  -> On {} non demuxed splits:", non_demuxed_splits.len());
    print_demux_stats(&non_demuxed_split_demux_values_counts);
    println!("  -> On {} demuxed splits:", demuxed_splits.len());
    print_demux_stats(&demuxed_split_demux_values_counts);

    Ok(())
}

fn print_demux_stats(counts: &[usize]) {
    let mean_val = mean(counts);
    let std_val = std_deviation(counts);
    let min_val = counts.iter().min().unwrap();
    let max_val = counts.iter().max().unwrap();
    println!(
        "{} ± {} in [{} … {}]:   {} ± {} in [{} … {}]",
        "Mean".green(),
        "σ".green(),
        "min".cyan(),
        "max".purple(),
        format!("{:>2}", mean_val).green(),
        format!("{}", std_val).green(),
        format!("{}", min_val).cyan(),
        format!("{}", max_val).purple(),
    );
    let q1 = percentile(counts, 1);
    let q25 = percentile(counts, 50);
    let q50 = percentile(counts, 50);
    let q75 = percentile(counts, 75);
    let q99 = percentile(counts, 75);
    println!(
        "{} [0.01, 0.25, 0.50, 0.75, 0.99] :   [{}, {}, {}, {}, {}]",
        "Quantiles".green(),
        format!("{}", q1).green(),
        format!("{}", q25).green(),
        format!("{}", q50).green(),
        format!("{}", q75).green(),
        format!("{}", q99).green(),
    );
}

fn mean(values: &[usize]) -> f32 {
    assert!(!values.is_empty());
    let sum: usize = values.iter().sum();
    sum as f32 / values.len() as f32
}

fn std_deviation(values: &[usize]) -> f32 {
    let mean = mean(values);
    let variance = values
        .iter()
        .map(|value| {
            let diff = mean - (*value as f32);
            diff * diff
        })
        .sum::<f32>()
        / values.len() as f32;
    variance.sqrt()
}

/// Return percentile of sorted values using linear interpolation.
fn percentile(sorted_values: &[usize], percent: usize) -> f32 {
    assert!(!sorted_values.is_empty());
    assert!(percent <= 100);
    if sorted_values.len() == 1 {
        return sorted_values[0] as f32;
    }
    if percent == 100 {
        return sorted_values[sorted_values.len() - 1] as f32;
    }
    let length = (sorted_values.len() - 1) as f32;
    let rank = (percent as f32 / 100f32) * length;
    let lrank = rank.floor();
    let d = rank - lrank;
    let n = lrank as usize;
    let lo = sorted_values[n] as f32;
    let hi = sorted_values[n + 1] as f32;
    lo + (hi - lo) * d
}
