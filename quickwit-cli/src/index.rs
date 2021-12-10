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

use std::collections::{HashSet, VecDeque};
use std::io::{stdout, Stdout, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use std::{env, fmt, io};

use anyhow::{bail, Context};
use chrono::Utc;
use clap::ArgMatches;
use colored::Colorize;
use itertools::Itertools;
use quickwit_actors::{ActorExitStatus, ActorHandle, ObservationType, Universe};
use quickwit_common::uri::normalize_uri;
use quickwit_common::GREEN_COLOR;
use quickwit_config::{IndexConfig, IndexerConfig, SourceConfig};
use quickwit_core::{create_index, delete_index, garbage_collect_index, reset_index};
use quickwit_index_config::match_tag_field_name;
use quickwit_indexing::actors::{IndexingPipeline, IndexingPipelineParams};
use quickwit_indexing::models::IndexingStatistics;
use quickwit_indexing::source::FileSourceParams;
use quickwit_indexing::{index_data, STD_IN_SOURCE_ID};
use quickwit_metastore::checkpoint::Checkpoint;
use quickwit_metastore::{IndexMetadata, MetastoreUriResolver, Split, SplitState};
use quickwit_proto::{SearchRequest, SearchResponse};
use quickwit_search::{single_node_search, SearchResponseRest};
use quickwit_storage::quickwit_storage_uri_resolver;
use quickwit_telemetry::payload::TelemetryEvent;
use serde_json::json;
use tracing::{debug, Level};

use crate::stats::{mean, percentile, std_deviation};
use crate::{parse_duration_with_unit, run_index_checklist, THROUGHPUT_WINDOW_SIZE};

#[derive(Debug, Eq, PartialEq)]
pub struct DescribeIndexArgs {
    pub metastore_uri: String,
    pub index_id: String,
}

#[derive(Debug, PartialEq)]
pub struct CreateIndexArgs {
    pub metastore_uri: String,
    pub index_config_uri: String,
    pub overwrite: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub struct IngestDocsArgs {
    pub metastore_uri: String,
    pub index_id: String,
    pub input_path_opt: Option<PathBuf>,
    pub data_dir_path: PathBuf,
    pub overwrite: bool,
}

#[derive(Debug, PartialEq, Eq, Default)]
pub struct SearchIndexArgs {
    pub metastore_uri: String,
    pub index_id: String,
    pub query: String,
    pub max_hits: usize,
    pub start_offset: usize,
    pub search_fields: Option<Vec<String>>,
    pub start_timestamp: Option<i64>,
    pub end_timestamp: Option<i64>,
    pub tags: Option<Vec<String>>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct DeleteIndexArgs {
    pub metastore_uri: String,
    pub index_id: String,
    pub dry_run: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub struct GarbageCollectIndexArgs {
    pub metastore_uri: String,
    pub index_id: String,
    pub grace_period: Duration,
    pub dry_run: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub struct MergeOrDemuxArgs {
    pub metastore_uri: String,
    pub index_id: String,
    pub data_dir_path: PathBuf,
}

#[derive(Debug, PartialEq)]
pub enum IndexCliCommand {
    Create(CreateIndexArgs),
    Describe(DescribeIndexArgs),
    Delete(DeleteIndexArgs),
    Demux(MergeOrDemuxArgs),
    Merge(MergeOrDemuxArgs),
    GarbageCollect(GarbageCollectIndexArgs),
    Ingest(IngestDocsArgs),
    Search(SearchIndexArgs),
}

impl IndexCliCommand {
    pub fn default_log_level(&self) -> Level {
        match self {
            Self::Ingest(_) => Level::INFO,
            Self::Merge(_) => Level::INFO,
            Self::Demux(_) => Level::INFO,
            _ => Level::ERROR,
        }
    }

    pub fn parse_cli_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches) = matches
            .subcommand()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse sub-matches."))?;
        match subcommand {
            "create" => Self::parse_create_args(submatches),
            "delete" => Self::parse_delete_args(submatches),
            "search" => Self::parse_search_args(submatches),
            "merge" => Self::parse_merge_args(submatches),
            "demux" => Self::parse_demux_args(submatches),
            "describe" => Self::parse_describe_args(submatches),
            "gc" => Self::parse_garbage_collect_args(submatches),
            "ingest" => Self::parse_ingest_args(submatches),
            _ => bail!("Index subcommand `{}` is not implemented.", subcommand),
        }
    }

    fn parse_describe_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_id = matches
            .value_of("index-id")
            .expect("`index-id` is a required arg.")
            .to_string();
        let metastore_uri = matches
            .value_of("metastore-uri")
            .map(normalize_uri)
            .expect("`metastore-uri` is a required arg.")?;
        Ok(Self::Describe(DescribeIndexArgs {
            metastore_uri,
            index_id,
        }))
    }

    fn parse_create_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let metastore_uri = matches
            .value_of("metastore-uri")
            .map(normalize_uri)
            .expect("`metastore-uri` is a required arg.")?;
        let index_config_uri = matches
            .value_of("index-config-uri")
            .map(normalize_uri)
            .expect("`index-config-uri` is a required arg.")?;
        let overwrite = matches.is_present("overwrite");

        Ok(Self::Create(CreateIndexArgs {
            metastore_uri,
            index_config_uri,
            overwrite,
        }))
    }

    fn parse_ingest_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let metastore_uri = matches
            .value_of("metastore-uri")
            .map(normalize_uri)
            .expect("`metastore-uri` is a required arg.")?;
        let index_id = matches
            .value_of("index-id")
            .expect("`index-id` is a required arg.")
            .to_string();
        let input_path_opt = matches.value_of("input-path").map(PathBuf::from);
        let data_dir_path: PathBuf = matches
            .value_of("data-dir-path")
            .map(PathBuf::from)
            .expect("`data-dir-path` is a required arg.");
        let overwrite = matches.is_present("overwrite");

        Ok(Self::Ingest(IngestDocsArgs {
            metastore_uri,
            index_id,
            input_path_opt,
            data_dir_path,
            overwrite,
        }))
    }

    fn parse_search_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let metastore_uri = matches
            .value_of("metastore-uri")
            .map(normalize_uri)
            .expect("`metastore-uri` is a required arg.")?;
        let index_id = matches
            .value_of("index-id")
            .expect("`index-id` is a required arg.")
            .to_string();
        let query = matches
            .value_of("query")
            .context("`query` is a required arg.")?
            .to_string();
        let max_hits = matches.value_of_t::<usize>("max-hits")?;
        let start_offset = matches.value_of_t::<usize>("start-offset")?;
        let search_fields = matches
            .values_of("search-fields")
            .map(|values| values.map(|value| value.to_string()).collect());
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
        let tags = matches
            .values_of("tags")
            .map(|values| values.map(|value| value.to_string()).collect());

        Ok(Self::Search(SearchIndexArgs {
            index_id,
            query,
            max_hits,
            start_offset,
            search_fields,
            start_timestamp,
            end_timestamp,
            tags,
            metastore_uri,
        }))
    }

    fn parse_merge_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let metastore_uri = matches
            .value_of("metastore-uri")
            .context("'metastore-uri' is a required arg.")
            .map(normalize_uri)??;
        let index_id = matches
            .value_of("index-id")
            .context("'index-id' is a required arg.")?
            .to_string();
        let data_dir_path: PathBuf = matches
            .value_of("data-dir-path")
            .map(PathBuf::from)
            .expect("`data-dir-path` is a required arg.");
        Ok(Self::Merge(MergeOrDemuxArgs {
            metastore_uri,
            index_id,
            data_dir_path,
        }))
    }

    fn parse_demux_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let metastore_uri = matches
            .value_of("metastore-uri")
            .context("'metastore-uri' is a required arg.")
            .map(normalize_uri)??;
        let index_id = matches
            .value_of("index-id")
            .context("'index-id' is a required arg.")?
            .to_string();
        let data_dir_path: PathBuf = matches
            .value_of("data-dir-path")
            .map(PathBuf::from)
            .expect("`data-dir-path` is a required arg.");
        Ok(Self::Demux(MergeOrDemuxArgs {
            metastore_uri,
            index_id,
            data_dir_path,
        }))
    }

    fn parse_garbage_collect_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let metastore_uri = matches
            .value_of("metastore-uri")
            .map(normalize_uri)
            .expect("`metastore-uri` is a required arg.")?;
        let index_id = matches
            .value_of("index-id")
            .expect("`index-id` is a required arg.")
            .to_string();
        let grace_period = matches
            .value_of("grace-period")
            .map(parse_duration_with_unit)
            .expect("`grace-period` should have a default value.")?;
        let dry_run = matches.is_present("dry-run");

        Ok(Self::GarbageCollect(GarbageCollectIndexArgs {
            index_id,
            grace_period,
            metastore_uri,
            dry_run,
        }))
    }

    fn parse_delete_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let metastore_uri = matches
            .value_of("metastore-uri")
            .map(normalize_uri)
            .expect("`metastore-uri` is a required arg.")?;
        let index_id = matches
            .value_of("index-id")
            .expect("`index-id` is a required arg.")
            .to_string();
        let dry_run = matches.is_present("dry-run");

        Ok(Self::Delete(DeleteIndexArgs {
            index_id,
            metastore_uri,
            dry_run,
        }))
    }

    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            Self::Create(args) => create_index_cli(args).await,
            Self::Describe(args) => describe_index_cli(args).await,
            Self::Ingest(args) => ingest_docs_cli(args).await,
            Self::Search(args) => search_index_cli(args).await,
            Self::Merge(args) => merge_or_demux_cli(args, true, false).await,
            Self::Demux(args) => merge_or_demux_cli(args, false, true).await,
            Self::GarbageCollect(args) => garbage_collect_index_cli(args).await,
            Self::Delete(args) => delete_index_cli(args).await,
        }
    }
}

pub async fn describe_index_cli(args: DescribeIndexArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "describe");
    let metastore_uri_resolver = MetastoreUriResolver::default();
    let metastore = metastore_uri_resolver.resolve(&args.metastore_uri).await?;
    let index_metadata = metastore.index_metadata(&args.index_id).await?;
    let splits = metastore
        .list_splits(&args.index_id, SplitState::Published, None, &[])
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
    println!("1. General infos");
    println!("===============================================================================");
    println!(
        "{:<35} {}",
        "Index id:".color(GREEN_COLOR),
        index_metadata.index_id
    );
    println!(
        "{:<35} {}",
        "Index uri:".color(GREEN_COLOR),
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
    println!("2. Statistics on splits");
    println!("===============================================================================");
    println!("Document count stats:");
    print_descriptive_stats(&splits_num_docs);
    println!();
    println!("Size in MB stats:");
    print_descriptive_stats(&splits_bytes);

    if let Some(demux_field_name) = &index_metadata.indexing_settings.demux_field {
        show_demux_stats(demux_field_name, &splits).await;
    }

    println!();
    Ok(())
}

pub async fn show_demux_stats(demux_field_name: &str, splits: &[Split]) {
    println!();
    println!("3. Demux stats");
    println!("===============================================================================");
    let demux_uniq_values: HashSet<String> = splits
        .iter()
        .map(|split| {
            split
                .split_metadata
                .tags
                .iter()
                .filter(|tag| match_tag_field_name(demux_field_name, tag))
                .cloned()
        })
        .flatten()
        .collect();
    println!(
        "{:<35} {}",
        "Demux field name:".color(GREEN_COLOR),
        demux_field_name
    );
    println!(
        "{:<35} {}",
        "Demux unique values count:".color(GREEN_COLOR),
        demux_uniq_values.len()
    );
    println!();
    println!("3.1 Split count per `{}` value", demux_field_name);
    println!("-------------------------------------------------");
    let mut split_counts_per_demux_values = Vec::new();
    for demux_value in demux_uniq_values {
        let split_count = splits
            .iter()
            .filter(|split| split.split_metadata.tags.contains(&demux_value))
            .count();
        split_counts_per_demux_values.push(split_count);
    }
    print_descriptive_stats(&split_counts_per_demux_values);

    let (non_demuxed_splits, demuxed_splits): (Vec<_>, Vec<_>) = splits
        .iter()
        .cloned()
        .partition(|split| split.split_metadata.demux_num_ops == 0);
    let non_demuxed_split_demux_values_counts = non_demuxed_splits
        .iter()
        .map(|split| {
            split
                .split_metadata
                .tags
                .iter()
                .filter(|tag| match_tag_field_name(demux_field_name, tag))
                .count()
        })
        .sorted()
        .collect_vec();
    let demuxed_split_demux_values_counts = demuxed_splits
        .iter()
        .map(|split| {
            split
                .split_metadata
                .tags
                .iter()
                .filter(|tag| match_tag_field_name(demux_field_name, tag))
                .count()
        })
        .sorted()
        .collect_vec();
    println!();
    println!("3.2 Demux unique values count per split");
    println!("-------------------------------------------------");
    println!(
        "{:<35} {}",
        "Non demux splits count:".color(GREEN_COLOR),
        non_demuxed_splits.len()
    );
    println!(
        "{:<35} {}",
        "Demux splits count:".color(GREEN_COLOR),
        demuxed_splits.len()
    );
    if !non_demuxed_splits.is_empty() {
        println!();
        println!("Stats on non demuxed splits:");
        print_descriptive_stats(&non_demuxed_split_demux_values_counts);
    }
    if !demuxed_splits.is_empty() {
        println!();
        println!("Stats on demuxed splits:");
        print_descriptive_stats(&demuxed_split_demux_values_counts);
    }
}

fn print_descriptive_stats(values: &[usize]) {
    let mean_val = mean(values);
    let std_val = std_deviation(values);
    let min_val = values.iter().min().unwrap();
    let max_val = values.iter().max().unwrap();
    println!(
        "{:<35} {} ± {} in [{} … {}]",
        "Mean ± σ in [min … max]:".color(GREEN_COLOR),
        format!("{:>2}", mean_val),
        format!("{}", std_val),
        format!("{}", min_val),
        format!("{}", max_val),
    );
    let q1 = percentile(values, 1);
    let q25 = percentile(values, 50);
    let q50 = percentile(values, 50);
    let q75 = percentile(values, 75);
    let q99 = percentile(values, 75);
    println!(
        "{:<35} [{}, {}, {}, {}, {}]",
        "Quantiles [1%, 25%, 50%, 75%, 99%]:".color(GREEN_COLOR),
        format!("{}", q1),
        format!("{}", q25),
        format!("{}", q50),
        format!("{}", q75),
        format!("{}", q99),
    );
}

pub async fn create_index_cli(args: CreateIndexArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "create-index");
    quickwit_telemetry::send_telemetry_event(TelemetryEvent::Create).await;
    let index_config = IndexConfig::from_file(&args.index_config_uri).await?;

    if args.overwrite {
        delete_index(&args.metastore_uri, &index_config.index_id, false).await?;
    }
    let index_metadata = IndexMetadata {
        index_id: index_config.index_id.clone(),
        index_uri: index_config.index_uri,
        checkpoint: Checkpoint::default(),
        doc_mapping: index_config.doc_mapping,
        indexing_settings: index_config.indexing_settings,
        search_settings: index_config.search_settings,
        sources: index_config.sources,
        create_timestamp: Utc::now().timestamp(),
    };
    create_index(&args.metastore_uri, index_metadata).await?;
    println!("Index `{}` successfully created.", index_config.index_id);
    Ok(())
}

pub async fn ingest_docs_cli(args: IngestDocsArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "ingest-docs");
    quickwit_telemetry::send_telemetry_event(TelemetryEvent::Ingest).await;

    // Override index source config(s) with ad-hoc source config.
    let source_id = args
        .input_path_opt
        .as_ref()
        .map(|_| "file-source")
        .unwrap_or(STD_IN_SOURCE_ID)
        .to_string();
    let source_type = "file".to_string();
    let params = serde_json::to_value(FileSourceParams {
        filepath: args.input_path_opt.clone(),
    })?;
    let source_config = SourceConfig {
        source_id,
        source_type,
        params,
    };
    run_index_checklist(&args.metastore_uri, &args.index_id, Some(&source_config)).await?;
    let metastore_uri_resolver = MetastoreUriResolver::default();
    let metastore = metastore_uri_resolver.resolve(&args.metastore_uri).await?;
    let mut index_metadata = metastore.index_metadata(&args.index_id).await?;
    let storage_uri_resolver = quickwit_storage_uri_resolver();
    let storage = storage_uri_resolver.resolve(&index_metadata.index_uri)?;

    // Override index source config(s) with ad-hoc source config.
    let source_id = args
        .input_path_opt
        .as_ref()
        .map(|_| "file-source")
        .unwrap_or(STD_IN_SOURCE_ID)
        .to_string();
    let source_type = "file".to_string();
    let params = serde_json::to_value(FileSourceParams {
        filepath: args.input_path_opt.clone(),
    })?;
    let source_config = SourceConfig {
        source_id,
        source_type,
        params,
    };
    index_metadata.sources = vec![source_config.clone()];
    let indexer_config = IndexerConfig {
        data_dir_path: args.data_dir_path,
        ..Default::default()
    };

    if args.overwrite {
        reset_index(&index_metadata, metastore.clone(), storage.clone()).await?;
    }
    let pipeline_params =
        IndexingPipelineParams::try_new(index_metadata, indexer_config, metastore, storage).await?;
    let pipeline = IndexingPipeline::new(pipeline_params);
    let universe = Universe::new();
    let (_pipeline_mailbox, pipeline_handle) = universe.spawn_actor(pipeline).spawn_async();

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
        start_statistics_reporting_loop(pipeline_handle, args.input_path_opt.clone()).await?;
    if statistics.num_published_splits > 0 {
        println!(
            "Now, you can query the index with the following command:\nquickwit index search \
             --index-id {} --metastore-uri {} --query \"my query\"",
            args.index_id, args.metastore_uri
        );
    }
    Ok(())
}

pub async fn search_index(args: SearchIndexArgs) -> anyhow::Result<SearchResponse> {
    debug!(args = ?args, "search-index");
    let storage_uri_resolver = quickwit_storage_uri_resolver();
    let metastore_uri_resolver = MetastoreUriResolver::default();
    let metastore = metastore_uri_resolver.resolve(&args.metastore_uri).await?;
    let search_request = SearchRequest {
        index_id: args.index_id,
        query: args.query.clone(),
        search_fields: args.search_fields.unwrap_or_default(),
        start_timestamp: args.start_timestamp,
        end_timestamp: args.end_timestamp,
        max_hits: args.max_hits as u64,
        start_offset: args.start_offset as u64,
        tags: args.tags.unwrap_or_default(),
    };
    let search_response: SearchResponse =
        single_node_search(&search_request, &*metastore, storage_uri_resolver.clone()).await?;
    Ok(search_response)
}

pub async fn search_index_cli(args: SearchIndexArgs) -> anyhow::Result<()> {
    let search_response: SearchResponse = search_index(args).await?;
    let search_response_rest = SearchResponseRest::try_from(search_response)?;
    let search_response_rest_json = serde_json::to_string_pretty(&search_response_rest)?;
    println!("{}", search_response_rest_json);
    Ok(())
}

pub async fn merge_or_demux_cli(
    args: MergeOrDemuxArgs,
    merge_enabled: bool,
    demux_enabled: bool,
) -> anyhow::Result<()> {
    debug!(args = ?args, merge_enabled=merge_enabled, demux_enabled=demux_enabled, "run-merge-operations");
    let source_config = SourceConfig {
        source_id: "void-source".to_string(),
        source_type: "void".to_string(),
        params: json!(null),
    };
    run_index_checklist(&args.metastore_uri, &args.index_id, Some(&source_config)).await?;
    let metastore_uri_resolver = MetastoreUriResolver::default();
    let metastore = metastore_uri_resolver.resolve(&args.metastore_uri).await?;
    let mut index_metadata = metastore.index_metadata(&args.index_id).await?;
    let storage_uri_resolver = quickwit_storage_uri_resolver();
    let storage = storage_uri_resolver.resolve(&index_metadata.index_uri)?;
    index_metadata.sources = vec![source_config];
    let indexer_config = IndexerConfig {
        data_dir_path: args.data_dir_path,
        ..Default::default()
    };
    index_metadata.indexing_settings.demux_enabled = demux_enabled;
    index_metadata.indexing_settings.merge_enabled = merge_enabled;
    index_data(index_metadata, indexer_config, metastore, storage).await?;
    Ok(())
}

pub async fn delete_index_cli(args: DeleteIndexArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "delete-index");
    quickwit_telemetry::send_telemetry_event(TelemetryEvent::Delete).await;

    let affected_files = delete_index(&args.metastore_uri, &args.index_id, args.dry_run).await?;
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
    println!("Index `{}` successfully deleted.", args.index_id);
    Ok(())
}

pub async fn garbage_collect_index_cli(args: GarbageCollectIndexArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "garbage-collect-index");
    quickwit_telemetry::send_telemetry_event(TelemetryEvent::GarbageCollect).await;

    let deleted_files = garbage_collect_index(
        &args.metastore_uri,
        &args.index_id,
        args.grace_period,
        args.dry_run,
    )
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
    input_path_opt: Option<PathBuf>,
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

    match pipeline_exit_status {
        ActorExitStatus::Success => {}
        ActorExitStatus::Quit
        | ActorExitStatus::DownstreamClosed
        | ActorExitStatus::Killed
        | ActorExitStatus::Panicked => {
            bail!(pipeline_exit_status)
        }
        ActorExitStatus::Failure(err) => {
            bail!(err);
        }
    }

    // If we have received zero docs at this point,
    // there is no point in displaying report.
    if pipeline_statistics.num_docs == 0 {
        return Ok(pipeline_statistics);
    }

    if input_path_opt.is_none() {
        display_statistics(
            &mut stdout_handle,
            &mut throughput_calculator,
            &pipeline_statistics,
        )?;
    }
    // display end of task report
    println!();
    let elapsed_secs = start_time.elapsed().as_secs();
    if elapsed_secs >= 60 {
        println!(
            "Indexed {} documents in {:.2$}min.",
            pipeline_statistics.num_docs,
            elapsed_secs.max(1) as f64 / 60f64,
            2
        );
    } else {
        println!(
            "Indexed {} documents in {}s.",
            pipeline_statistics.num_docs,
            elapsed_secs.max(1)
        );
    }
    Ok(pipeline_statistics)
}

struct Printer<'a> {
    pub stdout: &'a mut Stdout,
}

impl<'a> Printer<'a> {
    fn print_header(&mut self, header: &str) -> io::Result<()> {
        write!(&mut self.stdout, " {}", header.bright_blue())?;
        Ok(())
    }

    fn print_value(&mut self, fmt_args: fmt::Arguments) -> io::Result<()> {
        write!(&mut self.stdout, " {}", fmt_args)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.stdout.flush()
    }
}

fn display_statistics(
    stdout: &mut Stdout,
    throughput_calculator: &mut ThroughputCalculator,
    statistics: &IndexingStatistics,
) -> anyhow::Result<()> {
    let elapsed_duration = chrono::Duration::from_std(throughput_calculator.elapsed_time())?;
    let elapsed_time = format!(
        "{:02}:{:02}:{:02}",
        elapsed_duration.num_hours(),
        elapsed_duration.num_minutes() % 60,
        elapsed_duration.num_seconds() % 60
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
