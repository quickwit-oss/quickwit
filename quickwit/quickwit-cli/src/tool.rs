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

use std::collections::{HashSet, VecDeque};
use std::io::{IsTerminal, Stdout, Write, stdout};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, Instant};
use std::{env, fmt, io};

use anyhow::{Context, bail};
use clap::{ArgMatches, Command, arg};
use colored::{ColoredString, Colorize};
use humantime::format_duration;
use quickwit_actors::{ActorExitStatus, ActorHandle, Mailbox, Universe};
use quickwit_cluster::{
    ChannelTransport, Cluster, ClusterMember, FailureDetectorConfig, make_client_grpc_config,
};
use quickwit_common::pubsub::EventBroker;
use quickwit_common::runtimes::RuntimesConfig;
use quickwit_common::uri::Uri;
use quickwit_config::service::QuickwitService;
use quickwit_config::{
    CLI_SOURCE_ID, IndexerConfig, NodeConfig, SourceConfig, SourceInputFormat, SourceParams,
    TransformConfig, VecSourceParams,
};
use quickwit_index_management::{IndexService, clear_cache_directory};
use quickwit_indexing::IndexingPipeline;
use quickwit_indexing::actors::{IndexingService, MergePipeline, MergeSchedulerService};
use quickwit_indexing::models::{
    DetachIndexingPipeline, DetachMergePipeline, IndexingStatistics, SpawnPipeline,
};
use quickwit_ingest::IngesterPool;
use quickwit_metastore::IndexMetadataResponseExt;
use quickwit_proto::indexing::CpuCapacity;
use quickwit_proto::metastore::{IndexMetadataRequest, MetastoreService, MetastoreServiceClient};
use quickwit_proto::search::{CountHits, SearchResponse};
use quickwit_proto::types::{IndexId, PipelineUid, SourceId, SplitId};
use quickwit_search::{SearchResponseRest, single_node_search};
use quickwit_serve::{
    BodyFormat, SearchRequestQueryString, SortBy, search_request_from_api_request,
};
use quickwit_storage::{BundleStorage, Storage};
use thousands::Separable;
use tracing::{debug, info};

use crate::checklist::{GREEN_COLOR, RED_COLOR};
use crate::{
    THROUGHPUT_WINDOW_SIZE, config_cli_arg, get_resolvers, load_node_config, run_index_checklist,
    start_actor_runtimes,
};

pub fn build_tool_command() -> Command {
    Command::new("tool")
        .about("Performs utility operations. Requires a node config.")
        .arg(config_cli_arg())
        .subcommand(
            Command::new("local-ingest")
                .display_order(10)
                .about("Indexes NDJSON documents locally.")
                .long_about("Local ingest indexes locally NDJSON documents from a file or from stdin and uploads splits on the configured storage.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index")
                        .display_order(1)
                        .required(true),
                    arg!(--"input-path" <INPUT_PATH> "Location of the input file.")
                        .required(false),
                    arg!(--"input-format" <INPUT_FORMAT> "Format of the input data.")
                        .default_value("json")
                        .required(false),
                    arg!(--overwrite "Overwrites pre-existing index.")
                        .required(false),
                    arg!(--"transform-script" <SCRIPT> "VRL program to transform docs before ingesting.")
                        .required(false),
                    arg!(--"keep-cache" "Does not clear local cache directory upon completion.")
                        .required(false),
                ])
            )
        .subcommand(
            Command::new("local-search")
                .display_order(10)
                .about("Searches an index locally.")
                .long_about("Searchers an index directly on the configured storage without using a server.")
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
                    arg!(--"sort-by-field" <SORT_BY_FIELD> "Sort by field.")
                        .required(false),
                ])
            )
        .subcommand(
            Command::new("extract-split")
                .about("Downloads and extracts a split to a directory.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index")
                        .display_order(1)
                        .required(true),
                    arg!(--split <SPLIT> "ID of the target split")
                        .display_order(2)
                        .required(true),
                    arg!(--"target-dir" <TARGET_DIR> "Directory to extract the split to."),
                ])
            )
        .subcommand(
            Command::new("gc")
                .display_order(10)
                .about("Garbage collects stale staged splits and splits marked for deletion.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index")
                        .display_order(1)
                        .required(true),
                    arg!(--"grace-period" <GRACE_PERIOD> "Threshold period after which stale staged splits are garbage collected.")
                        .default_value("1h")
                        .required(false),
                    arg!(--"dry-run" "Executes the command in dry run mode and only displays the list of splits candidates for garbage collection.")
                        .required(false),
                ])
            )
        .subcommand(
            Command::new("merge")
                .display_order(10)
                .about("Merges all the splits for a given Node ID, index ID, source ID.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index.")
                        .display_order(1)
                        .required(true),
                    arg!(--source <SOURCE_ID> "ID of the target source.")
                        .display_order(2)
                        .required(true),
                ])
            )
        .arg_required_else_help(true)
}

#[derive(Debug, Eq, PartialEq)]
pub struct LocalIngestDocsArgs {
    pub config_uri: Uri,
    pub index_id: IndexId,
    pub input_path_opt: Option<Uri>,
    pub input_format: SourceInputFormat,
    pub overwrite: bool,
    pub vrl_script: Option<String>,
    pub clear_cache: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct LocalSearchArgs {
    pub config_uri: Uri,
    pub index_id: IndexId,
    pub query: String,
    pub aggregation: Option<String>,
    pub max_hits: usize,
    pub start_offset: usize,
    pub search_fields: Option<Vec<String>>,
    pub snippet_fields: Option<Vec<String>>,
    pub start_timestamp: Option<i64>,
    pub end_timestamp: Option<i64>,
    pub sort_by_field: Option<String>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct GarbageCollectIndexArgs {
    pub config_uri: Uri,
    pub index_id: IndexId,
    pub grace_period: Duration,
    pub dry_run: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct MergeArgs {
    pub config_uri: Uri,
    pub index_id: IndexId,
    pub source_id: SourceId,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ExtractSplitArgs {
    pub config_uri: Uri,
    pub index_id: IndexId,
    pub split_id: SplitId,
    pub target_dir: PathBuf,
}

#[derive(Debug, Eq, PartialEq)]
pub enum ToolCliCommand {
    GarbageCollect(GarbageCollectIndexArgs),
    LocalIngest(LocalIngestDocsArgs),
    LocalSearch(LocalSearchArgs),
    Merge(MergeArgs),
    ExtractSplit(ExtractSplitArgs),
}

impl ToolCliCommand {
    pub fn parse_cli_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches) = matches
            .remove_subcommand()
            .context("failed to parse tool subcommand")?;
        match subcommand.as_str() {
            "gc" => Self::parse_garbage_collect_args(submatches),
            "local-ingest" => Self::parse_local_ingest_args(submatches),
            "local-search" => Self::parse_local_search_args(submatches),
            "merge" => Self::parse_merge_args(submatches),
            "extract-split" => Self::parse_extract_split_args(submatches),
            _ => bail!("unknown tool subcommand `{subcommand}`"),
        }
    }

    fn parse_local_ingest_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let config_uri = matches
            .remove_one::<String>("config")
            .map(|uri_str| Uri::from_str(&uri_str))
            .expect("`config` should be a required arg.")?;
        let index_id = matches
            .remove_one::<String>("index")
            .expect("`index` should be a required arg.");
        let input_path_opt = if let Some(input_path) = matches.remove_one::<String>("input-path") {
            Some(Uri::from_str(&input_path)?)
        } else {
            None
        };

        let input_format = matches
            .remove_one::<String>("input-format")
            .map(|input_format| SourceInputFormat::from_str(&input_format))
            .expect("`input-format` should have a default value.")
            .map_err(|err| anyhow::anyhow!(err))?;
        let overwrite = matches.get_flag("overwrite");
        let vrl_script = matches.remove_one::<String>("transform-script");
        let clear_cache = !matches.get_flag("keep-cache");

        Ok(Self::LocalIngest(LocalIngestDocsArgs {
            config_uri,
            index_id,
            input_path_opt,
            input_format,
            overwrite,
            vrl_script,
            clear_cache,
        }))
    }

    fn parse_local_search_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let config_uri = matches
            .remove_one::<String>("config")
            .map(|uri_str| Uri::from_str(&uri_str))
            .expect("`config` should be a required arg.")?;
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
        let sort_by_field = matches.remove_one::<String>("sort-by-field");
        let start_timestamp = matches
            .remove_one::<String>("start-timestamp")
            .map(|ts| ts.parse())
            .transpose()?;
        let end_timestamp = matches
            .remove_one::<String>("end-timestamp")
            .map(|ts| ts.parse())
            .transpose()?;
        Ok(Self::LocalSearch(LocalSearchArgs {
            config_uri,
            index_id,
            query,
            aggregation,
            max_hits,
            start_offset,
            search_fields,
            snippet_fields,
            start_timestamp,
            end_timestamp,
            sort_by_field,
        }))
    }

    fn parse_merge_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let config_uri = matches
            .remove_one::<String>("config")
            .map(|uri_str| Uri::from_str(&uri_str))
            .expect("`config` should be a required arg.")?;
        let index_id = matches
            .remove_one::<String>("index")
            .expect("'index-id' should be a required arg.");
        let source_id = matches
            .remove_one::<String>("source")
            .expect("'source-id' should be a required arg.");
        Ok(Self::Merge(MergeArgs {
            index_id,
            source_id,
            config_uri,
        }))
    }

    fn parse_garbage_collect_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let config_uri = matches
            .get_one("config")
            .map(|uri_str: &String| Uri::from_str(uri_str))
            .expect("`config` should be a required arg.")?;
        let index_id = matches
            .remove_one::<String>("index")
            .expect("`index` should be a required arg.");
        let grace_period = matches
            .get_one("grace-period")
            .map(|duration_str: &String| humantime::parse_duration(duration_str))
            .expect("`grace-period` should have a default value.")?;
        let dry_run = matches.get_flag("dry-run");
        Ok(Self::GarbageCollect(GarbageCollectIndexArgs {
            index_id,
            grace_period,
            dry_run,
            config_uri,
        }))
    }

    fn parse_extract_split_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let index_id = matches
            .remove_one::<String>("index")
            .expect("`index` should be a required arg.");
        let split_id = matches
            .remove_one::<String>("split")
            .expect("`split` should be a required arg.");
        let config_uri = matches
            .remove_one::<String>("config")
            .map(|uri_str| Uri::from_str(&uri_str))
            .expect("`config` should be a required arg.")?;
        let target_dir = matches
            .remove_one::<String>("target-dir")
            .map(PathBuf::from)
            .expect("`target-dir` should be a required arg.");
        Ok(Self::ExtractSplit(ExtractSplitArgs {
            config_uri,
            index_id,
            split_id,
            target_dir,
        }))
    }

    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            Self::GarbageCollect(args) => garbage_collect_index_cli(args).await,
            Self::LocalIngest(args) => local_ingest_docs_cli(args).await,
            Self::LocalSearch(args) => local_search_cli(args).await,
            Self::Merge(args) => merge_cli(args).await,
            Self::ExtractSplit(args) => extract_split_cli(args).await,
        }
    }
}

pub async fn local_ingest_docs_cli(args: LocalIngestDocsArgs) -> anyhow::Result<()> {
    debug!(args=?args, "local-ingest-docs");
    println!("❯ Ingesting documents locally...");

    let config = load_node_config(&args.config_uri).await?;
    let (storage_resolver, metastore_resolver) =
        get_resolvers(&config.storage_configs, &config.metastore_configs);
    let mut metastore = metastore_resolver.resolve(&config.metastore_uri).await?;

    let source_params = if let Some(uri) = args.input_path_opt.as_ref() {
        SourceParams::file_from_uri(uri.clone())
    } else {
        SourceParams::stdin()
    };
    let transform_config = args
        .vrl_script
        .map(|vrl_script| TransformConfig::new(vrl_script, None));
    let source_config = SourceConfig {
        source_id: CLI_SOURCE_ID.to_string(),
        num_pipelines: NonZeroUsize::MIN,
        enabled: true,
        source_params,
        transform_config,
        input_format: args.input_format,
    };
    run_index_checklist(
        &mut metastore,
        &storage_resolver,
        &args.index_id,
        Some(&source_config),
    )
    .await?;

    if args.overwrite {
        let mut index_service = IndexService::new(metastore.clone(), storage_resolver.clone());
        index_service.clear_index(&args.index_id).await?;
    }
    // The indexing service needs to update its cluster chitchat state so that the control plane is
    // aware of the running tasks. We thus create a fake cluster to instantiate the indexing service
    // and avoid impacting potential control plane running on the cluster.
    let cluster = create_empty_cluster(&config).await?;
    let indexer_config = IndexerConfig {
        ..Default::default()
    };
    let runtimes_config = RuntimesConfig::default();
    start_actor_runtimes(
        runtimes_config,
        &HashSet::from_iter([QuickwitService::Indexer]),
    )?;
    let universe = Universe::new();
    let merge_scheduler_service_mailbox = universe.get_or_spawn_one();
    let indexing_server = IndexingService::new(
        config.node_id.clone(),
        config.data_dir_path.clone(),
        indexer_config,
        runtimes_config.num_threads_blocking,
        cluster,
        metastore,
        None,
        merge_scheduler_service_mailbox,
        IngesterPool::default(),
        storage_resolver,
        EventBroker::default(),
    )
    .await?;
    let (indexing_server_mailbox, indexing_server_handle) =
        universe.spawn_builder().spawn(indexing_server);
    let pipeline_id = indexing_server_mailbox
        .ask_for_res(SpawnPipeline {
            index_id: args.index_id.clone(),
            source_config,
            pipeline_uid: PipelineUid::random(),
        })
        .await?;
    let merge_pipeline_handle = indexing_server_mailbox
        .ask_for_res(DetachMergePipeline {
            pipeline_id: pipeline_id.merge_pipeline_id(),
        })
        .await?;
    let indexing_pipeline_handle = indexing_server_mailbox
        .ask_for_res(DetachIndexingPipeline { pipeline_id })
        .await?;

    if args.input_path_opt.is_none() && io::stdin().is_terminal() {
        let eof_shortcut = match env::consts::OS {
            "windows" => "CTRL+Z",
            _ => "CTRL+D",
        };
        println!(
            "Please, enter JSON documents one line at a time.\nEnd your input using \
             {eof_shortcut}."
        );
    }
    let statistics =
        start_statistics_reporting_loop(indexing_pipeline_handle, args.input_path_opt.is_none())
            .await?;
    merge_pipeline_handle
        .mailbox()
        .ask(quickwit_indexing::FinishPendingMergesAndShutdownPipeline)
        .await?;
    merge_pipeline_handle.join().await;
    // Shutdown the indexing server.
    universe
        .send_exit_with_success(&indexing_server_mailbox)
        .await?;
    indexing_server_handle.join().await;
    universe.quit().await;
    if statistics.num_published_splits > 0 {
        println!(
            "Now, you can query the index with the following command:\nquickwit index search \
             --index {} --config ./config/quickwit.yaml --query \"my query\"",
            args.index_id
        );
    }

    if args.clear_cache {
        println!("Clearing local cache directory...");
        clear_cache_directory(&config.data_dir_path).await?;
        println!("{} Local cache directory cleared.", "✔".color(GREEN_COLOR));
    }

    match statistics.num_invalid_docs {
        0 => {
            println!("{} Documents successfully indexed.", "✔".color(GREEN_COLOR));
            Ok(())
        }
        _ => bail!("failed to ingest all the documents"),
    }
}

pub async fn local_search_cli(args: LocalSearchArgs) -> anyhow::Result<()> {
    debug!(args=?args, "local-search");
    println!("❯ Searching directly on the index storage (without calling REST API)...");
    let config = load_node_config(&args.config_uri).await?;
    let (storage_resolver, metastore_resolver) =
        get_resolvers(&config.storage_configs, &config.metastore_configs);
    let metastore: MetastoreServiceClient =
        metastore_resolver.resolve(&config.metastore_uri).await?;
    let aggs = args
        .aggregation
        .map(|agg_string| serde_json::from_str(&agg_string))
        .transpose()?;
    let sort_by: SortBy = args.sort_by_field.map(SortBy::from).unwrap_or_default();
    let search_request_query_string = SearchRequestQueryString {
        query: args.query,
        start_offset: args.start_offset as u64,
        max_hits: args.max_hits as u64,
        search_fields: args.search_fields,
        snippet_fields: args.snippet_fields,
        start_timestamp: args.start_timestamp,
        end_timestamp: args.end_timestamp,
        aggs,
        format: BodyFormat::Json,
        sort_by,
        count_all: CountHits::CountAll,
        allow_failed_splits: false,
    };
    let search_request =
        search_request_from_api_request(vec![args.index_id], search_request_query_string)?;
    debug!(search_request=?search_request, "search-request");
    let search_response: SearchResponse =
        single_node_search(search_request, metastore, storage_resolver).await?;
    let search_response_rest = SearchResponseRest::try_from(search_response)?;
    let search_response_json = serde_json::to_string_pretty(&search_response_rest)?;
    println!("{search_response_json}");
    Ok(())
}

pub async fn merge_cli(args: MergeArgs) -> anyhow::Result<()> {
    debug!(args=?args, "run-merge-operations");
    println!("❯ Merging splits locally...");
    let config = load_node_config(&args.config_uri).await?;
    let (storage_resolver, metastore_resolver) =
        get_resolvers(&config.storage_configs, &config.metastore_configs);
    let mut metastore = metastore_resolver.resolve(&config.metastore_uri).await?;
    run_index_checklist(&mut metastore, &storage_resolver, &args.index_id, None).await?;
    // The indexing service needs to update its cluster chitchat state so that the control plane is
    // aware of the running tasks. We thus create a fake cluster to instantiate the indexing service
    // and avoid impacting potential control plane running on the cluster.
    let cluster = create_empty_cluster(&config).await?;
    let runtimes_config = RuntimesConfig::default();
    start_actor_runtimes(
        runtimes_config,
        &HashSet::from_iter([QuickwitService::Indexer]),
    )?;
    let indexer_config = IndexerConfig::default();
    let universe = Universe::new();
    let merge_scheduler_service: Mailbox<MergeSchedulerService> = universe.get_or_spawn_one();
    let indexing_server = IndexingService::new(
        config.node_id,
        config.data_dir_path,
        indexer_config,
        runtimes_config.num_threads_blocking,
        cluster,
        metastore,
        None,
        merge_scheduler_service,
        IngesterPool::default(),
        storage_resolver,
        EventBroker::default(),
    )
    .await?;
    let (indexing_service_mailbox, indexing_service_handle) =
        universe.spawn_builder().spawn(indexing_server);
    let pipeline_id = indexing_service_mailbox
        .ask_for_res(SpawnPipeline {
            index_id: args.index_id,
            source_config: SourceConfig {
                source_id: args.source_id,
                num_pipelines: NonZeroUsize::MIN,
                enabled: true,
                source_params: SourceParams::Vec(VecSourceParams::default()),
                transform_config: None,
                input_format: SourceInputFormat::Json,
            },
            pipeline_uid: PipelineUid::random(),
        })
        .await?;
    let pipeline_handle: ActorHandle<MergePipeline> = indexing_service_mailbox
        .ask_for_res(DetachMergePipeline {
            pipeline_id: pipeline_id.merge_pipeline_id(),
        })
        .await?;

    let mut check_interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        check_interval.tick().await;

        pipeline_handle.refresh_observe();
        let observation = pipeline_handle.last_observation();

        if observation.num_ongoing_merges == 0 {
            info!("merge pipeline has no more ongoing merges, exiting");
            break;
        }

        if pipeline_handle.state().is_exit() {
            info!("merge pipeline has exited, exiting");
            break;
        }
    }

    let (pipeline_exit_status, _pipeline_statistics) = pipeline_handle.quit().await;
    indexing_service_handle.quit().await;
    if !matches!(
        pipeline_exit_status,
        ActorExitStatus::Success | ActorExitStatus::Quit
    ) {
        bail!(pipeline_exit_status);
    }
    println!("{} Merge successful.", "✔".color(GREEN_COLOR));
    Ok(())
}

pub async fn garbage_collect_index_cli(args: GarbageCollectIndexArgs) -> anyhow::Result<()> {
    debug!(args=?args, "garbage-collect-index");
    println!("❯ Garbage collecting index...");

    let config = load_node_config(&args.config_uri).await?;
    let (storage_resolver, metastore_resolver) =
        get_resolvers(&config.storage_configs, &config.metastore_configs);
    let metastore = metastore_resolver.resolve(&config.metastore_uri).await?;
    let mut index_service = IndexService::new(metastore, storage_resolver);
    let removal_info = index_service
        .garbage_collect_index(&args.index_id, args.grace_period, args.dry_run)
        .await?;
    if removal_info.removed_split_entries.is_empty() && removal_info.failed_splits.is_empty() {
        println!("No dangling files to garbage collect.");
        return Ok(());
    }

    if args.dry_run {
        println!("The following files will be garbage collected.");
        for split_info in removal_info.removed_split_entries {
            println!(" - {}", split_info.file_name.display());
        }
        return Ok(());
    }

    if !removal_info.failed_splits.is_empty() {
        println!("The following splits were attempted to be removed, but failed.");
        for split_info in &removal_info.failed_splits {
            println!(" - {}", split_info.split_id);
        }
        println!(
            "{} Splits were unable to be removed.",
            removal_info.failed_splits.len()
        );
    }

    let deleted_bytes: u64 = removal_info
        .removed_split_entries
        .iter()
        .map(|split_info| split_info.file_size_bytes.as_u64())
        .sum();
    println!(
        "{}MB of storage garbage collected.",
        deleted_bytes / 1_000_000
    );

    if removal_info.failed_splits.is_empty() {
        println!(
            "{} Index successfully garbage collected.",
            "✔".color(GREEN_COLOR)
        );
    } else if removal_info.removed_split_entries.is_empty()
        && !removal_info.failed_splits.is_empty()
    {
        println!("{} Failed to garbage collect index.", "✘".color(RED_COLOR));
    } else {
        println!(
            "{} Index partially garbage collected.",
            "✘".color(RED_COLOR)
        );
    }

    Ok(())
}

async fn extract_split_cli(args: ExtractSplitArgs) -> anyhow::Result<()> {
    debug!(args=?args, "extract-split");
    println!("❯ Extracting split...");

    let config = load_node_config(&args.config_uri).await?;
    let (storage_resolver, metastore_resolver) =
        get_resolvers(&config.storage_configs, &config.metastore_configs);
    let metastore = metastore_resolver.resolve(&config.metastore_uri).await?;
    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(args.index_id))
        .await?
        .deserialize_index_metadata()?;
    let index_storage = storage_resolver.resolve(index_metadata.index_uri()).await?;
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
        println!("Copying {out_path:?}");
        bundle_storage.copy_to_file(path, &out_path).await?;
    }

    println!("{} Split successfully extracted.", "✔".color(GREEN_COLOR));
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
        pipeline_handle.refresh_observe();

        let observation = pipeline_handle.last_observation();

        // Let's not display live statistics to allow screen to scroll.
        if observation.num_docs > 0 {
            display_statistics(&mut stdout_handle, &mut throughput_calculator, &observation)?;
        }

        if pipeline_handle.state().is_exit() {
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

impl Printer<'_> {
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

async fn create_empty_cluster(config: &NodeConfig) -> anyhow::Result<Cluster> {
    let self_node = ClusterMember {
        node_id: config.node_id.clone(),
        generation_id: quickwit_cluster::GenerationId::now(),
        is_ready: false,
        enabled_services: HashSet::new(),
        gossip_advertise_addr: config.gossip_advertise_addr,
        grpc_advertise_addr: config.grpc_advertise_addr,
        indexing_cpu_capacity: CpuCapacity::zero(),
        indexing_tasks: Vec::new(),
    };
    let client_grpc_config = make_client_grpc_config(&config.grpc_config)?;
    let cluster = Cluster::join(
        config.cluster_id.clone(),
        self_node,
        config.gossip_advertise_addr,
        Vec::new(),
        config.gossip_interval,
        FailureDetectorConfig::default(),
        &ChannelTransport::default(),
        client_grpc_config,
    )
    .await?;

    Ok(cluster)
}
