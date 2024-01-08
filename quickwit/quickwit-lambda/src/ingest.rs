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

///////////////////////////////////////////////////////
// NOTE: Archived in case we want to to fork the CLI //
///////////////////////////////////////////////////////

use std::collections::HashSet;
use std::io::IsTerminal;
use std::num::NonZeroUsize;
use std::{env, io};

use anyhow::{bail, Context};
use chitchat::FailureDetectorConfig;
use quickwit_actors::Universe;
use quickwit_cli::{
    run_index_checklist, start_actor_runtimes, tool::start_statistics_reporting_loop,
    tool::LocalIngestDocsArgs,
};
use quickwit_cluster::{ChannelTransport, Cluster, ClusterMember};
use quickwit_common::runtimes::RuntimesConfig;
use quickwit_common::uri::Uri;
use quickwit_config::service::QuickwitService;
use quickwit_config::{
    ConfigFormat, IndexerConfig, MetastoreConfigs, NodeConfig, SourceConfig, SourceParams,
    StorageConfigs, TransformConfig, CLI_INGEST_SOURCE_ID,
};
use quickwit_index_management::{clear_cache_directory, IndexService};
use quickwit_indexing::actors::{IndexingService, MergePipelineId};
use quickwit_indexing::models::{DetachIndexingPipeline, DetachMergePipeline, SpawnPipeline};
use quickwit_metastore::MetastoreResolver;
use quickwit_storage::{load_file, StorageResolver};
use tracing::{debug, info};

async fn create_empty_cluster(config: &NodeConfig) -> anyhow::Result<Cluster> {
    let self_node = ClusterMember::new(
        config.node_id.clone(),
        quickwit_cluster::GenerationId::now(),
        false,
        HashSet::new(),
        config.gossip_advertise_addr,
        config.grpc_advertise_addr,
        Vec::new(),
    );
    let cluster = Cluster::join(
        config.cluster_id.clone(),
        self_node,
        config.gossip_advertise_addr,
        Vec::new(),
        FailureDetectorConfig::default(),
        &ChannelTransport::default(),
    )
    .await?;
    Ok(cluster)
}

fn get_resolvers(
    storage_configs: &StorageConfigs,
    metastore_configs: &MetastoreConfigs,
) -> (StorageResolver, MetastoreResolver) {
    // The CLI tests rely on the unconfigured singleton resolvers, so it's better to return them if
    // the storage and metastore configs are not set.
    if storage_configs.is_empty() && metastore_configs.is_empty() {
        return (
            StorageResolver::unconfigured(),
            MetastoreResolver::unconfigured(),
        );
    }
    let storage_resolver = StorageResolver::configured(storage_configs);
    let metastore_resolver =
        MetastoreResolver::configured(storage_resolver.clone(), metastore_configs);
    (storage_resolver, metastore_resolver)
}

async fn load_node_config(config_uri: &Uri) -> anyhow::Result<NodeConfig> {
    let config_content = load_file(&StorageResolver::unconfigured(), config_uri)
        .await
        .context("Failed to load node config.")?;
    let config_format = ConfigFormat::sniff_from_uri(config_uri)?;
    let config = NodeConfig::load(config_format, config_content.as_slice())
        .await
        .with_context(|| format!("Failed to parse node config `{config_uri}`."))?;
    info!(config_uri=%config_uri, config=?config, "Loaded node config.");
    Ok(config)
}

pub async fn local_ingest_docs_cli(args: LocalIngestDocsArgs) -> anyhow::Result<()> {
    debug!(args=?args, "local-ingest-docs");

    let config = load_node_config(&args.config_uri).await?;
    let (storage_resolver, metastore_resolver) =
        get_resolvers(&config.storage_configs, &config.metastore_configs);
    let metastore = metastore_resolver.resolve(&config.metastore_uri).await?;

    let source_params = if let Some(filepath) = args.input_path_opt.as_ref() {
        SourceParams::file(filepath)
    } else {
        SourceParams::stdin()
    };
    let transform_config = args
        .vrl_script
        .map(|vrl_script| TransformConfig::new(vrl_script, None));
    let source_config = SourceConfig {
        source_id: CLI_INGEST_SOURCE_ID.to_string(),
        max_num_pipelines_per_indexer: NonZeroUsize::new(1).expect("1 is always non-zero."),
        desired_num_pipelines: NonZeroUsize::new(1).expect("1 is always non-zero."),
        enabled: true,
        source_params,
        transform_config,
        input_format: args.input_format,
    };
    run_index_checklist(
        &*metastore,
        &storage_resolver,
        &args.index_id,
        Some(&source_config),
    )
    .await?;

    if args.overwrite {
        let index_service = IndexService::new(metastore.clone(), storage_resolver.clone());
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
    let indexing_server = IndexingService::new(
        config.node_id.clone(),
        config.data_dir_path.clone(),
        indexer_config,
        runtimes_config.num_threads_blocking,
        cluster,
        metastore,
        None,
        storage_resolver,
    )
    .await?;
    let universe = Universe::new();
    let (indexing_server_mailbox, indexing_server_handle) =
        universe.spawn_builder().spawn(indexing_server);
    let pipeline_id = indexing_server_mailbox
        .ask_for_res(SpawnPipeline {
            index_id: args.index_id.clone(),
            source_config,
            pipeline_ord: 0,
        })
        .await?;
    let merge_pipeline_handle = indexing_server_mailbox
        .ask_for_res(DetachMergePipeline {
            pipeline_id: MergePipelineId::from(&pipeline_id),
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
    merge_pipeline_handle.quit().await;
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
        println!("{} Local cache directory cleared.", "✔");
    }

    match statistics.num_invalid_docs {
        0 => {
            println!("{} Documents successfully indexed.", "✔");
            Ok(())
        }
        _ => bail!("Failed to ingest all the documents."),
    }
}
