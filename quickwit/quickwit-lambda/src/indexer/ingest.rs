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

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use chitchat::transport::ChannelTransport;
use chitchat::FailureDetectorConfig;
use quickwit_actors::Universe;
use quickwit_cli::tool::start_statistics_reporting_loop;
use quickwit_cli::{run_index_checklist, start_actor_runtimes};
use quickwit_cluster::{Cluster, ClusterMember};
use quickwit_common::pubsub::EventBroker;
use quickwit_common::runtimes::RuntimesConfig;
use quickwit_common::uri::Uri;
use quickwit_config::merge_policy_config::MergePolicyConfig;
use quickwit_config::service::QuickwitService;
use quickwit_config::{
    load_index_config_from_user_config, ConfigFormat, IndexConfig, IndexerConfig, NodeConfig,
    SourceConfig, SourceInputFormat, SourceParams, TransformConfig, CLI_INGEST_SOURCE_ID,
};
use quickwit_index_management::{clear_cache_directory, IndexService};
use quickwit_indexing::actors::{IndexingService, MergePipelineId};
use quickwit_indexing::models::{
    DetachIndexingPipeline, DetachMergePipeline, IndexingStatistics, SpawnPipeline,
};
use quickwit_ingest::IngesterPool;
use quickwit_metastore::CreateIndexRequestExt;
use quickwit_proto::indexing::CpuCapacity;
use quickwit_proto::metastore::{CreateIndexRequest, MetastoreError, MetastoreService};
use quickwit_proto::types::NodeId;
use quickwit_storage::StorageResolver;
use tracing::{debug, info, instrument};

use super::environment::{CONFIGURATION_TEMPLATE, DISABLE_MERGE, INDEX_CONFIG_URI, INDEX_ID};
use crate::utils::load_node_config;

#[derive(Debug, Eq, PartialEq)]
pub struct IngestArgs {
    pub input_path: PathBuf,
    pub input_format: SourceInputFormat,
    pub overwrite: bool,
    pub vrl_script: Option<String>,
    pub clear_cache: bool,
}

async fn create_empty_cluster(config: &NodeConfig) -> anyhow::Result<Cluster> {
    let self_node = ClusterMember {
        node_id: NodeId::new(config.node_id.clone()),
        generation_id: quickwit_cluster::GenerationId::now(),
        is_ready: false,
        enabled_services: HashSet::new(),
        gossip_advertise_addr: config.gossip_advertise_addr,
        grpc_advertise_addr: config.grpc_advertise_addr,
        indexing_tasks: Vec::new(),
        indexing_cpu_capacity: CpuCapacity::zero(),
    };
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

/// TODO refactor with `dir_and_filename` in file source
pub fn dir_and_filename(filepath: &Path) -> anyhow::Result<(Uri, &Path)> {
    let dir_uri: Uri = filepath
        .parent()
        .context("Parent directory could not be resolved")?
        .to_str()
        .context("Path cannot be turned to string")?
        .parse()?;
    let file_name = filepath
        .file_name()
        .context("Path does not appear to be a file")?;
    Ok((dir_uri, file_name.as_ref()))
}

#[instrument(level = "debug", skip(resolver))]
async fn load_index_config(
    resolver: &StorageResolver,
    default_index_root_uri: &Uri,
) -> anyhow::Result<IndexConfig> {
    let (dir, file) = dir_and_filename(&Path::new(&*INDEX_CONFIG_URI))?;
    let index_config_storage = resolver.resolve(&dir).await?;
    let bytes = index_config_storage.get_all(file).await?;
    let mut index_config = load_index_config_from_user_config(
        ConfigFormat::Yaml,
        bytes.as_slice(),
        default_index_root_uri,
    )?;
    if *DISABLE_MERGE {
        debug!("force disable merges");
        index_config.indexing_settings.merge_policy = MergePolicyConfig::Nop;
    }
    Ok(index_config)
}

pub async fn ingest(args: IngestArgs) -> anyhow::Result<IndexingStatistics> {
    debug!(args=?args, "lambda-ingest");
    let (config, storage_resolver, mut metastore) =
        load_node_config(CONFIGURATION_TEMPLATE).await?;

    let source_params = SourceParams::file(args.input_path);
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

    let checklist_result = run_index_checklist(
        &mut metastore,
        &storage_resolver,
        &*INDEX_ID,
        Some(&source_config),
    )
    .await;
    if let Err(e) = checklist_result {
        let is_not_found = e.downcast_ref().is_some_and(|meta_error| match meta_error {
            MetastoreError::NotFound(_) => true,
            _ => false,
        });
        if !is_not_found {
            bail!(e);
        }
        info!(
            index_id = *INDEX_ID,
            index_config_uri = *INDEX_CONFIG_URI,
            "Index not found, creating it"
        );
        let index_config =
            load_index_config(&storage_resolver, &config.default_index_root_uri).await?;
        if index_config.index_id != *INDEX_ID {
            bail!(
                "Expected index ID was {} but config file had {}",
                *INDEX_ID,
                index_config.index_id,
            );
        }
        metastore
            .create_index(CreateIndexRequest::try_from_index_config(index_config)?)
            .await?;
        debug!("Index created");
    } else if args.overwrite {
        info!(
            index_id = *INDEX_ID,
            "Overwrite enabled, clearing existing index",
        );
        let mut index_service = IndexService::new(metastore.clone(), storage_resolver.clone());
        index_service.clear_index(&*INDEX_ID).await?;
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
        IngesterPool::default(),
        storage_resolver,
        EventBroker::default(),
    )
    .await?;
    let universe = Universe::new();
    let (indexing_server_mailbox, indexing_server_handle) =
        universe.spawn_builder().spawn(indexing_server);
    let pipeline_id = indexing_server_mailbox
        .ask_for_res(SpawnPipeline {
            index_id: INDEX_ID.clone(),
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
    debug!("Wait for indexing statistics");
    let statistics = start_statistics_reporting_loop(indexing_pipeline_handle, false).await?;
    debug!("Indexing completed, tear down actors");
    merge_pipeline_handle.quit().await;
    universe
        .send_exit_with_success(&indexing_server_mailbox)
        .await?;
    indexing_server_handle.join().await;
    universe.quit().await;

    if args.clear_cache {
        info!("Clearing local cache directory...");
        clear_cache_directory(&config.data_dir_path).await?;
        info!("Local cache directory cleared.");
    }

    if statistics.num_invalid_docs > 0 {
        bail!("Failed to ingest {} documents", statistics.num_invalid_docs)
    }
    Ok(statistics)
}
