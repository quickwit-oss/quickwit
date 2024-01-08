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
use std::path::PathBuf;

use anyhow::bail;
use chitchat::transport::ChannelTransport;
use chitchat::FailureDetectorConfig;
use quickwit_actors::Universe;
use quickwit_cli::tool::start_statistics_reporting_loop;
use quickwit_cli::{run_index_checklist, start_actor_runtimes};
use quickwit_cluster::{Cluster, ClusterMember};
use quickwit_common::pubsub::EventBroker;
use quickwit_common::runtimes::RuntimesConfig;
use quickwit_config::service::QuickwitService;
use quickwit_config::{
    IndexerConfig, NodeConfig, SourceConfig, SourceInputFormat, SourceParams, TransformConfig,
    CLI_INGEST_SOURCE_ID,
};
use quickwit_index_management::{clear_cache_directory, IndexService};
use quickwit_indexing::actors::{IndexingService, MergePipelineId};
use quickwit_indexing::models::{
    DetachIndexingPipeline, DetachMergePipeline, IndexingStatistics, SpawnPipeline,
};
use quickwit_ingest::IngesterPool;
use tracing::{debug, info};

use crate::utils::load_node_config;

const CONFIGURATION_TEMPLATE: &str = "version: 0.6
node_id: lambda-indexer
metastore_uri: s3://${METASTORE_BUCKET}
default_index_root_uri: s3://${INDEX_BUCKET}
data_dir: /tmp
";

#[derive(Debug, Eq, PartialEq)]
pub struct IngestArgs {
    pub index_id: String,
    pub input_path: PathBuf,
    pub input_format: SourceInputFormat,
    pub overwrite: bool,
    pub vrl_script: Option<String>,
    pub clear_cache: bool,
}

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

pub async fn ingest(args: IngestArgs) -> anyhow::Result<IndexingStatistics> {
    debug!(args=?args, "lambda-ingest");
    let (config, storage_resolver, metastore) = load_node_config(CONFIGURATION_TEMPLATE).await?;

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

    let statistics = start_statistics_reporting_loop(indexing_pipeline_handle, false).await?;
    merge_pipeline_handle.quit().await;
    // Shutdown the indexing server.
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
