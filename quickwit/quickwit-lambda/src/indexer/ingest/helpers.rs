// Copyright (C) 2024 Quickwit, Inc.
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
use quickwit_actors::{ActorHandle, Mailbox, Universe};
use quickwit_cli::run_index_checklist;
use quickwit_cluster::{Cluster, ClusterMember};
use quickwit_common::pubsub::EventBroker;
use quickwit_common::runtimes::RuntimesConfig;
use quickwit_common::uri::Uri;
use quickwit_config::merge_policy_config::MergePolicyConfig;
use quickwit_config::service::QuickwitService;
use quickwit_config::{
    load_index_config_from_user_config, ConfigFormat, IndexConfig, NodeConfig, SourceConfig,
    SourceInputFormat, SourceParams, TransformConfig, CLI_SOURCE_ID,
};
use quickwit_index_management::IndexService;
use quickwit_indexing::actors::{
    IndexingService, MergePipeline, MergePipelineId, MergeSchedulerService,
};
use quickwit_indexing::models::{DetachIndexingPipeline, DetachMergePipeline, SpawnPipeline};
use quickwit_indexing::IndexingPipeline;
use quickwit_ingest::IngesterPool;
use quickwit_janitor::{start_janitor_service, JanitorService};
use quickwit_metastore::CreateIndexRequestExt;
use quickwit_proto::indexing::CpuCapacity;
use quickwit_proto::metastore::{
    CreateIndexRequest, MetastoreError, MetastoreService, MetastoreServiceClient,
};
use quickwit_proto::types::{NodeId, PipelineUid};
use quickwit_search::SearchJobPlacer;
use quickwit_storage::StorageResolver;
use quickwit_telemetry::payload::{QuickwitFeature, QuickwitTelemetryInfo, TelemetryEvent};
use tracing::{debug, info, instrument};

use crate::environment::INDEX_ID;
use crate::indexer::environment::{DISABLE_JANITOR, DISABLE_MERGE, INDEX_CONFIG_URI};

/// The indexing service needs to update its cluster chitchat state so that the control plane is
/// aware of the running tasks. We thus create a fake cluster to instantiate the indexing service
/// and avoid impacting potential control plane running on the cluster.
pub(super) async fn create_empty_cluster(
    config: &NodeConfig,
    services: &[QuickwitService],
) -> anyhow::Result<Cluster> {
    let self_node = ClusterMember {
        node_id: NodeId::new(config.node_id.clone()),
        generation_id: quickwit_cluster::GenerationId::now(),
        is_ready: false,
        enabled_services: HashSet::from_iter(services.to_owned()),
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
        config.gossip_interval,
        FailureDetectorConfig::default(),
        &ChannelTransport::default(),
    )
    .await?;
    Ok(cluster)
}

/// TODO refactor with `dir_and_filename` in file source
fn dir_and_filename(filepath: &Path) -> anyhow::Result<(Uri, &Path)> {
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
pub(super) async fn load_index_config(
    resolver: &StorageResolver,
    default_index_root_uri: &Uri,
) -> anyhow::Result<IndexConfig> {
    let (dir, file) = dir_and_filename(Path::new(&*INDEX_CONFIG_URI))?;
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

pub(super) async fn send_telemetry() {
    let services: HashSet<String> =
        HashSet::from_iter([QuickwitService::Indexer.as_str().to_string()]);
    let telemetry_info =
        QuickwitTelemetryInfo::new(services, HashSet::from_iter([QuickwitFeature::AwsLambda]));
    let _telemetry_handle_opt = quickwit_telemetry::start_telemetry_loop(telemetry_info);
    quickwit_telemetry::send_telemetry_event(TelemetryEvent::RunCommand).await;
}

pub(super) fn configure_source(
    input_path: PathBuf,
    input_format: SourceInputFormat,
    vrl_script: Option<String>,
) -> SourceConfig {
    let source_params = SourceParams::file(input_path);
    let transform_config = vrl_script.map(|vrl_script| TransformConfig::new(vrl_script, None));
    SourceConfig {
        source_id: CLI_SOURCE_ID.to_string(),
        num_pipelines: NonZeroUsize::new(1).expect("1 is always non-zero."),
        enabled: true,
        source_params,
        transform_config,
        input_format,
    }
}

/// Check if the index exists, creating or overwriting it if necessary
pub(super) async fn init_index_if_necessary(
    metastore: &mut MetastoreServiceClient,
    storage_resolver: &StorageResolver,
    source_config: &SourceConfig,
    default_index_root_uri: &Uri,
    overwrite: bool,
) -> anyhow::Result<()> {
    let checklist_result =
        run_index_checklist(metastore, storage_resolver, &INDEX_ID, Some(source_config)).await;
    if let Err(e) = checklist_result {
        let is_not_found = e
            .downcast_ref()
            .is_some_and(|meta_error| matches!(meta_error, MetastoreError::NotFound(_)));
        if !is_not_found {
            bail!(e);
        }
        info!(
            index_id = *INDEX_ID,
            index_config_uri = *INDEX_CONFIG_URI,
            "Index not found, creating it"
        );
        let index_config = load_index_config(storage_resolver, default_index_root_uri).await?;
        if index_config.index_id != *INDEX_ID {
            bail!(
                "Expected index ID was {} but config file had {}",
                *INDEX_ID,
                index_config.index_id,
            );
        }
        metastore
            .create_index(CreateIndexRequest::try_from_index_config(&index_config)?)
            .await?;
        info!("index created");
    } else if overwrite {
        info!(
            index_id = *INDEX_ID,
            "Overwrite enabled, clearing existing index",
        );
        let mut index_service = IndexService::new(metastore.clone(), storage_resolver.clone());
        index_service.clear_index(&INDEX_ID).await?;
    }
    Ok(())
}

pub(super) async fn spawn_services(
    universe: &Universe,
    cluster: Cluster,
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
    node_config: &NodeConfig,
    runtime_config: RuntimesConfig,
) -> anyhow::Result<(
    ActorHandle<IndexingService>,
    Option<Mailbox<JanitorService>>,
)> {
    let event_broker = EventBroker::default();

    // spawn merge scheduler service
    let merge_scheduler_service =
        MergeSchedulerService::new(node_config.indexer_config.merge_concurrency.get());
    let (merge_scheduler_service_mailbox, _) =
        universe.spawn_builder().spawn(merge_scheduler_service);

    // spawn indexer service
    let indexing_service = IndexingService::new(
        node_config.node_id.clone(),
        node_config.data_dir_path.clone(),
        node_config.indexer_config.clone(),
        runtime_config.num_threads_blocking,
        cluster,
        metastore.clone(),
        None,
        merge_scheduler_service_mailbox.clone(),
        IngesterPool::default(),
        storage_resolver.clone(),
        event_broker.clone(),
    )
    .await?;
    let (_, indexing_service_handle) = universe.spawn_builder().spawn(indexing_service);

    // spawn janitor service
    let janitor_service_opt = if *DISABLE_JANITOR {
        None
    } else {
        Some(
            start_janitor_service(
                universe,
                node_config,
                metastore,
                SearchJobPlacer::default(),
                storage_resolver,
                event_broker,
                false,
            )
            .await?,
        )
    };
    Ok((indexing_service_handle, janitor_service_opt))
}

pub(super) async fn spawn_pipelines(
    indexing_server_mailbox: &Mailbox<IndexingService>,
    source_config: SourceConfig,
) -> anyhow::Result<(ActorHandle<IndexingPipeline>, ActorHandle<MergePipeline>)> {
    let pipeline_id = indexing_server_mailbox
        .ask_for_res(SpawnPipeline {
            index_id: INDEX_ID.clone(),
            source_config,
            pipeline_uid: PipelineUid::default(),
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
    Ok((indexing_pipeline_handle, merge_pipeline_handle))
}

pub(super) async fn wait_for_merges(
    merge_pipeline_handle: ActorHandle<MergePipeline>,
) -> anyhow::Result<()> {
    // TODO: find a way to stop the MergePlanner actor in the MergePipeline,
    // otherwise a new merge might be scheduled after this loop. That shouldn't
    // have any concrete impact as the merge will be immediately cancelled, but
    // it might generate errors during the universe shutdown (i.e "Failed to
    // acquire permit")
    loop {
        let state = merge_pipeline_handle.state();
        let obs = merge_pipeline_handle.observe().await;
        debug!(state=?state, ongoing=obs.num_ongoing_merges, "merge pipeline state");
        if obs.num_ongoing_merges == 0 {
            break;
        }
        // We tolerate a relatively low refresh rate because the indexer
        // typically runs for longuer periods of times and merges happen only
        // occasionally.
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }
    Ok(())
}
