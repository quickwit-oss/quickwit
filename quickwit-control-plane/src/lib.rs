use std::sync::Arc;

use actors::{IndexService, IndexingPlanner, IndexingScheduler};
use cluster_client::ClusterClient;
use quickwit_actors::{Mailbox, Universe};
use quickwit_cluster::Cluster;
use quickwit_common::uri::Uri;
use quickwit_metastore::Metastore;
use quickwit_storage::StorageUriResolver;

pub mod actors;
pub mod index_service_client;
pub mod cluster_client;


/// Starting Control Plane.
// TODO: should we add an actor to supervise the planner, scheduler and index service?
// Not sure: theses actors should never exit.
pub async fn start_control_plane_service(
    universe: &Universe,
    cluster: Arc<Cluster>,
    metastore: Arc<dyn Metastore>,
    storage_resolver: StorageUriResolver,
    default_index_root_uri: Uri,
) -> anyhow::Result<Mailbox<IndexService>> {
    let cluster_client = Arc::new(ClusterClient::create_and_keep_updated(cluster.clone()).await?);
    let scheduler = IndexingScheduler::new(cluster_client);
    let (scheduler_mailbox, _) = universe.spawn_actor(scheduler).spawn();
    let indexing_planner = IndexingPlanner::new(metastore.clone(), scheduler_mailbox);
    let (indexing_planner_mailbox, _) = universe.spawn_actor(indexing_planner).spawn();
    let index_service = IndexService::new(metastore, cluster, indexing_planner_mailbox, storage_resolver, default_index_root_uri);
    let (index_service_mailbox, _) = universe
            .spawn_actor(index_service)
            .spawn();
    Ok(index_service_mailbox)
}
