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

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use futures::future::try_join_all;
use futures::{Stream, StreamExt};
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox};
use quickwit_cache_storage::CacheStorageService;
use quickwit_cluster::ClusterChange;
use quickwit_common::rendezvous_hasher::sort_by_rendez_vous_hash;
use quickwit_common::uri::{Protocol, Uri};
use quickwit_metastore::{IndexMetadata, ListSplitsQuery, Metastore};
use quickwit_proto::cache_storage::{
    CacheStorageServiceClient, NotifySplitsChangeRequest, SplitsChangeNotification,
};
use quickwit_proto::control_plane::ControlPlaneResult;
use serde::Serialize;
use tokio::sync::RwLock;
use tower::timeout::Timeout;
use tracing::{debug, error};

#[derive(Debug, Clone, Default, Serialize)]
pub struct CacheStorageControllerState {}

/// Resides in the control plane. Responsible for receiving notification about the changes in
/// published splits and forwarding these notifications to CacheStorageServices that resides on
/// search nodes, that in turn are responsible for maintaining the local cache.
pub struct CacheStorageController {
    metastore: Arc<dyn Metastore>,
    state: CacheStorageControllerState,
    split_to_node_map: HashMap<String, HashSet<String>>,
    pool: CacheStorageServicePool,
}

impl fmt::Debug for CacheStorageController {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("CacheStorageController")
            .field("metastore_uri", &self.metastore.uri())
            .finish()
    }
}

#[async_trait]
impl Actor for CacheStorageController {
    type ObservableState = CacheStorageControllerState;

    fn observable_state(&self) -> Self::ObservableState {
        self.state.clone()
    }

    fn name(&self) -> String {
        "CacheStorageController".to_string()
    }
}

impl CacheStorageController {
    /// Creates a new
    pub fn new(metastore: Arc<dyn Metastore>, pool: CacheStorageServicePool) -> Self {
        Self {
            metastore,
            state: CacheStorageControllerState::default(),
            split_to_node_map: HashMap::new(),
            pool,
        }
    }

    fn find_nodes(
        split_id: &str,
        _index_metadata: &IndexMetadata,
        nodes: &[String],
    ) -> Vec<String> {
        // This is a temporary implementation that always returns 1 node and doesn't support weights
        // for now
        let mut node_ids = nodes.to_owned();
        sort_by_rendez_vous_hash(&mut node_ids, split_id);
        node_ids.truncate(1);
        node_ids
    }

    async fn update_cache(&mut self, ctx: &ActorContext<Self>) -> anyhow::Result<()> {
        let _protect_guard = ctx.protect_zone();
        //        let current_splits_vec = self.split_to_node_map.keys().collect_vec();
        let mut current_splits: HashSet<String> =
            HashSet::from_iter(self.split_to_node_map.keys().cloned());
        let mut updated_nodes = HashSet::new();
        let available_nodes = self.pool.available_nodes().await;
        let mut node_to_split_map: HashMap<String, Vec<(String, Uri)>> = HashMap::new();
        for index_metadata in self.metastore.list_indexes_metadatas().await? {
            let index_uid = index_metadata.index_uid.clone();
            let storage_uri = index_metadata.index_uri();
            if storage_uri.protocol() != Protocol::Cache {
                continue;
            }
            let splits_query = ListSplitsQuery::for_index(index_uid)
                .with_split_state(quickwit_metastore::SplitState::Published);
            for split in self.metastore.list_splits(splits_query).await? {
                let split_id = split.split_id().to_string();
                let allocated_nodes = HashSet::from_iter(
                    Self::find_nodes(&split_id, &index_metadata, &available_nodes)
                        .iter()
                        .cloned(),
                );
                current_splits.remove(&split_id);
                // Figure out which nodes were updated and have to be notified
                if let Some(current_allocation) = self.split_to_node_map.get(&split_id) {
                    let diff = current_allocation.difference(&allocated_nodes);
                    for node in diff {
                        if available_nodes.contains(node) {
                            updated_nodes.insert(node.clone());
                        }
                    }
                } else {
                    for node in allocated_nodes.iter() {
                        if available_nodes.contains(node) {
                            updated_nodes.insert(node.clone());
                        }
                    }
                }
                for node in allocated_nodes.iter() {
                    if let Some(splits) = node_to_split_map.get_mut(node) {
                        splits.push((split_id.clone(), storage_uri.clone()));
                    } else {
                        node_to_split_map
                            .insert(node.clone(), vec![(split_id.clone(), storage_uri.clone())]);
                    }
                }
                self.split_to_node_map.insert(split_id, allocated_nodes);
            }
        }
        try_join_all(
            updated_nodes
                .iter()
                .cloned()
                .map(|node| self.notify_split_change(&node_to_split_map, node)),
        )
        .await?;
        Ok(())
    }

    async fn notify_split_change(
        &self,
        node_to_split_map: &HashMap<String, Vec<(String, Uri)>>,
        node: String,
    ) -> anyhow::Result<()> {
        let splits = node_to_split_map.get(&node).expect("Not possible.");
        if let Some(mut service) = self.pool.service(&node).await {
            let request = NotifySplitsChangeRequest {
                splits_change: splits
                    .iter()
                    .map(|(split_id, storage_uri)| SplitsChangeNotification {
                        storage_uri: storage_uri.to_string(),
                        split_id: split_id.clone(),
                    })
                    .collect(),
            };
            quickwit_proto::cache_storage::CacheStorageService::notify_split_change(
                &mut service,
                request,
            )
            .await?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct CacheUpdateRequest {}

#[async_trait]
impl Handler<CacheUpdateRequest> for CacheStorageController {
    type Reply = ControlPlaneResult<()>;

    async fn handle(
        &mut self,
        _request: CacheUpdateRequest,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        debug!("Splits or nodes changed: updating storage cache.");
        self.update_cache(ctx)
            .await
            .context("Error when updating storage cache")?;
        Ok(Ok(()))
    }
}

#[derive(Clone, Default)]
pub struct CacheStorageServicePool {
    services: Arc<RwLock<HashMap<String, CacheStorageServiceClient>>>,
}

impl CacheStorageServicePool {
    async fn available_nodes(&self) -> Vec<String> {
        self.services.read().await.keys().cloned().collect()
    }

    async fn service(&self, node: &String) -> Option<CacheStorageServiceClient> {
        self.services.read().await.get(node).cloned()
    }

    /// Listens to cluster stream changes and notifies relevant cache storage services
    pub fn listen_for_changes(
        &self,
        local_cache_storage_service: Option<Mailbox<CacheStorageService>>,
        cache_storage_contoller: Mailbox<CacheStorageController>,
        cluster_change_stream: impl Stream<Item = ClusterChange> + Send + 'static,
    ) {
        let services = self.services.clone();
        let future = async move {
            cluster_change_stream
                .for_each(|change| async {
                    match change {
                        ClusterChange::Add(node) | ClusterChange::Update(node)
                            if node.is_cache_storage_enabled() =>
                        {
                            let node_id = node.node_id().to_string();
                            if node.is_self_node() {
                                if let Some(local_cache_storage_service_clone) =
                                    local_cache_storage_service.clone()
                                {
                                    let client = CacheStorageServiceClient::from_mailbox(
                                        local_cache_storage_service_clone,
                                    );
                                    if services.write().await.insert(node_id, client).is_none() {
                                        if let Err(err) = cache_storage_contoller
                                            .send_message(CacheUpdateRequest {})
                                            .await
                                        {
                                            error!(
                                                cause=?err,
                                                "Failed to send local cache storage message"
                                            );
                                        }
                                    }
                                }
                            } else {
                                let timeout_channel =
                                    Timeout::new(node.channel(), Duration::from_secs(30));
                                let client =
                                    CacheStorageServiceClient::from_channel(timeout_channel);
                                if services.write().await.insert(node_id, client).is_none() {
                                    if let Err(err) = cache_storage_contoller
                                        .send_message(CacheUpdateRequest {})
                                        .await
                                    {
                                        error!(
                                            cause=?err,
                                            "Failed to send to cache storage message"
                                        );
                                    }
                                }
                            }
                        }
                        ClusterChange::Remove(node) => {
                            let node_id = node.node_id().to_string();
                            services.write().await.remove(&node_id);
                            if let Err(err) = cache_storage_contoller
                                .send_message(CacheUpdateRequest {})
                                .await
                            {
                                error!(
                                    cause=?err,
                                    "Failed to send cache storage message"
                                );
                            }
                        }
                        _ => {}
                    };
                })
                .await;
        };
        tokio::spawn(future);
    }
}

#[cfg(test)]
mod tests {}
