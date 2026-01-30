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

use std::sync::Arc;

use arc_swap::ArcSwap;
use quickwit_cluster::{Cluster, ListenerHandle};
use quickwit_control_plane::index_routing_table::INDEX_ROUTING_TABLE_UID_KEY;
use quickwit_proto::metastore::{
    GetIndexRoutingTableRequest, IndexRoutingRule, MetastoreResult, MetastoreService,
    MetastoreServiceClient,
};
use quickwit_proto::types::IndexId;
use tracing::info;

/// IndexRouter holds the routing rules and subscribes to chitchat updates
/// to keep them synchronized with the control plane.
#[derive(Clone)]
pub struct IndexRouter {
    rules: Arc<ArcSwap<Vec<IndexRoutingRule>>>,
    /// We must maintain a reference to the subscription handle to continue receiving
    /// notifications. Otherwise, the subscription is dropped.
    _listener_handle_opt: Option<Arc<ListenerHandle>>,
}

impl IndexRouter {
    /// Creates a new IndexRouter with initial rules fetched from metastore and
    /// subscribes to chitchat updates for routing table version changes.
    pub async fn create_and_subscribe(
        metastore: MetastoreServiceClient,
        cluster: &Cluster,
    ) -> anyhow::Result<Self> {
        let initial_rules = Self::get_rules(&metastore).await?;
        let rules = Arc::new(ArcSwap::from(Arc::new(initial_rules)));

        let listener_handle = Self::subscribe(rules.clone(), metastore, cluster).await;

        Ok(Self {
            rules,
            _listener_handle_opt: Some(Arc::new(listener_handle)),
        })
    }

    /// Fetches routing rules from metastore. Returns an empty vector if not set.
    async fn get_rules(
        metastore: &MetastoreServiceClient,
    ) -> MetastoreResult<Vec<IndexRoutingRule>> {
        let response = metastore
            .get_index_routing_table(GetIndexRoutingTableRequest {})
            .await?;

        Ok(response.rules)
    }

    /// Subscribes to chitchat updates for routing table version changes.
    /// When the version changes, fetches the latest rules from metastore.
    async fn subscribe(
        rules: Arc<ArcSwap<Vec<IndexRoutingRule>>>,
        metastore: MetastoreServiceClient,
        cluster: &Cluster,
    ) -> ListenerHandle {
        cluster
            .subscribe(INDEX_ROUTING_TABLE_UID_KEY, move |_| {
                let rules = rules.clone();
                let metastore = metastore.clone();
                tokio::spawn(async move {
                    // get the new rules or crash
                    let new_rules = Self::get_rules(&metastore)
                        .await
                        .expect("failed to fetch routing rules from metastore");

                    rules.store(Arc::new(new_rules));
                    info!("updated indexing routing rules");
                });
            })
            .await
    }

    // this is temporary, let's actually apply the filter for each log in a feature PR
    pub fn get_catch_all_index_id(&self) -> Option<IndexId> {
        let rules = self.rules.load();
        rules.iter().find_map(|rule| {
            if rule.filter == "*" {
                Some(rule.index_id.to_string())
            } else {
                None
            }
        })
    }

    // TODO: implement `route_log(&self, doc: &DatadogLogMsg) -> Option<IndexId>`
    // that evaluates filter rules against the document.

    #[cfg(test)]
    pub fn for_test(rules: Vec<IndexRoutingRule>) -> Self {
        Self {
            rules: Arc::new(ArcSwap::from(Arc::new(rules))),
            _listener_handle_opt: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use quickwit_actors::Universe;
    use quickwit_cluster::{ChannelTransport, create_cluster_for_test};
    use quickwit_config::{ClusterConfig, IndexConfig};
    use quickwit_control_plane::IndexerPool;
    use quickwit_control_plane::control_plane::ControlPlane;
    use quickwit_ingest::IngesterPool;
    use quickwit_metastore::{CreateIndexRequestExt, FileBackedMetastore};
    use quickwit_proto::metastore::{
        CreateIndexRequest, MetastoreService, SetIndexRoutingTableRequest,
    };
    use quickwit_storage::RamStorage;

    use super::*;

    #[test]
    fn test_get_catch_all_index_id() {
        let router = IndexRouter::for_test(vec![
            IndexRoutingRule {
                filter: "service:test".to_string(),
                index_id: "test-index".to_string(),
            },
            IndexRoutingRule {
                filter: "*".to_string(),
                index_id: "datadog".to_string(),
            },
        ]);

        let index_id = router.get_catch_all_index_id();

        assert_eq!(index_id, Some("datadog".to_string()));
    }

    #[test]
    fn test_get_catch_all_index_id_no_catch_all() {
        let router = IndexRouter::for_test(vec![IndexRoutingRule {
            filter: "service:test".to_string(),
            index_id: "test-index".to_string(),
        }]);

        let index_id = router.get_catch_all_index_id();

        assert_eq!(index_id, None);
    }

    #[test]
    fn test_get_catch_all_index_id_empty_rules() {
        let router = IndexRouter::for_test(vec![]);

        let index_id = router.get_catch_all_index_id();

        assert_eq!(index_id, None);
    }

    #[tokio::test]
    async fn test_index_router_receives_routing_table_update_via_chitchat() {
        // 1. Create shared metastore (both nodes read from it)
        let storage = Arc::new(RamStorage::default());
        let metastore = FileBackedMetastore::try_new(storage, None).await.unwrap();

        // Create test indexes
        for index_id in ["index-a", "index-b"] {
            let index_config = IndexConfig::for_test(index_id, &format!("ram:///{index_id}"));
            let create_request =
                CreateIndexRequest::try_from_index_and_source_configs(&index_config, &[]).unwrap();
            metastore.create_index(create_request).await.unwrap();
        }

        // 2. Create two cluster nodes with ChannelTransport
        let transport = ChannelTransport::default();

        // Control plane node
        let control_plane_cluster =
            create_cluster_for_test(vec![], &["control_plane"], &transport, true)
                .await
                .unwrap();

        // Searcher node (joins control plane's cluster)
        let searcher_cluster = create_cluster_for_test(
            vec![control_plane_cluster.gossip_advertise_addr().to_string()],
            &["searcher"],
            &transport,
            true,
        )
        .await
        .unwrap();

        // Wait for nodes to discover each other
        control_plane_cluster
            .wait_for_ready_members(|m| m.len() == 2, Duration::from_secs(5))
            .await
            .unwrap();

        // 3. Spawn control plane actor (uses cluster for broadcasting)
        let universe = Universe::with_accelerated_time();
        let mut cluster_config = ClusterConfig::for_test();
        cluster_config.enforce_index_routing_table_consistency = true;

        let (control_plane_mailbox, _handle, mut readiness_rx) = ControlPlane::spawn(
            &universe,
            cluster_config,
            control_plane_cluster.self_node_id().into(),
            control_plane_cluster.clone(),
            IndexerPool::default(),
            IngesterPool::default(),
            MetastoreServiceClient::new(metastore.clone()),
        );

        // Wait for control plane to be ready
        tokio::time::timeout(Duration::from_secs(5), readiness_rx.wait_for(|r| *r))
            .await
            .unwrap()
            .unwrap();

        // 4. Create IndexRouter on searcher node (subscribes to cluster)
        let index_router = IndexRouter::create_and_subscribe(
            MetastoreServiceClient::new(metastore.clone()),
            &searcher_cluster,
        )
        .await
        .unwrap();

        // 5. Set routing table via control plane
        let request = SetIndexRoutingTableRequest {
            rules: vec![
                IndexRoutingRule {
                    filter: "service:a".to_string(),
                    index_id: "index-a".to_string(),
                },
                IndexRoutingRule {
                    filter: "*".to_string(),
                    index_id: "index-b".to_string(),
                },
            ],
        };
        control_plane_mailbox.ask(request).await.unwrap().unwrap();

        // 6. Wait for chitchat propagation and verify IndexRouter received update
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if index_router.get_catch_all_index_id() == Some("index-b".to_string()) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("IndexRouter did not receive routing table update within timeout");

        // 7. Cleanup
        universe.assert_quit().await;
    }
}
