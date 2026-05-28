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
use quickwit_datadog_log_router::LogRouter;
use quickwit_metastore::ListIndexesMetadataResponseExt;
use quickwit_proto::metastore::{
    GetIndexRoutingTableRequest, IndexRoutingRule, ListIndexesMetadataRequest, MetastoreService,
    MetastoreServiceClient,
};
use tracing::info;

/// IndexRouter holds the routing rules and subscribes to chitchat updates
/// to keep them synchronized with the control plane.
#[derive(Clone)]
pub struct IndexRouter {
    log_router_storage: Arc<ArcSwap<LogRouter>>,
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
        let log_router = LogRouter::create_from_rules(initial_rules)?;

        let log_router_storage = Arc::new(ArcSwap::from(Arc::new(log_router)));

        let listener_handle = Self::subscribe(log_router_storage.clone(), metastore, cluster).await;

        Ok(Self {
            log_router_storage,
            _listener_handle_opt: Some(Arc::new(listener_handle)),
        })
    }

    async fn get_rules(
        metastore: &MetastoreServiceClient,
    ) -> anyhow::Result<Vec<IndexRoutingRule>> {
        get_or_default_routing_rules(metastore).await
    }

    /// Subscribes to chitchat updates for routing table version changes.
    /// When the version changes, fetches the latest rules from metastore.
    async fn subscribe(
        log_router_storage: Arc<ArcSwap<LogRouter>>,
        metastore: MetastoreServiceClient,
        cluster: &Cluster,
    ) -> ListenerHandle {
        cluster
            .subscribe(INDEX_ROUTING_TABLE_UID_KEY, move |_| {
                let log_router_storage = log_router_storage.clone();
                let metastore = metastore.clone();
                tokio::spawn(async move {
                    let result = Self::get_rules(&metastore)
                        .await
                        .and_then(LogRouter::create_from_rules);

                    match result {
                        Ok(log_router) => {
                            log_router_storage.store(Arc::new(log_router));
                            info!("updated index routing rules");
                        }
                        Err(e) => {
                            tracing::error!(error = ?e, "failed to update routing rules, keeping old rules");
                        }
                    }
                });
            })
            .await
    }

    /// Acquires a guard for batch routing operations.
    /// Use this when routing multiple logs to avoid cloning `IndexId` for each log.
    pub fn get_router(&self) -> arc_swap::Guard<Arc<LogRouter>> {
        self.log_router_storage.load()
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test(rules: &[(&str, &str)]) -> Self {
        let rules = rules
            .iter()
            .map(
                |(filter, index_id)| quickwit_proto::metastore::IndexRoutingRule {
                    filter: filter.to_string(),
                    index_id: index_id.to_string(),
                },
            )
            .collect();
        Self {
            log_router_storage: Arc::new(ArcSwap::from(Arc::new(
                LogRouter::create_from_rules(rules).unwrap(),
            ))),
            _listener_handle_opt: None,
        }
    }
}

/// Fetches routing rules from metastore. If no routing table is configured, returns a default
/// catch-all rule pointing to the "datadog" index (preferred) or the first index alphabetically.
pub async fn get_or_default_routing_rules(
    metastore: &MetastoreServiceClient,
) -> anyhow::Result<Vec<IndexRoutingRule>> {
    let rules = metastore
        .get_index_routing_table(GetIndexRoutingTableRequest {})
        .await?
        .rules;

    if !rules.is_empty() {
        return Ok(rules);
    }

    let indexes_metadata = metastore
        .list_indexes_metadata(ListIndexesMetadataRequest::all())
        .await?
        .deserialize_indexes_metadata()
        .await?;

    let has_datadog = indexes_metadata.iter().any(|m| m.index_id() == "datadog");
    let default_index_id = if has_datadog {
        "datadog".to_string()
    } else {
        let Some(first) = indexes_metadata.iter().map(|m| m.index_id()).min() else {
            return Ok(Vec::new());
        };
        first.to_string()
    };

    info!(
        default_index_id,
        "no routing table configured, using default catch-all rule"
    );
    Ok(vec![IndexRoutingRule {
        filter: "*".to_string(),
        index_id: default_index_id,
    }])
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
        CreateIndexRequest, IndexRoutingRule, MetastoreService, SetIndexRoutingTableRequest,
    };
    use quickwit_storage::RamStorage;

    use super::*;

    #[test]
    fn test_routing() {
        let router = IndexRouter::for_test(&[
            ("service:test", "exact-match"),
            ("service:te*", "prefix-match"),
            ("service:*", "any-service"),
            ("*", "catch-all"),
        ]);

        let guard = router.get_router();

        // Doc with service:test -> matches first rule (exact match)
        assert_eq!(
            guard.resolve_index(
                &|key| match key {
                    "service" => Some("test"),
                    _ => None,
                },
                &|_| None
            ),
            Some("exact-match")
        );

        // Doc with service:testing -> matches second rule (prefix te*)
        assert_eq!(
            guard.resolve_index(
                &|key| match key {
                    "service" => Some("testing"),
                    _ => None,
                },
                &|_| None
            ),
            Some("prefix-match")
        );

        // Doc with service:b -> matches third rule (any service)
        assert_eq!(
            guard.resolve_index(
                &|key| match key {
                    "service" => Some("b"),
                    _ => None,
                },
                &|_| None
            ),
            Some("any-service")
        );

        // Doc with no service -> matches catch-all
        assert_eq!(guard.resolve_index(&|_| None, &|_| None), Some("catch-all"));
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
        let cluster_config = ClusterConfig::for_test();

        let (control_plane_mailbox, _handle, mut readiness_rx) = ControlPlane::spawn(
            &universe,
            cluster_config,
            control_plane_cluster.self_node_id(),
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
        // The catch-all rule "*" should route to "index-b"
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let guard = index_router.get_router();
                if guard.resolve_index(&|_| None, &|_| None) == Some("index-b") {
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

    #[tokio::test]
    async fn test_index_router_defaults_to_datadog_when_no_routing_table() {
        let storage = Arc::new(RamStorage::default());
        let metastore = FileBackedMetastore::try_new(storage, None).await.unwrap();
        for index_id in ["index-a", "datadog", "index-b"] {
            let index_config = IndexConfig::for_test(index_id, &format!("ram:///{index_id}"));
            let create_request =
                CreateIndexRequest::try_from_index_and_source_configs(&index_config, &[]).unwrap();
            metastore.create_index(create_request).await.unwrap();
        }

        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(vec![], &["indexer"], &transport, true)
            .await
            .unwrap();
        let router =
            IndexRouter::create_and_subscribe(MetastoreServiceClient::new(metastore), &cluster)
                .await
                .unwrap();

        let guard = router.get_router();
        assert_eq!(guard.resolve_index(&|_| None, &|_| None), Some("datadog"));
    }
}
