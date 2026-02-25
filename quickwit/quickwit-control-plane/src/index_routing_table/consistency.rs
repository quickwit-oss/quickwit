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

//! Helper functions for maintaining routing table consistency during index mutations.

use quickwit_cluster::ClusterKvPublisher;
use quickwit_proto::metastore::{
    GetIndexRoutingTableRequest, IndexRoutingRule, MetastoreResult, MetastoreService,
    MetastoreServiceClient, SetIndexRoutingTableRequest,
};

/// Chitchat key used to broadcast routing table changes to other nodes.
pub const INDEX_ROUTING_TABLE_UID_KEY: &str = "indexer.routing_table.ulid";

/// Broadcasts to other nodes via Chitchat that the routing table has changed.
pub async fn broadcast_routing_table_change(cluster_kv_publisher: &dyn ClusterKvPublisher) {
    let ulid = ulid::Ulid::new().to_string();
    cluster_kv_publisher
        .set_self_key_value(INDEX_ROUTING_TABLE_UID_KEY.to_string(), ulid)
        .await;
}

/// Called before deleting an index. Removes rules referencing the index.
pub async fn on_delete_index(
    metastore: &MetastoreServiceClient,
    index_id: &str,
    cluster_kv_publisher: &dyn ClusterKvPublisher,
) -> MetastoreResult<()> {
    let response = metastore
        .get_index_routing_table(GetIndexRoutingTableRequest {})
        .await?;

    if response.rules.is_empty() {
        return Ok(());
    }

    // Remove rules referencing the deleted index
    let new_rules: Vec<IndexRoutingRule> = response
        .rules
        .into_iter()
        .filter(|rule| rule.index_id != index_id)
        .collect();

    metastore
        .set_index_routing_table(SetIndexRoutingTableRequest { rules: new_rules })
        .await?;

    broadcast_routing_table_change(cluster_kv_publisher).await;

    Ok(())
}
