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
//!
//! These functions are called by the control plane handlers when
//! `enforce_index_routing_table_consistency` is enabled in the cluster config.

use quickwit_cluster::ClusterKvPublisher;
use quickwit_proto::metastore::{
    GetIndexRoutingTableRequest, IndexRoutingRule, MetastoreError, MetastoreResult,
    MetastoreService, MetastoreServiceClient, SetIndexRoutingTableRequest,
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

/// Called after creating an index. Appends a catch-all rule for the new index.
///
/// This ensures the new index can receive documents even if no explicit routing
/// rule exists for it.
pub async fn on_create_index(
    metastore: &MetastoreServiceClient,
    index_id: &str,
    cluster_kv_publisher: &dyn ClusterKvPublisher,
) -> MetastoreResult<()> {
    let response = metastore
        .get_index_routing_table(GetIndexRoutingTableRequest {})
        .await?;

    let mut rules = response.rules;

    // Append catch-all rule for new index
    let new_rule = IndexRoutingRule {
        filter: "*".to_string(),
        index_id: index_id.to_string(),
    };
    rules.push(new_rule);

    metastore
        .set_index_routing_table(SetIndexRoutingTableRequest { rules })
        .await?;

    broadcast_routing_table_change(cluster_kv_publisher).await;

    Ok(())
}

/// Called before deleting an index. Removes rules referencing the index.
///
/// Returns an error if removing the rules would leave the routing table without
/// a catch-all rule, which would violate the routing table invariant.
pub async fn on_delete_index(
    metastore: &MetastoreServiceClient,
    index_id: &str,
    cluster_kv_publisher: &dyn ClusterKvPublisher,
) -> MetastoreResult<()> {
    let response = metastore
        .get_index_routing_table(GetIndexRoutingTableRequest {})
        .await?;

    if response.rules.is_empty() {
        // No routing table exists, nothing to do
        return Ok(());
    }

    // Remove rules referencing the deleted index
    let new_rules: Vec<IndexRoutingRule> = response
        .rules
        .into_iter()
        .filter(|rule| rule.index_id != index_id)
        .collect();

    // Validate: must still have a catch-all rule
    let has_catch_all = new_rules.iter().any(|rule| rule.filter == "*");
    if !has_catch_all {
        return Err(MetastoreError::InvalidArgument {
            message: format!(
                "cannot delete index `{index_id}`: it would leave the routing table without a \
                 catch-all rule"
            ),
        });
    }

    metastore
        .set_index_routing_table(SetIndexRoutingTableRequest { rules: new_rules })
        .await?;

    broadcast_routing_table_change(cluster_kv_publisher).await;

    Ok(())
}
