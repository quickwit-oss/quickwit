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

//! Maintenance mode management for the Quickwit control plane.
//!
//! When maintenance mode is enabled:
//! - Metadata mutations (index/source CRUD) are allowed but the indexing plan is not rebuilt.
//! - The indexing plan is frozen: it is not rebuilt when indexers join or leave.
//! - Shard scaling (up/down) and rebalancing are paused.
//! - The frozen plan and maintenance metadata are persisted to the metastore `kv` table so they
//!   survive control plane restarts.
//!
//! # Persistence
//!
//! The state is persisted in the metastore `kv` table under the
//! [`KV_KEY_MAINTENANCE_STATE`] key. The value is a JSON envelope with the
//! with some basic metadata and the binary encoded plan.

use base64::Engine as _;
use prost::Message;
use quickwit_proto::control_plane::{MaintenanceFrozenPlan, MaintenanceFrozenPlanForNode};
use quickwit_proto::metastore::{
    DeleteKvRequest, GetKvRequest, MetastoreService, MetastoreServiceClient, SetKvRequest,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tracing::info;

use crate::indexing_plan::PhysicalIndexingPlan;

/// Key in the metastore `kv` table for the combined maintenance state.
pub const KV_KEY_MAINTENANCE_STATE: &str = "control_plane_maintenance_state";

pub const LATEST_MAINTENANCE_FROZEN_PLAN_VERSION: MaintenanceFrozenPlanVersion =
    MaintenanceFrozenPlanVersion::V1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MaintenanceFrozenPlanVersion {
    /// The frozen plan is encoded as protobuf and stored under the
    /// "frozen_plan" key as a base64 string.
    V1 = 1,
}

/// Metadata persisted alongside the maintenance mode flag.
///
/// The `enabled_at` field stores a human-readable RFC 3339 datetime string
/// (e.g., `"2024-06-15T14:30:00Z"`), making it easy to inspect directly in the database.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MaintenanceModeMetadata {
    /// RFC 3339 formatted UTC datetime when maintenance mode was enabled.
    enabled_at: String,
    /// The version of the maintenance state schema.
    version: MaintenanceFrozenPlanVersion,
}

impl MaintenanceModeMetadata {
    /// Creates a new metadata instance with `enabled_at` set to the current UTC time.
    pub fn new_now() -> Self {
        Self {
            enabled_at: now_rfc3339(),
            version: LATEST_MAINTENANCE_FROZEN_PLAN_VERSION,
        }
    }
}

/// In-memory maintenance mode state for the control plane.
#[derive(Debug, Clone, Default)]
pub struct MaintenanceState {
    /// If `Some`, maintenance mode is active with the given metadata.
    metadata: Option<MaintenanceModeMetadata>,
}

impl MaintenanceState {
    /// Returns `true` if maintenance mode is currently active.
    pub fn is_active(&self) -> bool {
        self.metadata.is_some()
    }

    /// Returns the metadata if maintenance mode is active.
    pub fn metadata(&self) -> Option<&MaintenanceModeMetadata> {
        self.metadata.as_ref()
    }

    /// Returns the metadata if maintenance mode is active.
    pub fn enabled_at(&self) -> Option<String> {
        self.metadata
            .as_ref()
            .map(|metadata| metadata.enabled_at.clone())
    }

    /// Enables maintenance mode.
    /// Returns the metadata that was set.
    pub fn enable(&mut self) -> MaintenanceModeMetadata {
        let metadata = MaintenanceModeMetadata {
            enabled_at: now_rfc3339(),
            version: LATEST_MAINTENANCE_FROZEN_PLAN_VERSION,
        };
        self.metadata = Some(metadata.clone());
        info!(
            enabled_at = %metadata.enabled_at,
            version = ?metadata.version,
            "maintenance mode enabled"
        );
        metadata
    }

    /// Disables maintenance mode.
    /// Returns `true` if it was previously active.
    pub fn disable(&mut self) -> bool {
        let was_active = self.metadata.is_some();
        self.metadata = None;
        if was_active {
            info!("maintenance mode disabled");
        }
        was_active
    }

    /// Loads maintenance state from persisted metadata.
    pub fn load_from_metadata(&mut self, metadata: MaintenanceModeMetadata) {
        info!(
            enabled_at = %metadata.enabled_at,
            "loaded maintenance mode from persisted state"
        );
        self.metadata = Some(metadata);
    }
}

// -- Persistence Trait --

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MaintenancePersistedState {
    pub metadata: MaintenanceModeMetadata,
    pub frozen_plan: PhysicalIndexingPlan,
}

impl MaintenancePersistedState {
    pub fn serialize(&self) -> anyhow::Result<String> {
        match self.metadata.version {
            MaintenanceFrozenPlanVersion::V1 => self.serialize_v1(),
        }
    }

    pub fn deserialize(encoded: &str) -> anyhow::Result<Self> {
        let envelope: serde_json::Value = serde_json::from_str(encoded)?;
        let metadata: MaintenanceModeMetadata =
            serde_json::from_value(envelope["metadata"].clone())?;
        let frozen_plan = match metadata.version {
            MaintenanceFrozenPlanVersion::V1 => {
                Self::deserialize_v1_frozen_plan(envelope["frozen_plan"].as_str().ok_or_else(
                    || anyhow::anyhow!("missing frozen_plan field in maintenance state"),
                )?)?
            }
        };
        Ok(Self {
            metadata,
            frozen_plan,
        })
    }

    fn deserialize_v1_frozen_plan(encoded: &str) -> anyhow::Result<PhysicalIndexingPlan> {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|err| anyhow::anyhow!("failed to base64 decode frozen plan: {err}"))?;
        let proto_state = MaintenanceFrozenPlan::decode(&decoded[..])
            .map_err(|err| anyhow::anyhow!("failed to decode protobuf frozen plan: {err}"))?;

        // Collect all indexer node IDs to initialize the plan
        let indexer_ids: Vec<String> = proto_state
            .state_per_node
            .iter()
            .map(|node_state| node_state.index_id.clone())
            .collect();

        let mut plan = PhysicalIndexingPlan::with_indexer_ids(&indexer_ids);

        for node_state in proto_state.state_per_node {
            for task in node_state.indexing_tasks {
                plan.add_indexing_task(&node_state.index_id, task);
            }
        }
        Ok(plan)
    }

    fn serialize_v1(&self) -> anyhow::Result<String> {
        let proto_state = self.frozen_plan_to_proto();

        // Encode the protobuf message to binary
        let mut buf = Vec::new();
        prost::Message::encode(&proto_state, &mut buf)
            .map_err(|err| anyhow::anyhow!("failed to encode protobuf: {err}"))?;

        // Base64 encode the binary data
        let base64_encoded = base64::engine::general_purpose::STANDARD.encode(&buf);

        let json_value = serde_json::json!({
            "frozen_plan": base64_encoded,
            "metadata": serde_json::to_value(&self.metadata)?,
        });
        Ok(serde_json::to_string(&json_value)?)
    }

    /// Converts the frozen plan to the protobuf representation.
    fn frozen_plan_to_proto(&self) -> MaintenanceFrozenPlan {
        let state_per_node: Vec<MaintenanceFrozenPlanForNode> = self
            .frozen_plan
            .indexing_tasks_per_indexer()
            .iter()
            .map(|(node_id, tasks)| MaintenanceFrozenPlanForNode {
                index_id: node_id.clone(),
                indexing_tasks: tasks.clone(),
            })
            .collect();

        MaintenanceFrozenPlan { state_per_node }
    }
}

/// Persists maintenance state using the metastore's `GetKv`/`SetKv`/`DeleteKv`
/// RPCs to the PostgreSQL `kv` table.
#[derive(Debug, Clone)]
pub struct MetastoreKvPersistence {
    metastore: MetastoreServiceClient,
}

impl MetastoreKvPersistence {
    pub fn new(metastore: MetastoreServiceClient) -> Self {
        Self { metastore }
    }

    /// Loads the maintenance state from persistent storage. Returns `None` if
    /// no maintenance state is persisted.
    ///
    /// Panics if the state can't be fetched or deserialized.
    pub async fn load(&self) -> Option<MaintenancePersistedState> {
        let response = self
            .metastore
            .clone()
            .get_kv(GetKvRequest {
                key: KV_KEY_MAINTENANCE_STATE.to_string(),
            })
            .await
            .expect("failed to get maintenance state from metastore");
        let encoded = response.value?; // return None if no value is set
        let persisted = MaintenancePersistedState::deserialize(&encoded)
            .expect("failed to deserialize maintenance state from metastore");
        Some(persisted)
    }

    /// Persists the maintenance metadata and frozen plan atomically.
    pub async fn save(
        &self,
        metadata: &MaintenanceModeMetadata,
        frozen_plan: &PhysicalIndexingPlan,
    ) -> anyhow::Result<()> {
        let persisted = MaintenancePersistedState {
            metadata: metadata.clone(),
            frozen_plan: frozen_plan.clone(),
        };
        let serialized = persisted.serialize()?;
        self.metastore
            .clone()
            .set_kv(SetKvRequest {
                key: KV_KEY_MAINTENANCE_STATE.to_string(),
                value: serialized,
            })
            .await?;
        Ok(())
    }

    /// Clears all persisted maintenance state.
    pub async fn clear(&self) -> anyhow::Result<()> {
        self.metastore
            .clone()
            .delete_kv(DeleteKvRequest {
                key: KV_KEY_MAINTENANCE_STATE.to_string(),
            })
            .await?;
        Ok(())
    }
}

// -- Helper functions --

/// Serializes a `PhysicalIndexingPlan` to a JSON string for use in API responses.
pub fn serialize_frozen_plan(plan: &PhysicalIndexingPlan) -> serde_json::Result<String> {
    serde_json::to_string(plan)
}

/// Returns the current UTC time formatted as an RFC 3339 string.
fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .expect("formatting OffsetDateTime as RFC 3339 should never fail")
}

#[cfg(test)]
mod tests {
    use quickwit_proto::metastore::{
        EmptyResponse, GetKvResponse, MetastoreServiceClient, MockMetastoreService,
    };

    use super::*;

    #[test]
    fn test_maintenance_state_default_is_inactive() {
        let state = MaintenanceState::default();
        assert!(!state.is_active());
        assert!(state.metadata().is_none());
    }

    #[test]
    fn test_maintenance_state_enable_disable() {
        let mut state = MaintenanceState::default();

        // Enable
        let metadata = state.enable();
        assert!(state.is_active());
        assert!(!metadata.enabled_at.is_empty());
        // Should be a valid RFC 3339 datetime
        assert!(
            OffsetDateTime::parse(&metadata.enabled_at, &Rfc3339).is_ok(),
            "enabled_at should be valid RFC 3339: {}",
            metadata.enabled_at
        );

        // Disable
        let was_active = state.disable();
        assert!(was_active);
        assert!(!state.is_active());

        // Disable again is a no-op
        let was_active = state.disable();
        assert!(!was_active);
    }

    #[test]
    fn test_current_persisted_state_version_roundtrip() {
        let metadata = MaintenanceModeMetadata {
            enabled_at: "2024-06-15T14:30:00Z".to_string(),
            version: LATEST_MAINTENANCE_FROZEN_PLAN_VERSION,
        };
        let plan = PhysicalIndexingPlan::with_indexer_ids(&[
            "indexer-1".to_string(),
            "indexer-2".to_string(),
        ]);
        let state = MaintenancePersistedState {
            metadata: metadata.clone(),
            frozen_plan: plan.clone(),
        };
        let serialized = state
            .serialize()
            .expect("failed to serialize maintenance state");
        let deserialized: MaintenancePersistedState =
            MaintenancePersistedState::deserialize(&serialized).unwrap();
        assert_eq!(deserialized, state);
    }

    /// Validates that an existing V1 serialization can still be deserialized.
    #[test]
    fn test_postcard_v1_deserialization_stability() {
        let metadata = MaintenanceModeMetadata {
            enabled_at: "2024-06-15T14:30:00Z".to_string(),
            version: MaintenanceFrozenPlanVersion::V1,
        };
        let plan = PhysicalIndexingPlan::with_indexer_ids(&["indexer-1".to_string()]);
        let expected_state = MaintenancePersistedState {
            metadata: metadata.clone(),
            frozen_plan: plan.clone(),
        };
        // // this was used to generate the `encoded` string
        // println!(
        //     "{}",
        //     expected_state
        //         .serialize()
        //         .expect("failed to serialize expected state")
        // );
        let encoded = r#"{"frozen_plan":"EgsKCWluZGV4ZXItMQ==","metadata":{"enabled_at":"2024-06-15T14:30:00Z","version":"V1"}}"#;
        let deserialized = MaintenancePersistedState::deserialize(encoded).unwrap();
        assert_eq!(deserialized, expected_state);
    }

    #[tokio::test]
    async fn test_metastore_persistence_save_and_load() {
        let mut mock_metastore = MockMetastoreService::new();

        // Initially empty
        mock_metastore
            .expect_get_kv()
            .times(1)
            .returning(|_| Ok(GetKvResponse { value: None }));

        // Save
        mock_metastore
            .expect_set_kv()
            .times(1)
            .returning(|_| Ok(EmptyResponse {}));

        // Load
        let metadata = MaintenanceModeMetadata {
            enabled_at: "2024-01-15T10:00:00Z".to_string(),
            version: MaintenanceFrozenPlanVersion::V1,
        };
        let plan = PhysicalIndexingPlan::with_indexer_ids(&["indexer-1".to_string()]);
        let expected_state = MaintenancePersistedState {
            metadata: metadata.clone(),
            frozen_plan: plan.clone(),
        };
        let expected_encoded = expected_state.serialize().unwrap();

        mock_metastore.expect_get_kv().times(1).returning(move |_| {
            Ok(GetKvResponse {
                value: Some(expected_encoded.clone()),
            })
        });

        // Clear
        mock_metastore
            .expect_delete_kv()
            .times(1)
            .returning(|_| Ok(EmptyResponse {}));

        // One final load to verify cleared
        mock_metastore
            .expect_get_kv()
            .times(1)
            .returning(|_| Ok(GetKvResponse { value: None }));

        let metastore_client = MetastoreServiceClient::from_mock(mock_metastore);
        let persistence = MetastoreKvPersistence::new(metastore_client);

        // Initially empty
        let loaded = persistence.load().await;
        assert!(loaded.is_none());

        // Save
        persistence.save(&metadata, &plan).await.unwrap();

        // Load
        let loaded = persistence.load().await.unwrap();
        assert_eq!(loaded.metadata, metadata);
        assert_eq!(loaded.frozen_plan, plan);

        // Clear
        persistence.clear().await.unwrap();
        let loaded = persistence.load().await;
        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn test_metastore_persistence_overwrite() {
        let mut mock_metastore = MockMetastoreService::new();

        let metadata1 = MaintenanceModeMetadata {
            enabled_at: "2024-01-01T00:00:00Z".to_string(),
            version: MaintenanceFrozenPlanVersion::V1,
        };
        let plan1 = PhysicalIndexingPlan::with_indexer_ids(&["a".to_string()]);

        let metadata2 = MaintenanceModeMetadata {
            enabled_at: "2024-06-01T12:00:00Z".to_string(),
            version: MaintenanceFrozenPlanVersion::V1,
        };
        let plan2 = PhysicalIndexingPlan::with_indexer_ids(&["b".to_string()]);

        // First save
        mock_metastore
            .expect_set_kv()
            .times(1)
            .returning(|_| Ok(EmptyResponse {}));

        // Second save (overwrite)
        mock_metastore
            .expect_set_kv()
            .times(1)
            .returning(|_| Ok(EmptyResponse {}));

        // Load - return the second state
        let expected_state2 = MaintenancePersistedState {
            metadata: metadata2.clone(),
            frozen_plan: plan2.clone(),
        };
        let expected_encoded2 = expected_state2.serialize().unwrap();

        mock_metastore.expect_get_kv().times(1).returning(move |_| {
            Ok(GetKvResponse {
                value: Some(expected_encoded2.clone()),
            })
        });

        let metastore_client = MetastoreServiceClient::from_mock(mock_metastore);
        let persistence = MetastoreKvPersistence::new(metastore_client);

        persistence.save(&metadata1, &plan1).await.unwrap();
        persistence.save(&metadata2, &plan2).await.unwrap();

        let loaded = persistence.load().await.unwrap();
        assert_eq!(loaded.metadata, metadata2);
        assert_eq!(loaded.frozen_plan, plan2);
    }
}
