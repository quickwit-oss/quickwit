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

//! Index Routing Table handlers for the Control Plane.
//!
//! The routing table is an ordered list of rules where each rule has a filter (string)
//! and an index_id. It is used to route documents to the appropriate index.
//!
//! This module contains:
//! - `SetIndexRoutingTableRequest` handler: validates and persists routing table
//! - `consistency` module: helpers for automatic routing table updates on index mutations

use async_trait::async_trait;
use quickwit_actors::{ActorContext, ActorExitStatus, Handler};
use quickwit_proto::control_plane::{ControlPlaneError, ControlPlaneResult};
use quickwit_proto::metastore::{
    EmptyResponse, MetastoreError, MetastoreService, SetIndexRoutingTableRequest,
};
use tracing::debug;

use crate::control_plane::{ControlPlane, convert_metastore_error};

pub mod consistency;

pub use consistency::INDEX_ROUTING_TABLE_UID_KEY;

#[cfg(test)]
mod tests;

#[async_trait]
impl Handler<SetIndexRoutingTableRequest> for ControlPlane {
    type Reply = ControlPlaneResult<EmptyResponse>;

    async fn handle(
        &mut self,
        request: SetIndexRoutingTableRequest,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        debug!("setting index routing table");

        // Validate filter syntax.
        if let Err(e) =
            quickwit_datadog_log_router::LogRouter::create_from_rules(request.rules.clone())
        {
            return Ok(Err(ControlPlaneError::from(
                MetastoreError::InvalidArgument {
                    message: e.to_string(),
                },
            )));
        }

        // Validate: all index_ids must reference existing indexes.
        for rule in &request.rules {
            if self.model.index_uid(&rule.index_id).is_none() {
                return Ok(Err(ControlPlaneError::from(
                    MetastoreError::InvalidArgument {
                        message: format!(
                            "routing table references non-existent index: `{}`",
                            rule.index_id
                        ),
                    },
                )));
            }
        }

        if let Err(metastore_error) = ctx
            .protect_future(self.metastore.set_index_routing_table(request))
            .await
        {
            return convert_metastore_error(metastore_error);
        };

        // Broadcast the change to other nodes via Chitchat.
        consistency::broadcast_routing_table_change(&*self.cluster_kv_publisher).await;

        Ok(Ok(EmptyResponse {}))
    }
}
