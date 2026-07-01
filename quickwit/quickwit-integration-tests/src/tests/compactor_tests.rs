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

use std::time::Duration;

use quickwit_actors::ActorExitStatus;
use quickwit_config::service::QuickwitService;

use crate::test_utils::ClusterSandboxBuilder;

/// A standalone compactor must drain gracefully on shutdown: the decommission path runs to
/// completion and the supervisor and its pipelines exit cleanly.
#[tokio::test]
async fn test_standalone_compactor_graceful_decommission() {
    let mut sandbox = ClusterSandboxBuilder::default()
        .enable_standalone_compactors()
        .add_node([
            QuickwitService::ControlPlane,
            QuickwitService::Metastore,
            QuickwitService::Janitor,
        ])
        .add_node([QuickwitService::Compactor])
        .build_and_start()
        .await;

    sandbox.wait_for_cluster_num_ready_nodes(2).await.unwrap();

    // With no merge in flight the compactor reaches `Finished` immediately, so the shutdown must
    // return well before the 300s decommission timeout (a regression would block until then).
    let exit_statuses = tokio::time::timeout(
        Duration::from_secs(30),
        sandbox.shutdown_services([QuickwitService::Compactor]),
    )
    .await
    .expect("compactor decommission should not block on the decommission timeout")
    .unwrap();

    assert_eq!(exit_statuses.len(), 1);
    for (actor, status) in &exit_statuses[0] {
        assert!(
            matches!(status, ActorExitStatus::Success | ActorExitStatus::Quit),
            "actor `{actor}` exited uncleanly: {status:?}"
        );
    }

    sandbox.shutdown().await.unwrap();
}
