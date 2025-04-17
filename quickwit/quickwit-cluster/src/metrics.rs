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

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Weak;
use std::time::Duration;

use chitchat::{Chitchat, ChitchatId};
use once_cell::sync::Lazy;
use quickwit_common::metrics::{IntCounter, IntGauge, new_counter, new_gauge};
use tokio::sync::Mutex;

use crate::member::NodeStateExt;

pub struct ClusterMetrics {
    pub live_nodes: IntGauge,
    pub ready_nodes: IntGauge,
    pub zombie_nodes: IntGauge,
    pub dead_nodes: IntGauge,
    pub cluster_state_size_bytes: IntGauge,
    pub node_state_size_bytes: IntGauge,
    pub node_state_keys: IntGauge,
    pub gossip_recv_messages_total: IntCounter,
    pub gossip_recv_bytes_total: IntCounter,
    pub gossip_sent_messages_total: IntCounter,
    pub gossip_sent_bytes_total: IntCounter,
    pub grpc_gossip_rounds_total: IntCounter,
}

impl Default for ClusterMetrics {
    fn default() -> Self {
        ClusterMetrics {
            live_nodes: new_gauge(
                "live_nodes",
                "The number of live nodes observed locally.",
                "cluster",
                &[],
            ),
            ready_nodes: new_gauge(
                "ready_nodes",
                "The number of ready nodes observed locally.",
                "cluster",
                &[],
            ),
            zombie_nodes: new_gauge(
                "zombie_nodes",
                "The number of zombie nodes observed locally.",
                "cluster",
                &[],
            ),
            dead_nodes: new_gauge(
                "dead_nodes",
                "The number of dead nodes observed locally.",
                "cluster",
                &[],
            ),
            cluster_state_size_bytes: new_gauge(
                "cluster_state_size_bytes",
                "The size of the cluster state in bytes.",
                "cluster",
                &[],
            ),
            node_state_keys: new_gauge(
                "node_state_keys",
                "The number of keys in the node state.",
                "cluster",
                &[],
            ),
            node_state_size_bytes: new_gauge(
                "node_state_size_bytes",
                "The size of the node state in bytes.",
                "cluster",
                &[],
            ),
            gossip_recv_messages_total: new_counter(
                "gossip_recv_messages_total",
                "Total number of gossip messages received.",
                "cluster",
                &[],
            ),
            gossip_recv_bytes_total: new_counter(
                "gossip_recv_bytes_total",
                "Total amount of gossip data received in bytes.",
                "cluster",
                &[],
            ),
            gossip_sent_messages_total: new_counter(
                "gossip_sent_messages_total",
                "Total number of gossip messages sent.",
                "cluster",
                &[],
            ),
            gossip_sent_bytes_total: new_counter(
                "gossip_sent_bytes_total",
                "Total amount of gossip data sent in bytes.",
                "cluster",
                &[],
            ),
            grpc_gossip_rounds_total: new_counter(
                "grpc_gossip_rounds_total",
                "Total number of gRPC gossip rounds performed with peer nodes.",
                "cluster",
                &[],
            ),
        }
    }
}

pub static CLUSTER_METRICS: Lazy<ClusterMetrics> = Lazy::new(ClusterMetrics::default);

pub(crate) fn spawn_metrics_task(
    weak_chitchat: Weak<Mutex<Chitchat>>,
    self_chitchat_id: ChitchatId,
) {
    const METRICS_INTERVAL: Duration = Duration::from_secs(15);

    const SIZE_OF_GENERATION_ID: usize = std::mem::size_of::<u64>();
    const SIZE_OF_SOCKET_ADDR: usize = std::mem::size_of::<SocketAddr>();

    let future = async move {
        let mut interval = tokio::time::interval(METRICS_INTERVAL);

        while let Some(chitchat) = weak_chitchat.upgrade() {
            interval.tick().await;

            let mut num_ready_nodes = 0;
            let mut cluster_state_size_bytes = 0;

            let chitchat_guard = chitchat.lock().await;
            let live_nodes: HashSet<&ChitchatId> = chitchat_guard.live_nodes().collect();

            let num_live_nodes = live_nodes.len();
            let num_zombie_nodes = chitchat_guard.scheduled_for_deletion_nodes().count();
            let num_dead_nodes = chitchat_guard.dead_nodes().count();

            for (chitchat_id, node_state) in chitchat_guard.node_states() {
                if live_nodes.contains(chitchat_id) && node_state.is_ready() {
                    num_ready_nodes += 1;
                }
                let chitchat_id_size_bytes =
                    chitchat_id.node_id.len() + SIZE_OF_GENERATION_ID + SIZE_OF_SOCKET_ADDR;
                let node_state_size_bytes = node_state.size_bytes();

                cluster_state_size_bytes += chitchat_id_size_bytes + node_state_size_bytes;

                if *chitchat_id == self_chitchat_id {
                    CLUSTER_METRICS
                        .node_state_keys
                        .set(node_state.num_key_values() as i64);
                    CLUSTER_METRICS
                        .node_state_size_bytes
                        .set(node_state_size_bytes as i64);
                }
            }
            drop(chitchat_guard);

            CLUSTER_METRICS.live_nodes.set(num_live_nodes as i64);
            CLUSTER_METRICS.ready_nodes.set(num_ready_nodes as i64);
            CLUSTER_METRICS.zombie_nodes.set(num_zombie_nodes as i64);
            CLUSTER_METRICS.dead_nodes.set(num_dead_nodes as i64);

            CLUSTER_METRICS
                .cluster_state_size_bytes
                .set(cluster_state_size_bytes as i64);
        }
    };
    tokio::spawn(future);
}
