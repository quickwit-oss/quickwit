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
use std::sync::{LazyLock, Weak};
use std::time::Duration;

use chitchat::{Chitchat, ChitchatId};
use quickwit_metrics::{Counter, Gauge, counter, gauge};
use tokio::sync::Mutex;

use crate::member::NodeStateExt;

pub struct ClusterMetrics {
    pub live_nodes: Gauge,
    pub ready_nodes: Gauge,
    pub zombie_nodes: Gauge,
    pub dead_nodes: Gauge,
    pub cluster_state_size_bytes: Gauge,
    pub node_state_size_bytes: Gauge,
    pub node_state_keys: Gauge,
    pub gossip_recv_messages_total: Counter,
    pub gossip_recv_bytes_total: Counter,
    pub gossip_sent_messages_total: Counter,
    pub gossip_sent_bytes_total: Counter,
    pub grpc_gossip_rounds_total: Counter,
}

static LIVE_NODES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "live_nodes",
        description: "The number of live nodes observed locally.",
        subsystem: "cluster",
    )
});

static READY_NODES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "ready_nodes",
        description: "The number of ready nodes observed locally.",
        subsystem: "cluster",
    )
});

static ZOMBIE_NODES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "zombie_nodes",
        description: "The number of zombie nodes observed locally.",
        subsystem: "cluster",
    )
});

static DEAD_NODES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "dead_nodes",
        description: "The number of dead nodes observed locally.",
        subsystem: "cluster",
    )
});

static CLUSTER_STATE_SIZE_BYTES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "cluster_state_size_bytes",
        description: "The size of the cluster state in bytes.",
        subsystem: "cluster",
    )
});

static NODE_STATE_KEYS: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "node_state_keys",
        description: "The number of keys in the node state.",
        subsystem: "cluster",
    )
});

static NODE_STATE_SIZE_BYTES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "node_state_size_bytes",
        description: "The size of the node state in bytes.",
        subsystem: "cluster",
    )
});

static GOSSIP_RECV_MESSAGES_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "gossip_recv_messages_total",
        description: "Total number of gossip messages received.",
        subsystem: "cluster",
    )
});

static GOSSIP_RECV_BYTES_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "gossip_recv_bytes_total",
        description: "Total amount of gossip data received in bytes.",
        subsystem: "cluster",
    )
});

static GOSSIP_SENT_MESSAGES_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "gossip_sent_messages_total",
        description: "Total number of gossip messages sent.",
        subsystem: "cluster",
    )
});

static GOSSIP_SENT_BYTES_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "gossip_sent_bytes_total",
        description: "Total amount of gossip data sent in bytes.",
        subsystem: "cluster",
    )
});

static GRPC_GOSSIP_ROUNDS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "grpc_gossip_rounds_total",
        description: "Total number of gRPC gossip rounds performed with peer nodes.",
        subsystem: "cluster",
    )
});

impl Default for ClusterMetrics {
    fn default() -> Self {
        ClusterMetrics {
            live_nodes: LIVE_NODES.clone(),
            ready_nodes: READY_NODES.clone(),
            zombie_nodes: ZOMBIE_NODES.clone(),
            dead_nodes: DEAD_NODES.clone(),
            cluster_state_size_bytes: CLUSTER_STATE_SIZE_BYTES.clone(),
            node_state_keys: NODE_STATE_KEYS.clone(),
            node_state_size_bytes: NODE_STATE_SIZE_BYTES.clone(),
            gossip_recv_messages_total: GOSSIP_RECV_MESSAGES_TOTAL.clone(),
            gossip_recv_bytes_total: GOSSIP_RECV_BYTES_TOTAL.clone(),
            gossip_sent_messages_total: GOSSIP_SENT_MESSAGES_TOTAL.clone(),
            gossip_sent_bytes_total: GOSSIP_SENT_BYTES_TOTAL.clone(),
            grpc_gossip_rounds_total: GRPC_GOSSIP_ROUNDS_TOTAL.clone(),
        }
    }
}

pub static CLUSTER_METRICS: LazyLock<ClusterMetrics> = LazyLock::new(ClusterMetrics::default);

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
                        .set(node_state.num_key_values() as f64);
                    CLUSTER_METRICS
                        .node_state_size_bytes
                        .set(node_state_size_bytes as f64);
                }
            }
            drop(chitchat_guard);

            CLUSTER_METRICS.live_nodes.set(num_live_nodes as f64);
            CLUSTER_METRICS.ready_nodes.set(num_ready_nodes as f64);
            CLUSTER_METRICS.zombie_nodes.set(num_zombie_nodes as f64);
            CLUSTER_METRICS.dead_nodes.set(num_dead_nodes as f64);

            CLUSTER_METRICS
                .cluster_state_size_bytes
                .set(cluster_state_size_bytes as f64);
        }
    };
    tokio::spawn(future);
}
