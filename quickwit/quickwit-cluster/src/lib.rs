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

#![deny(clippy::disallowed_methods)]

mod change;
mod cluster;
mod grpc_gossip;
mod grpc_service;
mod member;
mod metrics;
mod node;

use std::net::SocketAddr;
use std::time::Duration;

use async_trait::async_trait;
use chitchat::ChitchatEnvelope;
pub use chitchat::transport::ChannelTransport as ChitchatTransport;
use chitchat::transport::{RecvOutcome, SendOutcome, Socket, Transport, UdpSocket};
pub use chitchat::{FailureDetectorConfig, KeyChangeEvent, ListenerHandle};
pub use grpc_service::cluster_grpc_server;
use quickwit_config::NodeConfig;
use quickwit_config::service::QuickwitService;
use quickwit_proto::indexing::CpuCapacity;
use quickwit_proto::ingest::ingester::IngesterStatus;
use quickwit_transport::ChannelFactory;
use time::OffsetDateTime;

#[cfg(any(test, feature = "testsuite"))]
pub use crate::change::for_test::*;
pub use crate::change::{ClusterChange, ClusterChangeStream};
pub use crate::cluster::{Cluster, ClusterSnapshot, NodeIdSchema};
#[cfg(any(test, feature = "testsuite"))]
pub use crate::cluster::{
    create_cluster_for_test, create_cluster_for_test_with_id, grpc_addr_from_listen_addr_for_test,
};
pub use crate::member::{ClusterMember, INDEXING_CPU_CAPACITY_KEY};
use crate::metrics::{
    GOSSIP_RECV_BYTES_TOTAL, GOSSIP_RECV_MESSAGES_TOTAL, GOSSIP_SENT_BYTES_TOTAL,
    GOSSIP_SENT_MESSAGES_TOTAL,
};
pub use crate::node::ClusterNode;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct GenerationId(u64);

impl GenerationId {
    pub fn as_u64(&self) -> u64 {
        self.0
    }

    pub fn now() -> Self {
        Self(OffsetDateTime::now_utc().unix_timestamp_nanos() as u64)
    }
}

impl From<u64> for GenerationId {
    fn from(generation_id: u64) -> Self {
        Self(generation_id)
    }
}

struct CountingUdpTransport;

struct CountingUdpSocket {
    socket: UdpSocket,
}

#[async_trait]
impl Socket for CountingUdpSocket {
    fn local_addr(&self) -> anyhow::Result<SocketAddr> {
        self.socket.local_addr()
    }

    async fn send(
        &mut self,
        to: SocketAddr,
        envelope: ChitchatEnvelope,
    ) -> anyhow::Result<SendOutcome> {
        let outcome = self.socket.send(to, envelope).await?;
        GOSSIP_SENT_MESSAGES_TOTAL.inc();
        GOSSIP_SENT_BYTES_TOTAL.inc_by(outcome.num_bytes_sent as u64);
        Ok(outcome)
    }

    async fn recv(&mut self) -> anyhow::Result<RecvOutcome> {
        let outcome = self.socket.recv().await?;
        GOSSIP_RECV_MESSAGES_TOTAL.inc();
        GOSSIP_RECV_BYTES_TOTAL.inc_by(outcome.num_bytes_received as u64);
        Ok(outcome)
    }
}

#[async_trait]
impl Transport for CountingUdpTransport {
    async fn open(&self, listen_addr: SocketAddr) -> anyhow::Result<Box<dyn Socket>> {
        let socket = UdpSocket::open(listen_addr).await?;
        Ok(Box::new(CountingUdpSocket { socket }))
    }
}

pub async fn start_cluster_service(node_config: &NodeConfig) -> anyhow::Result<Cluster> {
    let cluster_id = node_config.cluster_id.clone();
    let gossip_listen_addr = node_config.gossip_listen_addr;
    let peer_seed_addrs = node_config.peer_seed_addrs().await?;
    let indexing_tasks = Vec::new();

    let node_id = node_config.node_id.clone();
    let generation_id = GenerationId::now();
    let is_ready = false;
    let indexing_cpu_capacity = if node_config.is_service_enabled(QuickwitService::Indexer) {
        node_config.indexer_config.cpu_capacity
    } else {
        CpuCapacity::zero()
    };
    let self_node = ClusterMember {
        node_id,
        generation_id,
        is_ready,
        enabled_services: node_config.enabled_services.clone(),
        gossip_advertise_addr: node_config.gossip_advertise_addr,
        grpc_advertise_addr: node_config.grpc_advertise_addr,
        indexing_tasks,
        indexing_cpu_capacity,
        ingester_status: IngesterStatus::default(),
        availability_zone: node_config.availability_zone.clone(),
        enable_standalone_compactors: node_config.enable_standalone_compactors,
    };
    let failure_detector_config = FailureDetectorConfig {
        dead_node_grace_period: Duration::from_mins(15),
        ..Default::default()
    };
    let channel_factory = ChannelFactory::for_grpc(&node_config.grpc_config)?;
    let cluster = Cluster::join(
        cluster_id,
        self_node,
        gossip_listen_addr,
        peer_seed_addrs,
        node_config.gossip_interval,
        node_config.gossip_protocol_version,
        failure_detector_config,
        &CountingUdpTransport,
        channel_factory,
    )
    .await?;
    if node_config
        .enabled_services
        .contains(&QuickwitService::Indexer)
    {
        cluster
            .set_self_key_value(INDEXING_CPU_CAPACITY_KEY, indexing_cpu_capacity)
            .await;
    }
    Ok(cluster)
}
