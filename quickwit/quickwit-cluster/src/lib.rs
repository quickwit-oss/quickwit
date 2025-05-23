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
pub use chitchat::transport::ChannelTransport;
use chitchat::transport::{Socket, Transport, UdpSocket};
use chitchat::{ChitchatMessage, Serializable};
pub use chitchat::{FailureDetectorConfig, KeyChangeEvent, ListenerHandle};
pub use grpc_service::cluster_grpc_server;
use quickwit_common::metrics::IntCounter;
use quickwit_common::tower::ClientGrpcConfig;
use quickwit_config::service::QuickwitService;
use quickwit_config::{GrpcConfig, NodeConfig, TlsConfig};
use quickwit_proto::indexing::CpuCapacity;
use quickwit_proto::tonic::transport::{Certificate, ClientTlsConfig, Identity};
use time::OffsetDateTime;

#[cfg(any(test, feature = "testsuite"))]
pub use crate::change::for_test::*;
pub use crate::change::{ClusterChange, ClusterChangeStream, ClusterChangeStreamFactory};
pub use crate::cluster::{Cluster, ClusterSnapshot, NodeIdSchema};
#[cfg(any(test, feature = "testsuite"))]
pub use crate::cluster::{
    create_cluster_for_test, create_cluster_for_test_with_id, grpc_addr_from_listen_addr_for_test,
};
pub use crate::member::{ClusterMember, INDEXING_CPU_CAPACITY_KEY};
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
    gossip_recv: IntCounter,
    gossip_recv_bytes: IntCounter,
    gossip_send: IntCounter,
    gossip_send_bytes: IntCounter,
}

#[async_trait]
impl Socket for CountingUdpSocket {
    async fn send(&mut self, to: SocketAddr, msg: ChitchatMessage) -> anyhow::Result<()> {
        let msg_len = msg.serialized_len() as u64;
        self.socket.send(to, msg).await?;
        self.gossip_send.inc();
        self.gossip_send_bytes.inc_by(msg_len);
        Ok(())
    }

    async fn recv(&mut self) -> anyhow::Result<(SocketAddr, ChitchatMessage)> {
        let (socket_addr, msg) = self.socket.recv().await?;
        self.gossip_recv.inc();
        let msg_len = msg.serialized_len() as u64;
        self.gossip_recv_bytes.inc_by(msg_len);
        Ok((socket_addr, msg))
    }
}

#[async_trait]
impl Transport for CountingUdpTransport {
    async fn open(&self, listen_addr: SocketAddr) -> anyhow::Result<Box<dyn Socket>> {
        let socket = UdpSocket::open(listen_addr).await?;
        Ok(Box::new(CountingUdpSocket {
            socket,
            gossip_recv: crate::metrics::CLUSTER_METRICS
                .gossip_recv_messages_total
                .clone(),
            gossip_recv_bytes: crate::metrics::CLUSTER_METRICS
                .gossip_recv_bytes_total
                .clone(),
            gossip_send: crate::metrics::CLUSTER_METRICS
                .gossip_sent_messages_total
                .clone(),
            gossip_send_bytes: crate::metrics::CLUSTER_METRICS
                .gossip_sent_bytes_total
                .clone(),
        }))
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
    };
    let failure_detector_config = FailureDetectorConfig {
        dead_node_grace_period: Duration::from_secs(2 * 60 * 60), // 2 hours
        ..Default::default()
    };
    let client_grpc_config = make_client_grpc_config(&node_config.grpc_config)?;
    let cluster = Cluster::join(
        cluster_id,
        self_node,
        gossip_listen_addr,
        peer_seed_addrs,
        node_config.gossip_interval,
        failure_detector_config,
        &CountingUdpTransport,
        client_grpc_config,
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

pub fn make_client_grpc_config(grpc_config: &GrpcConfig) -> anyhow::Result<ClientGrpcConfig> {
    let tls_config_opt = grpc_config
        .tls
        .as_ref()
        .map(make_client_tls_config)
        .transpose()?;
    Ok(ClientGrpcConfig {
        keep_alive_opt: grpc_config.keep_alive.clone().map(Into::into),
        tls_config_opt,
    })
}

fn make_client_tls_config(tls_config: &TlsConfig) -> anyhow::Result<ClientTlsConfig> {
    let pem = std::fs::read_to_string(&tls_config.ca_path)?;
    let ca = Certificate::from_pem(pem);
    let mut tls = ClientTlsConfig::new().ca_certificate(ca);

    if tls_config.validate_client {
        let cert = std::fs::read_to_string(&tls_config.cert_path)?;
        let key = std::fs::read_to_string(&tls_config.key_path)?;
        let identity = Identity::from_pem(cert, key);
        tls = tls.identity(identity);
    }
    if let Some(expected_name) = &tls_config.expected_name {
        tls = tls.domain_name(expected_name);
    }

    Ok(tls)
}
