// Copyright (C) 2021 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use scuttlebutt::server::ScuttleServer;
use scuttlebutt::{FailureDetectorConfig, NodeId};
use tokio::sync::watch;
use tokio::time::timeout;
use tokio_stream::wrappers::WatchStream;
use tokio_stream::StreamExt;
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::error::ClusterResult;

// /// A member information.
// #[derive(Clone, Debug, PartialEq)]
// pub struct Member {
//     /// An ID that makes a member unique.
//     pub node_id: String,

//     /// Listen address.
//     pub listen_addr: SocketAddr,

//     /// If true, it means self.
//     pub is_self: bool,
// }

// TODO change this to member
#[derive(Clone, Debug, PartialEq)]
pub struct Member {
    /// An ID that makes a member unique.
    pub node_unique_id: String,
    /// timestamp (ms) when node starts.
    pub generation: i64,
    /// advertised UdpServerSocket
    pub gossip_public_address: SocketAddr,
    /// If true, it means self.
    pub is_self: bool,
}

impl Member {
    pub fn new(node_unique_id: String, generation: i64, gossip_public_address: SocketAddr) -> Self {
        Self {
            node_unique_id,
            gossip_public_address,
            generation,
            is_self: true,
        }
    }

    pub fn internal_id(&self) -> String {
        format!("{}/{}", self.node_unique_id, self.generation)
    }
}

impl TryFrom<NodeId> for Member {
    type Error = anyhow::Error;

    fn try_from(node_id: NodeId) -> Result<Self, Self::Error> {
        let (node_unique_id_str, generation_str) =
            node_id.id.split_once("/").ok_or(anyhow::anyhow!(""))?;

        let gossip_public_address: SocketAddr = node_id.gossip_public_address.parse()?;
        Ok(Self {
            node_unique_id: node_unique_id_str.to_string(),
            generation: generation_str.parse()?,
            gossip_public_address,
            is_self: false,
        })
    }
}

impl From<Member> for NodeId {
    fn from(member: Member) -> Self {
        Self::new(
            member.internal_id(),
            member.gossip_public_address.to_string(),
        )
    }
}

/// This is an implementation of a cluster using the SWIM protocol.
pub struct Cluster {
    pub node_id: String,
    /// A socket address that represents itself.
    pub listen_addr: SocketAddr,

    /// The actual cluster that implement the SWIM protocol.
    scuttlebutt_server: ScuttleServer,

    /// A receiver(channel) for exchanging members in a cluster.
    members: watch::Receiver<Vec<Member>>,

    /// A stop flag of cluster monitoring task.
    /// Once the cluster is created, a task to monitor cluster events will be started.
    /// Nodes do not need to be monitored for events once they are detached from the cluster.
    /// You need to update this value to get out of the task loop.
    stop: Arc<AtomicBool>,
}

impl Cluster {
    /// Create a cluster given a host key and a listen address.
    /// When a cluster is created, the thread that monitors cluster events
    /// will be started at the same time.
    pub fn new(me: Member, listen_addr: SocketAddr, seed_nodes: &[String]) -> ClusterResult<Self> {
        info!(member=?me, listen_addr=?listen_addr, "Create new cluster.");
        let scuttlebutt_server = ScuttleServer::spawn(
            NodeId::from(me.clone()),
            seed_nodes,
            listen_addr.to_string(),
            FailureDetectorConfig::default(),
        );
        let scuttlebutt = scuttlebutt_server.scuttlebutt();

        let (members_sender, members_receiver) = watch::channel(Vec::new());

        // Create cluster.
        let cluster = Cluster {
            node_id: me.internal_id(),
            listen_addr,
            scuttlebutt_server,
            members: members_receiver,
            stop: Arc::new(AtomicBool::new(false)),
        };

        // Add itself as the initial member of the cluster.
        let initial_members: Vec<Member> = vec![me.clone()];
        if members_sender.send(initial_members).is_err() {
            error!("Failed to add itself as the initial member of the cluster.");
        }

        // Prepare to start a task that will monitor cluster events.
        let task_stop = cluster.stop.clone();
        tokio::task::spawn(async move {
            let mut node_change_receiver = scuttlebutt.lock().await.live_nodes_watcher();

            while let Some(members_set) = node_change_receiver.next().await {
                let mut members = members_set
                    .into_iter()
                    .map(Member::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                members.push(me.clone());

                if task_stop.load(Ordering::Relaxed) {
                    debug!("receive a stop signal");
                    break;
                }

                if members_sender.send(members).is_err() {
                    // Somehow the cluster has been dropped.
                    error!("Failed to send a member list.");
                    break;
                }
            }
            Result::<(), anyhow::Error>::Ok(())
        });

        Ok(cluster)
    }

    /// Return watchstream for monitoring change of `members`
    pub fn member_change_watcher(&self) -> WatchStream<Vec<Member>> {
        WatchStream::new(self.members.clone())
    }

    /// Return `members` list
    pub fn members(&self) -> Vec<Member> {
        self.members.borrow().clone()
    }

    /// Leave the cluster.
    pub async fn leave(&self) {
        info!(self_addr = ?self.listen_addr, "Leaving the cluster.");
        // TODO: ask if we need to implement leave/join
        // self.scuttlebutt_server.leave().await;
        self.stop.store(true, Ordering::Relaxed);
    }

    /// Leave the cluster.
    pub async fn shutdown(self) {
        info!(self_addr = ?self.listen_addr, "Leaving the cluster.");
        let result = self.scuttlebutt_server.shutdown().await;
        if let Err(error) = result {
            error!(self_addr = ?self.listen_addr, error = ?error, "Error while shuting down.");
        }

        self.stop.store(true, Ordering::Relaxed);
    }

    /// Convenience method for testing that waits for the predicate to hold true for the cluster's
    /// members.
    pub async fn wait_for_members<F>(
        self: &Cluster,
        mut predicate: F,
        timeout_after: Duration,
    ) -> anyhow::Result<()>
    where
        F: FnMut(&Vec<Member>) -> bool,
    {
        timeout(
            timeout_after,
            self.member_change_watcher()
                .skip_while(|members| !predicate(members))
                .next(),
        )
        .await?;
        Ok(())
    }
}

pub fn create_cluster_for_test_with_id(
    peer_uuid: String,
    seeds: &[String],
) -> anyhow::Result<Cluster> {
    let port = quickwit_common::net::find_available_port()?;
    let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
    let cluster = Cluster::new(Member::new(peer_uuid, 1, peer_addr), peer_addr, seeds)?;
    Ok(cluster)
}

/// Creates a local cluster listening on a random port.
pub fn create_cluster_for_test(seeds: &[String]) -> anyhow::Result<Cluster> {
    let peer_uuid = Uuid::new_v4().to_string();
    let cluster = create_cluster_for_test_with_id(peer_uuid, seeds)?;
    Ok(cluster)
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use itertools::Itertools;
    use tokio::time::sleep;

    use super::*;

    #[tokio::test]
    async fn test_cluster_single_node() -> anyhow::Result<()> {
        let cluster = create_cluster_for_test(&[])?;

        let members: Vec<SocketAddr> = cluster
            .members()
            .iter()
            .map(|member| member.gossip_public_address)
            .collect();
        let expected_members = vec![cluster.listen_addr];
        assert_eq!(members, expected_members);

        cluster.leave().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_multiple_nodes() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let cluster1 = create_cluster_for_test(&[])?;
        let node_1 = cluster1.listen_addr.to_string();
        let cluster2 = create_cluster_for_test(&[node_1.clone()])?;
        let cluster3 = create_cluster_for_test(&[node_1])?;

        let wait_secs = Duration::from_secs(40);

        for cluster in [&cluster1, &cluster2, &cluster3] {
            cluster
                .wait_for_members(|members| members.len() == 3, wait_secs)
                .await
                .unwrap();
        }
        let members: Vec<SocketAddr> = cluster1
            .members()
            .iter()
            .map(|member| member.gossip_public_address)
            .sorted()
            .collect();
        let mut expected_members = vec![
            cluster1.listen_addr,
            cluster2.listen_addr,
            cluster3.listen_addr,
        ];
        expected_members.sort();
        assert_eq!(members, expected_members);

        cluster2.shutdown().await;
        cluster1
            .wait_for_members(|members| members.len() == 2, wait_secs)
            .await
            .unwrap();

        cluster3.shutdown().await;
        cluster1
            .wait_for_members(|members| members.len() == 1, wait_secs)
            .await
            .unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_rejoin_with_different_id_issue_1018() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let cluster1 = create_cluster_for_test_with_id("cluster1".to_string(), &[])?;
        let node_1 = cluster1.listen_addr.to_string();
        let cluster2 = create_cluster_for_test_with_id("cluster2".to_string(), &[node_1.clone()])?;

        let wait_secs = Duration::from_secs(40);

        for cluster in [&cluster1, &cluster2] {
            cluster
                .wait_for_members(|members| members.len() == 2, wait_secs)
                .await
                .unwrap();
        }
        let members: Vec<SocketAddr> = cluster1
            .members()
            .iter()
            .map(|member| member.gossip_public_address)
            .sorted()
            .collect();
        let mut expected_members = vec![cluster1.listen_addr, cluster2.listen_addr];
        expected_members.sort();
        assert_eq!(members, expected_members);

        let cluster2_listen_addr = cluster2.listen_addr;
        cluster2.shutdown().await;
        cluster1
            .wait_for_members(|members| members.len() == 1, wait_secs)
            .await
            .unwrap();

        sleep(Duration::from_secs(3)).await;

        let cluster2 = Cluster::new(
            Member::new("newid".to_string(), 1, cluster2_listen_addr),
            cluster2_listen_addr,
            &[node_1],
        )?;

        for _ in 0..4_000 {
            if cluster1.members().len() > 2 {
                panic!("too many members");
            }
            sleep(Duration::from_millis(1)).await;
        }

        sleep(Duration::from_secs(60)).await;
        assert!(!cluster1
            .members()
            .iter()
            .any(|member| (*member).node_unique_id == "cluster2"));
        assert!(!cluster2
            .members()
            .iter()
            .any(|member| (*member).node_unique_id == "cluster2"));

        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_rejoin_with_different_id_3_nodes_issue_1018() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let cluster1 = create_cluster_for_test_with_id("cluster1".to_string(), &[])?;
        let node_1 = cluster1.listen_addr.to_string();
        let cluster2 = create_cluster_for_test_with_id("cluster2".to_string(), &[node_1.clone()])?;
        let node_2 = cluster2.listen_addr.to_string();
        let cluster3 = create_cluster_for_test_with_id("cluster3".to_string(), &[node_2])?;

        let wait_secs = Duration::from_secs(40);

        for cluster in [&cluster1, &cluster2] {
            cluster
                .wait_for_members(|members| members.len() == 3, wait_secs)
                .await
                .unwrap();
        }
        let members: Vec<SocketAddr> = cluster1
            .members()
            .iter()
            .map(|member| member.gossip_public_address)
            .sorted()
            .collect();
        let mut expected_members = vec![
            cluster1.listen_addr,
            cluster2.listen_addr,
            cluster3.listen_addr,
        ];
        expected_members.sort();
        assert_eq!(members, expected_members);

        let cluster2_listen_addr = cluster2.listen_addr;
        let cluster3_listen_addr = cluster3.listen_addr;
        cluster2.shutdown().await;
        cluster3.shutdown().await;
        cluster1
            .wait_for_members(|members| members.len() == 1, wait_secs)
            .await
            .unwrap();

        sleep(Duration::from_secs(3)).await;

        let cluster2 = Cluster::new(
            Member::new("newid".to_string(), 1, cluster2_listen_addr),
            cluster2_listen_addr,
            &[node_1],
        )?;
        let node_2 = cluster2.listen_addr.to_string();

        let cluster3 = Cluster::new(
            Member::new("newid2".to_string(), 1, cluster3_listen_addr),
            cluster3_listen_addr,
            &[node_2],
        )?;

        sleep(Duration::from_secs(60)).await;
        assert!(!cluster1
            .members()
            .iter()
            .any(|member| (*member).node_unique_id == "cluster2"));
        assert!(!cluster2
            .members()
            .iter()
            .any(|member| (*member).node_unique_id == "cluster2"));
        assert!(!cluster3
            .members()
            .iter()
            .any(|member| (*member).node_unique_id == "cluster2"));

        assert!(!cluster1
            .members()
            .iter()
            .any(|member| (*member).node_unique_id == "cluster3"));
        assert!(!cluster2
            .members()
            .iter()
            .any(|member| (*member).node_unique_id == "cluster3"));
        assert!(!cluster2
            .members()
            .iter()
            .any(|member| (*member).node_unique_id == "cluster3"));

        Ok(())
    }
}
