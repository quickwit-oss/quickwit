//  Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::TryRecvError;
use std::sync::Arc;

use artillery_core::epidemic::prelude::{
    ArtilleryMember, ArtilleryMemberEvent, ArtilleryMemberState, Cluster as ArtilleryCluster,
    ClusterConfig as ArtilleryClusterConfig,
};
use artillery_core::errors::ArtilleryError;
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;
use tracing::*;
use uuid::Uuid;

use crate::error::{ClusterError, ClusterResult};

/// The ID that makes the cluster unique.
const CLUSTER_ID: &str = "quickwit-cluster";

/// Reads the key that makes a node unique from the given file.
/// If the file does not exist, it generates an ID and writes it to the file
/// so that it can be reused on reboot.
pub fn read_host_key(host_key_path: &Path) -> ClusterResult<Uuid> {
    let host_key;

    if host_key_path.exists() {
        let host_key_contents =
            fs::read(host_key_path).map_err(|err| ClusterError::ReadHostKeyError {
                cause: anyhow::anyhow!(err),
            })?;
        host_key = Uuid::from_slice(host_key_contents.as_slice()).map_err(|err| {
            ClusterError::ReadHostKeyError {
                cause: anyhow::anyhow!(err),
            }
        })?;
        info!(host_key=?host_key, host_key_path=?host_key_path, "Read existing host key.");
    } else {
        if let Some(dir) = host_key_path.parent() {
            if !dir.exists() {
                fs::create_dir_all(dir).map_err(|err| ClusterError::WriteHostKeyError {
                    cause: anyhow::anyhow!(err),
                })?;
            }
        }
        host_key = Uuid::new_v4();
        fs::write(host_key_path, host_key.as_bytes()).map_err(|err| {
            ClusterError::WriteHostKeyError {
                cause: anyhow::anyhow!(err),
            }
        })?;
        info!(host_key=?host_key, host_key_path=?host_key_path, "Create new host key.");
    }

    Ok(host_key)
}

/// A member information.
#[derive(Clone, Debug, PartialEq)]
pub struct Member {
    /// An ID that makes a member unique.
    pub host_key: Uuid,

    /// Listen address.
    pub listen_addr: SocketAddr,

    /// If true, it means self.
    pub is_self: bool,
}

/// This is an implementation of a cluster using the SWIM protocol.
pub struct Cluster {
    /// A socket address that represents itself.
    pub listen_addr: SocketAddr,

    /// The actual cluster that implement the SWIM protocol.
    artillery_cluster: Arc<ArtilleryCluster>,

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
    pub fn new(host_key: Uuid, listen_addr: SocketAddr) -> ClusterResult<Self> {
        info!( host_key=?host_key, listen_addr=?listen_addr, "Create new cluster.");
        let config = ArtilleryClusterConfig {
            cluster_key: CLUSTER_ID.as_bytes().to_vec(),
            listen_addr,
            ..Default::default()
        };
        let (artillery_cluster, _) =
            ArtilleryCluster::new_cluster(host_key, config).map_err(|err| match err {
                ArtilleryError::Io(io_err) => ClusterError::UDPPortBindingError {
                    port: listen_addr.port(),
                    cause: io_err,
                },
                _ => ClusterError::CreateClusterError {
                    cause: anyhow::anyhow!(err),
                },
            })?;

        let (members_sender, members_receiver) = watch::channel(Vec::new());

        // Create cluster.
        let cluster = Cluster {
            listen_addr,
            artillery_cluster: Arc::new(artillery_cluster),
            members: members_receiver,
            stop: Arc::new(AtomicBool::new(false)),
        };

        // Add itself as the initial member of the cluster.
        let member = Member {
            host_key,
            listen_addr,
            is_self: true,
        };
        let initial_members: Vec<Member> = vec![member];
        if members_sender.send(initial_members).is_err() {
            error!("Failed to add itself as the initial member of the cluster.");
        }

        // Prepare to start a task that will monitor cluster events.
        let task_inner = cluster.artillery_cluster.clone();
        let task_listen_addr = cluster.listen_addr;
        let task_stop = cluster.stop.clone();

        // Start to monitor the cluster events.
        tokio::task::spawn_blocking(move || {
            loop {
                match task_inner.events.try_recv() {
                    Ok((artillery_members, artillery_member_event)) => {
                        log_artillery_event(artillery_member_event);
                        let updated_memberlist: Vec<Member> = artillery_members
                            .into_iter()
                            .filter(|member| match member.state() {
                                ArtilleryMemberState::Alive | ArtilleryMemberState::Suspect => true,
                                ArtilleryMemberState::Down | ArtilleryMemberState::Left => false,
                            })
                            .map(|member| convert_member(member, task_listen_addr))
                            .collect();
                        debug!(updated_memberlist=?updated_memberlist);
                        if members_sender.send(updated_memberlist).is_err() {
                            // Somehow the cluster has been dropped.
                            error!("Failed to send a member list.");
                            break;
                        }
                    }
                    Err(TryRecvError::Disconnected) => {
                        debug!("channel disconnected");
                        break;
                    }
                    Err(TryRecvError::Empty) => {
                        if task_stop.load(Ordering::Relaxed) {
                            debug!("receive a stop signal");
                            break;
                        }
                    }
                }
            }
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

    /// Specify the address of a running node and join the cluster to which the node belongs.
    pub fn add_peer_node(&self, peer_addr: SocketAddr) {
        info!(peer_addr=?peer_addr, "Add peer node.");
        self.artillery_cluster.add_seed_node(peer_addr);
    }

    /// Leave the cluster.
    pub fn leave(&self) {
        info!("Leave the cluster.");
        self.artillery_cluster.leave_cluster();
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// Convert the Artillery's member into Quickwit's one.
fn convert_member(member: ArtilleryMember, self_listen_addr: SocketAddr) -> Member {
    let listen_addr = if let Some(addr) = member.remote_host() {
        addr
    } else {
        self_listen_addr
    };

    Member {
        host_key: member.host_key(),
        listen_addr,
        is_self: member.is_current(),
    }
}

/// Output member event as log.
fn log_artillery_event(artillery_member_event: ArtilleryMemberEvent) {
    match artillery_member_event {
        ArtilleryMemberEvent::Joined(artillery_member) => {
            info!(host_key=?artillery_member.host_key(), remote_host=?artillery_member.remote_host(), "Joined.");
        }
        ArtilleryMemberEvent::WentUp(artillery_member) => {
            info!(host_key=?artillery_member.host_key(), remote_host=?artillery_member.remote_host(), "Went up.");
        }
        ArtilleryMemberEvent::SuspectedDown(artillery_member) => {
            warn!(host_key=?artillery_member.host_key(), remote_host=?artillery_member.remote_host(), "Suspected down.");
        }
        ArtilleryMemberEvent::WentDown(artillery_member) => {
            error!(host_key=?artillery_member.host_key(), remote_host=?artillery_member.remote_host(), "Went down.");
        }
        ArtilleryMemberEvent::Left(artillery_member) => {
            info!(host_key=?artillery_member.host_key(), remote_host=?artillery_member.remote_host(), "Left.");
        }
        ArtilleryMemberEvent::Payload(artillery_member, message) => {
            info!(host_key=?artillery_member.host_key(), remote_host=?artillery_member.remote_host(), message=?message, "Payload.");
        }
    };
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::thread;
    use std::time;

    use artillery_core::epidemic::prelude::{ArtilleryMember, ArtilleryMemberState};

    use crate::cluster::{convert_member, read_host_key, Member};
    use crate::test_utils::{available_port, test_cluster};

    #[tokio::test]
    async fn test_cluster_read_host_key() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let host_key_path = tmp_dir.path().join("host_key");

        // Since the directory does not exist, generate UUID on the specified host_key_path.
        let host_key1 = read_host_key(host_key_path.as_path()).unwrap();
        println!("host_key1={:?}", host_key1);

        // Read the generated UUID.
        let host_key2 = read_host_key(host_key_path.as_path()).unwrap();
        println!("host_key2={:?}", host_key2);

        assert_eq!(host_key1, host_key2);

        tmp_dir.close().unwrap();
    }

    #[tokio::test]
    async fn test_cluster_convert_member() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let host_key_path = tmp_dir.path().join("host_key");
        let host_key = read_host_key(host_key_path.as_path()).unwrap();
        println!("host_key={:?}", host_key);

        let tmp_port = available_port().unwrap();
        let addr_string = format!("127.0.0.1:{}", tmp_port);
        let remote_host: SocketAddr = addr_string.as_str().parse().unwrap();
        println!("remote_host={:?}", remote_host);

        {
            let artillery_member =
                ArtilleryMember::new(host_key, remote_host, 0, ArtilleryMemberState::Alive);
            println!("artillery_member={:?}", artillery_member);

            let member = convert_member(artillery_member, remote_host);
            println!("member={:?}", member);

            let expected = Member {
                host_key,
                listen_addr: remote_host,
                is_self: false,
            };

            assert_eq!(member, expected);
        }

        {
            let artillery_member = ArtilleryMember::current(host_key);
            println!("artillery_member={:?}", artillery_member);

            let member = convert_member(artillery_member, remote_host);
            println!("member={:?}", member);

            let expected = Member {
                host_key,
                listen_addr: remote_host,
                is_self: true,
            };

            assert_eq!(member, expected);
        }

        tmp_dir.close().unwrap();
    }

    #[tokio::test]
    async fn test_cluster_single_node() -> anyhow::Result<()> {
        let tmp_dir = tempfile::tempdir()?;

        let cluster = test_cluster(tmp_dir.path().join("host_key").as_path())?;

        let members: Vec<SocketAddr> = cluster
            .members()
            .iter()
            .map(|member| member.listen_addr)
            .collect();
        println!("member={:?}", members);
        let expected = vec![cluster.listen_addr];
        assert_eq!(members, expected);

        cluster.leave();

        tmp_dir.close()?;

        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_multiple_node() -> anyhow::Result<()> {
        let tmp_dir = tempfile::tempdir()?;

        let cluster1 = test_cluster(tmp_dir.path().join("host_key1").as_path())?;

        let cluster2 = test_cluster(tmp_dir.path().join("host_key2").as_path())?;
        cluster2.add_peer_node(cluster1.listen_addr);

        let cluster3 = test_cluster(tmp_dir.path().join("host_key3").as_path())?;
        cluster3.add_peer_node(cluster1.listen_addr);

        // Wait for the cluster to be configured.
        thread::sleep(time::Duration::from_secs(5));

        let mut members: Vec<SocketAddr> = cluster1
            .members()
            .iter()
            .map(|member| member.listen_addr)
            .collect();
        members.sort();
        println!("{:?}", members);
        let mut expected = vec![
            cluster1.listen_addr,
            cluster2.listen_addr,
            cluster3.listen_addr,
        ];
        expected.sort();
        println!("{:?}", expected);
        assert_eq!(members, expected);

        cluster1.leave();
        cluster2.leave();
        cluster3.leave();

        tmp_dir.close().unwrap();

        Ok(())
    }
}
