use std::convert::AsRef;
use std::net::SocketAddr;

use flume;
use tracing::debug;
use uuid::Uuid;

use super::state::ArtilleryEpidemic;
use crate::cluster_config::ClusterConfig;
use crate::errors::*;
use crate::state::{ArtilleryClusterEvent, ArtilleryClusterRequest};

#[derive(Debug)]
pub struct Cluster {
    comm: flume::Sender<ArtilleryClusterRequest>,
}

impl Cluster {
    pub fn create_and_start(
        host_key: Uuid,
        config: ClusterConfig,
    ) -> Result<(Cluster, flume::Receiver<ArtilleryClusterEvent>)> {
        let (event_tx, event_rx) = flume::unbounded::<ArtilleryClusterEvent>();
        let (internal_tx, mut internal_rx) = flume::unbounded::<ArtilleryClusterRequest>();

        let (poll, state) =
            ArtilleryEpidemic::new(host_key, config, event_tx, internal_tx.clone())?;

        debug!("Starting Artillery Cluster");
        tokio::task::spawn_blocking(move || {
            ArtilleryEpidemic::event_loop(&mut internal_rx, poll, state)
                .expect("Failed to create event loop");
        });

        let cluster = Cluster { comm: internal_tx };
        Ok((cluster, event_rx))
    }

    pub fn add_seed_node(&self, addr: SocketAddr) {
        let _ = self.comm.send(ArtilleryClusterRequest::AddSeed(addr));
    }

    pub fn send_payload<T: AsRef<str>>(&self, id: Uuid, msg: T) {
        self.comm
            .send(ArtilleryClusterRequest::Payload(
                id,
                msg.as_ref().to_string(),
            ))
            .unwrap();
    }

    pub fn leave_cluster(&self) {
        let _ = self.comm.send(ArtilleryClusterRequest::LeaveCluster);
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let _ = self.comm.send(ArtilleryClusterRequest::Exit);
    }
}
