use std::{
    net::{SocketAddr, ToSocketAddrs},
    time::Duration,
};

// ARTIL = 27845
/// Default Epidemic Port
pub const CONST_INFECTION_PORT: u16 = 27845;

// Not sure MIO handles this correctly.
// Behave like this is the size. Normally 512 is enough.
/// Default UDP cast packet size
pub const CONST_PACKET_SIZE: usize = 1 << 16;

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub cluster_key: Vec<u8>,
    pub ping_interval: Duration,
    pub network_mtu: usize,
    pub ping_request_host_count: usize,
    pub ping_timeout: Duration,
    pub listen_addr: SocketAddr,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        let directed = SocketAddr::from(([127, 0, 0, 1], CONST_INFECTION_PORT));

        ClusterConfig {
            cluster_key: b"default".to_vec(),
            ping_interval: Duration::from_secs(1),
            network_mtu: CONST_PACKET_SIZE,
            ping_request_host_count: 3,
            ping_timeout: Duration::from_secs(3),
            listen_addr: directed.to_socket_addrs().unwrap().next().unwrap(),
        }
    }
}
