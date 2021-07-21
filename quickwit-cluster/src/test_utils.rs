use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::path::Path;

use crate::cluster::{read_host_key, Cluster};

fn available_port() -> io::Result<u16> {
    match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => Ok(listener.local_addr().unwrap().port()),
        Err(e) => Err(e),
    }
}

pub fn test_cluster(host_key_path: &Path) -> anyhow::Result<Cluster> {
    let host_key = read_host_key(host_key_path)?;
    let listen_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), available_port()?);
    let cluster = Cluster::new(host_key, listen_addr)?;

    Ok(cluster)
}
