// Copyright (C) 2022 Quickwit, Inc.
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

use std::fmt::Display;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener};
use std::str::FromStr;

use anyhow::{bail, Context};
use itertools::Itertools;
use once_cell::sync::OnceCell;
use pnet::datalink::{self, NetworkInterface};
use pnet::ipnetwork::IpNetwork;
use serde::{Deserialize, Serialize, Serializer};
use tokio::net::{lookup_host, ToSocketAddrs};

/// Represents a host, i.e. an IP address (`127.0.0.1`) or a hostname (`localhost`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Host {
    Hostname(String),
    IpAddr(IpAddr),
}

impl Host {
    /// Returns [`true`] for the "unspecified" address (all bits set to zero).
    pub fn is_unspecified(&self) -> bool {
        match &self {
            Host::Hostname(_) => false,
            Host::IpAddr(ip_addr) => ip_addr.is_unspecified(),
        }
    }

    /// Appends `port` to `self` and returns a [`HostAddr`].
    pub fn with_port(&self, port: u16) -> HostAddr {
        HostAddr {
            host: self.clone(),
            port,
        }
    }

    /// Resolves the host if necessary and returns an [`IpAddr`].
    pub async fn resolve(&self) -> anyhow::Result<IpAddr> {
        match &self {
            Host::Hostname(hostname) => get_socket_addr(&(hostname.as_str(), 0))
                .await
                .map(|socket_addr| socket_addr.ip()),
            Host::IpAddr(ip_addr) => Ok(*ip_addr),
        }
    }
}

impl Default for Host {
    fn default() -> Self {
        Host::IpAddr(IpAddr::V4(Ipv4Addr::LOCALHOST))
    }
}

impl Display for Host {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Host::Hostname(hostname) => hostname.fmt(formatter),
            Host::IpAddr(ip_addr) => ip_addr.fmt(formatter),
        }
    }
}

impl Serialize for Host {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        match self {
            Host::Hostname(hostname) => hostname.serialize(serializer),
            Host::IpAddr(ip_addr) => ip_addr.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Host {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        let host_str: String = Deserialize::deserialize(deserializer)?;
        host_str.parse().map_err(serde::de::Error::custom)
    }
}

impl From<IpAddr> for Host {
    fn from(ip_addr: IpAddr) -> Self {
        Host::IpAddr(ip_addr)
    }
}

impl From<Ipv4Addr> for Host {
    fn from(ip_addr: Ipv4Addr) -> Self {
        Host::IpAddr(IpAddr::V4(ip_addr))
    }
}

impl From<Ipv6Addr> for Host {
    fn from(ip_addr: Ipv6Addr) -> Self {
        Host::IpAddr(IpAddr::V6(ip_addr))
    }
}

impl FromStr for Host {
    type Err = anyhow::Error;

    fn from_str(host: &str) -> Result<Self, Self::Err> {
        if let Ok(ip_addr) = host.parse::<IpAddr>() {
            return Ok(Self::IpAddr(ip_addr));
        }
        if is_valid_hostname(host) {
            return Ok(Self::Hostname(host.to_string()));
        }
        bail!("Failed to parse host: `{host}`.")
    }
}

/// Represents an address `<host>:<port>` where `host` can be an IP address or a hostname.
#[derive(Clone, Debug)]
pub struct HostAddr {
    host: Host,
    port: u16,
}

impl HostAddr {
    /// Attempts to parse a `host_addr`.
    /// If no port is defined, it just accepts the host and uses the given default port.
    ///
    /// This function supports:
    /// - IPv4
    /// - IPv4:port
    /// - IPv6
    /// - \[IPv6\]:port -- IpV6 contains colon. It is customary to require bracket for this reason.
    /// - hostname
    /// - hostname:port
    pub fn parse_with_default_port(host_addr: &str, default_port: u16) -> anyhow::Result<Self> {
        if let Ok(socket_addr) = host_addr.parse::<SocketAddr>() {
            return Ok(Self {
                host: Host::IpAddr(socket_addr.ip()),
                port: socket_addr.port(),
            });
        }
        if let Ok(ip_addr) = host_addr.parse::<IpAddr>() {
            return Ok(Self {
                host: Host::IpAddr(ip_addr),
                port: default_port,
            });
        }
        let (hostname, port) = if let Some((hostname_str, port_str)) = host_addr.split_once(':') {
            let port_u16 = port_str.parse::<u16>().with_context(|| {
                format!("Failed to parse address `{}`: port is invalid.", host_addr)
            })?;
            (hostname_str, port_u16)
        } else {
            (host_addr, default_port)
        };
        if !is_valid_hostname(hostname) {
            bail!(
                "Failed to parse address `{}`: hostname is invalid.",
                host_addr
            )
        }
        Ok(Self {
            host: Host::Hostname(hostname.to_string()),
            port,
        })
    }

    /// Resolves the host if necessary and returns a `SocketAddr`.
    pub async fn resolve(&self) -> anyhow::Result<SocketAddr> {
        self.host
            .resolve()
            .await
            .map(|ip_addr| SocketAddr::new(ip_addr, self.port))
    }

    /// Skips DNS resolution if possible and returns the host address as a `SocketAddr`.
    pub fn to_socket_addr(self) -> Option<SocketAddr> {
        if let Host::IpAddr(ip_addr) = self.host {
            Some(SocketAddr::new(ip_addr, self.port))
        } else {
            None
        }
    }
}

impl Display for HostAddr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self.host {
            Host::IpAddr(IpAddr::V6(_)) => write!(formatter, "[{}]:{}", self.host, self.port),
            _ => write!(formatter, "{}:{}", self.host, self.port),
        }
    }
}

/// Finds a random available TCP port.
///
/// This function induces a race condition, use it only in unit tests.
pub fn find_available_tcp_port() -> anyhow::Result<u16> {
    let socket: SocketAddr = ([127, 0, 0, 1], 0u16).into();
    let listener = TcpListener::bind(socket)?;
    let port = listener.local_addr()?.port();
    Ok(port)
}

/// Attempts to find the private IP of the host. Returns the matching interface name along with it.
pub fn find_private_ip() -> Option<(String, IpAddr)> {
    _find_private_ip(&datalink::interfaces())
}

fn _find_private_ip(interfaces: &[NetworkInterface]) -> Option<(String, IpAddr)> {
    // The way we do this is the following:
    // 1. List the network interfaces
    // 2. Filter out the interfaces that are not up
    // 3. Filter out the networks that are not routable and private
    // 4. Sort the IP addresses by:
    //      - type (IPv4 first)
    //      - mode (default first)
    //      - size of network address space (desc)
    // 5. Pick the first one
    interfaces
        .iter()
        .filter(|interface| interface.is_up())
        .flat_map(|interface| {
            interface
                .ips
                .iter()
                .filter(|ip_net| is_forwardable_ip(&ip_net.ip()) && is_private_ip(&ip_net.ip()))
                .map(move |ip_net| (interface, ip_net))
        })
        .sorted_by_key(|(interface, ip_net)| {
            (
                ip_net.is_ipv6(),
                is_dormant(interface),
                std::cmp::Reverse(ip_net.prefix()),
            )
        })
        .next()
        .map(|(interface, ip_net)| (interface.name.clone(), ip_net.ip()))
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn is_dormant(interface: &NetworkInterface) -> bool {
    interface.is_dormant()
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn is_dormant(_interface: &NetworkInterface) -> bool {
    false
}

/// Converts an object into a resolved `SocketAddr`.
pub async fn get_socket_addr<T: ToSocketAddrs + std::fmt::Debug>(
    addr: &T,
) -> anyhow::Result<SocketAddr> {
    lookup_host(addr)
        .await
        .with_context(|| format!("Failed to parse address or resolve hostname {addr:?}."))?
        .next()
        .ok_or_else(|| {
            anyhow::anyhow!("DNS resolution did not yield any record for hostname {addr:?}.")
        })
}

fn is_forwardable_ip(ip_addr: &IpAddr) -> bool {
    static NON_FORWARDABLE_NETWORKS: OnceCell<Vec<IpNetwork>> = OnceCell::new();
    NON_FORWARDABLE_NETWORKS
        .get_or_init(|| {
            // Blacklist of non-forwardable IP blocks taken from RFC6890
            [
                "0.0.0.0/8",
                "127.0.0.0/8",
                "169.254.0.0/16",
                "192.0.0.0/24",
                "192.0.2.0/24",
                "198.51.100.0/24",
                "2001:10::/28",
                "2001:db8::/32",
                "203.0.113.0/24",
                "240.0.0.0/4",
                "255.255.255.255/32",
                "::/128",
                "::1/128",
                "::ffff:0:0/96",
                "fe80::/10",
            ]
            .iter()
            .map(|network| network.parse().expect("Failed to parse network range. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues."))
            .collect()
        })
        .iter()
        .all(|network| !network.contains(*ip_addr))
}

fn is_private_ip(ip_addr: &IpAddr) -> bool {
    static PRIVATE_NETWORKS: OnceCell<Vec<IpNetwork>> = OnceCell::new();
    PRIVATE_NETWORKS
        .get_or_init(|| {
            ["192.168.0.0/16", "172.16.0.0/12", "10.0.0.0/8", "fc00::/7"]
                .iter()
                .map(|network| network.parse().expect("Failed to parse network range. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues."))
                .collect()
        })
        .iter()
        .any(|network| network.contains(*ip_addr))
}

/// Returns whether a hostname is valid according to [IETF RFC 1123](https://tools.ietf.org/html/rfc1123).
///
/// A hostname is valid if the following conditions are met:
///
/// - It does not start or end with `-` or `.`.
/// - It does not contain any characters outside of the alphanumeric range, except for `-` and `.`.
/// - It is not empty.
/// - It is 253 or fewer characters.
/// - Its labels (characters separated by `.`) are not empty.
/// - Its labels are 63 or fewer characters.
/// - Its labels do not start or end with '-' or '.'.
fn is_valid_hostname(hostname: &str) -> bool {
    if hostname.is_empty() || hostname.len() > 253 {
        return false;
    }
    if !hostname
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '.')
    {
        return false;
    }
    if hostname.split('.').any(|label| {
        label.is_empty() || label.len() > 63 || label.starts_with('-') || label.ends_with('-')
    }) {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use std::net::Ipv6Addr;

    use pnet::ipnetwork::{Ipv4Network, Ipv6Network};

    use super::*;

    #[test]
    fn test_parse_host() {
        assert_eq!(
            "127.0.0.1".parse::<Host>().unwrap(),
            Host::from(Ipv4Addr::LOCALHOST)
        );
        assert_eq!(
            "::1".parse::<Host>().unwrap(),
            Host::from(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1))
        );
        assert_eq!(
            "localhost".parse::<Host>().unwrap(),
            Host::Hostname("localhost".to_string())
        );
    }

    #[test]
    fn test_deserialize_host() {
        assert_eq!(
            serde_json::from_str::<Host>("\"127.0.0.1\"").unwrap(),
            Host::from(Ipv4Addr::LOCALHOST)
        );
        assert_eq!(
            serde_json::from_str::<Host>("\"::1\"").unwrap(),
            Host::from(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1))
        );
        assert_eq!(
            serde_json::from_str::<Host>("\"localhost\"").unwrap(),
            Host::Hostname("localhost".to_string())
        );
    }

    #[test]
    fn test_serialize_host() {
        assert_eq!(
            serde_json::to_value(Host::from(Ipv4Addr::LOCALHOST)).unwrap(),
            serde_json::Value::String("127.0.0.1".to_string())
        );
        assert_eq!(
            serde_json::to_value(Host::from(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1))).unwrap(),
            serde_json::Value::String("::1".to_string())
        );
        assert_eq!(
            serde_json::to_value(Host::Hostname("localhost".to_string())).unwrap(),
            serde_json::Value::String("localhost".to_string())
        );
    }

    fn test_parse_addr_helper(addr: &str, expected_addr_opt: Option<&str>) {
        let addr_res = HostAddr::parse_with_default_port(addr, 1337);
        if let Some(expected_addr) = expected_addr_opt {
            assert!(
                addr_res.is_ok(),
                "Parsing `{}` was expected to succeed.",
                addr
            );
            assert_eq!(addr_res.unwrap().to_string(), expected_addr);
        } else {
            assert!(
                addr_res.is_err(),
                "Parsing `{}` was expected to fail, got `{}`",
                addr,
                addr_res.unwrap()
            );
        }
    }

    #[tokio::test]
    async fn test_parse_addr_with_ips() {
        // IPv4
        test_parse_addr_helper("127.0.0.1", Some("127.0.0.1:1337"));
        test_parse_addr_helper("127.0.0.1:100", Some("127.0.0.1:100"));
        test_parse_addr_helper("127.0..1:100", None);

        // IPv6
        test_parse_addr_helper(
            "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
            Some("[2001:db8:85a3::8a2e:370:7334]:1337"),
        );
        test_parse_addr_helper("2001:0db8:85a3:0000:0000:8a2e:0370:7334:1000", None);
        test_parse_addr_helper(
            "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:1000",
            Some("[2001:db8:85a3::8a2e:370:7334]:1000"),
        );
        test_parse_addr_helper("[2001:0db8:1000", None);
        test_parse_addr_helper("2001:0db8:85a3:0000:0000:8a2e:0370:7334]:1000", None);

        // Hostname
        test_parse_addr_helper("google.com", Some("google.com:1337"));
        test_parse_addr_helper("google.com:1000", Some("google.com:1000"));
    }

    #[test]
    fn test_is_valid_hostname() {
        for hostname in &[
            "VaLiD-HoStNaMe",
            "50-name",
            "235235",
            "example.com",
            "VaLid.HoStNaMe",
            "123.456",
        ] {
            assert!(
                is_valid_hostname(hostname),
                "Hostname `{hostname}` is valid.",
            );
        }

        for hostname in &[
            "-invalid-name",
            "also-invalid-",
            "asdf@fasd",
            "@asdfl",
            "asd f@",
            ".invalid",
            "invalid.name.",
            "foo.label-is-way-to-longgggggggggggggggggggggggggggggggggggggggggggg.org",
            "invalid.-starting.char",
            "invalid.ending-.char",
            "empty..label",
        ] {
            assert!(
                !is_valid_hostname(hostname),
                "Hostname `{hostname}` is invalid."
            );
        }
    }

    #[test]
    fn test_find_private_ip() {
        assert!(_find_private_ip(&[]).is_none());

        let interfaces = [
            NetworkInterface {
                name: "lo".to_string(),
                description: "".to_string(),
                index: 1,
                mac: None,
                ips: vec![
                    IpNetwork::V4(Ipv4Network::new("127.0.0.1".parse().unwrap(), 8).unwrap()),
                    IpNetwork::V6(Ipv6Network::new("::1".parse().unwrap(), 128).unwrap()),
                ],
                flags: 65609,
            },
            NetworkInterface {
                name: "docker0".to_string(),
                description: "".to_string(),
                index: 2,
                mac: None,
                ips: vec![
                    IpNetwork::V6(
                        Ipv6Network::new("fe80::42:69ff:fe8e:e739".parse().unwrap(), 64).unwrap(),
                    ),
                    IpNetwork::V4(Ipv4Network::new("172.17.0.1".parse().unwrap(), 8).unwrap()),
                ],
                flags: 4099,
            },
            NetworkInterface {
                name: "eth0".to_string(),
                description: "".to_string(),
                index: 3,
                mac: None,
                ips: vec![
                    IpNetwork::V6(
                        Ipv6Network::new("fe80::84ed:78c:ec06:bf53".parse().unwrap(), 64).unwrap(),
                    ),
                    IpNetwork::V4(Ipv4Network::new("192.168.1.70".parse().unwrap(), 24).unwrap()),
                ],
                flags: 69699,
            },
        ];
        let (interface_name, ip_addr) = _find_private_ip(&interfaces).unwrap();
        assert_eq!(interface_name, "eth0");
        assert_eq!(ip_addr, "192.168.1.70".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn test_is_forwardable_ip() {
        for ip in ["192.168.0.42", "172.16.0.42", "10.0.0.42"] {
            assert!(
                is_forwardable_ip(&ip.parse::<IpAddr>().unwrap()),
                "IP `{ip}` is forwardable!"
            );
        }
        for ip in ["127.0.0.42", "169.254.0.42", "192.0.0.42"] {
            assert!(
                !is_forwardable_ip(&ip.parse::<IpAddr>().unwrap()),
                "IP `{ip}` is not forwardable!"
            );
        }
    }

    #[test]
    fn test_is_private_ip() {
        for ip in ["192.168.0.42", "172.16.0.42", "10.0.0.42"] {
            assert!(
                is_private_ip(&ip.parse::<IpAddr>().unwrap()),
                "IP `{ip}` is private!"
            );
        }
        for ip in ["192.169.0.42", "172.32.0.42", "11.0.0.42"] {
            assert!(
                !is_private_ip(&ip.parse::<IpAddr>().unwrap()),
                "IP `{ip}` is public!"
            );
        }
    }
}
