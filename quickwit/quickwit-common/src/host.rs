// Copyright (C) 2023 Quickwit, Inc.
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
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::str::FromStr;

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize, Serializer};

/// Represents a host, i.e. an IP address (`127.0.0.1`) or a hostname (`localhost`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Host {
    Hostname(String),
    IpAddr(IpAddr),
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
    where
        S: Serializer,
    {
        match self {
            Host::Hostname(hostname) => hostname.serialize(serializer),
            Host::IpAddr(ip_addr) => ip_addr.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Host {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
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
    pub(crate) host: Host,
    pub(crate) port: u16,
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
                format!("Failed to parse address `{host_addr}`: port is invalid.")
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
}

impl Display for HostAddr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self.host {
            Host::IpAddr(IpAddr::V6(_)) => write!(formatter, "[{}]:{}", self.host, self.port),
            _ => write!(formatter, "{}:{}", self.host, self.port),
        }
    }
}

/// Returns whether a hostname is valid according to [RFC 1123](https://www.rfc-editor.org/rfc/rfc1123).
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
pub fn is_valid_hostname(hostname: &str) -> bool {
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
