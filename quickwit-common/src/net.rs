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
use std::net::{IpAddr, SocketAddr, TcpListener};

use anyhow::{bail, Context};
use tokio::net::{lookup_host, ToSocketAddrs};

/// Represents a host, i.e. an IP address (`127.0.0.1`) or a hostname (`localhost`).
#[derive(Clone, Debug)]
pub enum Host {
    Hostname(String),
    IpAddr(IpAddr),
}

impl Display for Host {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Host::Hostname(hostname) => hostname.fmt(formatter),
            Host::IpAddr(ip_addr) => ip_addr.fmt(formatter),
        }
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
    pub async fn to_socket_addr(&self) -> anyhow::Result<SocketAddr> {
        match &self.host {
            Host::IpAddr(ip_addr) => Ok(SocketAddr::new(*ip_addr, self.port)),
            Host::Hostname(hostname) => get_socket_addr(&(hostname.as_str(), self.port)).await,
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
pub fn find_available_tcp_port() -> anyhow::Result<u16> {
    let socket: SocketAddr = ([127, 0, 0, 1], 0u16).into();
    let listener = TcpListener::bind(socket)?;
    let port = listener.local_addr()?.port();
    Ok(port)
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
    use super::*;

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
}
