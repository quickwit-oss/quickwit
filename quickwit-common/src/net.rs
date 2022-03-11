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

use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, ToSocketAddrs};

/// Finds a random available port.
pub fn find_available_port() -> anyhow::Result<u16> {
    let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
    let listener = TcpListener::bind(socket)?;
    let port = listener.local_addr()?.port();
    Ok(port)
}

/// Converts an object into a resolved `SocketAddr`.
pub fn get_socket_addr<T: ToSocketAddrs + std::fmt::Debug>(addr: &T) -> anyhow::Result<SocketAddr> {
    addr.to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow::anyhow!("Failed to resolve address `{:?}`.", addr))
}

/// Returns true if the socket addr is a valid socket address containing a port.
///
/// If the socket adddress looks invalid to begin with, we may return false or true.
fn contains_port(addr: &str) -> bool {
    // [IPv6]:port
    if let Some((_, colon_port)) = addr[1..].rsplit_once(']') {
        return colon_port.starts_with(':');
    }
    if let Some((host, _port)) = addr[1..].rsplit_once(':') {
        // if host contains a ":" then is thi is probably a IPv6 address.
        return !host.contains(':');
    }
    false
}

/// Attempts to parse a `socket_addr`.
/// If no port is defined, it just accepts the address and uses the given default port.
///
/// This function supports
/// - IPv4
/// - IPv4:port
/// - IPv6
/// - \[IPv6\]:port -- IpV6 contains colon. It is customary to require bracket for this reason.
/// - hostname
/// - hostname:port
/// with or without a port.
///
/// Note that this function returns a SocketAddr, so that if a hostname
/// is given, DNS resolution will happen once and for all.
pub fn parse_socket_addr_with_default_port(
    addr: &str,
    default_port: u16,
) -> anyhow::Result<SocketAddr> {
    if contains_port(addr) {
        get_socket_addr(&addr)
    } else {
        get_socket_addr(&(addr, default_port))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_parse_socket_addr_helper(addr: &str, expected_opt: Option<&str>) {
        let socket_addr_res = parse_socket_addr_with_default_port(addr, 1337);
        if let Some(expected) = expected_opt {
            assert!(
                socket_addr_res.is_ok(),
                "Parsing `{}` was expected to succeed",
                addr
            );
            let socket_addr = socket_addr_res.unwrap();
            let expected_socket_addr: SocketAddr = expected.parse().unwrap();
            assert_eq!(socket_addr, expected_socket_addr);
        } else {
            assert!(
                socket_addr_res.is_err(),
                "Parsing `{}` was expected to fail",
                addr
            );
        }
    }

    #[test]
    fn test_parse_socket_addr_with_ips() {
        test_parse_socket_addr_helper("127.0.0.1", Some("127.0.0.1:1337"));
        test_parse_socket_addr_helper("127.0.0.1:100", Some("127.0.0.1:100"));
        test_parse_socket_addr_helper("127.0..1:100", None);
        test_parse_socket_addr_helper(
            "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
            Some("[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:1337"),
        );
        test_parse_socket_addr_helper("2001:0db8:85a3:0000:0000:8a2e:0370:7334:1000", None);
        test_parse_socket_addr_helper(
            "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:1000",
            Some("[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:1000"),
        );
        test_parse_socket_addr_helper("[2001:0db8:1000", None);
        test_parse_socket_addr_helper("2001:0db8:85a3:0000:0000:8a2e:0370:7334]:1000", None);
    }

    // This test require DNS.
    #[test]
    fn test_parse_socket_addr_with_resolution() {
        let socket_addr = parse_socket_addr_with_default_port("google.com:1000", 1337).unwrap();
        assert_eq!(socket_addr.port(), 1000);
        let socket_addr = parse_socket_addr_with_default_port("google.com", 1337).unwrap();
        assert_eq!(socket_addr.port(), 1337);
    }
}
