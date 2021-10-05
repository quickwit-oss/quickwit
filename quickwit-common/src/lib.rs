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

mod coolid;
pub mod metrics;

pub use coolid::new_coolid;

/// Filenames used for hotcache files.
pub const HOTCACHE_FILENAME: &str = "hotcache";

#[derive(Debug, PartialEq, Eq)]
pub enum QuickwitEnv {
    UNSET,
    LOCAL,
}

impl Default for QuickwitEnv {
    fn default() -> Self {
        Self::UNSET
    }
}

pub fn get_quickwit_env() -> QuickwitEnv {
    match std::env::var("QUICKWIT_ENV") {
        Ok(val) if val.to_lowercase().trim() == "local" => QuickwitEnv::LOCAL,
        Ok(val) => panic!("QUICKWIT_ENV value `{}` is not supported", val),
        Err(_) => QuickwitEnv::UNSET,
    }
}

pub fn setup_logging_for_tests() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        env_logger::builder().format_timestamp(None).init();
    });
}

pub fn split_file(split_id: &str) -> String {
    format!("{}.split", split_id)
}

pub mod net {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, ToSocketAddrs};

    /// Finds a random available port.
    pub fn find_available_port() -> anyhow::Result<u16> {
        let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let listener = TcpListener::bind(socket)?;
        let port = listener.local_addr()?.port();
        Ok(port)
    }

    /// Converts this string to a resolved `SocketAddr`.
    pub fn socket_addr_from_str(addr_str: &str) -> anyhow::Result<SocketAddr> {
        addr_str
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| anyhow::anyhow!("Failed to resolve address `{}`.", addr_str))
    }
}
