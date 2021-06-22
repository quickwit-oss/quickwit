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

pub mod rendezvous_hasher;

use std::net::{SocketAddr, ToSocketAddrs};

use anyhow;

const GRPC_PORT_INC: u16 = 1;

/// Compute the gRPC port from the base port.
/// Add 1 to the base port to get the gRPC port.
pub fn get_grpc_addr(listen_addr: SocketAddr) -> SocketAddr {
    SocketAddr::new(listen_addr.ip(), listen_addr.port() + GRPC_PORT_INC)
}

pub fn to_socket_addr(addr_str: &str) -> anyhow::Result<SocketAddr> {
    if let Some(addr) = addr_str.to_socket_addrs()?.next() {
        Ok(addr)
    } else {
        Err(anyhow::anyhow!(
            "Cannot convert {} to SocketAddr.",
            addr_str
        ))
    }
}
