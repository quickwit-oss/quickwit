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

//! [`ClusterNodeState`] is a wrapper around [`chitchat::NodeState`] that provides

use std::collections::BTreeSet;
use std::error::Error;
use std::net::SocketAddr;
use std::str::FromStr;

use anyhow::Context;
use chitchat::NodeState as ChitchatNodeState;
use quickwit_config::service::QuickwitService;

use crate::member::{GRPC_ADVERTISE_ADDR_KEY, READINESS_KEY, READINESS_VALUE_READY};

struct ClusterNodeState {
    is_ready: bool,
    grpc_advertise_addr: SocketAddr,
    indexing_tasks: Vec<String>,
}

impl TryFrom<ChitchatNodeState> for ClusterNodeState {
    type Error = anyhow::Error;

    fn try_from(node_state: ChitchatNodeState) -> Result<Self, Self::Error> {
        let is_ready = find_key(&node_state, READINESS_KEY)? == READINESS_VALUE_READY;
        let grpc_advertise_addr: SocketAddr = find_key_then_parse_value(
            &node_state,
            GRPC_ADVERTISE_ADDR_KEY,
            "gRPC advertise address",
        )?;
        let indexing_tasks = Vec::new();
        let node_state = Self {
            is_ready,
            grpc_advertise_addr,
            indexing_tasks,
        };
        Ok(node_state)
    }
}

fn find_key<'a>(node_state: &'a ChitchatNodeState, key: &str) -> anyhow::Result<&'a str> {
    node_state
        .get(key)
        .with_context(|| format!("Could not find `{key}` key in node state."))
}

fn find_key_then_parse_value<'a, T>(
    node_state: &'a ChitchatNodeState,
    key: &str,
    label: &str,
) -> anyhow::Result<T>
where
    T: FromStr,
    <T as FromStr>::Err: Error + Send + Sync + 'static,
{
    let value = node_state
        .get(key)
        .with_context(|| format!("Could not find `{key}` key in node state."))?;
    value
        .parse::<T>()
        .with_context(|| format!("Failed to parse {label} `{key}`."))
}
