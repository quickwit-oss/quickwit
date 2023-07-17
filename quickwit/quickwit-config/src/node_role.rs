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

use std::collections::HashSet;
use std::fmt::Display;
use std::str::FromStr;

use anyhow::bail;
use enum_iterator::{all, Sequence};
use itertools::Itertools;
use serde::Serialize;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Sequence)]
#[serde(into = "&'static str")]
pub enum NodeRole {
    ControlPlane,
    Indexer,
    Janitor,
    Metastore,
    Searcher,
}

#[allow(clippy::from_over_into)]
impl Into<&'static str> for NodeRole {
    fn into(self) -> &'static str {
        self.as_str()
    }
}

impl NodeRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeRole::ControlPlane => "control_plane",
            NodeRole::Indexer => "indexer",
            NodeRole::Searcher => "searcher",
            NodeRole::Janitor => "janitor",
            NodeRole::Metastore => "metastore",
        }
    }

    pub fn all_roles() -> HashSet<NodeRole> {
        all::<NodeRole>().collect()
    }
}

impl Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for NodeRole {
    type Err = anyhow::Error;

    fn from_str(role_str: &str) -> Result<Self, Self::Err> {
        match role_str {
            "control-plane" | "control_plane" => Ok(NodeRole::ControlPlane),
            "indexer" => Ok(NodeRole::Indexer),
            "searcher" => Ok(NodeRole::Searcher),
            "janitor" => Ok(NodeRole::Janitor),
            "metastore" => Ok(NodeRole::Metastore),
            _ => {
                bail!(
                    "Failed to parse role `{role_str}`. Supported node roles are: `{}`.",
                    NodeRole::all_roles().iter().join("`, `")
                )
            }
        }
    }
}
