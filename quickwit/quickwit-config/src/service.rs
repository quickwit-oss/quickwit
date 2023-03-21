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
pub enum QuickwitService {
    ControlPlane,
    Indexer,
    Searcher,
    Janitor,
    Metastore,
}

#[allow(clippy::from_over_into)]
impl Into<&'static str> for QuickwitService {
    fn into(self) -> &'static str {
        self.as_str()
    }
}

impl QuickwitService {
    pub fn as_str(&self) -> &'static str {
        match self {
            QuickwitService::ControlPlane => "control_plane",
            QuickwitService::Indexer => "indexer",
            QuickwitService::Searcher => "searcher",
            QuickwitService::Janitor => "janitor",
            QuickwitService::Metastore => "metastore",
        }
    }

    pub fn supported_services() -> HashSet<QuickwitService> {
        all::<QuickwitService>().collect()
    }
}

impl Display for QuickwitService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for QuickwitService {
    type Err = anyhow::Error;

    fn from_str(service_str: &str) -> Result<Self, Self::Err> {
        match service_str {
            "control_plane" => Ok(QuickwitService::ControlPlane),
            "indexer" => Ok(QuickwitService::Indexer),
            "searcher" => Ok(QuickwitService::Searcher),
            "janitor" => Ok(QuickwitService::Janitor),
            "metastore" => Ok(QuickwitService::Metastore),
            _ => {
                bail!(
                    "Failed to parse service `{service_str}`. Supported services are: `{}`.",
                    QuickwitService::supported_services().iter().join("`, `")
                )
            }
        }
    }
}
