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

use std::collections::BTreeSet;
use std::fmt::Display;
use std::str::FromStr;

use anyhow::bail;
use enum_iterator::{all, Sequence};
use itertools::Itertools;
use serde::Serialize;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Sequence)]
#[serde(into = "&'static str")]
pub enum QuickwitService {
    ControlPlane,
    Indexer,
    Ingester,
    Janitor,
    Metastore,
    Searcher,
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
            QuickwitService::Ingester => "ingester",
            QuickwitService::Janitor => "janitor",
            QuickwitService::Metastore => "metastore",
            QuickwitService::Searcher => "searcher",
        }
    }

    pub fn supported_services() -> BTreeSet<QuickwitService> {
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
        let service = match service_str {
            "control_plane" => QuickwitService::ControlPlane,
            "indexer" => QuickwitService::Indexer,
            "ingester" => QuickwitService::Ingester,
            "janitor" => QuickwitService::Janitor,
            "metastore" => QuickwitService::Metastore,
            "searcher" => QuickwitService::Searcher,
            _ => {
                bail!(
                    "Failed to parse service `{service_str}`. Supported services are: `{}`.",
                    QuickwitService::supported_services().iter().join("`, `")
                )
            }
        };
        Ok(service)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_quickwit_service() {
        for service in QuickwitService::supported_services() {
            let service_str = service.as_str();
            let parsed_service = QuickwitService::from_str(service_str).unwrap();
            assert_eq!(service, parsed_service);
        }
    }
}
