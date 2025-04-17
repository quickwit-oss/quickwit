// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;
use std::fmt::Display;
use std::str::FromStr;

use anyhow::bail;
use enum_iterator::{Sequence, all};
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
            "control-plane" | "control_plane" => Ok(QuickwitService::ControlPlane),
            "indexer" => Ok(QuickwitService::Indexer),
            "searcher" => Ok(QuickwitService::Searcher),
            "janitor" => Ok(QuickwitService::Janitor),
            "metastore" => Ok(QuickwitService::Metastore),
            _ => {
                bail!(
                    "failed to parse service `{service_str}`. supported services are: `{}`",
                    QuickwitService::supported_services().iter().join("`, `")
                )
            }
        }
    }
}
