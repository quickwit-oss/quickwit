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

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
pub struct ConstWriteAmplificationMergePolicyConfig {
    /// Number of splits to merge together in a single merge operation.
    #[serde(default = "default_merge_factor")]
    pub merge_factor: usize,
    /// Maximum number of splits that can be merged together in a single merge operation.
    #[serde(default = "default_max_merge_factor")]
    pub max_merge_factor: usize,
    /// Maximum number of merges that a given split should undergo.
    #[serde(default = "default_max_merge_ops")]
    pub max_merge_ops: usize,
}

impl Default for ConstWriteAmplificationMergePolicyConfig {
    fn default() -> ConstWriteAmplificationMergePolicyConfig {
        ConstWriteAmplificationMergePolicyConfig {
            max_merge_ops: default_max_merge_ops(),
            merge_factor: default_merge_factor(),
            max_merge_factor: default_max_merge_factor(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct StableLogMergePolicyConfig {
    /// Number of docs below which all splits are considered as belonging to the same level.
    #[serde(default = "default_min_level_num_docs")]
    pub min_level_num_docs: usize,
    /// Number of splits to merge together in a single merge operation.
    #[serde(default = "default_merge_factor")]
    pub merge_factor: usize,
    /// Maximum number of splits that can be merged together in a single merge operation.
    #[serde(default = "default_max_merge_factor")]
    pub max_merge_factor: usize,
}

fn default_merge_factor() -> usize {
    10
}

fn default_max_merge_factor() -> usize {
    12
}

fn default_max_merge_ops() -> usize {
    4
}

fn default_min_level_num_docs() -> usize {
    100_000
}

impl Default for StableLogMergePolicyConfig {
    fn default() -> Self {
        StableLogMergePolicyConfig {
            min_level_num_docs: default_min_level_num_docs(),
            merge_factor: default_merge_factor(),
            max_merge_factor: default_max_merge_factor(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
#[serde(tag = "type")]
pub enum MergePolicyConfig {
    #[serde(rename = "no_merge")]
    Nop,
    #[serde(rename = "limit_merge")]
    ConstWriteAmplification(ConstWriteAmplificationMergePolicyConfig),
    #[serde(rename = "stable_log")]
    #[serde(alias = "default")]
    StableLog(StableLogMergePolicyConfig),
}

impl Default for MergePolicyConfig {
    fn default() -> Self {
        MergePolicyConfig::StableLog(StableLogMergePolicyConfig::default())
    }
}

impl MergePolicyConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        let (merge_factor, max_merge_factor) = match self {
            MergePolicyConfig::Nop => {
                return Ok(());
            }
            MergePolicyConfig::ConstWriteAmplification(config) => {
                (config.merge_factor, config.max_merge_factor)
            }
            MergePolicyConfig::StableLog(config) => (config.merge_factor, config.max_merge_factor),
        };
        if max_merge_factor < merge_factor {
            anyhow::bail!(
                "Index config merge policy `max_merge_factor` must be superior or equal to \
                 `merge_factor`."
            );
        }
        Ok(())
    }
}
