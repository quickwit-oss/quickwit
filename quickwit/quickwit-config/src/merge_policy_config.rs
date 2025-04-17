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

use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

fn is_zero(value: &usize) -> bool {
    *value == 0
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
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
    /// Duration relative to `split.created_timestamp` after which a split
    /// becomes mature.
    /// If `now() >= split.created_timestamp + maturation_period` then
    /// the split is considered mature.
    #[schema(value_type = String)]
    #[serde(default = "default_maturation_period")]
    #[serde(deserialize_with = "parse_human_duration")]
    #[serde(serialize_with = "serialize_duration")]
    pub maturation_period: Duration,
    #[serde(default)]
    #[serde(skip_serializing_if = "is_zero")]
    pub max_finalize_merge_operations: usize,
    /// Splits with a number of docs higher than
    /// `max_finalize_split_num_docs` will not be considered
    /// for finalize split merge operations.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_finalize_split_num_docs: Option<usize>,
}

impl Default for ConstWriteAmplificationMergePolicyConfig {
    fn default() -> ConstWriteAmplificationMergePolicyConfig {
        ConstWriteAmplificationMergePolicyConfig {
            max_merge_ops: default_max_merge_ops(),
            merge_factor: default_merge_factor(),
            max_merge_factor: default_max_merge_factor(),
            maturation_period: default_maturation_period(),
            max_finalize_merge_operations: 0,
            max_finalize_split_num_docs: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
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
    /// Duration relative to `split.created_timestamp` after which a split
    /// becomes mature.
    /// If `now() >= split.created_timestamp + maturation_period` then
    /// the split is mature.
    #[schema(value_type = String)]
    #[serde(default = "default_maturation_period")]
    #[serde(deserialize_with = "parse_human_duration")]
    #[serde(serialize_with = "serialize_duration")]
    pub maturation_period: Duration,
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

fn default_maturation_period() -> Duration {
    Duration::from_secs(48 * 3600)
}

impl Default for StableLogMergePolicyConfig {
    fn default() -> Self {
        StableLogMergePolicyConfig {
            min_level_num_docs: default_min_level_num_docs(),
            merge_factor: default_merge_factor(),
            max_merge_factor: default_max_merge_factor(),
            maturation_period: default_maturation_period(),
        }
    }
}

fn parse_human_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where D: Deserializer<'de> {
    let value: String = Deserialize::deserialize(deserializer)?;
    let duration = humantime::parse_duration(&value).map_err(|error| {
        de::Error::custom(format!(
            "failed to parse human-readable duration `{value}`: {error:?}",
        ))
    })?;
    Ok(duration)
}

fn serialize_duration<S>(value: &Duration, s: S) -> Result<S::Ok, S::Error>
where S: Serializer {
    let value_str = humantime::format_duration(*value).to_string();
    s.serialize_str(&value_str)
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, Hash, utoipa::ToSchema)]
#[serde(tag = "type")]
#[serde(deny_unknown_fields)]
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
    pub fn noop() -> Self {
        MergePolicyConfig::Nop
    }

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
                "index config merge policy `max_merge_factor` must be superior or equal to \
                 `merge_factor`"
            );
        }
        Ok(())
    }
}
