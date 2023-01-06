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

use anyhow::bail;
use serde::{Deserialize, Serialize};

use super::TransformConfig;
use crate::{
    validate_identifier, SourceConfig, SourceParams, CLI_INGEST_SOURCE_ID, INGEST_API_SOURCE_ID,
};

type SourceConfigForSerialization = SourceConfigV0_4;

#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
#[serde(tag = "version")]
pub enum VersionedSourceConfig {
    #[serde(rename = "0.4")]
    V0_4(SourceConfigV0_4),
}

impl From<SourceConfig> for SourceConfigV0_4 {
    fn from(source_config: SourceConfig) -> Self {
        SourceConfigV0_4 {
            source_id: source_config.source_id,
            max_num_pipelines_per_indexer: source_config.max_num_pipelines_per_indexer,
            desired_num_pipelines: source_config.desired_num_pipelines,
            enabled: source_config.enabled,
            source_params: source_config.source_params,
            transform: source_config.transform_config,
        }
    }
}

impl From<SourceConfig> for VersionedSourceConfig {
    fn from(source_config: SourceConfig) -> Self {
        VersionedSourceConfig::V0_4(source_config.into())
    }
}

impl TryFrom<VersionedSourceConfig> for SourceConfig {
    type Error = anyhow::Error;

    fn try_from(versioned_source_config: VersionedSourceConfig) -> anyhow::Result<Self> {
        let v1: SourceConfigV0_4 = versioned_source_config.into();
        v1.validate_and_build()
    }
}

impl From<VersionedSourceConfig> for SourceConfigForSerialization {
    fn from(versioned_source_config: VersionedSourceConfig) -> Self {
        match versioned_source_config {
            VersionedSourceConfig::V0_4(v0_4) => v0_4,
        }
    }
}

fn default_max_num_pipelines_per_indexer() -> usize {
    1
}

fn default_desired_num_pipelines() -> usize {
    1
}

fn default_source_enabled() -> bool {
    true
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct SourceConfigV0_4 {
    pub source_id: String,

    #[serde(
        default = "default_max_num_pipelines_per_indexer",
        alias = "num_pipelines"
    )]
    pub max_num_pipelines_per_indexer: usize,

    #[serde(default = "default_desired_num_pipelines")]
    pub desired_num_pipelines: usize,

    // Denotes if this source is enabled.
    #[serde(default = "default_source_enabled")]
    pub enabled: bool,

    #[serde(flatten)]
    pub source_params: SourceParams,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub transform: Option<TransformConfig>,
}

impl SourceConfigV0_4 {
    /// Checks the validity of the `SourceConfig` as a "serializable source".
    ///
    /// Two remarks:
    /// - This does not check connectivity. (See `check_connectivity(..)`)
    /// This just validate configuration, without performing any IO.
    /// - This is only here to validate user input.
    /// When ingesting from stdin, we programmatically create an invalid `SourceConfig`.
    ///
    /// TODO refactor #1065
    pub(crate) fn validate_and_build(self) -> anyhow::Result<SourceConfig> {
        if self.source_id != CLI_INGEST_SOURCE_ID && self.source_id != INGEST_API_SOURCE_ID {
            validate_identifier("Source ID", &self.source_id)?;
        }
        match &self.source_params {
            // We want to forbid source_config with no filepath
            SourceParams::File(file_params) => {
                if file_params.filepath.is_none() {
                    bail!(
                        "Source `{}` of type `file` must contain a filepath.",
                        self.source_id
                    )
                }
            }
            SourceParams::Kafka(_) | SourceParams::Kinesis(_) => {
                // TODO consider any validation opportunity
            }
            SourceParams::Vec(_)
            | SourceParams::Void(_)
            | SourceParams::IngestApi
            | SourceParams::IngestCli => {}
        }

        if let Some(transform_config) = &self.transform {
            transform_config.compile_vrl_script()?;
        }

        Ok(SourceConfig {
            source_id: self.source_id,
            max_num_pipelines_per_indexer: self.max_num_pipelines_per_indexer,
            desired_num_pipelines: self.desired_num_pipelines,
            enabled: self.enabled,
            source_params: self.source_params,
            transform_config: self.transform,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_config_validation() {
        {
            let source_config = SourceConfigForSerialization {
                source_id: "file_source".to_string(),
                max_num_pipelines_per_indexer: 1,
                desired_num_pipelines: 1,
                enabled: true,
                source_params: SourceParams::stdin(),
                transform: None,
            };
            assert!(source_config
                .validate_and_build()
                .unwrap_err()
                .to_string()
                .contains("must contain a filepath"));
        }
        {
            let source_config = SourceConfigForSerialization {
                source_id: "kafka_source".to_string(),
                max_num_pipelines_per_indexer: 1,
                desired_num_pipelines: 1,
                enabled: true,
                source_params: SourceParams::void(),
                transform: Some(TransformConfig::for_test("foo")),
            };
            assert!(source_config
                .validate_and_build()
                .unwrap_err()
                .to_string()
                .contains("Failed to compile"));
        }
    }
}
