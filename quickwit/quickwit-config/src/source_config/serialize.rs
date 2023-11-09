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

use std::num::NonZeroUsize;

use anyhow::bail;
use serde::{Deserialize, Serialize};

use super::{TransformConfig, RESERVED_SOURCE_IDS};
use crate::{validate_identifier, ConfigFormat, SourceConfig, SourceInputFormat, SourceParams};

type SourceConfigForSerialization = SourceConfigV0_6;

#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
#[serde(tag = "version")]
pub enum VersionedSourceConfig {
    #[serde(rename = "0.6")]
    // Retro compatibility.
    #[serde(alias = "0.5")]
    #[serde(alias = "0.4")]
    V0_6(SourceConfigV0_6),
}

impl From<VersionedSourceConfig> for SourceConfigForSerialization {
    fn from(versioned_source_config: VersionedSourceConfig) -> Self {
        match versioned_source_config {
            VersionedSourceConfig::V0_6(v0_6) => v0_6,
        }
    }
}

/// Parses and validates an [`SourceConfig`] as supplied by a user with a given [`ConfigFormat`],
/// and config content.
pub fn load_source_config_from_user_config(
    config_format: ConfigFormat,
    config_content: &[u8],
) -> anyhow::Result<SourceConfig> {
    let versioned_source_config: VersionedSourceConfig = config_format.parse(config_content)?;
    let source_config_for_serialization: SourceConfigForSerialization =
        versioned_source_config.into();
    source_config_for_serialization.validate_and_build()
}

impl SourceConfigForSerialization {
    /// Checks the validity of the `SourceConfig` as a "deserializable source".
    ///
    /// Two remarks:
    /// - This does not check connectivity. (See `check_connectivity(..)`)
    /// This just validate configuration, without performing any IO.
    /// - This is only here to validate user input.
    /// When ingesting from stdin, we programmatically create an invalid `SourceConfig`.
    ///
    /// TODO refactor #1065
    fn validate_and_build(self) -> anyhow::Result<SourceConfig> {
        if !RESERVED_SOURCE_IDS.contains(&self.source_id.as_str()) {
            validate_identifier("Source ID", &self.source_id)?;
        }
        let desired_num_pipelines = NonZeroUsize::new(self.desired_num_pipelines)
            .ok_or_else(|| anyhow::anyhow!("`desired_num_pipelines` must be strictly positive"))?;
        let max_num_pipelines_per_indexer = NonZeroUsize::new(self.max_num_pipelines_per_indexer)
            .ok_or_else(|| {
            anyhow::anyhow!("`max_num_pipelines_per_indexer` must be strictly positive")
        })?;
        match &self.source_params {
            // We want to forbid source_config with no filepath
            SourceParams::File(file_params) => {
                if file_params.filepath.is_none() {
                    bail!(
                        "source `{}` of type `file` must contain a filepath",
                        self.source_id
                    )
                }
            }
            SourceParams::Kafka(_) | SourceParams::Kinesis(_) | SourceParams::Pulsar(_) => {
                // TODO consider any validation opportunity
            }
            SourceParams::GcpPubSub(_)
            | SourceParams::Ingest
            | SourceParams::IngestApi
            | SourceParams::IngestCli
            | SourceParams::Vec(_)
            | SourceParams::Void(_) => {}
        }
        match &self.source_params {
            SourceParams::GcpPubSub(_) | SourceParams::Kafka(_) => {}
            _ => {
                if self.desired_num_pipelines > 1 || self.max_num_pipelines_per_indexer > 1 {
                    bail!("Quickwit currently supports multiple pipelines only for GCP PubSub or Kafka sources. open an issue https://github.com/quickwit-oss/quickwit/issues if you need the feature for other source types");
                }
            }
        }

        if let Some(transform_config) = &self.transform {
            if matches!(
                self.input_format,
                SourceInputFormat::OtlpTraceJson | SourceInputFormat::OtlpTraceProtobuf
            ) {
                bail!("VRL transforms are not supported for OTLP input formats");
            }
            transform_config.validate_vrl_script()?;
        }

        Ok(SourceConfig {
            source_id: self.source_id,
            max_num_pipelines_per_indexer,
            desired_num_pipelines,
            enabled: self.enabled,
            source_params: self.source_params,
            transform_config: self.transform,
            input_format: self.input_format,
        })
    }
}

impl From<SourceConfig> for SourceConfigV0_6 {
    fn from(source_config: SourceConfig) -> Self {
        SourceConfigV0_6 {
            source_id: source_config.source_id,
            max_num_pipelines_per_indexer: source_config.max_num_pipelines_per_indexer.get(),
            desired_num_pipelines: source_config.desired_num_pipelines.get(),
            enabled: source_config.enabled,
            source_params: source_config.source_params,
            transform: source_config.transform_config,
            input_format: source_config.input_format,
        }
    }
}

impl From<SourceConfig> for VersionedSourceConfig {
    fn from(source_config: SourceConfig) -> Self {
        VersionedSourceConfig::V0_6(source_config.into())
    }
}

impl TryFrom<VersionedSourceConfig> for SourceConfig {
    type Error = anyhow::Error;

    fn try_from(versioned_source_config: VersionedSourceConfig) -> anyhow::Result<Self> {
        let v1: SourceConfigV0_6 = versioned_source_config.into();
        v1.validate_and_build()
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
pub struct SourceConfigV0_6 {
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

    // Denotes the input data format.
    #[serde(default)]
    pub input_format: SourceInputFormat,
}
