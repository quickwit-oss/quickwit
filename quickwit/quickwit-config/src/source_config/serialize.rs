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

use std::num::NonZeroUsize;

use anyhow::{bail, ensure};
use quickwit_proto::types::SourceId;
use serde::{Deserialize, Serialize};

use super::{TransformConfig, RESERVED_SOURCE_IDS};
use crate::{
    validate_identifier, ConfigFormat, FileSourceParams, SourceConfig, SourceInputFormat,
    SourceParams,
};

type SourceConfigForSerialization = SourceConfigV0_9;

#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
#[serde(tag = "version")]
pub enum VersionedSourceConfig {
    #[serde(rename = "0.9")]
    #[serde(alias = "0.8")]
    V0_9(SourceConfigV0_9),
    // Retro compatibility.
    #[serde(rename = "0.7")]
    V0_7(SourceConfigV0_7),
}

impl From<VersionedSourceConfig> for SourceConfigForSerialization {
    fn from(versioned_source_config: VersionedSourceConfig) -> Self {
        match versioned_source_config {
            VersionedSourceConfig::V0_7(v0_7) => v0_7.into(),
            VersionedSourceConfig::V0_9(v0_9) => v0_9,
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

/// Parses and validates a [`SourceConfig`] update.
///
/// Ensures that the new configuration is valid in itself and compared to the
/// current source config. If the new configuration omits some fields, the
/// default values will be used, not those of the current source config.
pub fn load_source_config_update(
    config_format: ConfigFormat,
    config_content: &[u8],
    current_source_config: &SourceConfig,
) -> anyhow::Result<SourceConfig> {
    let versioned_source_config: VersionedSourceConfig = config_format.parse(config_content)?;
    let source_config_for_serialization: SourceConfigForSerialization =
        versioned_source_config.into();
    let new_source_config = source_config_for_serialization.validate_and_build()?;

    ensure!(
        current_source_config.source_id == new_source_config.source_id,
        "existing `source_id` {} does not match updated `source_id` {}",
        current_source_config.source_id,
        new_source_config.source_id
    );

    current_source_config
        .source_params
        .validate_update(&new_source_config.source_params)?;

    Ok(new_source_config)
}

impl SourceConfigForSerialization {
    /// Checks the validity of the `SourceConfig` as a "deserializable source".
    ///
    /// Two remarks:
    /// - This does not check connectivity, it just validate configuration, without performing any
    ///   IO. See `check_connectivity(..)`.
    /// - This is used each time the `SourceConfig` is deserialized (at creation but also during
    ///   communications with the metastore). When ingesting from stdin, we programmatically create
    ///   an invalid `SourceConfig` and only use it locally.
    fn validate_and_build(self) -> anyhow::Result<SourceConfig> {
        if !RESERVED_SOURCE_IDS.contains(&self.source_id.as_str()) {
            validate_identifier("source", &self.source_id)?;
        }
        let num_pipelines = NonZeroUsize::new(self.num_pipelines)
            .ok_or_else(|| anyhow::anyhow!("`num_pipelines` must be strictly positive"))?;
        match &self.source_params {
            SourceParams::Stdin => {
                bail!(
                    "stdin can only be used as source through the CLI command `quickwit tool \
                     local-ingest`"
                );
            }
            SourceParams::File(_)
            | SourceParams::Kafka(_)
            | SourceParams::Kinesis(_)
            | SourceParams::Pulsar(_) => {
                // TODO consider any validation opportunity
            }
            SourceParams::PubSub(_)
            | SourceParams::Ingest
            | SourceParams::IngestApi
            | SourceParams::IngestCli
            | SourceParams::Vec(_)
            | SourceParams::Void(_) => {}
        }
        match &self.source_params {
            SourceParams::PubSub(_)
            | SourceParams::Kafka(_)
            | SourceParams::File(FileSourceParams::Notifications(_)) => {}
            _ => {
                if self.num_pipelines > 1 {
                    bail!("Quickwit currently supports multiple pipelines only for GCP PubSub or Kafka sources. open an issue https://github.com/quickwit-oss/quickwit/issues if you need the feature for other source types");
                }
            }
        }

        if let Some(transform_config) = &self.transform {
            if matches!(
                self.input_format,
                SourceInputFormat::OtlpLogsJson
                    | SourceInputFormat::OtlpLogsProtobuf
                    | SourceInputFormat::OtlpTracesJson
                    | SourceInputFormat::OtlpTracesProtobuf
            ) {
                bail!("VRL transforms are not supported for OTLP input formats");
            }
            transform_config.validate_vrl_script()?;
        }

        Ok(SourceConfig {
            source_id: self.source_id,
            num_pipelines,
            enabled: self.enabled,
            source_params: self.source_params,
            transform_config: self.transform,
            input_format: self.input_format,
        })
    }
}

impl From<SourceConfig> for SourceConfigV0_9 {
    fn from(source_config: SourceConfig) -> Self {
        SourceConfigV0_9 {
            source_id: source_config.source_id,
            num_pipelines: source_config.num_pipelines.get(),
            enabled: source_config.enabled,
            source_params: source_config.source_params,
            transform: source_config.transform_config,
            input_format: source_config.input_format,
        }
    }
}

impl From<SourceConfig> for VersionedSourceConfig {
    fn from(source_config: SourceConfig) -> Self {
        VersionedSourceConfig::V0_9(source_config.into())
    }
}

impl TryFrom<VersionedSourceConfig> for SourceConfig {
    type Error = anyhow::Error;

    fn try_from(versioned_source_config: VersionedSourceConfig) -> anyhow::Result<Self> {
        let v1: SourceConfigV0_9 = versioned_source_config.into();
        v1.validate_and_build()
    }
}

fn default_max_num_pipelines_per_indexer() -> usize {
    1
}

fn default_num_pipelines() -> usize {
    1
}

fn default_source_enabled() -> bool {
    true
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SourceConfigV0_7 {
    #[schema(value_type = String)]
    pub source_id: SourceId,

    #[serde(
        default = "default_max_num_pipelines_per_indexer",
        alias = "num_pipelines"
    )]
    pub max_num_pipelines_per_indexer: usize,

    #[serde(default = "default_num_pipelines")]
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SourceConfigV0_9 {
    #[schema(value_type = String)]
    pub source_id: SourceId,

    #[serde(default = "default_num_pipelines")]
    pub num_pipelines: usize,

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

impl From<SourceConfigV0_7> for SourceConfigV0_9 {
    fn from(source_config_v0_7: SourceConfigV0_7) -> Self {
        let SourceConfigV0_7 {
            source_id,
            max_num_pipelines_per_indexer: _,
            desired_num_pipelines,
            enabled,
            source_params,
            transform,
            input_format,
        } = source_config_v0_7;
        SourceConfigV0_9 {
            source_id,
            num_pipelines: desired_num_pipelines,
            enabled,
            source_params,
            transform,
            input_format,
        }
    }
}
