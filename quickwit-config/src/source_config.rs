// Copyright (C) 2021 Quickwit, Inc.
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

use std::path::{Path, PathBuf};

use anyhow::bail;
use quickwit_common::uri::Uri;
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SourceConfig {
    pub source_id: String,
    #[serde(flatten)]
    pub source_params: SourceParams,
}

impl SourceConfig {
    /// Check the validity of the `SourceConfig` as a "serializable source".
    ///
    /// Two remarks:
    /// - This does not check connectivity. (See `check_connectivity(..)`)
    /// This just validate configuration, without performing any IO.
    /// - This is only here to validate user input.
    /// When ingesting from StdIn, we programmatically create an invalid `SourceConfig`.
    ///
    /// TODO refactor #1065
    pub fn validate(&self) -> anyhow::Result<()> {
        match &self.source_params {
            // We want to forbid source_config with no filepath
            SourceParams::File(file_params) => {
                if file_params.filepath.is_none() {
                    bail!(
                        "Source `{}` of type `file` must contain a `filepath`",
                        self.source_id
                    )
                }
                Ok(())
            }
            SourceParams::Kafka(_) | SourceParams::Kinesis(_) => {
                // TODO consider any validation opportunity
                Ok(())
            }
            SourceParams::Vec(_) | SourceParams::Void(_) => Ok(()),
        }
    }

    pub fn source_type(&self) -> &str {
        match self.source_params {
            SourceParams::File(_) => "file",
            SourceParams::Kafka(_) => "kafka",
            SourceParams::Kinesis(_) => "kinesis",
            SourceParams::Vec(_) => "vec",
            SourceParams::Void(_) => "void",
        }
    }

    // TODO: Remove after source factory refactor.
    pub fn params(&self) -> serde_json::Value {
        match &self.source_params {
            SourceParams::File(params) => serde_json::to_value(params),
            SourceParams::Kafka(params) => serde_json::to_value(params),
            SourceParams::Kinesis(params) => serde_json::to_value(params),
            SourceParams::Vec(params) => serde_json::to_value(params),
            SourceParams::Void(params) => serde_json::to_value(params),
        }
        .unwrap()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "source_type", content = "params")]
pub enum SourceParams {
    #[serde(rename = "file")]
    File(FileSourceParams),
    #[serde(rename = "kafka")]
    Kafka(KafkaSourceParams),
    #[serde(rename = "kinesis")]
    Kinesis(KinesisSourceParams),
    #[serde(rename = "vec")]
    Vec(VecSourceParams),
    #[serde(rename = "void")]
    Void(VoidSourceParams),
}

impl SourceParams {
    pub fn file<P: AsRef<Path>>(filepath: P) -> Self {
        Self::File(FileSourceParams::file(filepath))
    }

    pub fn stdin() -> Self {
        Self::File(FileSourceParams::stdin())
    }

    pub fn void() -> Self {
        Self::Void(VoidSourceParams)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileSourceParams {
    /// Path of the file to read. Assume stdin if None.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    #[serde(deserialize_with = "absolute_filepath_from_str")]
    pub filepath: Option<PathBuf>, //< If None read from stdin.
}

// Deserializing a filepath string into an absolute filepath.
fn absolute_filepath_from_str<'de, D>(deserializer: D) -> Result<Option<PathBuf>, D::Error>
where D: Deserializer<'de> {
    let filepath_opt: Option<String> = Deserialize::deserialize(deserializer)?;
    if let Some(filepath) = filepath_opt {
        let uri = Uri::try_new(&filepath).map_err(D::Error::custom)?;
        Ok(uri.filepath().map(|path| path.to_path_buf()))
    } else {
        Ok(None)
    }
}

impl FileSourceParams {
    pub fn file<P: AsRef<Path>>(filepath: P) -> Self {
        FileSourceParams {
            filepath: Some(filepath.as_ref().to_path_buf()),
        }
    }

    pub fn stdin() -> Self {
        FileSourceParams { filepath: None }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaSourceParams {
    /// Name of the topic that the source consumes.
    pub topic: String,
    /// Kafka client log level. Possible values are `debug`, `info`, `warn`, and `error`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_log_level: Option<String>,
    /// Kafka client configuration parameters.
    #[serde(default = "serde_json::Value::default")]
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    pub client_params: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RegionOrEndpoint {
    Region(String),
    Endpoint(String),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "KinesisSourceParamsInner")]
pub struct KinesisSourceParams {
    pub stream_name: String,
    pub region_or_endpoint: Option<RegionOrEndpoint>,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
struct KinesisSourceParamsInner {
    pub stream_name: String,
    pub region: Option<String>,
    pub endpoint: Option<String>,
}

impl TryFrom<KinesisSourceParamsInner> for KinesisSourceParams {
    type Error = &'static str;

    fn try_from(value: KinesisSourceParamsInner) -> Result<Self, Self::Error> {
        if value.region.is_some() && value.endpoint.is_some() {
            return Err(
                "Kinesis source parameters `region` and `endpoint` are mutually exclusive.",
            );
        }
        let region = value.region.map(RegionOrEndpoint::Region);
        let endpoint = value.endpoint.map(RegionOrEndpoint::Endpoint);
        let region_or_endpoint = region.or(endpoint);

        Ok(KinesisSourceParams {
            stream_name: value.stream_name,
            region_or_endpoint,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VecSourceParams {
    pub items: Vec<String>,
    pub batch_num_docs: usize,
    #[serde(default)]
    pub partition: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VoidSourceParams;

#[cfg(test)]
mod tests {
    use quickwit_common::uri::Uri;

    use crate::source_config::RegionOrEndpoint;
    use crate::{FileSourceParams, KinesisSourceParams};

    #[test]
    fn test_file_source_params_serialization() {
        {
            let yaml = r#"
                filepath: source-path.json
            "#;
            let file_params = serde_yaml::from_str::<FileSourceParams>(yaml).unwrap();
            let uri = Uri::try_new("source-path.json").unwrap();
            assert_eq!(
                file_params.filepath.unwrap().as_path(),
                uri.filepath().unwrap()
            )
        }
    }

    #[test]
    fn test_kinesis_source_params_serialization() {
        {
            {
                let yaml = r#"
                    stream_name: my-stream
                "#;
                assert_eq!(
                    serde_yaml::from_str::<KinesisSourceParams>(yaml).unwrap(),
                    KinesisSourceParams {
                        stream_name: "my-stream".to_string(),
                        region_or_endpoint: None,
                    }
                );
            }
            {
                let yaml = r#"
                    stream_name: my-stream
                    region: us-west-1
                "#;
                assert_eq!(
                    serde_yaml::from_str::<KinesisSourceParams>(yaml).unwrap(),
                    KinesisSourceParams {
                        stream_name: "my-stream".to_string(),
                        region_or_endpoint: Some(RegionOrEndpoint::Region("us-west-1".to_string())),
                    }
                );
            }
            {
                let yaml = r#"
                    stream_name: my-stream
                    region: us-west-1
                    endpoint: https://localhost:4566
                "#;
                let error = serde_yaml::from_str::<KinesisSourceParams>(yaml).unwrap_err();
                assert!(error.to_string().starts_with("Kinesis source parameters "));
            }
        }
    }
}
