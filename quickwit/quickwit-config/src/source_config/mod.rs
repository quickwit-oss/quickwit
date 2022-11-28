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

mod serialize;

use std::path::{Path, PathBuf};
use std::str::FromStr;

use quickwit_common::uri::Uri;
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value as JsonValue;
// For backward compatibility.
use serialize::VersionedSourceConfig;

use crate::{is_false, ConfigFormat, TestableForRegression};

/// Reserved source ID for the `quickwit index ingest` CLI command.
pub const CLI_INGEST_SOURCE_ID: &str = "_ingest-cli-source";

/// Reserved source ID used for Quickwit ingest API.
pub const INGEST_API_SOURCE_ID: &str = "_ingest-api-source";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(into = "VersionedSourceConfig")]
#[serde(try_from = "VersionedSourceConfig")]
pub struct SourceConfig {
    pub source_id: String,

    #[doc(hidden)]
    /// Number of indexing pipelines spawned for the source on each indexer.
    /// Therefore, if there exists `n` indexers in the cluster, there will be `n` * `num_pipelines`
    /// indexing pipelines running for the source.
    pub num_pipelines: usize,

    // Denotes if this source is enabled.
    pub enabled: bool,

    pub source_params: SourceParams,
}

impl SourceConfig {
    /// Parses and validates a [`SourceConfig`] from a given URI and config content.
    pub fn load(uri: &Uri, file_content: &[u8]) -> anyhow::Result<Self> {
        let config_format = ConfigFormat::sniff_from_uri(uri)?;
        config_format.parse(file_content)
    }

    pub fn source_type(&self) -> &str {
        match self.source_params {
            SourceParams::File(_) => "file",
            SourceParams::Kafka(_) => "kafka",
            SourceParams::Kinesis(_) => "kinesis",
            SourceParams::Vec(_) => "vec",
            SourceParams::Void(_) => "void",
            SourceParams::IngestApi => "ingest-api",
            SourceParams::IngestCli => "ingest-cli",
        }
    }

    // TODO: Remove after source factory refactor.
    pub fn params(&self) -> JsonValue {
        match &self.source_params {
            SourceParams::File(params) => serde_json::to_value(params),
            SourceParams::Kafka(params) => serde_json::to_value(params),
            SourceParams::Kinesis(params) => serde_json::to_value(params),
            SourceParams::Vec(params) => serde_json::to_value(params),
            SourceParams::Void(params) => serde_json::to_value(params),
            SourceParams::IngestApi => serde_json::to_value(()),
            SourceParams::IngestCli => serde_json::to_value(()),
        }
        .unwrap()
    }

    pub fn num_pipelines(&self) -> Option<usize> {
        match &self.source_params {
            SourceParams::Kafka(_) | SourceParams::Void(_) => Some(self.num_pipelines),
            _ => None,
        }
    }

    /// Creates the default ingest-api source config.
    pub fn ingest_api_default() -> SourceConfig {
        SourceConfig {
            source_id: INGEST_API_SOURCE_ID.to_string(),
            num_pipelines: 1,
            enabled: true,
            source_params: SourceParams::IngestApi,
        }
    }

    /// Creates the default cli-ingest source config.
    pub fn cli_ingest_source() -> SourceConfig {
        SourceConfig {
            source_id: CLI_INGEST_SOURCE_ID.to_string(),
            num_pipelines: 1,
            enabled: true,
            source_params: SourceParams::IngestCli,
        }
    }
}

impl TestableForRegression for SourceConfig {
    fn sample_for_regression() -> Self {
        SourceConfig {
            source_id: "kafka-source".to_string(),
            num_pipelines: 2,
            enabled: true,
            source_params: SourceParams::Kafka(KafkaSourceParams {
                topic: "kafka-topic".to_string(),
                client_log_level: None,
                client_params: serde_json::json!({}),
                enable_backfill_mode: false,
            }),
        }
    }

    fn test_equality(&self, other: &Self) {
        assert_eq!(self, other);
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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
    #[serde(rename = "ingest-api")]
    IngestApi,
    #[serde(rename = "ingest-cli")]
    IngestCli,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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
        let uri = Uri::from_str(&filepath).map_err(D::Error::custom)?;
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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
    pub client_params: JsonValue,
    /// When backfill mode is enabled, the source exits after reaching the end of the topic.
    #[serde(default)]
    #[serde(skip_serializing_if = "is_false")]
    pub enable_backfill_mode: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RegionOrEndpoint {
    Region(String),
    Endpoint(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "KinesisSourceParamsInner")]
pub struct KinesisSourceParams {
    pub stream_name: String,
    #[serde(flatten)]
    pub region_or_endpoint: Option<RegionOrEndpoint>,
    /// When backfill mode is enabled, the source exits after reaching the end of the stream.
    #[serde(skip_serializing_if = "is_false")]
    pub enable_backfill_mode: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
struct KinesisSourceParamsInner {
    pub stream_name: String,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    #[serde(default)]
    pub enable_backfill_mode: bool,
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
            enable_backfill_mode: value.enable_backfill_mode,
        })
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VecSourceParams {
    pub docs: Vec<String>,
    pub batch_num_docs: usize,
    #[serde(default)]
    pub partition: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VoidSourceParams;

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use quickwit_common::uri::Uri;
    use serde_json::json;

    use super::*;
    use crate::source_config::RegionOrEndpoint;
    use crate::{ConfigFormat, FileSourceParams, KinesisSourceParams};

    fn get_source_config_filepath(source_config_filename: &str) -> String {
        format!(
            "{}/resources/tests/source_config/{}",
            env!("CARGO_MANIFEST_DIR"),
            source_config_filename
        )
    }

    #[tokio::test]
    async fn test_load_kafka_source_config() {
        let source_config_filepath = get_source_config_filepath("kafka-source.json");
        let file_content = std::fs::read_to_string(&source_config_filepath).unwrap();
        let source_config_uri = Uri::from_str(&source_config_filepath).unwrap();
        let source_config =
            SourceConfig::load(&source_config_uri, file_content.as_bytes()).unwrap();
        let expected_source_config = SourceConfig {
            source_id: "hdfs-logs-kafka-source".to_string(),
            num_pipelines: 2,
            enabled: true,
            source_params: SourceParams::Kafka(KafkaSourceParams {
                topic: "cloudera-cluster-logs".to_string(),
                client_log_level: None,
                client_params: json! {{"bootstrap.servers": "localhost:9092"}},
                enable_backfill_mode: false,
            }),
        };
        assert_eq!(source_config, expected_source_config);
        assert_eq!(source_config.num_pipelines().unwrap(), 2);
    }

    #[test]
    fn test_kafka_source_params_serialization() {
        {
            let params = KafkaSourceParams {
                topic: "my-topic".to_string(),
                client_log_level: None,
                client_params: json!(null),
                enable_backfill_mode: false,
            };
            let params_yaml = serde_yaml::to_string(&params).unwrap();

            assert_eq!(
                serde_yaml::from_str::<KafkaSourceParams>(&params_yaml).unwrap(),
                params,
            )
        }
        {
            let params = KafkaSourceParams {
                topic: "my-topic".to_string(),
                client_log_level: Some("info".to_string()),
                client_params: json! {{"bootstrap.servers": "localhost:9092"}},
                enable_backfill_mode: false,
            };
            let params_yaml = serde_yaml::to_string(&params).unwrap();

            assert_eq!(
                serde_yaml::from_str::<KafkaSourceParams>(&params_yaml).unwrap(),
                params,
            )
        }
    }

    #[test]
    fn test_kafka_source_params_deserialization() {
        {
            let yaml = r#"
                    topic: my-topic
                "#;
            assert_eq!(
                serde_yaml::from_str::<KafkaSourceParams>(yaml).unwrap(),
                KafkaSourceParams {
                    topic: "my-topic".to_string(),
                    client_log_level: None,
                    client_params: json!(null),
                    enable_backfill_mode: false,
                }
            );
        }
        {
            let yaml = r#"
                    topic: my-topic
                    client_log_level: info
                    client_params:
                        bootstrap.servers: localhost:9092
                    enable_backfill_mode: true
                "#;
            assert_eq!(
                serde_yaml::from_str::<KafkaSourceParams>(yaml).unwrap(),
                KafkaSourceParams {
                    topic: "my-topic".to_string(),
                    client_log_level: Some("info".to_string()),
                    client_params: json! {{"bootstrap.servers": "localhost:9092"}},
                    enable_backfill_mode: true,
                }
            );
        }
    }

    #[tokio::test]
    async fn test_load_kinesis_source_config() {
        let source_config_filepath = get_source_config_filepath("kinesis-source.yaml");
        let file_content = std::fs::read_to_string(&source_config_filepath).unwrap();
        let source_config_uri = Uri::from_str(&source_config_filepath).unwrap();
        let source_config =
            SourceConfig::load(&source_config_uri, file_content.as_bytes()).unwrap();
        let expected_source_config = SourceConfig {
            source_id: "hdfs-logs-kinesis-source".to_string(),
            num_pipelines: 1,
            enabled: true,
            source_params: SourceParams::Kinesis(KinesisSourceParams {
                stream_name: "emr-cluster-logs".to_string(),
                region_or_endpoint: None,
                enable_backfill_mode: false,
            }),
        };
        assert_eq!(source_config, expected_source_config);
        assert!(source_config.num_pipelines().is_none());
    }

    #[test]
    fn test_file_source_params_serialization() {
        {
            let yaml = r#"
                filepath: source-path.json
            "#;
            let file_params = serde_yaml::from_str::<FileSourceParams>(yaml).unwrap();
            let uri = Uri::from_str("source-path.json").unwrap();
            assert_eq!(
                file_params.filepath.unwrap().as_path(),
                uri.filepath().unwrap()
            )
        }
    }

    #[test]
    fn test_kinesis_source_params_serialization() {
        {
            let params = KinesisSourceParams {
                stream_name: "my-stream".to_string(),
                region_or_endpoint: None,
                enable_backfill_mode: false,
            };
            let params_yaml = serde_yaml::to_string(&params).unwrap();

            assert_eq!(
                serde_yaml::from_str::<KinesisSourceParams>(&params_yaml).unwrap(),
                params,
            )
        }
        {
            let params = KinesisSourceParams {
                stream_name: "my-stream".to_string(),
                region_or_endpoint: Some(RegionOrEndpoint::Region("us-west-1".to_string())),
                enable_backfill_mode: false,
            };
            let params_yaml = serde_yaml::to_string(&params).unwrap();

            assert_eq!(
                serde_yaml::from_str::<KinesisSourceParams>(&params_yaml).unwrap(),
                params,
            )
        }
        {
            let params = KinesisSourceParams {
                stream_name: "my-stream".to_string(),
                region_or_endpoint: Some(RegionOrEndpoint::Endpoint(
                    "https://localhost:4566".to_string(),
                )),
                enable_backfill_mode: false,
            };
            let params_yaml = serde_yaml::to_string(&params).unwrap();

            assert_eq!(
                ConfigFormat::Yaml
                    .parse::<KinesisSourceParams>(params_yaml.as_bytes())
                    .unwrap(),
                params,
            )
        }
    }

    #[test]
    fn test_kinesis_source_params_deserialization() {
        {
            let yaml = r#"
                    stream_name: my-stream
                "#;
            assert_eq!(
                serde_yaml::from_str::<KinesisSourceParams>(yaml).unwrap(),
                KinesisSourceParams {
                    stream_name: "my-stream".to_string(),
                    region_or_endpoint: None,
                    enable_backfill_mode: false,
                }
            );
        }
        {
            let yaml = r#"
                    stream_name: my-stream
                    region: us-west-1
                    enable_backfill_mode: true
                "#;
            assert_eq!(
                serde_yaml::from_str::<KinesisSourceParams>(yaml).unwrap(),
                KinesisSourceParams {
                    stream_name: "my-stream".to_string(),
                    region_or_endpoint: Some(RegionOrEndpoint::Region("us-west-1".to_string())),
                    enable_backfill_mode: true,
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

    #[tokio::test]
    async fn test_load_ingest_api_source_config() {
        let source_config_filepath = get_source_config_filepath("ingest-api-source.json");
        let file_content = std::fs::read(&source_config_filepath).unwrap();
        let source_config: SourceConfig = ConfigFormat::Json.parse(&file_content).unwrap();
        let expected_source_config = SourceConfig {
            source_id: INGEST_API_SOURCE_ID.to_string(),
            num_pipelines: 1,
            enabled: true,
            source_params: SourceParams::IngestApi,
        };
        assert_eq!(source_config, expected_source_config);
        assert!(source_config.num_pipelines().is_none());
    }
}
