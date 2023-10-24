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

pub(crate) mod serialize;

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use bytes::Bytes;
use quickwit_common::is_false;
use quickwit_common::uri::Uri;
use quickwit_proto::metastore::SourceType;
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value as JsonValue;
pub use serialize::load_source_config_from_user_config;
// For backward compatibility.
use serialize::VersionedSourceConfig;

use crate::TestableForRegression;

/// Reserved source ID for the `quickwit index ingest` CLI command.
pub const CLI_INGEST_SOURCE_ID: &str = "_ingest-cli-source";

/// Reserved source ID used for Quickwit ingest API.
pub const INGEST_API_SOURCE_ID: &str = "_ingest-api-source";

/// Reserved source ID used for native Quickwit ingest.
/// (this is for ingest v2)
pub const INGEST_SOURCE_ID: &str = "_ingest-source";

pub const RESERVED_SOURCE_IDS: &[&str] =
    &[CLI_INGEST_SOURCE_ID, INGEST_API_SOURCE_ID, INGEST_SOURCE_ID];

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(into = "VersionedSourceConfig")]
#[serde(try_from = "VersionedSourceConfig")]
pub struct SourceConfig {
    pub source_id: String,

    /// Maximum number of indexing pipelines spawned for the source on a given indexer.
    /// The maximum is reached only if there is enough `desired_num_pipelines` to run.
    /// The value is only used by sources that Quickwit knows how to distribute across
    /// pipelines/nodes, that is for Kafka sources only.
    /// Example:
    /// - `max_num_pipelines_per_indexer=2`
    /// - `desired_num_pipelines=1`
    /// => Only one pipeline will run on one indexer.
    pub max_num_pipelines_per_indexer: NonZeroUsize,
    /// Number of desired indexing pipelines to run on a cluster for the source.
    /// This number could not be reach if there is not enough indexers.
    /// The value is only used by sources that Quickwit knows how to distribute across
    /// pipelines/nodes, that is for Kafka sources only.
    /// Example:
    /// - `max_num_pipelines_per_indexer=1`
    /// - `desired_num_pipelines=2`
    /// - 1 indexer
    /// => Only one pipeline will start on the sole indexer.
    pub desired_num_pipelines: NonZeroUsize,

    // Denotes if this source is enabled.
    pub enabled: bool,

    pub source_params: SourceParams,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "transform")]
    pub transform_config: Option<TransformConfig>,

    // Denotes the input data format.
    #[serde(default)]
    pub input_format: SourceInputFormat,
}

impl SourceConfig {
    pub fn source_type(&self) -> SourceType {
        match self.source_params {
            SourceParams::File(_) => SourceType::File,
            SourceParams::GcpPubSub(_) => SourceType::GcpPubsub,
            SourceParams::Ingest => SourceType::IngestV2,
            SourceParams::IngestApi => SourceType::IngestV1,
            SourceParams::IngestCli => SourceType::Cli,
            SourceParams::Kafka(_) => SourceType::Kafka,
            SourceParams::Kinesis(_) => SourceType::Kinesis,
            SourceParams::Pulsar(_) => SourceType::Pulsar,
            SourceParams::Vec(_) => SourceType::Vec,
            SourceParams::Void(_) => SourceType::Void,
        }
    }

    // TODO: Remove after source factory refactor.
    pub fn params(&self) -> JsonValue {
        match &self.source_params {
            SourceParams::File(params) => serde_json::to_value(params),
            SourceParams::GcpPubSub(params) => serde_json::to_value(params),
            SourceParams::Ingest => serde_json::to_value(()),
            SourceParams::IngestApi => serde_json::to_value(()),
            SourceParams::IngestCli => serde_json::to_value(()),
            SourceParams::Kafka(params) => serde_json::to_value(params),
            SourceParams::Kinesis(params) => serde_json::to_value(params),
            SourceParams::Pulsar(params) => serde_json::to_value(params),
            SourceParams::Vec(params) => serde_json::to_value(params),
            SourceParams::Void(params) => serde_json::to_value(params),
        }
        .expect("`SourceParams` should be JSON serializable")
    }

    /// Creates an ingest source.
    pub fn ingest_default() -> Self {
        Self {
            source_id: INGEST_SOURCE_ID.to_string(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(1).expect("1 should be non-zero"),
            desired_num_pipelines: NonZeroUsize::new(1).expect("1 should be non-zero"),
            enabled: false,
            source_params: SourceParams::Ingest,
            transform_config: None,
            input_format: SourceInputFormat::Json,
        }
    }

    /// Creates the default ingest-api source config.
    pub fn ingest_api_default() -> Self {
        Self {
            source_id: INGEST_API_SOURCE_ID.to_string(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(1).expect("1 should be non-zero"),
            desired_num_pipelines: NonZeroUsize::new(1).expect("1 should be non-zero"),
            enabled: true,
            source_params: SourceParams::IngestApi,
            transform_config: None,
            input_format: SourceInputFormat::Json,
        }
    }

    /// Creates the default cli-ingest source config.
    pub fn cli_ingest_source() -> Self {
        Self {
            source_id: CLI_INGEST_SOURCE_ID.to_string(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(1).expect("1 should be non-zero"),
            desired_num_pipelines: NonZeroUsize::new(1).expect("1 should be non-zero"),
            enabled: true,
            source_params: SourceParams::IngestCli,
            transform_config: None,
            input_format: SourceInputFormat::Json,
        }
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test(source_id: &str, source_params: SourceParams) -> Self {
        Self {
            source_id: source_id.to_string(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(1).expect("1 should be non-zero"),
            desired_num_pipelines: NonZeroUsize::new(1).expect("1 should be non-zero"),
            enabled: true,
            source_params,
            transform_config: None,
            input_format: SourceInputFormat::Json,
        }
    }
}

impl TestableForRegression for SourceConfig {
    fn sample_for_regression() -> Self {
        SourceConfig {
            source_id: "kafka-source".to_string(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(2).unwrap(),
            desired_num_pipelines: NonZeroUsize::new(2).unwrap(),
            enabled: true,
            source_params: SourceParams::Kafka(KafkaSourceParams {
                topic: "kafka-topic".to_string(),
                client_log_level: None,
                client_params: serde_json::json!({}),
                enable_backfill_mode: false,
            }),
            transform_config: Some(TransformConfig {
                vrl_script: ".message = downcase(string!(.message))".to_string(),
                timezone: default_timezone(),
            }),
            input_format: SourceInputFormat::Json,
        }
    }

    fn test_equality(&self, other: &Self) {
        assert_eq!(self, other);
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceInputFormat {
    #[default]
    Json,
    OtlpTraceJson,
    #[serde(alias = "otlp_trace_proto")]
    OtlpTraceProtobuf,
    #[serde(alias = "plain")]
    PlainText,
}

impl FromStr for SourceInputFormat {
    type Err = String;

    fn from_str(format_str: &str) -> Result<Self, String> {
        match format_str {
            "json" => Ok(Self::Json),
            "plain" => Ok(Self::PlainText),
            unknown => Err(format!("unknown source input format: `{unknown}`")),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "source_type", content = "params", rename_all = "snake_case")]
pub enum SourceParams {
    File(FileSourceParams),
    GcpPubSub(GcpPubSubSourceParams),
    Ingest,
    #[serde(rename = "ingest-api")]
    IngestApi,
    #[serde(rename = "ingest-cli")]
    IngestCli,
    Kafka(KafkaSourceParams),
    Kinesis(KinesisSourceParams),
    Pulsar(PulsarSourceParams),
    Vec(VecSourceParams),
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct FileSourceParams {
    /// Path of the file to read. Assume stdin if None.
    #[schema(value_type = String)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    #[serde(deserialize_with = "absolute_filepath_from_str")]
    pub filepath: Option<PathBuf>, //< If None read from stdin.
}

/// Deserializing as an URI first to validate the input.
///
/// TODO: we might want to replace `PathBuf` with `Uri` directly in
/// `FileSourceParams`
fn absolute_filepath_from_str<'de, D>(deserializer: D) -> Result<Option<PathBuf>, D::Error>
where D: Deserializer<'de> {
    let filepath_opt: Option<String> = Deserialize::deserialize(deserializer)?;
    if let Some(filepath) = filepath_opt {
        let uri = Uri::from_str(&filepath).map_err(D::Error::custom)?;
        Ok(Some(PathBuf::from(uri.as_str())))
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct KafkaSourceParams {
    /// Name of the topic that the source consumes.
    pub topic: String,
    /// Kafka client log level. Possible values are `debug`, `info`, `warn`, and `error`.
    #[schema(value_type = String)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_log_level: Option<String>,
    /// Kafka client configuration parameters.
    #[schema(value_type = Object)]
    #[serde(default = "serde_json::Value::default")]
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    pub client_params: JsonValue,
    /// When backfill mode is enabled, the source exits after reaching the end of the topic.
    #[serde(default)]
    #[serde(skip_serializing_if = "is_false")]
    pub enable_backfill_mode: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct GcpPubSubSourceParams {
    /// Name of the subscription that the source consumes.
    pub subscription: String,
    /// When backfill mode is enabled, the source exits after reaching the end of the topic.
    #[serde(default)]
    #[serde(skip_serializing_if = "is_false")]
    pub enable_backfill_mode: bool,
    /// GCP service account credentials (`None` will use default via
    /// GOOGLE_APPLICATION_CREDENTIALS)
    /// Path to a google_cloud_auth::credentials::CredentialsFile serialized in JSON. See also
    /// `<https://cloud.google.com/docs/authentication/application-default-credentials>` and
    /// `<https://github.com/yoshidan/google-cloud-rust/tree/main/pubsub#automatically>` and
    /// `<https://docs.rs/google-cloud-auth/0.12.0/google_cloud_auth/credentials/struct.CredentialsFile.html>`.
    pub credentials_file: Option<String>,
    /// GCP project ID (Defaults to credentials file project ID).
    pub project_id: Option<String>,
    /// Maximum number of messages returned by a pull request (default 1,000)
    pub max_messages_per_pull: Option<i32>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum RegionOrEndpoint {
    Region(String),
    Endpoint(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
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
            return Err("Kinesis source parameters `region` and `endpoint` are mutually exclusive");
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

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct VecSourceParams {
    #[schema(value_type = Vec<String>)]
    pub docs: Vec<Bytes>,
    pub batch_num_docs: usize,
    #[serde(default)]
    pub partition: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct VoidSourceParams;

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PulsarSourceParams {
    /// List of the topics that the source consumes.
    pub topics: Vec<String>,
    #[serde(deserialize_with = "pulsar_uri")]
    /// The connection URI for pulsar.
    pub address: String,
    #[schema(default = "quickwit")]
    #[serde(default = "default_consumer_name")]
    /// The name to register with the pulsar source.
    pub consumer_name: String,
    // Serde yaml has some specific behaviour when deserializing
    // enums (see https://github.com/dtolnay/serde-yaml/issues/342)
    // and requires explicitly stating `default` in order to make the parameter
    // optional on the yaml config.
    #[serde(default, with = "serde_yaml::with::singleton_map")]
    /// Authentication for pulsar.
    pub authentication: Option<PulsarSourceAuth>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum PulsarSourceAuth {
    Token(String),
    Oauth2 {
        issuer_url: String,
        credentials_url: String,
        audience: Option<String>,
        scope: Option<String>,
    },
}

// Deserializing a string into an pulsar uri.
fn pulsar_uri<'de, D>(deserializer: D) -> Result<String, D::Error>
where D: Deserializer<'de> {
    let uri: String = Deserialize::deserialize(deserializer)?;

    if uri.strip_prefix("pulsar://").is_none() {
        return Err(Error::custom(format!(
            "invalid Pulsar uri provided, must be in the format of `pulsar://host:port/path`. \
             got: `{uri}`"
        )));
    }

    Ok(uri)
}

fn default_consumer_name() -> String {
    "quickwit".to_string()
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct TransformConfig {
    /// [VRL] source code of the transform compiled to a VRL [`Program`](vrl::compiler::Program).
    ///
    /// [VRL]: https://vector.dev/docs/reference/vrl/
    #[serde(rename = "script")]
    vrl_script: String,

    /// Timezone used in the VRL [`Program`](vrl::compiler::Program) for date and time
    /// manipulations. Defaults to `UTC` if not timezone is specified.
    #[serde(default = "default_timezone")]
    timezone: String,
}

fn default_timezone() -> String {
    "UTC".to_string()
}

impl TransformConfig {
    /// Creates a new [`TransformConfig`] instance from the provided VRL script and optional
    /// timezone.
    pub fn new(vrl_script: String, timezone_opt: Option<String>) -> Self {
        Self {
            vrl_script,
            timezone: timezone_opt.unwrap_or_else(default_timezone),
        }
    }

    #[cfg(feature = "vrl")]
    pub(crate) fn validate_vrl_script(&self) -> anyhow::Result<()> {
        self.compile_vrl_script()?;
        Ok(())
    }

    #[cfg(not(feature = "vrl"))]
    pub(crate) fn validate_vrl_script(&self) -> anyhow::Result<()> {
        // If we are missing the VRL feature we do not return an error here,
        // to avoid breaking unit tests.
        //
        // We do return an explicit error on instanciation of the program however.
        Ok(())
    }

    #[cfg(feature = "vrl")]
    /// Compiles the VRL script to a VRL [`Program`](vrl::compiler::Program) and returns it along
    /// with the timezone.
    pub fn compile_vrl_script(
        &self,
    ) -> anyhow::Result<(vrl::compiler::Program, vrl::compiler::TimeZone)> {
        use anyhow::Context;
        let timezone = vrl::compiler::TimeZone::parse(&self.timezone).with_context(|| {
            format!(
                "failed to parse timezone: `{}`. timezone must be a valid name \
            in the TZ database: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones",
                self.timezone,
            )
        })?;
        // Append "\n." to the script to return the entire document and not only the modified
        // fields.
        let vrl_script = self.vrl_script.clone() + "\n.";
        let functions = vrl_stdlib::all();

        let compilation_res = match vrl::compiler::compile(&vrl_script, &functions) {
            Ok(compilation_res) => compilation_res,
            Err(diagnostics) => {
                let mut formatter = vrl::diagnostic::Formatter::new(&vrl_script, diagnostics);
                formatter.enable_colors(!quickwit_common::no_color());
                anyhow::bail!("failed to compile VRL script:\n {formatter}")
            }
        };

        let vrl::compiler::CompilationResult {
            program, warnings, ..
        } = compilation_res;

        if !warnings.is_empty() {
            let mut formatter = vrl::diagnostic::Formatter::new(&vrl_script, warnings);
            formatter.enable_colors(!quickwit_common::no_color());
            tracing::warn!("VRL program compiled with some warnings: {formatter}");
        }
        Ok((program, timezone))
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test(vrl_script: &str) -> Self {
        Self {
            vrl_script: vrl_script.to_string(),
            timezone: default_timezone(),
        }
    }
}

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
        let config_format = ConfigFormat::sniff_from_uri(&source_config_uri).unwrap();
        let source_config =
            load_source_config_from_user_config(config_format, file_content.as_bytes()).unwrap();
        let expected_source_config = SourceConfig {
            source_id: "hdfs-logs-kafka-source".to_string(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(2).unwrap(),
            desired_num_pipelines: NonZeroUsize::new(2).unwrap(),
            enabled: true,
            source_params: SourceParams::Kafka(KafkaSourceParams {
                topic: "cloudera-cluster-logs".to_string(),
                client_log_level: None,
                client_params: json! {{"bootstrap.servers": "localhost:9092"}},
                enable_backfill_mode: false,
            }),
            transform_config: Some(TransformConfig {
                vrl_script: ".message = downcase(string!(.message))".to_string(),
                timezone: "local".to_string(),
            }),
            input_format: SourceInputFormat::Json,
        };
        assert_eq!(source_config, expected_source_config);
        assert_eq!(source_config.desired_num_pipelines.get(), 2);
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
        let config_format = ConfigFormat::sniff_from_uri(&source_config_uri).unwrap();
        let source_config =
            load_source_config_from_user_config(config_format, file_content.as_bytes()).unwrap();
        let expected_source_config = SourceConfig {
            source_id: "hdfs-logs-kinesis-source".to_string(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(1).expect("1 should be non-zero"),
            desired_num_pipelines: NonZeroUsize::new(1).expect("1 should be non-zero"),
            enabled: true,
            source_params: SourceParams::Kinesis(KinesisSourceParams {
                stream_name: "emr-cluster-logs".to_string(),
                region_or_endpoint: None,
                enable_backfill_mode: false,
            }),
            transform_config: Some(TransformConfig {
                vrl_script: ".message = downcase(string!(.message))".to_string(),
                timezone: "local".to_string(),
            }),
            input_format: SourceInputFormat::Json,
        };
        assert_eq!(source_config, expected_source_config);
        assert_eq!(source_config.desired_num_pipelines.get(), 1);
    }

    #[tokio::test]
    async fn test_load_invalid_source_config() {
        {
            let content = r#"
            {
                "version": "0.6",
                "source_id": "hdfs-logs-void-source",
                "desired_num_pipelines": 0,
                "max_num_pipelines_per_indexer": 1,
                "source_type": "void",
                "params": {}
            }
            "#;
            let error = load_source_config_from_user_config(ConfigFormat::Json, content.as_bytes())
                .unwrap_err();
            assert!(error
                .to_string()
                .contains("`desired_num_pipelines` must be"));
        }
        {
            let content = r#"
            {
                "version": "0.6",
                "source_id": "hdfs-logs-void-source",
                "desired_num_pipelines": 1,
                "max_num_pipelines_per_indexer": 0,
                "source_type": "void",
                "params": {}
            }
            "#;
            let error = load_source_config_from_user_config(ConfigFormat::Json, content.as_bytes())
                .unwrap_err();
            assert!(error
                .to_string()
                .contains("`max_num_pipelines_per_indexer` must be"));
        }
        {
            let content = r#"
            {
                "version": "0.6",
                "source_id": "hdfs-logs-void-source",
                "desired_num_pipelines": 1,
                "max_num_pipelines_per_indexer": 2,
                "source_type": "void",
                "params": {}
            }
            "#;
            let error = load_source_config_from_user_config(ConfigFormat::Json, content.as_bytes())
                .unwrap_err();
            assert!(error.to_string().contains("supports multiple pipelines"));
        }
        {
            let content = r#"
            {
                "version": "0.6",
                "source_id": "hdfs-logs-void-source",
                "desired_num_pipelines": 2,
                "max_num_pipelines_per_indexer": 1,
                "source_type": "void",
                "params": {}
            }
            "#;
            let error = load_source_config_from_user_config(ConfigFormat::Json, content.as_bytes())
                .unwrap_err();
            assert!(error.to_string().contains("supports multiple pipelines"));
        }
    }

    #[tokio::test]
    async fn test_load_valid_distributed_source_config() {
        {
            let content = r#"
            {
                "version": "0.6",
                "source_id": "hdfs-logs-kafka-source",
                "desired_num_pipelines": 3,
                "max_num_pipelines_per_indexer": 3,
                "source_type": "kafka",
                "params": {
                    "topic": "my-topic"
                }
            }
            "#;
            let source_config =
                load_source_config_from_user_config(ConfigFormat::Json, content.as_bytes())
                    .unwrap();
            assert_eq!(source_config.desired_num_pipelines.get(), 3);
            assert_eq!(source_config.max_num_pipelines_per_indexer.get(), 3);
        }
        {
            let content = r#"
            {
                "version": "0.6",
                "source_id": "hdfs-logs-pulsar-source",
                "desired_num_pipelines": 3,
                "max_num_pipelines_per_indexer": 3,
                "source_type": "pulsar",
                "params": {
                    "topics": ["my-topic"],
                    "address": "http://localhost:6650"
                }
            }
            "#;
            load_source_config_from_user_config(ConfigFormat::Json, content.as_bytes())
                .unwrap_err();
            // TODO: uncomment asserts once distributed indexing is activated for pulsar.
            // assert_eq!(source_config.desired_num_pipelines(), 3);
            // assert_eq!(source_config.max_num_pipelines_per_indexer(), 3);
        }
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
                Path::new(uri.as_str())
            );
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

    #[test]
    fn test_pulsar_source_params_deserialization() {
        {
            let yaml = r#"
                    topics:
                        - my-topic
                    address: pulsar://localhost:6560
                    consumer_name: my-pulsar-consumer
                "#;
            assert_eq!(
                serde_yaml::from_str::<PulsarSourceParams>(yaml).unwrap(),
                PulsarSourceParams {
                    topics: vec!["my-topic".to_string()],
                    address: "pulsar://localhost:6560".to_string(),
                    consumer_name: "my-pulsar-consumer".to_string(),
                    authentication: None,
                }
            );
        }

        {
            let yaml = r#"
                    topics:
                        - my-topic
                    address: pulsar://localhost:6560
                    consumer_name: my-pulsar-consumer
                    authentication:
                        token: my-token
                "#;
            assert_eq!(
                serde_yaml::from_str::<PulsarSourceParams>(yaml).unwrap(),
                PulsarSourceParams {
                    topics: vec!["my-topic".to_string()],
                    address: "pulsar://localhost:6560".to_string(),
                    consumer_name: "my-pulsar-consumer".to_string(),
                    authentication: Some(PulsarSourceAuth::Token("my-token".to_string())),
                }
            );
        }

        {
            let yaml = r#"
                    topics:
                        - my-topic
                    address: pulsar://localhost:6560
                    consumer_name: my-pulsar-consumer
                    authentication:
                        oauth2:
                            issuer_url: https://my-issuer:9000/path
                            credentials_url: https://my-credentials.com/path
                "#;
            assert_eq!(
                serde_yaml::from_str::<PulsarSourceParams>(yaml).unwrap(),
                PulsarSourceParams {
                    topics: vec!["my-topic".to_string()],
                    address: "pulsar://localhost:6560".to_string(),
                    consumer_name: "my-pulsar-consumer".to_string(),
                    authentication: Some(PulsarSourceAuth::Oauth2 {
                        issuer_url: "https://my-issuer:9000/path".to_string(),
                        credentials_url: "https://my-credentials.com/path".to_string(),
                        audience: None,
                        scope: None,
                    }),
                }
            );
        }

        {
            let yaml = r#"
                    topics:
                        - my-topic
                    address: pulsar://localhost:6560
                    consumer_name: my-pulsar-consumer
                    authentication:
                        oauth2:
                            issuer_url: https://my-issuer:9000/path
                            credentials_url: https://my-credentials.com/path
                            audience: my-audience
                            scope: "read+write"
                "#;
            assert_eq!(
                serde_yaml::from_str::<PulsarSourceParams>(yaml).unwrap(),
                PulsarSourceParams {
                    topics: vec!["my-topic".to_string()],
                    address: "pulsar://localhost:6560".to_string(),
                    consumer_name: "my-pulsar-consumer".to_string(),
                    authentication: Some(PulsarSourceAuth::Oauth2 {
                        issuer_url: "https://my-issuer:9000/path".to_string(),
                        credentials_url: "https://my-credentials.com/path".to_string(),
                        audience: Some("my-audience".to_string()),
                        scope: Some("read+write".to_string()),
                    }),
                }
            );
        }

        {
            let yaml = r#"
                    topics:
                        - my-topic
                "#;
            serde_yaml::from_str::<PulsarSourceParams>(yaml)
                .expect_err("Parameters should error on missing address");
        }

        {
            let yaml = r#"
                    topics:
                        - my-topic
                    address: pulsar://localhost:6560
                "#;
            assert_eq!(
                serde_yaml::from_str::<PulsarSourceParams>(yaml).unwrap(),
                PulsarSourceParams {
                    topics: vec!["my-topic".to_string()],
                    address: "pulsar://localhost:6560".to_string(),
                    consumer_name: default_consumer_name(),
                    authentication: None,
                }
            );
        }

        {
            let yaml = r#"
                    topics:
                        - my-topic
                    address: invalid-address
                "#;
            serde_yaml::from_str::<PulsarSourceParams>(yaml)
                .expect_err("Pulsar config should reject invalid address");
        }

        {
            let yaml = r#"
                    topics:
                        - my-topic
                    address: pulsar://some-host:80/valid-path
                "#;
            assert_eq!(
                serde_yaml::from_str::<PulsarSourceParams>(yaml).unwrap(),
                PulsarSourceParams {
                    topics: vec!["my-topic".to_string()],
                    address: "pulsar://some-host:80/valid-path".to_string(),
                    consumer_name: default_consumer_name(),
                    authentication: None,
                }
            );
        }

        {
            let yaml = r#"
                    topics:
                        - my-topic
                    address: pulsar://2345:0425:2CA1:0000:0000:0567:5673:23b5:80/valid-path
                "#;
            assert_eq!(
                serde_yaml::from_str::<PulsarSourceParams>(yaml).unwrap(),
                PulsarSourceParams {
                    topics: vec!["my-topic".to_string()],
                    address: "pulsar://2345:0425:2CA1:0000:0000:0567:5673:23b5:80/valid-path"
                        .to_string(),
                    consumer_name: default_consumer_name(),
                    authentication: None,
                }
            );
        }
    }

    #[cfg(feature = "vrl")]
    #[tokio::test]
    async fn test_load_ingest_api_source_config() {
        let source_config_filepath = get_source_config_filepath("ingest-api-source.json");
        let file_content = std::fs::read(source_config_filepath).unwrap();
        let source_config: SourceConfig = ConfigFormat::Json.parse(&file_content).unwrap();
        let expected_source_config = SourceConfig {
            source_id: INGEST_API_SOURCE_ID.to_string(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(1).expect("1 should be non-zero"),
            desired_num_pipelines: NonZeroUsize::new(1).expect("1 should be non-zero"),
            enabled: true,
            source_params: SourceParams::IngestApi,
            transform_config: Some(TransformConfig {
                vrl_script: ".message = downcase(string!(.message))".to_string(),
                timezone: default_timezone(),
            }),
            input_format: SourceInputFormat::Json,
        };
        assert_eq!(source_config, expected_source_config);
        assert_eq!(source_config.desired_num_pipelines.get(), 1);
    }

    #[test]
    fn test_transform_config_serialization() {
        {
            let transform_config = TransformConfig {
                vrl_script: ".message = downcase(string!(.message))".to_string(),
                timezone: "local".to_string(),
            };
            let transform_config_yaml = serde_yaml::to_string(&transform_config).unwrap();
            assert_eq!(
                serde_yaml::from_str::<TransformConfig>(&transform_config_yaml).unwrap(),
                transform_config,
            );
        }
        {
            let transform_config = TransformConfig {
                vrl_script: ".message = downcase(string!(.message))".to_string(),
                timezone: default_timezone(),
            };
            let transform_config_yaml = serde_yaml::to_string(&transform_config).unwrap();
            assert_eq!(
                serde_yaml::from_str::<TransformConfig>(&transform_config_yaml).unwrap(),
                transform_config,
            );
        }
    }

    #[test]
    fn test_transform_config_deserialization() {
        {
            let transform_config_yaml = r#"
                script: .message = downcase(string!(.message))
            "#;
            let transform_config =
                serde_yaml::from_str::<TransformConfig>(transform_config_yaml).unwrap();

            let expected_transform_config = TransformConfig {
                vrl_script: ".message = downcase(string!(.message))".to_string(),
                timezone: default_timezone(),
            };
            assert_eq!(transform_config, expected_transform_config);
        }
        {
            let transform_config_yaml = r#"
                script: .message = downcase(string!(.message))
                timezone: Turkey
            "#;
            let transform_config =
                serde_yaml::from_str::<TransformConfig>(transform_config_yaml).unwrap();

            let expected_transform_config = TransformConfig {
                vrl_script: ".message = downcase(string!(.message))".to_string(),
                timezone: "Turkey".to_string(),
            };
            assert_eq!(transform_config, expected_transform_config);
        }
    }

    #[cfg(feature = "vrl")]
    #[test]
    fn test_transform_config_compile_vrl_script() {
        {
            let transform_config = TransformConfig {
                vrl_script: ".message = downcase(string!(.message))".to_string(),
                timezone: "Turkey".to_string(),
            };
            transform_config.compile_vrl_script().unwrap();
        }
        {
            let transform_config = TransformConfig {
                vrl_script: r#"
                . = parse_json!(string!(.message))
                .timestamp = to_unix_timestamp(to_timestamp!(.timestamp))
                del(.username)
                .message = downcase(string!(.message))
                "#
                .to_string(),
                timezone: default_timezone(),
            };
            transform_config.compile_vrl_script().unwrap();
        }
        {
            let transform_config = TransformConfig {
                vrl_script: ".message = downcase(string!(.message))".to_string(),
                timezone: "foo".to_string(),
            };
            let error = transform_config.compile_vrl_script().unwrap_err();
            assert!(error.to_string().starts_with("failed to parse timezone"));
        }
        {
            let transform_config = TransformConfig {
                vrl_script: "foo".to_string(),
                timezone: "Turkey".to_string(),
            };
            let error = transform_config.compile_vrl_script().unwrap_err();
            assert!(error.to_string().starts_with("failed to compile"));
        }
    }

    #[tokio::test]
    async fn test_source_config_plain_text_input_format() {
        let file_content = r#"{
            "version": "0.6",
            "source_id": "logs-file-source",
            "desired_num_pipelines": 1,
            "max_num_pipelines_per_indexer": 1,
            "source_type": "file",
            "params": {"filepath": "/test_non_json_corpus.txt"},
            "input_format": "plain_text"
        }"#;
        let source_config =
            load_source_config_from_user_config(ConfigFormat::Json, file_content.as_bytes())
                .unwrap();
        assert_eq!(source_config.input_format, SourceInputFormat::PlainText);
    }
}
