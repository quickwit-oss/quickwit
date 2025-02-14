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

#![deny(clippy::disallowed_methods)]

use std::hash::Hasher;
use std::str::FromStr;

use anyhow::{bail, ensure, Context};
use json_comments::StripComments;
use once_cell::sync::Lazy;
use quickwit_common::get_bool_from_env;
use quickwit_common::net::is_valid_hostname;
use quickwit_common::uri::Uri;
use quickwit_proto::types::NodeIdRef;
use regex::Regex;

mod cluster_config;
mod config_value;
mod index_config;
mod index_template;
pub mod merge_policy_config;
mod metastore_config;
mod node_config;
mod qw_env_vars;
pub mod service;
mod source_config;
mod storage_config;
mod templating;

pub use cluster_config::ClusterConfig;
// We export that one for backward compatibility.
// See #2048
use index_config::serialize::{IndexConfigV0_8, VersionedIndexConfig};
pub use index_config::{
    build_doc_mapper, load_index_config_from_user_config, load_index_config_update, IndexConfig,
    IndexingResources, IndexingSettings, RetentionPolicy, SearchSettings,
};
pub use quickwit_doc_mapper::DocMapping;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value as JsonValue;
use siphasher::sip::SipHasher;
use source_config::FileSourceParamsForSerde;
pub use source_config::{
    load_source_config_from_user_config, load_source_config_update, FileSourceMessageType,
    FileSourceNotification, FileSourceParams, FileSourceSqs, KafkaSourceParams,
    KinesisSourceParams, PubSubSourceParams, PulsarSourceAuth, PulsarSourceParams,
    RegionOrEndpoint, SourceConfig, SourceInputFormat, SourceParams, TransformConfig,
    VecSourceParams, VoidSourceParams, CLI_SOURCE_ID, INGEST_API_SOURCE_ID, INGEST_V2_SOURCE_ID,
};
use tracing::warn;

use crate::index_template::IndexTemplateV0_8;
pub use crate::index_template::{IndexTemplate, IndexTemplateId, VersionedIndexTemplate};
use crate::merge_policy_config::{
    ConstWriteAmplificationMergePolicyConfig, MergePolicyConfig, StableLogMergePolicyConfig,
};
pub use crate::metastore_config::{
    MetastoreBackend, MetastoreConfig, MetastoreConfigs, PostgresMetastoreConfig,
};
pub use crate::node_config::{
    IndexerConfig, IngestApiConfig, JaegerConfig, NodeConfig, SearcherConfig, SplitCacheLimits,
    StorageTimeoutPolicy, DEFAULT_QW_CONFIG_PATH,
};
use crate::source_config::serialize::{SourceConfigV0_7, SourceConfigV0_9, VersionedSourceConfig};
pub use crate::storage_config::{
    AzureStorageConfig, FileStorageConfig, GoogleCloudStorageConfig, RamStorageConfig,
    S3StorageConfig, StorageBackend, StorageBackendFlavor, StorageConfig, StorageConfigs,
};

/// Returns true if the ingest API v2 is enabled.
pub fn enable_ingest_v2() -> bool {
    static ENABLE_INGEST_V2: Lazy<bool> =
        Lazy::new(|| get_bool_from_env("QW_ENABLE_INGEST_V2", true));
    *ENABLE_INGEST_V2
}

/// Returns true if the ingest API v1 is disabled.
pub fn disable_ingest_v1() -> bool {
    static DISABLE_INGEST_V1: Lazy<bool> =
        Lazy::new(|| get_bool_from_env("QW_DISABLE_INGEST_V1", false));
    *DISABLE_INGEST_V1
}

#[derive(utoipa::OpenApi)]
#[openapi(components(schemas(
    IndexingResources,
    IndexingSettings,
    SearchSettings,
    RetentionPolicy,
    MergePolicyConfig,
    DocMapping,
    VersionedSourceConfig,
    SourceConfigV0_7,
    SourceConfigV0_9,
    VersionedIndexConfig,
    IndexConfigV0_8,
    VersionedIndexTemplate,
    IndexTemplateV0_8,
    SourceInputFormat,
    SourceParams,
    FileSourceMessageType,
    FileSourceNotification,
    FileSourceParamsForSerde,
    FileSourceSqs,
    PubSubSourceParams,
    KafkaSourceParams,
    KinesisSourceParams,
    PulsarSourceParams,
    PulsarSourceAuth,
    RegionOrEndpoint,
    ConstWriteAmplificationMergePolicyConfig,
    StableLogMergePolicyConfig,
    TransformConfig,
    VecSourceParams,
    VoidSourceParams,
)))]
/// Schema used for the OpenAPI generation which are apart of this crate.
pub struct ConfigApiSchemas;

/// Checks whether an identifier conforms to Quickwit naming conventions.
pub fn validate_identifier(label: &str, value: &str) -> anyhow::Result<()> {
    static IDENTIFIER_REGEX: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"^[a-zA-Z][a-zA-Z0-9-_\.]{2,254}$").expect("regular expression should compile")
    });
    ensure!(
        IDENTIFIER_REGEX.is_match(value),
        "{label} ID `{value}` is invalid: identifiers must match the following regular \
         expression: `^[a-zA-Z][a-zA-Z0-9-_\\.]{{2,254}}$`"
    );
    Ok(())
}

/// Checks whether an index ID pattern conforms to Quickwit conventions.
/// Index ID patterns accept the same characters as identifiers AND accept `*`
/// chars to allow for glob-like patterns.
pub fn validate_index_id_pattern(pattern: &str, allow_negative: bool) -> anyhow::Result<()> {
    static IDENTIFIER_REGEX_WITH_GLOB_PATTERN: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"^[a-zA-Z\*][a-zA-Z0-9-_\.\*]{0,254}$")
            .expect("regular expression should compile")
    });
    static IDENTIFIER_REGEX_WITH_GLOB_PATTERN_NEGATIVE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"^-?[a-zA-Z\*][a-zA-Z0-9-_\.\*]{0,254}$")
            .expect("regular expression should compile")
    });

    let regex = if allow_negative {
        &IDENTIFIER_REGEX_WITH_GLOB_PATTERN_NEGATIVE
    } else {
        &IDENTIFIER_REGEX_WITH_GLOB_PATTERN
    };

    if !regex.is_match(pattern) {
        bail!(
            "index ID pattern `{pattern}` is invalid: patterns must match the following regular \
             expression: `^[a-zA-Z\\*][a-zA-Z0-9-_\\.\\*]{{0,254}}$`"
        );
    }
    // Forbid multiple stars in the pattern to force the user making simpler patterns
    // as multiple stars does not bring any value.
    if pattern.contains("**") {
        bail!(
            "index ID pattern `{pattern}` is invalid: patterns must not contain multiple \
             consecutive `*`"
        );
    }
    // If there is no star in the pattern, we need at least 3 characters.
    if !pattern.contains('*') && pattern.len() < 3 {
        bail!(
            "index ID pattern `{pattern}` is invalid: an index ID must have at least 3 characters"
        );
    }
    Ok(())
}

pub fn validate_node_id(node_id: &NodeIdRef) -> anyhow::Result<()> {
    if !is_valid_hostname(node_id.as_str()) {
        bail!(
            "node identifier `{node_id}` is invalid. node identifiers must be valid short \
             hostnames (see RFC 1123)"
        );
    }
    Ok(())
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ConfigFormat {
    Json,
    Toml,
    Yaml,
}

impl ConfigFormat {
    pub fn as_str(&self) -> &'static str {
        match self {
            ConfigFormat::Json => "json",
            ConfigFormat::Toml => "toml",
            ConfigFormat::Yaml => "yaml",
        }
    }

    pub fn sniff_from_uri(uri: &Uri) -> anyhow::Result<ConfigFormat> {
        let extension_str: &str = uri.extension().with_context(|| {
            format!(
                "failed to parse config file `{uri}`: file extension is missing. supported file \
                 formats and extensions are JSON (.json), TOML (.toml), and YAML (.yaml or .yml)"
            )
        })?;
        ConfigFormat::from_str(extension_str)
            .with_context(|| format!("failed to identify configuration file format {uri}"))
    }

    pub fn parse<T>(&self, payload: &[u8]) -> anyhow::Result<T>
    where T: DeserializeOwned {
        match self {
            ConfigFormat::Json => {
                let mut json_value: JsonValue =
                    serde_json::from_reader(StripComments::new(payload))?;
                let version_value = json_value.get_mut("version").context("missing version")?;
                if let Some(version_number) = version_value.as_u64() {
                    warn!(version_value=?version_value, "`version` should be a string");
                    *version_value = JsonValue::String(version_number.to_string());
                }
                serde_json::from_value(json_value).context("failed to parse JSON file")
            }
            ConfigFormat::Toml => {
                let payload_str = std::str::from_utf8(payload)
                    .context("configuration file contains invalid UTF-8 characters")?;
                let mut toml_value: toml::Value =
                    toml::from_str(payload_str).context("failed to parse TOML file")?;
                let version_value = toml_value.get_mut("version").context("missing version")?;
                if let Some(version_number) = version_value.as_integer() {
                    warn!(version_value=?version_value, "`version` should be a string");
                    *version_value = toml::Value::String(version_number.to_string());
                    let reserialized = toml::to_string(version_value)
                        .context("failed to reserialize toml config")?;
                    toml::from_str(&reserialized).context("failed to parse TOML file")
                } else {
                    toml::from_str(payload_str).context("failed to parse TOML file")
                }
            }
            ConfigFormat::Yaml => {
                serde_yaml::from_slice(payload).context("failed to parse YAML file")
            }
        }
    }
}

impl FromStr for ConfigFormat {
    type Err = anyhow::Error;

    fn from_str(ext: &str) -> anyhow::Result<Self> {
        match ext {
            "json" => Ok(Self::Json),
            "toml" => Ok(Self::Toml),
            "yaml" | "yml" => Ok(Self::Yaml),
            _ => bail!(
                "file extension `.{ext}` is not supported. supported file formats and extensions \
                 are JSON (.json), TOML (.toml), and YAML (.yaml or .yml)",
            ),
        }
    }
}

pub trait TestableForRegression: Serialize + DeserializeOwned {
    /// Produces an instance of `Self` whose serialization output will be tested against future
    /// versions of the format for backward compatibility.
    fn sample_for_regression() -> Self;

    /// Asserts that `self` and `other` are equal. It must panic if they are not.
    fn assert_equality(&self, other: &Self);
}

/// Returns a fingerprint (a hash) of all the parameters that should force an
/// indexing pipeline to restart upon index or source config updates.
pub fn indexing_pipeline_params_fingerprint(
    index_config: &IndexConfig,
    source_config: &SourceConfig,
) -> u64 {
    let mut hasher = SipHasher::new();
    hasher.write_u64(index_config.indexing_params_fingerprint());
    hasher.write_u64(source_config.indexing_params_fingerprint());
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::validate_identifier;
    use crate::validate_index_id_pattern;

    #[test]
    fn test_validate_identifier() {
        validate_identifier("cluster", "").unwrap_err();
        validate_identifier("cluster", "-").unwrap_err();
        validate_identifier("cluster", "_").unwrap_err();
        validate_identifier("cluster", "f").unwrap_err();
        validate_identifier("cluster", "fo").unwrap_err();
        validate_identifier("cluster", "_fo").unwrap_err();
        validate_identifier("cluster", "_foo").unwrap_err();
        validate_identifier("cluster", ".foo.bar").unwrap_err();
        validate_identifier("cluster", "foo").unwrap();
        validate_identifier("cluster", "f-_").unwrap();
        validate_identifier("index", "foo.bar").unwrap();

        assert!(validate_identifier("cluster", "foo!")
            .unwrap_err()
            .to_string()
            .contains("cluster ID `foo!` is invalid"));
    }

    #[test]
    fn test_validate_index_id_pattern() {
        validate_index_id_pattern("*", false).unwrap();
        validate_index_id_pattern("abc.*", false).unwrap();
        validate_index_id_pattern("ab", false).unwrap_err();
        validate_index_id_pattern("", false).unwrap_err();
        validate_index_id_pattern("**", false).unwrap_err();
        assert!(validate_index_id_pattern("foo!", false)
            .unwrap_err()
            .to_string()
            .contains("index ID pattern `foo!` is invalid:"));
        validate_index_id_pattern("-abc", true).unwrap();
        validate_index_id_pattern("-abc", false).unwrap_err();
    }
}
