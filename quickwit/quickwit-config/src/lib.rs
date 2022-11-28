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

#![deny(clippy::disallowed_methods)]

use std::str::FromStr;

use anyhow::{bail, Context};
use json_comments::StripComments;
use once_cell::sync::OnceCell;
use quickwit_common::uri::Uri;
use regex::Regex;

mod config_value;
mod index_config;
pub mod merge_policy_config;
mod quickwit_config;
mod qw_env_vars;
pub mod service;
mod source_config;
mod templating;

// We export that one for backward compatibility.
// See #2048
pub use index_config::{
    build_doc_mapper, load_index_config_from_user_config, DocMapping, IndexConfig,
    IndexingResources, IndexingSettings, RetentionPolicy, SearchSettings,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value as JsonValue;
pub use source_config::{
    FileSourceParams, KafkaSourceParams, KinesisSourceParams, RegionOrEndpoint, SourceConfig,
    SourceParams, VecSourceParams, VoidSourceParams, CLI_INGEST_SOURCE_ID, INGEST_API_SOURCE_ID,
};
use tracing::warn;

pub use crate::quickwit_config::{
    IndexerConfig, QuickwitConfig, SearcherConfig, DEFAULT_QW_CONFIG_PATH,
};

fn is_false(val: &bool) -> bool {
    !*val
}

/// Checks whether an identifier conforms to Quickwit object naming conventions.
pub fn validate_identifier(label: &str, value: &str) -> anyhow::Result<()> {
    static IDENTIFIER_REGEX: OnceCell<Regex> = OnceCell::new();

    if IDENTIFIER_REGEX
        .get_or_init(|| Regex::new(r"^[a-zA-Z][a-zA-Z0-9-_]{2,254}$").expect("Failed to compile regular expression. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues."))
        .is_match(value)
    {
        return Ok(());
    }
    bail!(
        "{label} identifier `{value}` is invalid. Identifiers must match the following regular \
         expression: `^[a-zA-Z][a-zA-Z0-9-_]{{2,254}}$`."
    );
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ConfigFormat {
    Json,
    Toml,
    Yaml,
}

impl ConfigFormat {
    pub fn sniff_from_uri(uri: &Uri) -> anyhow::Result<ConfigFormat> {
        let extension_str: &str = uri.extension().with_context(|| {
            anyhow::anyhow!(
                "Failed to read config file `{}`: file extension is missing. Supported file \
                 formats and extensions are JSON (.json), TOML (.toml), and YAML (.yaml or .yml).",
                uri
            )
        })?;
        ConfigFormat::from_str(extension_str)
            .with_context(|| format!("Failed to identify configuration file format {uri}."))
    }

    pub fn parse<T>(&self, payload: &[u8]) -> anyhow::Result<T>
    where T: DeserializeOwned {
        match self {
            ConfigFormat::Json => {
                let mut json_value: JsonValue =
                    serde_json::from_reader(StripComments::new(payload))?;
                let version_value = json_value.get_mut("version").context("Missing version.")?;
                if let Some(version_number) = version_value.as_u64() {
                    warn!("`version` is supposed to be a string.");
                    *version_value = JsonValue::String(version_number.to_string());
                }
                serde_json::from_value(json_value).context("Failed to read JSON file.")
            }
            ConfigFormat::Toml => {
                let mut toml_value: toml::Value =
                    toml::from_slice(payload).context("Failed to read TOML file.")?;
                let version_value = toml_value.get_mut("version").context("Missing version.")?;
                if let Some(version_number) = version_value.as_integer() {
                    warn!("`version` is supposed to be a string.");
                    *version_value = toml::Value::String(version_number.to_string());
                    let reserialized = toml::to_string(version_value)
                        .context("Failed to reserialize toml config.")?;
                    toml::from_str(&reserialized).context("Failed to read TOML file.")
                } else {
                    toml::from_slice(payload).context("Failed to read TOML file.")
                }
            }
            ConfigFormat::Yaml => {
                serde_yaml::from_slice(payload).context("Failed to read YAML file.")
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
                "File extension `.{ext}` is not supported. Supported file formats and extensions \
                 are JSON (.json), TOML (.toml), and YAML (.yaml or .yml).",
            ),
        }
    }
}

pub trait TestableForRegression: Serialize + DeserializeOwned {
    fn sample_for_regression() -> Self;
    fn test_equality(&self, other: &Self);
}

#[cfg(test)]
mod tests {
    use super::validate_identifier;

    #[test]
    fn test_validate_identifier() {
        validate_identifier("Cluster ID", "").unwrap_err();
        validate_identifier("Cluster ID", "-").unwrap_err();
        validate_identifier("Cluster ID", "_").unwrap_err();
        validate_identifier("Cluster ID", "f").unwrap_err();
        validate_identifier("Cluster ID", "fo").unwrap_err();
        validate_identifier("Cluster ID", "_fo").unwrap_err();
        validate_identifier("Cluster ID", "_foo").unwrap_err();
        validate_identifier("Cluster ID", "foo").unwrap();
        validate_identifier("Cluster ID", "f-_").unwrap();

        assert!(validate_identifier("Cluster ID", "foo!")
            .unwrap_err()
            .to_string()
            .contains("Cluster ID identifier `foo!` is invalid."));
    }
}
