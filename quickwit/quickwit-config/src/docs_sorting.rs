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

use std::collections::{HashMap, HashSet};

use anyhow::ensure;
use serde::{Deserialize, Serialize};

use crate::config_value::ConfigValue;
use crate::qw_env_vars::QW_ENABLE_DOCS_SORTING;

/// Configuration for document sorting.
///
/// Example YAML:
///
/// ```yaml
/// fingerprint:
///   fields:
///     - path: message
///       kind: tokenized
///     - path: service
///       kind: raw
///     - path: tag
///       kind: ignored
///     - path: custom
///       kind: ignored
/// ```
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DocsSortingConfig {
    pub fingerprint: FingerprintConfig,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DocsSortingConfigBuilder {
    fingerprint: FingerprintConfig,
}

impl DocsSortingConfigBuilder {
    pub(crate) fn build_optional(
        config_builder_opt: Option<Self>,
        env_vars: &HashMap<String, String>,
    ) -> anyhow::Result<Option<DocsSortingConfig>> {
        let enable_override =
            ConfigValue::<bool, QW_ENABLE_DOCS_SORTING>::none().resolve_optional(env_vars)?;

        let Some(config_builder) = config_builder_opt else {
            return Ok(None);
        };

        config_builder.fingerprint.validate()?;
        let config = DocsSortingConfig {
            fingerprint: config_builder.fingerprint,
        };

        match enable_override {
            Some(false) => Ok(None),
            Some(true) | None => Ok(Some(config)),
        }
    }
}

/// Configuration for computing document fingerprints.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FingerprintConfig {
    pub fields: Vec<FingerprintField>,
    #[serde(default = "default_max_grouping_tokens")]
    pub max_grouping_tokens: usize,
}

impl FingerprintConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.max_grouping_tokens > 0,
            "max grouping tokens must be greater than zero"
        );
        let mut paths = HashSet::with_capacity(self.fields.len());
        for field in &self.fields {
            field.validate()?;
            ensure!(
                paths.insert(field.path.as_str()),
                "duplicate document sorting path `{}`",
                field.path
            );
        }
        Ok(())
    }
}

impl Default for FingerprintConfig {
    fn default() -> Self {
        Self {
            fields: Vec::new(),
            max_grouping_tokens: default_max_grouping_tokens(),
        }
    }
}

fn default_max_grouping_tokens() -> usize {
    50
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FingerprintField {
    pub path: String,
    pub kind: FingerprintFieldKind,
}

impl FingerprintField {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.path.trim() == self.path,
            "document sorting path `{}` must not contain leading or trailing whitespace",
            self.path
        );
        ensure!(
            !self.path.is_empty(),
            "document sorting path must not be empty"
        );
        ensure!(
            !self.path.split('.').any(str::is_empty),
            "document sorting path `{}` must not contain empty components",
            self.path
        );
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FingerprintFieldKind {
    Tokenized,
    Raw,
    Ignored,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{
        DocsSortingConfigBuilder, FingerprintConfig, FingerprintField, FingerprintFieldKind,
    };

    #[test]
    fn validation_rejects_invalid_fields() {
        let config = FingerprintConfig {
            fields: vec![FingerprintField {
                path: "message..template".to_string(),
                kind: FingerprintFieldKind::Tokenized,
            }],
            ..Default::default()
        };
        let error = config.validate().err().unwrap();
        assert!(
            error
                .to_string()
                .contains("must not contain empty components"),
            "expected invalid path failure, got: {error:?}"
        );
    }

    #[test]
    fn default_uses_max_grouping_tokens() {
        let config = FingerprintConfig::default();
        assert_eq!(config.max_grouping_tokens, 50);
    }

    #[test]
    fn build_rejects_invalid_fields() {
        let config_builder = serde_yaml::from_str::<DocsSortingConfigBuilder>(
            r#"
fingerprint:
  fields:
    - path: message
      kind: tokenized
    - path: message
      kind: raw
"#,
        )
        .unwrap();
        let error = DocsSortingConfigBuilder::build_optional(Some(config_builder), &HashMap::new())
            .err()
            .unwrap();
        assert!(
            error
                .to_string()
                .contains("duplicate document sorting path `message`"),
            "expected duplicate path failure, got: {error:?}"
        );
    }

    #[test]
    fn build_rejects_zero_max_grouping_tokens() {
        let config_builder = serde_yaml::from_str::<DocsSortingConfigBuilder>(
            r#"
fingerprint:
  fields: []
  max_grouping_tokens: 0
"#,
        )
        .unwrap();
        let error = DocsSortingConfigBuilder::build_optional(Some(config_builder), &HashMap::new())
            .err()
            .unwrap();
        assert!(
            error
                .to_string()
                .contains("max grouping tokens must be greater than zero"),
            "expected invalid token limit failure, got: {error:?}"
        );
    }

    #[test]
    fn deserialization_rejects_malformed_yaml() {
        let error = serde_yaml::from_str::<DocsSortingConfigBuilder>("fingerprint:\n  fields: [")
            .err()
            .unwrap();
        assert!(
            error
                .to_string()
                .contains("did not find expected node content"),
            "expected parse failure, got: {error:?}"
        );
    }

    #[test]
    fn deserialization_rejects_unknown_fields() {
        let error = serde_yaml::from_str::<DocsSortingConfigBuilder>(
            r#"
fingerprint:
  fields:
    - path: message
      kind: tokenized
      extra: nope
"#,
        )
        .err()
        .unwrap();
        assert!(
            format!("{error:?}").contains("unknown field `extra`"),
            "expected unknown field failure, got: {error:?}"
        );
    }

    #[test]
    fn deserialization_rejects_unknown_kinds() {
        let error = serde_yaml::from_str::<DocsSortingConfigBuilder>(
            r#"
fingerprint:
  fields:
    - path: message
      kind: templated
"#,
        )
        .err()
        .unwrap();
        assert!(
            format!("{error:?}").contains("unknown variant `templated`"),
            "expected unknown kind failure, got: {error:?}"
        );
    }
}
