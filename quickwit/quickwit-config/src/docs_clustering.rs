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

use std::collections::HashMap;
use std::ops::Deref;

use anyhow::ensure;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::config_value::ConfigValue;
use crate::qw_env_vars::QW_DISABLE_DOCS_CLUSTERING;

const JSON_PATH_SEPARATOR: &str = ".";

/// Configuration for document clustering.
///
/// Document clustering groups documents by structure and field values, then orders those groups by
/// descending size. It applies only when publishing fresh splits, not during split merge
/// operations.
///
/// See the docs_clustering module in the quickwit-indexing crate for more details.
///
/// # Warning
///
/// Document clustering is experimental. Its configuration and behavior may change, and it should be
/// validated on representative workloads before production use.
///
/// # Example
///
/// For example, documents with the same JSON structure are clustered together, then grouped by
/// fields such as `service` or the token pattern of `message` within each structure group.
///
/// Example YAML:
///
/// ```yaml
/// - fingerprint:
///     - kind: structure
///       exclude: [custom]
/// - fingerprint:
///     - kind: raw
///       path: status
///     - kind: raw
///       path: service
///     - kind: tokenized
///       path: message
/// ```
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(transparent)]
pub struct DocsClusteringConfig {
    pub policies: Vec<ClusteringPolicy>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(transparent)]
pub(crate) struct DocsClusteringConfigBuilder {
    policies: Vec<ClusteringPolicy>,
}

impl DocsClusteringConfigBuilder {
    pub(crate) fn build_optional(
        config_builder_opt: Option<Self>,
        env_vars: &HashMap<String, String>,
    ) -> anyhow::Result<Option<DocsClusteringConfig>> {
        let disable_override =
            ConfigValue::<bool, QW_DISABLE_DOCS_CLUSTERING>::none().resolve_optional(env_vars)?;

        let Some(config_builder) = config_builder_opt else {
            return Ok(None);
        };

        match disable_override {
            Some(true) => Ok(None),
            Some(false) | None => {
                let config = DocsClusteringConfig {
                    policies: config_builder.policies,
                };
                config.validate()?;
                Ok(Some(config))
            }
        }
    }
}

impl DocsClusteringConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            !self.policies.is_empty(),
            "document clustering policies must contain at least one policy"
        );
        for policy in &self.policies {
            policy.validate()?;
        }
        Ok(())
    }
}

/// Defines how documents are clustered.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, untagged)]
pub enum ClusteringPolicy {
    /// Clusters documents using structure and configured field fingerprints.
    Fingerprint { fingerprint: FingerprintPolicy },
}

impl ClusteringPolicy {
    fn validate(&self) -> anyhow::Result<()> {
        match self {
            Self::Fingerprint { fingerprint } => {
                fingerprint.validate()?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(transparent)]
pub struct FingerprintPolicy {
    pub fingerprint: Vec<ClusteringMethod>,
}

impl FingerprintPolicy {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            !self.fingerprint.is_empty(),
            "document clustering fingerprint policy must contain at least one field"
        );
        for method in &self.fingerprint {
            method.validate()?;
        }
        Ok(())
    }
}

#[repr(transparent)]
#[derive(Debug, Clone, PartialEq)]
pub struct JsonPath(Box<[String]>);

impl Serialize for JsonPath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_str(&self.0.join(JSON_PATH_SEPARATOR))
    }
}

impl<'de> Deserialize<'de> for JsonPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let path = String::deserialize(deserializer)?;
        let json_path: Box<[String]> = path
            .split(JSON_PATH_SEPARATOR)
            .map(ToString::to_string)
            .collect();
        Ok(Self(json_path))
    }
}

impl JsonPath {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            !self.iter().any(|path_component| path_component.is_empty()),
            "document clustering path `{:?}` must not contain empty components",
            self
        );
        ensure!(
            !self
                .iter()
                .any(|path_component| path_component.trim() != path_component),
            "document clustering path `{:?}` must not contain leading or trailing whitespace",
            self
        );
        Ok(())
    }
}

impl Deref for JsonPath {
    type Target = [String];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A field used to partition documents.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClusteringMethod {
    /// Groups documents by JSON structure.
    ///
    /// For example, `{"message":"hello"}` and `{"body":"hello"}` belong to different groups
    /// because their field paths differ.
    Structure {
        /// Paths omitted from the structure fingerprint.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        exclude: Vec<JsonPath>,
    },
    /// Groups documents by the exact JSON value at `path`.
    ///
    /// For example, `{"service":"api"}` and `{"service":"worker"}` belong to different groups
    /// when `path` is `service`.
    Raw {
        /// Dot-separated path to the JSON value.
        path: JsonPath,
    },
    /// Groups documents by the pattern of the first `max_tokens` tokens in the string value at
    /// `path`.
    ///
    /// For example, `{"message":"request 123"}` and `{"message":"request 456"}` belong to the same
    /// group when `path` is `message`, because both values have the same token pattern.
    Tokenized {
        /// Dot-separated path to the string value.
        path: JsonPath,
        /// Maximum number of tokens to consider. Defaults to 50.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_tokens: Option<usize>,
    },
}

impl ClusteringMethod {
    fn validate(&self) -> anyhow::Result<()> {
        match self {
            Self::Structure { exclude } => {
                for path in exclude {
                    path.validate()?;
                }
            }
            Self::Raw { path } | Self::Tokenized { path, .. } => path.validate()?,
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{DocsClusteringConfig, DocsClusteringConfigBuilder};

    fn build_config(yaml: &str) -> anyhow::Result<Option<DocsClusteringConfig>> {
        build_config_with_env(yaml, &HashMap::new())
    }

    fn build_config_with_env(
        yaml: &str,
        env_vars: &HashMap<String, String>,
    ) -> anyhow::Result<Option<DocsClusteringConfig>> {
        let config_builder = serde_yaml::from_str::<DocsClusteringConfigBuilder>(yaml)?;
        DocsClusteringConfigBuilder::build_optional(Some(config_builder), env_vars)
    }

    #[test]
    fn build_accepts_structure_without_exclusions() {
        let config = build_config(
            r#"
- fingerprint:
    - kind: structure
- fingerprint:
    - path: message
      kind: tokenized
"#,
        )
        .unwrap();
        assert!(config.is_some());
    }

    #[test]
    fn build_can_be_disabled_by_env_var() {
        let env_vars =
            HashMap::from([("QW_DISABLE_DOCS_CLUSTERING".to_string(), "true".to_string())]);
        let config = build_config_with_env(
            r#"
- fingerprint:
    - kind: structure
- fingerprint:
    - path: message
      kind: tokenized
"#,
            &env_vars,
        )
        .unwrap();

        assert!(config.is_none());
    }

    #[test]
    fn build_false_env_var_preserves_config() {
        let env_vars = HashMap::from([(
            "QW_DISABLE_DOCS_CLUSTERING".to_string(),
            "false".to_string(),
        )]);
        let config = build_config_with_env(
            r#"
- fingerprint:
    - kind: structure
- fingerprint:
    - path: message
      kind: tokenized
"#,
            &env_vars,
        )
        .unwrap();

        assert!(config.is_some());
    }

    #[test]
    fn build_rejects_invalid_env_var() {
        let env_vars = HashMap::from([(
            "QW_DISABLE_DOCS_CLUSTERING".to_string(),
            "invalid".to_string(),
        )]);
        let error = DocsClusteringConfigBuilder::build_optional(None, &env_vars).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("failed to convert value `invalid`"),
            "expected invalid boolean error, got: {error}",
        );
    }

    #[test]
    fn config_validation_rejects_invalid_fingerprint_policies() {
        let test_cases = [
            (
                serde_json::json!([
                    {"fingerprint": []},
                    {"fingerprint": [{"path": "message", "kind": "tokenized"}]}
                ]),
                "fingerprint policy must contain at least one field",
            ),
            (
                serde_json::json!([
                    {"fingerprint": [{"kind": "structure"}]},
                    {"fingerprint": []}
                ]),
                "fingerprint policy must contain at least one field",
            ),
            (
                serde_json::json!([
                    {"fingerprint": [{"path": "", "kind": "raw"}]}
                ]),
                "must not contain empty components",
            ),
            (
                serde_json::json!([
                    {"fingerprint": [
                        {"path": "message..template", "kind": "tokenized"}
                    ]}
                ]),
                "must not contain empty components",
            ),
            (
                serde_json::json!([
                    {"fingerprint": [{"kind": "structure", "exclude": [" custom"]}]}
                ]),
                "must not contain leading or trailing whitespace",
            ),
        ];

        for (json_value, expected_error) in test_cases {
            let config: DocsClusteringConfig = serde_json::from_value(json_value).unwrap();
            let error = config.validate().unwrap_err();
            assert!(
                error.to_string().contains(expected_error),
                "expected `{expected_error}`, got: {error:?}"
            );
        }
    }

    #[test]
    fn config_validation_rejects_empty_policies() {
        let json_value = serde_json::json!([]);
        let expected_error = "document clustering policies must contain at least one policy";

        let config: DocsClusteringConfig = serde_json::from_value(json_value).unwrap();
        let error = config.validate().unwrap_err();
        assert!(
            error.to_string().contains(expected_error),
            "expected `{expected_error}`, got: {error:?}"
        );
    }

    #[test]
    fn disable_override_bypasses_invalid_json_path() {
        let env_vars =
            HashMap::from([("QW_DISABLE_DOCS_CLUSTERING".to_string(), "true".to_string())]);
        let config = build_config_with_env(
            r#"
- fingerprint:
    - path: message..template
      kind: tokenized
"#,
            &env_vars,
        );

        assert!(config.unwrap().is_none());
    }

    #[test]
    fn config_validation_accepts_arbitrary_policy_shapes() {
        let config: DocsClusteringConfig = serde_json::from_value(serde_json::json!([
            {"fingerprint": [{"path": "message", "kind": "raw"}]},
            {"fingerprint": [{"kind": "structure"}]},
            {"fingerprint": [{"kind": "structure"}]},
            {"fingerprint": [
                {"path": "service", "kind": "raw"},
                {"path": "message", "kind": "tokenized"}
            ]}
        ]))
        .unwrap();

        config.validate().unwrap();
    }

    #[test]
    fn deserialization_rejects_malformed_yaml() {
        let error = serde_yaml::from_str::<DocsClusteringConfigBuilder>("- fingerprint: [")
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
        let error = serde_yaml::from_str::<DocsClusteringConfigBuilder>(
            r#"
- fingerprint:
    - kind: structure
    - path: message
      kind: tokenized
      extra: nope
"#,
        )
        .err()
        .unwrap();
        assert!(
            format!("{error:?}").contains("did not match any variant"),
            "expected unknown field failure, got: {error:?}"
        );
    }

    #[test]
    fn deserialization_rejects_unknown_policy_fields() {
        let error = serde_yaml::from_str::<DocsClusteringConfigBuilder>(
            r#"
- fingerprint:
    - kind: structure
  extra: nope
- fingerprint:
    - path: message
      kind: tokenized
"#,
        )
        .err()
        .unwrap();
        assert!(
            format!("{error:?}").contains("did not match any variant"),
            "expected unknown policy field failure, got: {error:?}"
        );
    }

    #[test]
    fn deserialization_rejects_unknown_kinds() {
        let error = serde_yaml::from_str::<DocsClusteringConfigBuilder>(
            r#"
- fingerprint:
    - kind: structure
    - path: message
      kind: templated
"#,
        )
        .err()
        .unwrap();
        assert!(
            format!("{error:?}").contains("did not match any variant"),
            "expected unknown kind failure, got: {error:?}"
        );
    }

    #[test]
    fn serialization_uses_flattened_policy_format() {
        let config = build_config(
            r#"
- fingerprint:
    - kind: structure
      exclude: [custom]
- fingerprint:
    - path: custom
      kind: raw
    - path: message
      kind: tokenized
"#,
        )
        .unwrap()
        .unwrap();
        let serialized_config = serde_yaml::to_string(&config).unwrap();

        assert!(serialized_config.starts_with("- fingerprint:"));
        assert!(serialized_config.contains("fingerprint:"));
        assert!(serialized_config.contains("kind: structure"));
        assert!(!serialized_config.contains("grouping:"));
    }
}
