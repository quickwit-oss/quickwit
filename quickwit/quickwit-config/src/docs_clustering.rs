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

use anyhow::ensure;
use serde::{Deserialize, Serialize};

use crate::config_value::ConfigValue;
use crate::qw_env_vars::QW_DISABLE_DOCS_CLUSTERING;

/// Configuration for document clustering.
///
/// Document clustering groups documents by structure and field values, then orders those groups by
/// descending size. It applies only when publishing fresh splits, not during split merge
/// operations.
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
        for policy in &self.policies {
            policy.validate()?;
        }
        // TODO: Remove this constraint once we support arbitrary levels of clustering in the
        // runtime fingerprinter.
        self.validate_fingerprinter_limitations()?;
        Ok(())
    }

    // The runtime fingerprinter currently supports exactly two clustering levels.
    //
    // Implementation constraints are:
    // - The first policy must contain exactly one structure field
    // - The second policy may contain only raw and tokenized fields
    // - Additional policies are not supported
    //
    // TODO: Remove this constraint once the runtime fingerprinter supports arbitrary clustering
    // levels.
    fn validate_fingerprinter_limitations(&self) -> anyhow::Result<()> {
        ensure!(
            self.policies.len() == 2, // one structure field and one or more other fields
            "document clustering currently supports exactly two fingerprint policies"
        );

        // First policy must be a fingerprint policy with exactly one structure field
        let ClusteringPolicy::Fingerprint { fingerprint } = &self.policies[0];
        ensure!(
            fingerprint.fingerprint.len() == 1, // one fingerprinting policy
            "first document clustering fingerprint policy must contain exactly one field"
        );
        ensure!(
            matches!(
                fingerprint.fingerprint[0],
                ClusteringField::Structure { .. }
            ),
            "first document clustering fingerprint policy must contain the structure field"
        );

        let ClusteringPolicy::Fingerprint { fingerprint } = &self.policies[1];
        let has_structure_field = fingerprint
            .fingerprint
            .iter()
            .any(|field| matches!(field, ClusteringField::Structure { .. }));
        ensure!(
            !has_structure_field,
            "document clustering fingerprint must contain exactly one structure field"
        );
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
    pub fingerprint: Vec<ClusteringField>,
}

impl FingerprintPolicy {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            !self.fingerprint.is_empty(),
            "document clustering fingerprint policy must contain at least one field"
        );
        for field in &self.fingerprint {
            field.validate()?;
        }
        Ok(())
    }
}

/// A field used to partition documents.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClusteringField {
    /// Groups documents by JSON structure.
    ///
    /// For example, `{"message":"hello"}` and `{"body":"hello"}` belong to different groups
    /// because their field paths differ.
    Structure {
        /// Paths omitted from the structure fingerprint.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        exclude: Vec<String>,
    },
    /// Groups documents by the exact string value at `path`.
    ///
    /// For example, `{"service":"api"}` and `{"service":"worker"}` belong to different groups
    /// when `path` is `service`.
    Raw {
        /// Dot-separated path to the string value.
        path: String,
    },
    /// Groups documents by the pattern of the first 50 tokens in the string value at `path`.
    ///
    /// For example, `{"message":"request 123"}` and `{"message":"request 456"}` belong to the same
    /// group when `path` is `message`, because both values have the same token pattern.
    Tokenized {
        /// Dot-separated path to the string value.
        path: String,
    },
}

impl ClusteringField {
    fn validate(&self) -> anyhow::Result<()> {
        fn validate_json_path(path: &str) -> anyhow::Result<()> {
            ensure!(
                path.trim() == path,
                "document clustering path `{path}` must not contain leading or trailing whitespace"
            );
            ensure!(
                !path.is_empty(),
                "document clustering path must not be empty"
            );
            ensure!(
                !path.split('.').any(str::is_empty),
                "document clustering path `{path}` must not contain empty components"
            );
            Ok(())
        }

        match self {
            Self::Structure { exclude } => {
                for excluded_path in exclude {
                    validate_json_path(excluded_path)?;
                }
            }
            Self::Raw { path } => {
                validate_json_path(path)?;
            }
            Self::Tokenized { path } => {
                validate_json_path(path)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{
        ClusteringField, ClusteringPolicy, DocsClusteringConfig, DocsClusteringConfigBuilder,
    };

    fn build_config(yaml: &str) -> anyhow::Result<Option<DocsClusteringConfig>> {
        let config_builder = serde_yaml::from_str::<DocsClusteringConfigBuilder>(yaml)?;
        DocsClusteringConfigBuilder::build_optional(Some(config_builder), &HashMap::new())
    }

    #[test]
    fn build_accepts_flattened_fingerprint_policy() {
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
        .unwrap();

        let config = config.unwrap();
        let ClusteringPolicy::Fingerprint { fingerprint } = &config.policies[0];
        let ClusteringField::Structure {
            exclude: excluded_paths,
        } = &fingerprint.fingerprint[0]
        else {
            panic!("expected structure field");
        };
        assert_eq!(excluded_paths, &["custom".to_string()]);
        let ClusteringPolicy::Fingerprint { fingerprint } = &config.policies[1];
        assert_eq!(
            fingerprint.fingerprint,
            vec![
                ClusteringField::Raw {
                    path: "custom".to_string()
                },
                ClusteringField::Tokenized {
                    path: "message".to_string()
                },
            ]
        );
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
    fn config_validation_rejects_unsupported_fingerprinter_shapes() {
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
                    {"fingerprint": [{"path": "message", "kind": "raw"}]},
                    {"fingerprint": [{"path": "service", "kind": "raw"}]}
                ]),
                "first document clustering fingerprint policy must contain the structure field",
            ),
            (
                serde_json::json!([
                    {"fingerprint": [{"kind": "structure"}]},
                    {"fingerprint": [{"kind": "structure"}]}
                ]),
                "exactly one structure field",
            ),
            (
                serde_json::json!([
                    {"fingerprint": [{"kind": "structure"}]},
                    {"fingerprint": [
                        {"path": "message..template", "kind": "tokenized"}
                    ]}
                ]),
                "must not contain empty components",
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
    fn config_validation_rejects_wrong_policy_count() {
        let config: DocsClusteringConfig = serde_json::from_value(serde_json::json!([
            {"fingerprint": [{"kind": "structure"}]}
        ]))
        .unwrap();
        let error = config.validate().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("exactly two fingerprint policies")
        );
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
