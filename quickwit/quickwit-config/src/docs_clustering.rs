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
use crate::qw_env_vars::QW_DISABLE_DOCS_CLUSTERING;

/// Configuration for document clustering.
///
/// # Warning
///
/// Document clustering is experimental. Its configuration and behavior may change, and it should be
/// validated on representative workloads before production use.
///
/// The configuration allows to define how logs are grouped into buckets via the
/// [grouping](`GroupingConfig`) field.
///
/// Example YAML:
///
/// ```yaml
/// grouping:
///   fingerprint:
///     - path: "$"
///       kind: structure
///       exclude: [custom]
///   grouping:
///     fingerprint:
///       - path: custom
///         kind: raw
///       - path: message
///         kind: tokenized
/// ```
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DocsClusteringConfig {
    pub grouping: GroupingConfig,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DocsClusteringConfigBuilder {
    grouping: GroupingConfig,
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
                config_builder.validate()?;
                let config = DocsClusteringConfig {
                    grouping: config_builder.grouping,
                };
                Ok(Some(config))
            }
        }
    }

    fn validate(&self) -> anyhow::Result<()> {
        self.grouping.validate()
    }
}

/// Defines how documents are grouped for sorting.
///
/// A grouping level contains the fields used to partition documents and may contain a nested
/// grouping that further partitions each resulting group. For example, a recursive configuration
/// could first group documents by structure, then by service, and finally by message pattern:
///
/// ```text
/// structure($)
/// └── raw(service)
///     └── tokenized(message)
/// ```
///
/// Grouping field kinds:
///
/// - `structure` groups documents by their JSON structure. Paths listed in `exclude` are omitted
///   from the structure fingerprint.
/// - `raw` groups documents by the exact string value at the configured path.
/// - `tokenized` groups documents by the token pattern of the string at the configured path, so
///   values with the same shape can be grouped even when their literal contents differ.
///
/// Field paths use dot-separated JSON object keys.
///
/// **WARNING:** Although this type supports arbitrary nesting, the current fingerprinting
/// implementation requires exactly two levels:
///
/// - The root level starts with a `structure` field and the path must be `$`. Exclude paths can be
///   configured as desired.
/// - A second grouping level is required and may contain only `raw` and `tokenized` fields with no
///   restrictions on the paths.
/// - A third grouping level is not supported.
///
/// Example YAML:
///
/// ```yaml
/// grouping:
///   fingerprint:
///     - path: "$"
///       kind: structure
///       exclude: [custom]
///   grouping:
///     fingerprint:
///       - path: status
///         kind: raw
///       - path: body
///         kind: tokenized
/// ```
///
/// This limitation is expected to be removed in the future.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GroupingConfig {
    pub fingerprint: Vec<GroupingField>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grouping: Option<Box<GroupingConfig>>,
}

impl GroupingConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        fn validate_grouping_config(config: &GroupingConfig) -> anyhow::Result<()> {
            fn validate_json_path(path: &str) -> anyhow::Result<()> {
                ensure!(
                    path.trim() == path,
                    "document clustering path `{path}` must not contain leading or trailing \
                     whitespace"
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

            let mut grouping_paths = HashSet::with_capacity(config.fingerprint.len());
            let mut excluded_paths = HashSet::new();
            for field in &config.fingerprint {
                match field {
                    GroupingField::Structure { path, exclude } => {
                        validate_json_path(path)?;
                        ensure!(
                            grouping_paths.insert(path.as_str()),
                            "duplicate document clustering grouping path `{path}`"
                        );
                        for excluded_path in exclude {
                            validate_json_path(excluded_path)?;
                            ensure!(
                                excluded_paths.insert(excluded_path.as_str()),
                                "duplicate document clustering excluded path `{excluded_path}`"
                            );
                        }
                    }
                    GroupingField::Raw { path } => {
                        validate_json_path(path)?;
                        ensure!(
                            grouping_paths.insert(path.as_str()),
                            "duplicate document clustering grouping path `{path}`"
                        );
                    }
                    GroupingField::Tokenized { path } => {
                        validate_json_path(path)?;
                        ensure!(
                            grouping_paths.insert(path.as_str()),
                            "duplicate document clustering grouping path `{path}`"
                        );
                    }
                }
            }

            if let Some(child_grouping) = &config.grouping {
                validate_grouping_config(child_grouping)?;
            }
            Ok(())
        }

        validate_grouping_config(self)?;

        // TODO: Remove this constraint once we support arbitrary levels of grouping in the
        // fingerprinting implementation.
        self.validate_fingerprinter_limitations()?;

        Ok(())
    }

    // Current fingerprinting implementation is limited to two levels of grouping.
    //
    // Implementation constraints are:
    // - The root grouping must contain a structure field and it must be `$`
    // - The root grouping must contain a second grouping level and it must not contain a third
    //   grouping level
    // - The second grouping level must only contain raw and tokenized fields
    //
    // TODO: Remove this constraint once we support arbitrary levels of grouping in the
    // fingerprinting implementation.
    fn validate_fingerprinter_limitations(&self) -> anyhow::Result<()> {
        ensure!(
            self.fingerprint.len() == 1,
            "root document clustering grouping must contain exactly one structure field"
        );
        let Some(GroupingField::Structure { path, .. }) = self.fingerprint.first() else {
            return Err(anyhow::anyhow!(
                "root document clustering grouping must contain a structure field"
            ));
        };
        ensure!(
            path == "$",
            "root document clustering structure path must be `$`, got `{path}`"
        );

        let Some(child_grouping) = self.grouping.as_ref() else {
            return Err(anyhow::anyhow!(
                "document clustering grouping must contain a second grouping level"
            ));
        };
        ensure!(
            child_grouping.grouping.is_none(),
            "document clustering grouping currently supports exactly two levels"
        );
        ensure!(
            child_grouping
                .fingerprint
                .iter()
                .all(|field| !matches!(field, GroupingField::Structure { .. })),
            "second-level document clustering grouping only supports raw and tokenized fields"
        );
        Ok(())
    }
}

/// A field used to partition documents at one grouping level.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum GroupingField {
    /// Groups documents by JSON structure.
    ///
    /// For example, `{"message":"hello"}` and `{"body":"hello"}` belong to different groups
    /// because their field paths differ.
    Structure {
        /// Dot-separated path to the object whose structure is fingerprinted.
        path: String,
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{DocsClusteringConfig, DocsClusteringConfigBuilder, GroupingConfig, GroupingField};

    fn build_config(yaml: &str) -> anyhow::Result<Option<DocsClusteringConfig>> {
        let config_builder = serde_yaml::from_str::<DocsClusteringConfigBuilder>(yaml)?;
        DocsClusteringConfigBuilder::build_optional(Some(config_builder), &HashMap::new())
    }

    #[test]
    fn build_accepts_two_level_grouping_config() {
        let config = build_config(
            r#"
grouping:
  fingerprint:
    - path: "$"
      kind: structure
      exclude: [custom]
  grouping:
    fingerprint:
      - path: custom
        kind: raw
      - path: message
        kind: tokenized
"#,
        )
        .unwrap();

        let config = config.unwrap();
        let GroupingField::Structure {
            path,
            exclude: excluded_paths,
        } = &config.grouping.fingerprint[0]
        else {
            panic!("expected root structure field");
        };
        assert_eq!(path, "$");
        assert_eq!(excluded_paths, &["custom".to_string()]);
        assert_eq!(
            config.grouping.grouping.unwrap().fingerprint,
            vec![
                GroupingField::Raw {
                    path: "custom".to_string()
                },
                GroupingField::Tokenized {
                    path: "message".to_string()
                },
            ]
        );
    }

    #[test]
    fn build_accepts_structure_without_exclusions() {
        let config = build_config(
            r#"
grouping:
  fingerprint:
    - path: "$"
      kind: structure
  grouping:
    fingerprint:
      - path: message
        kind: tokenized
"#,
        )
        .unwrap();
        assert!(config.unwrap().grouping.grouping.is_some());
    }

    #[test]
    fn grouping_config_validation_rejects_invalid_structure_and_paths() {
        let test_cases = [
            (
                serde_json::json!({"fingerprint": []}),
                "exactly one structure field",
            ),
            (
                serde_json::json!({
                    "fingerprint": [{"path": "message", "kind": "raw"}]
                }),
                "must contain a structure field",
            ),
            (
                serde_json::json!({
                    "fingerprint": [{"path": "message", "kind": "structure"}]
                }),
                "structure path must be `$`",
            ),
            (
                serde_json::json!({
                    "fingerprint": [{"path": "$", "kind": "structure"}]
                }),
                "must contain a second grouping level",
            ),
            (
                serde_json::json!({
                    "fingerprint": [{"path": "$", "kind": "structure"}],
                    "grouping": {
                        "fingerprint": [{"path": "custom", "kind": "raw"}],
                        "grouping": {
                            "fingerprint": [{"path": "message", "kind": "tokenized"}]
                        }
                    }
                }),
                "supports exactly two levels",
            ),
            (
                serde_json::json!({
                    "fingerprint": [{"path": "$", "kind": "structure"}],
                    "grouping": {
                        "fingerprint": [{"path": "message", "kind": "structure"}]
                    }
                }),
                "only supports raw and tokenized fields",
            ),
            (
                serde_json::json!({
                    "fingerprint": [{
                        "path": "$",
                        "kind": "structure",
                        "exclude": ["custom", "custom"]
                    }],
                    "grouping": {
                        "fingerprint": [{"path": "message", "kind": "tokenized"}]
                    }
                }),
                "duplicate document clustering excluded path `custom`",
            ),
            (
                serde_json::json!({
                    "fingerprint": [{"path": "$", "kind": "structure"}],
                    "grouping": {
                        "fingerprint": [
                            {"path": "message", "kind": "raw"},
                            {"path": "message", "kind": "tokenized"}
                        ]
                    }
                }),
                "duplicate document clustering grouping path `message`",
            ),
            (
                serde_json::json!({
                    "fingerprint": [{"path": "$", "kind": "structure"}],
                    "grouping": {
                        "fingerprint": [{"path": "message..template", "kind": "tokenized"}]
                    }
                }),
                "must not contain empty components",
            ),
        ];

        for (json_value, expected_error) in test_cases {
            let config: GroupingConfig = serde_json::from_value(json_value).unwrap();
            let error = config.validate().unwrap_err();
            assert!(
                error.to_string().contains(expected_error),
                "expected `{expected_error}`, got: {error:?}"
            );
        }
    }

    #[test]
    fn deserialization_rejects_malformed_yaml() {
        let error =
            serde_yaml::from_str::<DocsClusteringConfigBuilder>("grouping:\n  fingerprint: [")
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
grouping:
  fingerprint:
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
        let error = serde_yaml::from_str::<DocsClusteringConfigBuilder>(
            r#"
grouping:
  fingerprint:
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

    #[test]
    fn serialization_uses_grouping_format() {
        let config = build_config(
            r#"
grouping:
  fingerprint:
    - path: "$"
      kind: structure
      exclude: [custom]
  grouping:
    fingerprint:
      - path: custom
        kind: raw
      - path: message
        kind: tokenized
"#,
        )
        .unwrap()
        .unwrap();
        let serialized_config = serde_yaml::to_string(&config).unwrap();

        assert!(serialized_config.contains("grouping:"));
        assert!(serialized_config.contains("fingerprint:"));
        assert!(serialized_config.contains("kind: structure"));
        assert!(!serialized_config.contains("max_grouping_tokens"));
    }
}
