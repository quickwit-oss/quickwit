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

mod serialize;

use anyhow::ensure;
use quickwit_common::uri::Uri;
use quickwit_proto::types::{DocMappingUid, IndexId};
use serde::{Deserialize, Serialize};
pub use serialize::{IndexTemplateV0_8, VersionedIndexTemplate};

use crate::index_config::validate_index_config;
use crate::{
    validate_identifier, validate_index_id_pattern, DocMapping, IndexConfig, IndexingSettings,
    RetentionPolicy, SearchSettings,
};

pub type IndexTemplateId = String;
pub type IndexIdPattern = String;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(into = "VersionedIndexTemplate")]
#[serde(from = "VersionedIndexTemplate")]
pub struct IndexTemplate {
    pub template_id: IndexTemplateId,
    pub index_id_patterns: Vec<IndexIdPattern>,
    #[serde(default)]
    pub index_root_uri: Option<Uri>,
    #[serde(default)]
    pub priority: usize,
    #[serde(default)]
    pub description: Option<String>,
    pub doc_mapping: DocMapping,
    #[serde(default)]
    pub indexing_settings: IndexingSettings,
    #[serde(default)]
    pub search_settings: SearchSettings,
    #[serde(rename = "retention")]
    #[serde(default)]
    pub retention_policy_opt: Option<RetentionPolicy>,
}

impl IndexTemplate {
    pub fn apply_template(
        &self,
        index_id: IndexId,
        default_index_root_uri: &Uri,
    ) -> anyhow::Result<IndexConfig> {
        let index_uri = self
            .index_root_uri
            .as_ref()
            .unwrap_or(default_index_root_uri)
            .join(&index_id)?;

        // Ensure that the doc mapping UID is truly unique per index.
        let mut doc_mapping = self.doc_mapping.clone();
        doc_mapping.doc_mapping_uid = DocMappingUid::random();

        let index_config = IndexConfig {
            index_id,
            index_uri,
            doc_mapping,
            indexing_settings: self.indexing_settings.clone(),
            search_settings: self.search_settings.clone(),
            retention_policy_opt: self.retention_policy_opt.clone(),
        };
        Ok(index_config)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        validate_identifier("template", &self.template_id)?;

        ensure!(
            !self.index_id_patterns.is_empty(),
            "`index_id_patterns` must not be empty"
        );
        for index_id_pattern in &self.index_id_patterns {
            validate_index_id_pattern(index_id_pattern, true)?;
        }
        validate_index_config(
            &self.doc_mapping,
            &self.indexing_settings,
            &self.search_settings,
            &self.retention_policy_opt,
        )?;
        Ok(())
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test(template_id: &str, index_id_patterns: &[&str], priority: usize) -> Self {
        let index_id_patterns: Vec<IndexIdPattern> = index_id_patterns
            .iter()
            .map(|pattern| pattern.to_string())
            .collect();

        let doc_mapping_json = r#"{
            "field_mappings": [
                {
                    "name": "ts",
                    "type": "datetime",
                    "fast": true
                },
                {
                    "name": "message",
                    "type": "json"
                }
            ],
            "timestamp_field": "ts"
        }"#;
        let doc_mapping: DocMapping = serde_json::from_str(doc_mapping_json).unwrap();

        IndexTemplate {
            template_id: template_id.to_string(),
            index_root_uri: Some(Uri::for_test("ram:///indexes")),
            index_id_patterns,
            priority,
            description: Some("Test description.".to_string()),
            doc_mapping,
            indexing_settings: IndexingSettings::default(),
            search_settings: SearchSettings::default(),
            retention_policy_opt: None,
        }
    }
}

#[cfg(any(test, feature = "testsuite"))]
impl crate::TestableForRegression for IndexTemplate {
    fn sample_for_regression() -> Self {
        let template_id = "test-template".to_string();
        let index_id_patterns = vec![
            "test-index-foo*".to_string(),
            "-test-index-foobar".to_string(),
        ];

        let doc_mapping_json = r#"{
            "doc_mapping_uid": "00000000000000000000000001",
            "field_mappings": [
                {
                    "name": "ts",
                    "type": "datetime",
                    "fast": true
                },
                {
                    "name": "message",
                    "type": "json"
                }
            ],
            "timestamp_field": "ts"
        }"#;
        let doc_mapping: DocMapping = serde_json::from_str(doc_mapping_json).unwrap();

        IndexTemplate {
            template_id: template_id.to_string(),
            index_root_uri: Some(Uri::for_test("ram:///indexes")),
            index_id_patterns,
            priority: 100,
            description: Some("Test description.".to_string()),
            doc_mapping,
            indexing_settings: IndexingSettings::default(),
            search_settings: SearchSettings::default(),
            retention_policy_opt: Some(RetentionPolicy {
                retention_period: "42 days".to_string(),
                evaluation_schedule: "daily".to_string(),
                jitter_secs: None,
            }),
        }
    }

    fn assert_equality(&self, other: &Self) {
        assert_eq!(self, other);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_template_serde() {
        let index_template_yaml = r#"
            version: 0.8

            template_id: test-template
            index_id_patterns:
              - test-index-*
              - -test-index-foo
            description: Test description.
            priority: 100
            doc_mapping:
              field_mappings:
                - name: ts
                  type: datetime
                  fast: true
                - name: message
                  type: json
              timestamp_field: ts
        "#;
        let index_template: IndexTemplate = serde_yaml::from_str(index_template_yaml).unwrap();
        assert_eq!(index_template.template_id, "test-template");
        assert_eq!(index_template.index_id_patterns.len(), 2);
        assert_eq!(
            index_template.index_id_patterns,
            ["test-index-*", "-test-index-foo"]
        );
        assert_eq!(index_template.priority, 100);
        assert_eq!(index_template.description.unwrap(), "Test description.");
        assert_eq!(index_template.doc_mapping.timestamp_field.unwrap(), "ts");
    }

    #[test]
    fn test_index_template_apply() {
        let mut index_template = IndexTemplate::for_test("test-template", &["test-index-*"], 0);

        index_template.indexing_settings = IndexingSettings {
            commit_timeout_secs: 42,
            ..Default::default()
        };
        index_template.search_settings = SearchSettings {
            default_search_fields: vec!["message".to_string()],
        };
        index_template.retention_policy_opt = Some(RetentionPolicy {
            retention_period: "42 days".to_string(),
            evaluation_schedule: "hourly".to_string(),
            jitter_secs: None,
        });
        let default_index_root_uri = Uri::for_test("s3://test-bucket/indexes");

        let index_config_foo = index_template
            .apply_template("test-index-foo".to_string(), &default_index_root_uri)
            .unwrap();

        assert_eq!(index_config_foo.index_id, "test-index-foo");
        assert_eq!(index_config_foo.index_uri, "ram:///indexes/test-index-foo");

        assert_eq!(index_config_foo.doc_mapping.timestamp_field.unwrap(), "ts");
        assert_eq!(index_config_foo.indexing_settings.commit_timeout_secs, 42);
        assert_eq!(
            index_config_foo.search_settings.default_search_fields,
            ["message"]
        );
        let retention_policy = index_config_foo.retention_policy_opt.unwrap();
        assert_eq!(retention_policy.retention_period, "42 days");
        assert_eq!(retention_policy.evaluation_schedule, "hourly");

        index_template.index_root_uri = None;

        let index_config_bar = index_template
            .apply_template("test-index-bar".to_string(), &default_index_root_uri)
            .unwrap();

        assert_eq!(index_config_bar.index_id, "test-index-bar");
        assert_eq!(
            index_config_bar.index_uri,
            "s3://test-bucket/indexes/test-index-bar"
        );
        assert_ne!(
            index_config_foo.doc_mapping.doc_mapping_uid,
            index_config_bar.doc_mapping.doc_mapping_uid
        );
    }

    #[test]
    fn test_index_template_validate() {
        let index_template = IndexTemplate::for_test("", &[], 0);
        let error = index_template.validate().unwrap_err();
        assert!(error.to_string().contains("template ID `` is invalid"));

        let index_template = IndexTemplate::for_test("test-template", &[], 0);
        let error = index_template.validate().unwrap_err();
        assert!(error.to_string().contains("empty"));

        let index_template = IndexTemplate::for_test("test-template", &[""], 0);
        let error = index_template.validate().unwrap_err();
        assert!(error.to_string().contains("index ID pattern `` is invalid"));

        let mut index_template = IndexTemplate::for_test("test-template", &["test-index-*"], 0);
        index_template.retention_policy_opt = Some(RetentionPolicy {
            retention_period: "".to_string(),
            evaluation_schedule: "".to_string(),
            jitter_secs: None,
        });
        let error = index_template.validate().unwrap_err();
        assert!(error
            .to_string()
            .contains("failed to parse retention period"));
    }
}
