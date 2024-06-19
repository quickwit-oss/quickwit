// Copyright (C) 2024 Quickwit, Inc.
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

use anyhow::ensure;
use quickwit_common::uri::Uri;
use quickwit_proto::types::IndexId;
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

        let index_config = IndexConfig {
            index_id,
            index_uri,
            doc_mapping: self.doc_mapping.clone(),
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
        });
        let default_index_root_uri = Uri::for_test("s3://test-bucket/indexes");

        let index_config = index_template
            .apply_template("test-index".to_string(), &default_index_root_uri)
            .unwrap();

        assert_eq!(index_config.index_id, "test-index");
        assert_eq!(index_config.index_uri, "ram:///indexes/test-index");

        assert_eq!(index_config.doc_mapping.timestamp_field.unwrap(), "ts");
        assert_eq!(index_config.indexing_settings.commit_timeout_secs, 42);
        assert_eq!(
            index_config.search_settings.default_search_fields,
            ["message"]
        );
        let retention_policy = index_config.retention_policy_opt.unwrap();
        assert_eq!(retention_policy.retention_period, "42 days");
        assert_eq!(retention_policy.evaluation_schedule, "hourly");

        index_template.index_root_uri = None;

        let index_config = index_template
            .apply_template("test-index".to_string(), &default_index_root_uri)
            .unwrap();

        assert_eq!(index_config.index_id, "test-index");
        assert_eq!(
            index_config.index_uri,
            "s3://test-bucket/indexes/test-index"
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
        });
        let error = index_template.validate().unwrap_err();
        assert!(error
            .to_string()
            .contains("failed to parse retention period"));
    }
}
