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

use quickwit_common::uri::Uri;
use serde::{Deserialize, Serialize};

use super::{IndexIdPattern, IndexTemplate, IndexTemplateId};
use crate::{DocMapping, IndexingSettings, RetentionPolicy, SearchSettings};

#[derive(Clone, Debug, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "version")]
pub enum VersionedIndexTemplate {
    #[serde(rename = "0.9")]
    #[serde(alias = "0.8")]
    #[serde(alias = "0.7")]
    V0_8(IndexTemplateV0_8),
}

#[derive(Clone, Debug, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct IndexTemplateV0_8 {
    #[schema(value_type = String)]
    pub template_id: IndexTemplateId,
    /// Glob patterns (e.g., `logs-foo*`) with negation by prepending `-` (e.g `-logs-fool`).
    #[schema(value_type = Vec<String>)]
    pub index_id_patterns: Vec<IndexIdPattern>,
    /// The actual index URI is the concatenation of this with the index id.
    #[schema(value_type = String)]
    #[serde(default)]
    pub index_root_uri: Option<Uri>,
    /// When multiple templates match an index, the one with the highest priority is selected.
    #[serde(default)]
    pub priority: usize,
    #[serde(default)]
    pub description: Option<String>,

    pub doc_mapping: DocMapping,
    #[serde(default)]
    pub indexing_settings: IndexingSettings,
    #[serde(default)]
    pub search_settings: SearchSettings,
    #[serde(default)]
    pub retention: Option<RetentionPolicy>,
}

impl From<VersionedIndexTemplate> for IndexTemplate {
    fn from(versioned_index_template: VersionedIndexTemplate) -> Self {
        match versioned_index_template {
            VersionedIndexTemplate::V0_8(v0_8) => v0_8.into(),
        }
    }
}

impl From<IndexTemplate> for VersionedIndexTemplate {
    fn from(index_template: IndexTemplate) -> Self {
        VersionedIndexTemplate::V0_8(index_template.into())
    }
}

impl From<IndexTemplateV0_8> for IndexTemplate {
    fn from(index_template_v0_8: IndexTemplateV0_8) -> Self {
        IndexTemplate {
            template_id: index_template_v0_8.template_id,
            index_id_patterns: index_template_v0_8.index_id_patterns,
            index_root_uri: index_template_v0_8.index_root_uri,
            priority: index_template_v0_8.priority,
            description: index_template_v0_8.description,
            doc_mapping: index_template_v0_8.doc_mapping,
            indexing_settings: index_template_v0_8.indexing_settings,
            search_settings: index_template_v0_8.search_settings,
            retention_policy_opt: index_template_v0_8.retention,
        }
    }
}

impl From<IndexTemplate> for IndexTemplateV0_8 {
    fn from(index_template: IndexTemplate) -> Self {
        IndexTemplateV0_8 {
            template_id: index_template.template_id,
            index_id_patterns: index_template.index_id_patterns,
            index_root_uri: index_template.index_root_uri,
            priority: index_template.priority,
            description: index_template.description,
            doc_mapping: index_template.doc_mapping,
            indexing_settings: index_template.indexing_settings,
            search_settings: index_template.search_settings,
            retention: index_template.retention_policy_opt,
        }
    }
}
