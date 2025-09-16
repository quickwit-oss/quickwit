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
use std::sync::Arc;
use std::time::Duration;

use quickwit_config::{IndexTemplate, IndexTemplateId};
use quickwit_proto::metastore::MetastoreResult;
use quickwit_proto::types::IndexId;
use quickwit_storage::Storage;
use uuid::Uuid;

use super::LazyIndexStatus;
use super::index_template_matcher::IndexTemplateMatcher;
use super::lazy_file_backed_index::LazyFileBackedIndex;
use super::manifest::{IndexStatus, Manifest};

#[derive(Default)]
pub(super) struct MetastoreState {
    pub indexes: HashMap<IndexId, LazyIndexStatus>,
    pub templates: HashMap<IndexTemplateId, IndexTemplate>,
    pub template_matcher: IndexTemplateMatcher,
    pub identity: Uuid,
}

impl MetastoreState {
    pub fn try_from_manifest(
        storage: Arc<dyn Storage>,
        manifest: Manifest,
        polling_interval_opt: Option<Duration>,
    ) -> MetastoreResult<Self> {
        let indexes = manifest
            .indexes
            .into_iter()
            .map(|(index_id, index_status)| match index_status {
                IndexStatus::Creating => (index_id, LazyIndexStatus::Creating),
                IndexStatus::Deleting => (index_id, LazyIndexStatus::Deleting),
                IndexStatus::Active => {
                    let lazy_index = LazyFileBackedIndex::new(
                        storage.clone(),
                        index_id.clone(),
                        polling_interval_opt,
                        None,
                    );
                    (index_id, LazyIndexStatus::Active(lazy_index))
                }
            })
            .collect();

        let template_matcher =
            IndexTemplateMatcher::try_from_index_templates(manifest.templates.values())?;

        let state = Self {
            indexes,
            templates: manifest.templates,
            template_matcher,
            identity: manifest.identity,
        };
        Ok(state)
    }

    pub fn as_manifest(&self) -> Manifest {
        let indexes = self
            .indexes
            .iter()
            .map(|(index_id, index_state)| {
                let index_status = match index_state {
                    LazyIndexStatus::Creating => IndexStatus::Creating,
                    LazyIndexStatus::Active(_) => IndexStatus::Active,
                    LazyIndexStatus::Deleting => IndexStatus::Deleting,
                };
                (index_id.clone(), index_status)
            })
            .collect();
        let templates = self.templates.clone();
        Manifest {
            indexes,
            templates,
            identity: self.identity,
        }
    }
}
