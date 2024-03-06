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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use quickwit_config::{IndexTemplate, IndexTemplateId};
use quickwit_proto::metastore::MetastoreResult;
use quickwit_proto::types::IndexId;
use quickwit_storage::Storage;

use super::index_template_matcher::IndexTemplateMatcher;
use super::lazy_file_backed_index::LazyFileBackedIndex;
use super::manifest::{IndexStatus, Manifest};
use super::LazyIndexStatus;

#[derive(Default)]
pub(super) struct MetastoreState {
    pub indexes: HashMap<IndexId, LazyIndexStatus>,
    pub templates: HashMap<IndexTemplateId, IndexTemplate>,
    pub template_matcher: IndexTemplateMatcher,
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
        Manifest { indexes, templates }
    }
}
