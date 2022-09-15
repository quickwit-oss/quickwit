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

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use quickwit_metastore::IndexMetadata;
use quickwit_storage::Storage;

use crate::merge_policy::{MergePolicy, StableMultitenantWithTimestampMergePolicy};
use crate::models::{IndexingDirectory, IndexingPipelineId};
use crate::{IndexingServiceError, IndexingSplitStore, IndexingSplitStoreParams};

/// A struct owning instances of resources that can be shared
/// among indexing pipelines.
#[derive(Clone)]
pub(crate) struct IndexingSharedResource {
    pub(crate) split_store: IndexingSplitStore,
    pub(crate) indexing_directory: IndexingDirectory,
}

/// A repository of resources that can be shared among
/// indexing pipelines of the same (index_id, source_id).
pub(crate) struct IndexingSharedResourceManager {
    /// A map of string to SharedResource. The key is
    /// a concatenation of both (index_id, source_id).
    shared_resources: HashMap<String, IndexingSharedResource>,
}

impl IndexingSharedResourceManager {
    pub fn new() -> Self {
        Self {
            shared_resources: HashMap::new(),
        }
    }

    /// Get an initialized resource or initializes a new
    /// one and returns it.
    pub async fn get_or_init_resource(
        &mut self,
        pipeline_id: &IndexingPipelineId,
        indexing_dir_path: PathBuf,
        storage: Arc<dyn Storage>,
        index_metadata: &IndexMetadata,
        max_num_bytes: usize,
        max_num_splits: usize,
    ) -> Result<IndexingSharedResource, IndexingServiceError> {
        let key = self.resource_key(pipeline_id);
        if let Some(resource) = self.shared_resources.get(&key) {
            return Ok(resource.clone());
        }

        let stable_multitenant_merge_policy = StableMultitenantWithTimestampMergePolicy {
            merge_enabled: index_metadata.indexing_settings.merge_enabled,
            merge_factor: index_metadata.indexing_settings.merge_policy.merge_factor,
            max_merge_factor: index_metadata
                .indexing_settings
                .merge_policy
                .max_merge_factor,
            split_num_docs_target: index_metadata.indexing_settings.split_num_docs_target,
            ..Default::default()
        };
        let merge_policy: Arc<dyn MergePolicy> = Arc::new(stable_multitenant_merge_policy);

        let indexing_directory_path = indexing_dir_path
            .join(&pipeline_id.index_id)
            .join(&pipeline_id.source_id);
        let indexing_directory = IndexingDirectory::create_in_dir(indexing_directory_path)
            .await
            .map_err(IndexingServiceError::InvalidParams)?;

        let split_store = IndexingSplitStore::create_with_local_store(
            storage.clone(),
            indexing_directory.cache_directory.as_path(),
            IndexingSplitStoreParams {
                max_num_bytes,
                max_num_splits,
            },
            merge_policy,
        )?;

        self.shared_resources.insert(
            key.clone(),
            IndexingSharedResource {
                indexing_directory,
                split_store,
            },
        );
        Ok(self.shared_resources.get(&key).unwrap().clone())
    }

    /// Get rid of not needed resources.
    pub fn retain_only<'a>(
        &mut self,
        active_pipeline_ids: impl Iterator<Item = &'a IndexingPipelineId>,
    ) {
        let active_keys: HashSet<String> = active_pipeline_ids
            .into_iter()
            .map(|pipeline_id| self.resource_key(pipeline_id))
            .collect();
        let current_keys: HashSet<String> = self.shared_resources.keys().cloned().collect();
        let deleted_keys = &current_keys - &active_keys;
        for key in deleted_keys {
            self.shared_resources.remove(&key);
        }
    }

    fn resource_key(&self, pipeline_id: &IndexingPipelineId) -> String {
        format!("{}_{}", pipeline_id.index_id, pipeline_id.source_id)
    }

    #[cfg(test)]
    pub(crate) fn get_keys(&self) -> HashSet<&str> {
        self.shared_resources
            .keys()
            .into_iter()
            .map(|v| v.as_str())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use quickwit_metastore::IndexMetadata;
    use quickwit_storage::RamStorage;
    use tempfile::TempDir;

    use super::{IndexingSharedResource, IndexingSharedResourceManager};
    use crate::models::IndexingPipelineId;
    use crate::IndexingServiceError;

    fn pipeline_id(index_id: &str, source_id: &str) -> IndexingPipelineId {
        IndexingPipelineId {
            index_id: index_id.to_string(),
            source_id: source_id.to_string(),
            node_id: "node-1".to_string(),
            pipeline_ord: 0,
        }
    }

    async fn get_or_init_resource<'a>(
        manager: &mut IndexingSharedResourceManager,
        pipeline_id: &IndexingPipelineId,
        temp_dir: &TempDir,
    ) -> Result<IndexingSharedResource, IndexingServiceError> {
        let indexing_dir_path = temp_dir
            .path()
            .join(&pipeline_id.index_id)
            .join(&pipeline_id.source_id);
        let storage = Arc::new(RamStorage::default());
        let index_metadata =
            IndexMetadata::for_test(pipeline_id.index_id.as_str(), "ram:///indexes/test-index");
        manager
            .get_or_init_resource(
                pipeline_id,
                indexing_dir_path,
                storage,
                &index_metadata,
                10,
                10,
            )
            .await
    }

    #[tokio::test]
    async fn test_shared_resource_manager() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let mut manager = IndexingSharedResourceManager::new();
        assert_eq!(manager.get_keys(), HashSet::new());

        get_or_init_resource(&mut manager, &pipeline_id("foo", "source-1"), &temp_dir).await?;
        get_or_init_resource(&mut manager, &pipeline_id("foo", "source-2"), &temp_dir).await?;
        get_or_init_resource(&mut manager, &pipeline_id("bar", "source-1"), &temp_dir).await?;
        get_or_init_resource(&mut manager, &pipeline_id("bar", "source-1"), &temp_dir).await?;
        get_or_init_resource(&mut manager, &pipeline_id("baz", "source-1"), &temp_dir).await?;
        get_or_init_resource(&mut manager, &pipeline_id("baz", "source-2"), &temp_dir).await?;
        get_or_init_resource(&mut manager, &pipeline_id("foo", "source-2"), &temp_dir).await?;
        assert_eq!(
            manager.get_keys(),
            HashSet::from_iter(vec![
                "bar_source-1",
                "baz_source-1",
                "baz_source-2",
                "foo_source-1",
                "foo_source-2"
            ])
        );

        manager.retain_only(
            vec![
                pipeline_id("foo", "source-2"),
                pipeline_id("baz", "source-1"),
                pipeline_id("baz", "source-2"),
            ]
            .iter(),
        );
        assert_eq!(
            manager.get_keys(),
            HashSet::from_iter(vec!["baz_source-1", "baz_source-2", "foo_source-2"])
        );
        Ok(())
    }
}
