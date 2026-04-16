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

use quickwit_config::{IndexConfig, build_doc_mapper};
use quickwit_doc_mapper::DocMapper;
use quickwit_indexing::merge_policy::{MergePolicy, merge_policy_from_settings};
use quickwit_metastore::{IndexMetadataResponseExt, SplitMaturity, SplitMetadata};
use quickwit_proto::metastore::{IndexMetadataRequest, MetastoreService, MetastoreServiceClient};
use quickwit_proto::types::{DocMappingUid, IndexUid};

/// Everything the planner needs to know about a single index.
pub struct IndexEntry {
    config: IndexConfig,
    merge_policy: Arc<dyn MergePolicy>,
    doc_mappers: HashMap<DocMappingUid, Arc<DocMapper>>,
}

impl IndexEntry {
    pub fn is_split_mature(&self, split: &SplitMetadata) -> bool {
        matches!(
            self.merge_policy
                .split_maturity(split.num_docs, split.num_merge_ops),
            SplitMaturity::Mature
        )
    }

    pub fn merge_policy(&self) -> &Arc<dyn MergePolicy> {
        &self.merge_policy
    }

    pub fn doc_mapping_json(&self) -> String {
        serde_json::to_string(&self.config.doc_mapping)
            .expect("doc mapping serialization should not fail")
    }

    pub fn search_settings_json(&self) -> String {
        serde_json::to_string(&self.config.search_settings)
            .expect("search settings serialization should not fail")
    }

    pub fn indexing_settings_json(&self) -> String {
        serde_json::to_string(&self.config.indexing_settings)
            .expect("indexing settings serialization should not fail")
    }

    pub fn retention_policy_json(&self) -> String {
        match &self.config.retention_policy_opt {
            Some(policy) => serde_json::to_string(policy)
                .expect("retention policy serialization should not fail"),
            None => String::new(),
        }
    }

    pub fn index_storage_uri(&self) -> String {
        self.config.index_uri.to_string()
    }
}

/// Caches per-index configuration, merge policies, and doc mappers.
/// Fetches from the metastore on demand. All accessors panic if called
/// for an index that hasn't been loaded.
pub struct IndexConfigMetastore {
    indexes: HashMap<IndexUid, IndexEntry>,
    metastore_client: MetastoreServiceClient,
}

impl IndexConfigMetastore {
    pub fn new(metastore_client: MetastoreServiceClient) -> Self {
        IndexConfigMetastore {
            indexes: HashMap::new(),
            metastore_client,
        }
    }

    /// Fetches an index config from the metastore and builds the derived
    /// artifacts.
    async fn fetch_index_config(
        &mut self,
        index_uid: &IndexUid,
        doc_mapping_uid: &DocMappingUid,
    ) -> anyhow::Result<()> {
        let response = self
            .metastore_client
            .index_metadata(IndexMetadataRequest {
                index_uid: Some(index_uid.clone()),
                index_id: None,
            })
            .await?;
        let index_metadata = response.deserialize_index_metadata()?;

        let doc_mapper = build_doc_mapper(
            &index_metadata.index_config.doc_mapping,
            &index_metadata.index_config.search_settings,
        )?;
        let merge_policy =
            merge_policy_from_settings(&index_metadata.index_config.indexing_settings);

        let mut doc_mappers = HashMap::new();
        doc_mappers.insert(*doc_mapping_uid, doc_mapper);
        let entry = IndexEntry {
            config: index_metadata.index_config,
            merge_policy,
            doc_mappers,
        };
        self.indexes.insert(index_uid.clone(), entry);
        Ok(())
    }

    /// Gets the index entry, fetching from the metastore if not cached.
    pub async fn get_or_fetch(
        &mut self,
        index_uid: &IndexUid,
        doc_mapping_uid: &DocMappingUid,
    ) -> anyhow::Result<&IndexEntry> {
        match self.indexes.get(index_uid) {
            Some(entry) if entry.doc_mappers.contains_key(doc_mapping_uid) => {}
            _ => self.fetch_index_config(index_uid, doc_mapping_uid).await?,
        }
        Ok(&self.indexes[index_uid])
    }

    pub async fn get_for_split(&mut self, split: &SplitMetadata) -> anyhow::Result<&IndexEntry> {
        self.get_or_fetch(&split.index_uid, &split.doc_mapping_uid)
            .await
    }

    pub fn get(&self, index_uid: &IndexUid) -> Option<&IndexEntry> {
        self.indexes.get(index_uid)
    }
}

#[cfg(test)]
mod tests {
    use quickwit_metastore::{IndexMetadata, IndexMetadataResponseExt};
    use quickwit_proto::metastore::{IndexMetadataResponse, MetastoreError, MockMetastoreService};

    use super::*;

    fn test_index_metadata() -> IndexMetadata {
        IndexMetadata::for_test("test-index", "ram:///test-index")
    }

    fn test_index_metadata_response(index_metadata: &IndexMetadata) -> IndexMetadataResponse {
        IndexMetadataResponse::try_from_index_metadata(index_metadata).unwrap()
    }

    #[tokio::test]
    async fn test_get_or_fetch_loads_and_caches() {
        let index_metadata = test_index_metadata();
        let response = test_index_metadata_response(&index_metadata);
        let index_uid = index_metadata.index_uid.clone();
        let doc_mapping_uid = DocMappingUid::default();

        let mut mock = MockMetastoreService::new();
        mock.expect_index_metadata()
            .times(1)
            .returning(move |_| Ok(response.clone()));

        let mut store = IndexConfigMetastore::new(MetastoreServiceClient::from_mock(mock));

        // First call fetches from metastore.
        let entry = store
            .get_or_fetch(&index_uid, &doc_mapping_uid)
            .await
            .unwrap();
        assert!(!entry.doc_mapping_json().is_empty());
        assert!(!entry.search_settings_json().is_empty());
        assert!(!entry.indexing_settings_json().is_empty());
        assert!(!entry.index_storage_uri().is_empty());

        // Second call hits cache (times(1) would panic otherwise).
        store
            .get_or_fetch(&index_uid, &doc_mapping_uid)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_get_or_fetch_metastore_error() {
        let mut mock = MockMetastoreService::new();
        mock.expect_index_metadata().returning(|_| {
            Err(MetastoreError::Internal {
                message: "test error".to_string(),
                cause: String::new(),
            })
        });

        let mut store = IndexConfigMetastore::new(MetastoreServiceClient::from_mock(mock));
        let result = store
            .get_or_fetch(&IndexUid::for_test("missing", 0), &DocMappingUid::default())
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_returns_none_before_fetch() {
        let mock = MockMetastoreService::new();
        let store = IndexConfigMetastore::new(MetastoreServiceClient::from_mock(mock));
        assert!(store.get(&IndexUid::for_test("test-index", 0)).is_none());
    }

    #[tokio::test]
    async fn test_get_returns_some_after_fetch() {
        let index_metadata = test_index_metadata();
        let response = test_index_metadata_response(&index_metadata);
        let index_uid = index_metadata.index_uid.clone();

        let mut mock = MockMetastoreService::new();
        mock.expect_index_metadata()
            .returning(move |_| Ok(response.clone()));

        let mut store = IndexConfigMetastore::new(MetastoreServiceClient::from_mock(mock));
        store
            .get_or_fetch(&index_uid, &DocMappingUid::default())
            .await
            .unwrap();

        assert!(store.get(&index_uid).is_some());
    }

    #[tokio::test]
    async fn test_get_for_split() {
        let index_metadata = test_index_metadata();
        let response = test_index_metadata_response(&index_metadata);
        let index_uid = index_metadata.index_uid.clone();

        let mut mock = MockMetastoreService::new();
        mock.expect_index_metadata()
            .returning(move |_| Ok(response.clone()));

        let mut store = IndexConfigMetastore::new(MetastoreServiceClient::from_mock(mock));
        let split = SplitMetadata {
            split_id: "split-1".to_string(),
            index_uid: index_uid.clone(),
            ..Default::default()
        };

        let entry = store.get_for_split(&split).await.unwrap();
        assert!(!entry.index_storage_uri().is_empty());
    }

    #[tokio::test]
    async fn test_is_split_mature() {
        let index_metadata = test_index_metadata();
        let response = test_index_metadata_response(&index_metadata);
        let index_uid = index_metadata.index_uid.clone();

        let mut mock = MockMetastoreService::new();
        mock.expect_index_metadata()
            .returning(move |_| Ok(response.clone()));

        let mut store = IndexConfigMetastore::new(MetastoreServiceClient::from_mock(mock));
        let entry = store
            .get_or_fetch(&index_uid, &DocMappingUid::default())
            .await
            .unwrap();

        let small_split = SplitMetadata {
            num_docs: 100,
            num_merge_ops: 0,
            ..Default::default()
        };
        assert!(!entry.is_split_mature(&small_split));

        let large_split = SplitMetadata {
            num_docs: 20_000_000,
            num_merge_ops: 0,
            ..Default::default()
        };
        assert!(entry.is_split_mature(&large_split));
    }

    #[tokio::test]
    async fn test_retention_policy_json_empty_when_none() {
        let index_metadata = test_index_metadata();
        let response = test_index_metadata_response(&index_metadata);
        let index_uid = index_metadata.index_uid.clone();

        let mut mock = MockMetastoreService::new();
        mock.expect_index_metadata()
            .returning(move |_| Ok(response.clone()));

        let mut store = IndexConfigMetastore::new(MetastoreServiceClient::from_mock(mock));
        let entry = store
            .get_or_fetch(&index_uid, &DocMappingUid::default())
            .await
            .unwrap();

        // Default test index has no retention policy.
        assert!(entry.retention_policy_json().is_empty());
    }
}
