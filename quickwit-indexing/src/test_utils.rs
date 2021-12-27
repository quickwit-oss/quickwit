// Copyright (C) 2021 Quickwit, Inc.
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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use quickwit_config::{IndexerConfig, SourceConfig};
use quickwit_index_config::IndexConfig as DocMapper;
use quickwit_metastore::{
    quickwit_metastore_uri_resolver, IndexMetadata, Metastore, Split, SplitMetadata, SplitState,
};
use quickwit_storage::{Storage, StorageUriResolver};

use crate::index_data;
use crate::models::IndexingStatistics;
use crate::source::VecSourceParams;

/// Creates a Test environment.
///
/// It makes it easy to create a test index, perfect for unit testing.
/// The test index content is entirely in RAM and isolated,
/// but the construction of the index involves temporary file directory.
pub struct TestSandbox {
    index_meta: IndexMetadata,
    doc_mapper: Arc<dyn DocMapper>,
    indexer_config: IndexerConfig,
    metastore: Arc<dyn Metastore>,
    storage_uri_resolver: StorageUriResolver,
    storage: Arc<dyn Storage>,
    add_docs_id: AtomicUsize,
    _temp_dir: tempfile::TempDir,
}

const METASTORE_URI: &str = "ram://quickwit-test-indexes";

fn index_uri(index_id: &str) -> String {
    format!("{}/{}", METASTORE_URI, index_id)
}

impl TestSandbox {
    /// Creates a new test environment.
    pub async fn create(
        index_id: &str,
        doc_mapping_yaml: &str,
        indexing_settings_yaml: &str,
        search_fields: &[&str],
    ) -> anyhow::Result<Self> {
        let index_uri = index_uri(index_id);
        let mut index_meta = IndexMetadata::for_test(index_id, &index_uri);
        index_meta.doc_mapping = serde_yaml::from_str(doc_mapping_yaml)?;
        index_meta.indexing_settings = serde_yaml::from_str(indexing_settings_yaml)?;
        index_meta.search_settings.default_search_fields = search_fields
            .iter()
            .map(|search_field| search_field.to_string())
            .collect();
        let doc_mapper = index_meta.build_doc_mapper()?;
        let (indexer_config, _temp_dir) = IndexerConfig::for_test()?;
        let metastore_uri_resolver = quickwit_metastore_uri_resolver();
        let metastore = metastore_uri_resolver.resolve(METASTORE_URI).await?;
        metastore.create_index(index_meta.clone()).await?;
        let storage_uri_resolver = StorageUriResolver::for_test();
        let storage = storage_uri_resolver.resolve(&index_uri)?;
        Ok(TestSandbox {
            index_meta,
            doc_mapper,
            indexer_config,
            metastore,
            storage_uri_resolver,
            storage,
            add_docs_id: AtomicUsize::default(),
            _temp_dir,
        })
    }

    /// Adds documents.
    ///
    /// The documents are expected to be `serde_json::Value`.
    /// They can be created using the `serde_json::json!` macro.
    pub async fn add_documents<I>(&self, split_docs: I) -> anyhow::Result<IndexingStatistics>
    where
        I: IntoIterator<Item = serde_json::Value> + 'static,
        I::IntoIter: Send,
    {
        let docs: Vec<String> = split_docs
            .into_iter()
            .map(|doc_json| doc_json.to_string())
            .collect();
        let source = SourceConfig {
            source_id: self.index_meta.index_id.clone(),
            source_type: "vec".to_string(),
            params: serde_json::to_value(VecSourceParams {
                items: docs,
                batch_num_docs: 10,
                partition: format!("add-docs-{}", self.add_docs_id.load(Ordering::SeqCst)),
            })?,
        };
        let mut index_meta = self.index_meta.clone();
        index_meta.sources.clear();
        index_meta.sources.insert(source.source_id.clone(), source);

        self.add_docs_id.fetch_add(1, Ordering::SeqCst);
        let statistics = index_data(
            self._temp_dir.path(),
            index_meta,
            self.indexer_config.clone(),
            self.metastore.clone(),
            self.storage.clone(),
        )
        .await?;
        Ok(statistics)
    }

    /// Returns the metastore of the TestSandbox.
    ///
    /// The metastore is a file-backed metastore.
    /// Its data can be found via the `storage` in
    /// the `ram://quickwit-test-indexes` directory.
    pub fn metastore(&self) -> Arc<dyn Metastore> {
        self.metastore.clone()
    }

    /// Returns the storage of the TestSandbox.
    pub fn storage(&self) -> Arc<dyn Storage> {
        self.storage.clone()
    }

    /// Returns the storage URI resolver of the TestSandbox.
    pub fn storage_uri_resolver(&self) -> StorageUriResolver {
        self.storage_uri_resolver.clone()
    }

    /// Returns the doc mapper of the TestSandbox.
    pub fn doc_mapper(&self) -> Arc<dyn DocMapper> {
        self.doc_mapper.clone()
    }
}

/// Mock split helper.
pub fn mock_split(split_id: &str) -> Split {
    Split {
        split_state: SplitState::Published,
        split_metadata: mock_split_meta(split_id),
        update_timestamp: 0,
    }
}

/// Mock split meta helper.
pub fn mock_split_meta(split_id: &str) -> SplitMetadata {
    SplitMetadata {
        split_id: split_id.to_string(),
        num_docs: 10,
        original_size_in_bytes: 256,
        time_range: None,
        create_timestamp: 0,
        tags: Default::default(),
        demux_num_ops: 0,
        footer_offsets: 700..800,
    }
}

#[cfg(test)]
mod tests {
    use super::TestSandbox;

    #[tokio::test]
    async fn test_test_sandbox() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let doc_mapping_yaml = r#"
            field_mappings:
              - name: title
                type: text
              - name: body
                type: text
              - name: url
                type: text
        "#;
        let test_sandbox =
            TestSandbox::create("test_index", doc_mapping_yaml, "{}", &["body"]).await?;
        let statistics = test_sandbox.add_documents(vec![
            serde_json::json!({"title": "Hurricane Fay", "body": "...", "url": "http://hurricane-fay"}),
            serde_json::json!({"title": "Ganimede", "body": "...", "url": "http://ganimede"}),
        ]).await?;
        assert_eq!(statistics.num_uploaded_splits, 1);
        let metastore = test_sandbox.metastore();
        {
            let splits = metastore.list_all_splits("test_index").await?;
            assert_eq!(splits.len(), 1);
            test_sandbox.add_documents(vec![
            serde_json::json!({"title": "Byzantine-Ottoman wars", "body": "...", "url": "http://biz-ottoman"}),
        ]).await?;
        }
        {
            let splits = metastore.list_all_splits("test_index").await?;
            assert_eq!(splits.len(), 2);
        }
        Ok(())
    }
}
