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

use quickwit_config::{build_doc_mapper, IndexerConfig, SourceConfig, SourceParams, VecSourceParams};
use quickwit_doc_mapper::DocMapper;
use quickwit_metastore::{
    quickwit_metastore_uri_resolver, IndexMetadata, Metastore, Split, SplitMetadata, SplitState,
};
use quickwit_storage::{Storage, StorageUriResolver};

use crate::actors::{IndexingServer, IndexingServerClient};
use crate::models::IndexingStatistics;

/// Creates a Test environment.
///
/// It makes it easy to create a test index, perfect for unit testing.
/// The test index content is entirely in RAM and isolated,
/// but the construction of the index involves temporary file directory.
pub struct TestSandbox {
    index_id: String,
    client: IndexingServerClient,
    doc_mapper: Arc<dyn DocMapper>,
    metastore: Arc<dyn Metastore>,
    storage_resolver: StorageUriResolver,
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
        let doc_mapper = build_doc_mapper(
            &index_meta.doc_mapping,
            &index_meta.search_settings,
            &index_meta.indexing_settings,
        )?;
        let temp_dir = tempfile::tempdir()?;
        let indexer_config = IndexerConfig::for_test()?;
        let metastore_uri_resolver = quickwit_metastore_uri_resolver();
        let metastore = metastore_uri_resolver.resolve(METASTORE_URI).await?;
        metastore.create_index(index_meta.clone()).await?;
        let storage_resolver = StorageUriResolver::for_test();
        let storage = storage_resolver.resolve(&index_uri)?;
        let client = IndexingServer::spawn(
            temp_dir.path().to_path_buf(),
            indexer_config,
            metastore.clone(),
            storage_resolver.clone(),
        );
        Ok(TestSandbox {
            index_id: index_id.to_string(),
            client,
            doc_mapper,
            metastore,
            storage_resolver,
            storage,
            add_docs_id: AtomicUsize::default(),
            _temp_dir: temp_dir,
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
        let add_docs_id = self.add_docs_id.fetch_add(1, Ordering::SeqCst);
        let source = SourceConfig {
            source_id: self.index_id.clone(),
            source_params: SourceParams::Vec(VecSourceParams {
                items: docs,
                batch_num_docs: 10,
                partition: format!("add-docs-{}", add_docs_id),
            }),
        };
        let pipeline_id = self
            .client
            .spawn_pipeline(self.index_id.clone(), source)
            .await?;
        let pipeline_handle = self.client.detach_pipeline(&pipeline_id).await?;
        let (_pipeline_exit_status, pipeline_statistics) = pipeline_handle.join().await;
        Ok(pipeline_statistics)
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
        self.storage_resolver.clone()
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
