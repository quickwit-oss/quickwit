/*
 * Copyright (C) 2021 Quickwit Inc.
 *
 * Quickwit is offered under the AGPL v3.0 and as commercial software.
 * For commercial licensing, contact us at hello@quickwit.io.
 *
 * AGPL:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use byte_unit::Byte;
use quickwit_index_config::IndexConfig;
use quickwit_indexing::actors::IndexerParams;
use quickwit_indexing::index_data;
use quickwit_indexing::models::{CommitPolicy, IndexingStatistics, ScratchDirectory};
use quickwit_indexing::source::{SourceConfig, VecSourceParams};
use quickwit_metastore::checkpoint::Checkpoint;
use quickwit_metastore::{
    IndexMetadata, Metastore, MetastoreUriResolver, SplitMetadata, SplitMetadataAndFooterOffsets,
    SplitState,
};
use quickwit_storage::StorageUriResolver;

/// Creates a Test environment.
///
/// It makes it easy to create a test index, perfect for unit testing.
/// The test index content is entirely in RAM and isolated,
/// but the construction of the index involves temporary file directory.
pub struct TestSandbox {
    index_id: String,
    storage_uri_resolver: StorageUriResolver,
    metastore: Arc<dyn Metastore>,
    add_docs_id: AtomicUsize,
}

impl TestSandbox {
    /// Creates a new test environment.
    pub async fn create(
        index_id: &str,
        index_config: Arc<dyn IndexConfig>,
    ) -> anyhow::Result<Self> {
        let metastore_uri = "ram://quickwit-test-indices";
        let index_metadata = IndexMetadata {
            index_id: index_id.to_string(),
            index_uri: format!("{}/{}", metastore_uri, index_id),
            index_config,
            checkpoint: Checkpoint::default(),
        };
        let storage_uri_resolver = StorageUriResolver::for_test();
        let metastore_uri_resolver = MetastoreUriResolver::default();
        let metastore = metastore_uri_resolver.resolve(metastore_uri).await?;
        metastore.create_index(index_metadata).await?;
        Ok(TestSandbox {
            index_id: index_id.to_string(),
            storage_uri_resolver,
            metastore,
            add_docs_id: AtomicUsize::default(),
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
        let source_config = SourceConfig {
            id: self.index_id.clone(),
            source_type: "vec".to_string(),
            params: serde_json::to_value(VecSourceParams {
                items: docs,
                batch_num_docs: 10,
                partition: format!("add_docs{}", self.add_docs_id.load(Ordering::SeqCst)),
            })?,
        };
        self.add_docs_id.fetch_add(1, Ordering::SeqCst);
        let indexer_params = IndexerParams {
            scratch_directory: ScratchDirectory::try_new_temp()?,
            heap_size: Byte::from_bytes(100_000_000),
            commit_policy: CommitPolicy {
                timeout: Duration::from_secs(3600),
                num_docs_threshold: 5_000_000,
            },
        };
        let statistics = index_data(
            self.index_id.clone(),
            self.metastore.clone(),
            indexer_params,
            source_config,
            self.storage_uri_resolver.clone(),
        )
        .await?;
        Ok(statistics)
    }

    /// Returns the metastore of the TestIndex
    ///
    /// The metastore is a single file metastore.
    /// Its data can be found via the `storage_uri_resolver` in
    /// the `ram://quickwit-test-indices` directory.
    pub fn metastore(&self) -> Arc<dyn Metastore> {
        self.metastore.clone()
    }

    /// Returns the storage uri resolver
    pub fn storage_uri_resolver(&self) -> StorageUriResolver {
        self.storage_uri_resolver.clone()
    }
}

/// Mock split meta helper.
pub fn mock_split_meta(split_id: &str) -> SplitMetadataAndFooterOffsets {
    SplitMetadataAndFooterOffsets {
        footer_offsets: 700..800,
        split_metadata: SplitMetadata {
            split_id: split_id.to_string(),
            split_state: SplitState::Published,
            num_records: 10,
            size_in_bytes: 256,
            time_range: None,
            generation: 1,
            update_timestamp: 0,
            tags: Default::default(),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::TestSandbox;
    use quickwit_index_config::WikipediaIndexConfig;

    #[tokio::test]
    async fn test_test_sandbox() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let index_config = Arc::new(WikipediaIndexConfig::new());
        let test_index_builder = TestSandbox::create("test_index", index_config).await?;
        let statistics = test_index_builder.add_documents(vec![
            serde_json::json!({"title": "Hurricane Fay", "body": "...", "url": "http://hurricane-fay"}),
            serde_json::json!({"title": "Ganimede", "body": "...", "url": "http://ganimede"}),
        ]).await?;
        assert_eq!(statistics.num_uploaded_splits, 1);
        let metastore = test_index_builder.metastore();
        {
            let splits = metastore.list_all_splits("test_index").await?;
            assert_eq!(splits.len(), 1);
            test_index_builder.add_documents(vec![
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
