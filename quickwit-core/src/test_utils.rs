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
use std::sync::Arc;

use quickwit_index_config::IndexConfig;
use quickwit_metastore::checkpoint::Checkpoint;
use quickwit_metastore::{IndexMetadata, Metastore, MetastoreUriResolver};
use quickwit_storage::StorageUriResolver;
use tempfile::tempdir;

use crate::index_data;
use crate::indexing::test_document_source;
use crate::IndexDataParams;
use crate::IndexingStatistics;

/// Creates a Test environment.
///
/// It makes it easy to create a test index, perfect for unit testing.
/// The test index content is entirely in RAM and isolated,
/// but the construction of the index involves temporary file directory.
pub struct TestSandbox {
    index_id: String,
    storage_uri_resolver: StorageUriResolver,
    metastore: Arc<dyn Metastore>,
}

impl TestSandbox {
    /// Creates a new test environment.
    pub async fn create(
        index_id: &str,
        index_config: Box<dyn IndexConfig>,
    ) -> anyhow::Result<Self> {
        let metastore_uri = "ram://quickwit-test-indices";
        let index_metadata = IndexMetadata {
            index_id: index_id.to_string(),
            index_uri: format!("{}/{}", metastore_uri, index_id),
            index_config,
            checkpoint: Checkpoint::default(),
        };
        let storage_uri_resolver = StorageUriResolver::default();
        let metastore = MetastoreUriResolver::with_storage_resolver(storage_uri_resolver.clone())
            .resolve(metastore_uri)
            .await?;
        metastore.create_index(index_metadata).await?;
        Ok(TestSandbox {
            index_id: index_id.to_string(),
            storage_uri_resolver,
            metastore,
        })
    }

    /// Adds documents.
    ///
    /// The documents are expected to be `serde_json::Value`.
    /// They can be created using the `serde_json::json!` macro.
    pub async fn add_documents<I>(&self, splits: I) -> anyhow::Result<Arc<IndexingStatistics>>
    where
        I: IntoIterator<Item = serde_json::Value> + 'static,
        I::IntoIter: Send,
    {
        let document_source = test_document_source(splits);
        let index_data_params = IndexDataParams {
            index_id: self.index_id.clone(),
            temp_dir: Arc::new(tempdir()?),
            num_threads: 1,
            heap_size: 100_000_000,
            overwrite: false,
        };
        let statistics = Arc::new(IndexingStatistics::default());
        index_data(
            self.metastore.clone(),
            index_data_params,
            document_source,
            self.storage_uri_resolver.clone(),
            statistics.clone(),
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

#[cfg(test)]
mod tests {
    use super::TestSandbox;
    use quickwit_index_config::WikipediaIndexConfig;

    #[tokio::test]
    async fn test_test_sandbox() -> anyhow::Result<()> {
        let index_config = Box::new(WikipediaIndexConfig::new());
        let test_index_builder = TestSandbox::create("test_index", index_config).await?;
        let statistics = test_index_builder.add_documents(vec![
            serde_json::json!({"title": "Hurricane Fay", "body": "...", "url": "http://hurricane-fay"}),
            serde_json::json!({"title": "Ganimede", "body": "...", "url": "http://ganimede"}),
        ]).await?;
        assert_eq!(statistics.num_uploaded_splits.get(), 1);
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
