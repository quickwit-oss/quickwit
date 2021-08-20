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

use anyhow::bail;
use byte_unit::Byte;
use quickwit_actors::{ActorExitStatus, Universe};
use quickwit_index_config::IndexConfig;
use quickwit_indexing::actors::IndexerParams;
use quickwit_indexing::models::{CommitPolicy, IndexingStatistics, ScratchDirectory};
use quickwit_indexing::source::SourceConfig;
use quickwit_indexing::IndexingPipelineParams;
use quickwit_metastore::checkpoint::Checkpoint;
use quickwit_metastore::{IndexMetadata, Metastore, MetastoreUriResolver};
use quickwit_storage::StorageUriResolver;
use serde_json::json;
use tempfile::tempdir;

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
        index_config: Arc<dyn IndexConfig>,
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
    pub async fn add_documents<I>(&self, documents: I) -> anyhow::Result<IndexingStatistics>
    where
        I: IntoIterator<Item = serde_json::Value> + 'static,
        I::IntoIter: Send,
    {
        let indexer_params = IndexerParams {
            scratch_directory: ScratchDirectory::try_new_temp()?,
            heap_size: Byte::from_bytes(100_000_000),
            commit_policy: CommitPolicy::default(),
        };
        let source_config = SourceConfig {
            id: "test-source".to_string(),
            source_type: "vec".to_string(),
            params: json!({
                "items": documents.into_iter().map(|doc_json| doc_json.to_string()).collect::<Vec<String>>(),
                "batch_num_docs": 1_000
            }),
        };
        let pipeline_params = IndexingPipelineParams {
            index_id: self.index_id.clone(),
            source_config,
            indexer_params,
            metastore: self.metastore.clone(),
            storage_uri_resolver: self.storage_uri_resolver(),
        };
        let indexing_pipeline = quickwit_indexing::IndexingPipelineSupervisor::new(pipeline_params);
        let universe = Universe::new();
        let (_pipeline_mailbox, pipeline_handler) = universe.spawn_async_actor(indexing_pipeline);
        let (exit_status, obs) = pipeline_handler.join().await;
        if !matches!(exit_status, ActorExitStatus::Success) {
            bail!(exit_status);
        }
        Ok(obs)
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
    use std::sync::Arc;

    use super::TestSandbox;
    use quickwit_index_config::WikipediaIndexConfig;

    #[tokio::test]
    async fn test_test_sandbox() -> anyhow::Result<()> {
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
