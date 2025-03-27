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

use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use quickwit_actors::{ActorHandle, Mailbox, Universe};
use quickwit_cluster::{create_cluster_for_test, ChannelTransport};
use quickwit_common::pubsub::EventBroker;
use quickwit_common::rand::append_random_suffix;
use quickwit_common::uri::Uri;
use quickwit_config::{
    build_doc_mapper, ConfigFormat, IndexConfig, IndexerConfig, IngestApiConfig, MetastoreConfigs,
    SourceConfig, SourceInputFormat, SourceParams, VecSourceParams, INGEST_API_SOURCE_ID,
};
use quickwit_doc_mapper::DocMapper;
use quickwit_ingest::{init_ingest_api, IngesterPool, QUEUES_DIR_NAME};
use quickwit_metastore::{
    CreateIndexRequestExt, MetastoreResolver, Split, SplitMetadata, SplitState,
};
use quickwit_proto::indexing::IndexingPipelineId;
use quickwit_proto::metastore::{CreateIndexRequest, MetastoreService, MetastoreServiceClient};
use quickwit_proto::types::{IndexUid, NodeId, PipelineUid, SourceId};
use quickwit_storage::{Storage, StorageResolver};
use serde_json::Value as JsonValue;

use crate::actors::{IndexingPipeline, IndexingService, MergePipeline};
use crate::models::{
    DetachIndexingPipeline, DetachMergePipeline, IndexingStatistics, SpawnPipeline,
};

/// Creates a Test environment.
///
/// It makes it easy to create a test index, perfect for unit testing.
/// The test index content is entirely in RAM and isolated,
/// but the construction of the index involves temporary file directory.
pub struct TestSandbox {
    node_id: NodeId,
    index_uid: IndexUid,
    source_id: SourceId,
    indexing_service: Mailbox<IndexingService>,
    doc_mapper: Arc<DocMapper>,
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
    storage: Arc<dyn Storage>,
    add_docs_id: AtomicUsize,
    universe: Universe,
    indexing_pipeline_id: Option<IndexingPipelineId>,
    _temp_dir: tempfile::TempDir,
}

const METASTORE_URI: &str = "ram://quickwit-test-indexes";

fn index_uri(index_id: &str) -> Uri {
    Uri::from_str(&format!("{METASTORE_URI}/{index_id}")).unwrap()
}

impl TestSandbox {
    /// Creates a new test environment.
    pub async fn create(
        index_id: &str,
        doc_mapping_yaml: &str,
        indexing_settings_yaml: &str,
        search_fields: &[&str],
    ) -> anyhow::Result<TestSandbox> {
        let node_id = NodeId::new(append_random_suffix("test-node"));
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        let index_uri = index_uri(index_id);
        let mut index_config = IndexConfig::for_test(index_id, index_uri.as_str());
        index_config.doc_mapping = ConfigFormat::Yaml.parse(doc_mapping_yaml.as_bytes())?;
        index_config.indexing_settings =
            ConfigFormat::Yaml.parse(indexing_settings_yaml.as_bytes())?;
        index_config.search_settings.default_search_fields = search_fields
            .iter()
            .map(|search_field| search_field.to_string())
            .collect();
        let source_config = SourceConfig::ingest_api_default();
        let storage_resolver = StorageResolver::for_test();
        let metastore_resolver =
            MetastoreResolver::configured(storage_resolver.clone(), &MetastoreConfigs::default());
        let metastore = metastore_resolver
            .resolve(&Uri::for_test(METASTORE_URI))
            .await?;
        let create_index_request = CreateIndexRequest::try_from_index_and_source_configs(
            &index_config,
            &[source_config.clone()],
        )?;
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await?
            .index_uid()
            .clone();
        let doc_mapper =
            build_doc_mapper(&index_config.doc_mapping, &index_config.search_settings)?;
        let temp_dir = tempfile::tempdir()?;
        let indexer_config = IndexerConfig::for_test()?;
        let num_blocking_threads = 1;
        let storage = storage_resolver.resolve(&index_uri).await?;
        let universe = Universe::with_accelerated_time();
        let merge_scheduler_mailbox = universe.get_or_spawn_one();
        let queues_dir_path = temp_dir.path().join(QUEUES_DIR_NAME);
        let ingest_api_service =
            init_ingest_api(&universe, &queues_dir_path, &IngestApiConfig::default()).await?;
        let indexing_service_actor = IndexingService::new(
            node_id.clone(),
            temp_dir.path().to_path_buf(),
            indexer_config,
            num_blocking_threads,
            cluster,
            metastore.clone(),
            Some(ingest_api_service),
            merge_scheduler_mailbox,
            IngesterPool::default(),
            storage_resolver.clone(),
            EventBroker::default(),
        )
        .await?;
        let (indexing_service, _indexing_service_handle) =
            universe.spawn_builder().spawn(indexing_service_actor);

        let indexing_pipeline_id = indexing_service
            .ask_for_res(SpawnPipeline {
                index_id: index_uid.index_id.to_string(),
                source_config,
                pipeline_uid: PipelineUid::for_test(1u128),
            })
            .await?;
        Ok(TestSandbox {
            node_id,
            index_uid,
            source_id: INGEST_API_SOURCE_ID.to_string(),
            indexing_service,
            doc_mapper,
            metastore,
            storage_resolver,
            storage,
            add_docs_id: AtomicUsize::default(),
            universe,
            indexing_pipeline_id: Some(indexing_pipeline_id),
            _temp_dir: temp_dir,
        })
    }

    /// Adds documents and waits for them to be indexed (creating a separate split).
    ///
    /// The documents are expected to be `JsonValue`.
    /// They can be created using the `serde_json::json!` macro.
    pub async fn add_documents<I>(&self, json_docs: I) -> anyhow::Result<IndexingStatistics>
    where
        I: IntoIterator<Item = JsonValue> + 'static,
        I::IntoIter: Send,
    {
        let docs: Vec<Bytes> = json_docs
            .into_iter()
            .map(|json_doc| Bytes::from(json_doc.to_string()))
            .collect();
        let add_docs_id = self.add_docs_id.fetch_add(1, Ordering::SeqCst);
        let source_config = SourceConfig {
            source_id: INGEST_API_SOURCE_ID.to_string(),
            num_pipelines: NonZeroUsize::new(1).unwrap(),
            enabled: true,
            source_params: SourceParams::Vec(VecSourceParams {
                docs,
                batch_num_docs: 10,
                partition: format!("add-docs-{add_docs_id}"),
            }),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let pipeline_id = self
            .indexing_service
            .ask_for_res(SpawnPipeline {
                index_id: self.index_uid.index_id.to_string(),
                source_config,
                pipeline_uid: PipelineUid::for_test(0u128),
            })
            .await?;
        let pipeline_handle = self
            .indexing_service
            .ask_for_res(DetachIndexingPipeline {
                pipeline_id: pipeline_id.clone(),
            })
            .await?;
        let (_pipeline_exit_status, pipeline_statistics) = pipeline_handle.join().await;
        Ok(pipeline_statistics)
    }

    /// Adds documents and waits for them to be indexed (creating a separate split).
    ///
    /// The documents are expected to be `JsonValue`.
    /// They can be created using the `serde_json::json!` macro.
    pub async fn add_documents_through_api<I>(&self, json_docs: I) -> anyhow::Result<()>
    where
        I: IntoIterator<Item = JsonValue> + 'static,
        I::IntoIter: Send,
    {
        let ingest_api_service_mailbox = self
            .universe
            .get_one::<quickwit_ingest::IngestApiService>()
            .unwrap();

        let batch_builder =
            quickwit_ingest::DocBatchBuilder::new(self.index_uid.index_id.to_string());
        let mut json_writer = batch_builder.json_writer();
        for doc in json_docs {
            json_writer.ingest_doc(doc)?;
        }
        let batch = json_writer.build();
        let ingest_request = quickwit_ingest::IngestRequest {
            doc_batches: vec![batch],
            commit: quickwit_ingest::CommitType::WaitFor as i32,
        };
        ingest_api_service_mailbox
            .ask_for_res(ingest_request)
            .await?;
        Ok(())
    }

    pub async fn take_indexing_and_merge_pipeline(
        &mut self,
    ) -> anyhow::Result<(ActorHandle<IndexingPipeline>, ActorHandle<MergePipeline>)> {
        let pipeline_id = self.indexing_pipeline_id.take().unwrap();
        let merge_pipeline_id = pipeline_id.merge_pipeline_id();
        let indexing_pipeline = self
            .indexing_service
            .ask_for_res(DetachIndexingPipeline { pipeline_id })
            .await?;
        let merge_pipeline = self
            .indexing_service
            .ask_for_res(DetachMergePipeline {
                pipeline_id: merge_pipeline_id,
            })
            .await?;

        Ok((indexing_pipeline, merge_pipeline))
    }

    /// Returns the metastore of the TestSandbox.
    ///
    /// The metastore is a file-backed metastore.
    /// Its data can be found via the `storage` in
    /// the `ram://quickwit-test-indexes` directory.
    pub fn metastore(&self) -> MetastoreServiceClient {
        self.metastore.clone()
    }

    /// Returns the storage of the TestSandbox.
    pub fn storage(&self) -> Arc<dyn Storage> {
        self.storage.clone()
    }

    /// Returns the storage resolver of the TestSandbox.
    pub fn storage_resolver(&self) -> StorageResolver {
        self.storage_resolver.clone()
    }

    /// Returns the doc mapper of the TestSandbox.
    pub fn doc_mapper(&self) -> Arc<DocMapper> {
        self.doc_mapper.clone()
    }

    /// Returns the node ID.
    pub fn node_id(&self) -> NodeId {
        self.node_id.clone()
    }

    /// Returns the index UID.
    pub fn index_uid(&self) -> IndexUid {
        self.index_uid.clone()
    }

    /// Returns the source ID.
    pub fn source_id(&self) -> SourceId {
        self.source_id.clone()
    }

    /// Returns the underlying universe.
    pub fn universe(&self) -> &Universe {
        &self.universe
    }

    /// Returns a Mailbox for the indexing service
    pub fn indexing_service(&self) -> Mailbox<IndexingService> {
        self.indexing_service.clone()
    }

    /// Gracefully quits all registered actors in the underlying universe and asserts that none of
    /// them panicked.
    ///
    /// This is useful for testing purposes to detect failed asserts in actors
    #[cfg(any(test, feature = "testsuite"))]
    pub async fn assert_quit(self) {
        self.universe.assert_quit().await
    }
}

/// Mock split builder.
pub struct MockSplitBuilder {
    split_metadata: SplitMetadata,
}

impl MockSplitBuilder {
    pub fn new(split_id: &str) -> Self {
        Self {
            split_metadata: mock_split_meta(split_id, &IndexUid::for_test("test-index", 0)),
        }
    }

    pub fn with_index_uid(mut self, index_uid: &IndexUid) -> Self {
        self.split_metadata.index_uid = index_uid.clone();
        self
    }

    pub fn build(self) -> Split {
        Split {
            split_state: SplitState::Published,
            split_metadata: self.split_metadata,
            update_timestamp: 0,
            publish_timestamp: None,
        }
    }
}

/// Mock split helper.
pub fn mock_split(split_id: &str) -> Split {
    MockSplitBuilder::new(split_id).build()
}

/// Mock split meta helper.
pub fn mock_split_meta(split_id: &str, index_uid: &IndexUid) -> SplitMetadata {
    SplitMetadata {
        index_uid: index_uid.clone(),
        split_id: split_id.to_string(),
        partition_id: 13u64,
        num_docs: if split_id == "split1" { 1_000_000 } else { 10 },
        uncompressed_docs_size_in_bytes: 256,
        time_range: Some(121000..=130198),
        create_timestamp: 0,
        footer_offsets: 700..800,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use quickwit_metastore::{ListSplitsRequestExt, MetastoreServiceStreamSplitsExt};
    use quickwit_proto::metastore::{ListSplitsRequest, MetastoreService};

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
            let splits = metastore
                .list_splits(
                    ListSplitsRequest::try_from_index_uid(test_sandbox.index_uid()).unwrap(),
                )
                .await?
                .collect_splits()
                .await?;
            assert_eq!(splits.len(), 1);
            test_sandbox.add_documents(vec![
            serde_json::json!({"title": "Byzantine-Ottoman wars", "body": "...", "url": "http://biz-ottoman"}),
        ]).await?;
        }
        {
            let splits = metastore
                .list_splits(
                    ListSplitsRequest::try_from_index_uid(test_sandbox.index_uid()).unwrap(),
                )
                .await?
                .collect_splits()
                .await?;
            assert_eq!(splits.len(), 2);
        }
        test_sandbox.assert_quit().await;
        Ok(())
    }
}
