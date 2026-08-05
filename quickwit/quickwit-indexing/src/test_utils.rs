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
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use quickwit_actors::{Mailbox, Universe};
use quickwit_cluster::{ChitchatTransport, create_cluster_for_test};
use quickwit_common::pubsub::EventBroker;
use quickwit_common::rand::append_random_suffix;
use quickwit_common::uri::Uri;
use quickwit_config::{
    ConfigFormat, INGEST_API_SOURCE_ID, IndexConfig, IndexerConfig, IngestApiConfig,
    MetastoreConfigs, SourceConfig, SourceInputFormat, SourceParams, VecSourceParams,
    build_doc_mapper,
};
use quickwit_doc_mapper::DocMapper;
use quickwit_ingest::{IngesterPool, QUEUES_DIR_NAME, init_ingest_api};
use quickwit_metastore::{
    CreateIndexRequestExt, MetastoreResolver, Split, SplitMetadata, SplitState,
};
use quickwit_proto::metastore::{CreateIndexRequest, MetastoreService, MetastoreServiceClient};
use quickwit_proto::types::{IndexUid, NodeId, PipelineUid, SourceId};
use quickwit_storage::{Storage, StorageResolver};
use serde_json::Value as JsonValue;

use crate::actors::IndexingService;
use crate::models::{DetachIndexingPipeline, IndexingStatistics, SpawnPipeline};
use crate::split_store::IndexingSplitCache;

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
        let node_id = NodeId::from_str(&append_random_suffix("test-node"));
        let transport = ChitchatTransport::default();
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
            std::slice::from_ref(&source_config),
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
            Some(merge_scheduler_mailbox),
            IngesterPool::default(),
            storage_resolver.clone(),
            EventBroker::default(),
            Arc::new(IndexingSplitCache::no_caching()),
            None,
        )
        .await?;
        let (indexing_service, _indexing_service_handle) =
            universe.spawn_builder().spawn(indexing_service_actor);
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
            num_pipelines: NonZeroUsize::MIN,
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
        split_id: split_id.into(),
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
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use quickwit_common::shared_consts::SPLIT_RECOVERY_METADATA_FILE_NAME;
    use quickwit_common::test_utils::wait_until_predicate;
    use quickwit_config::IndexConfig;
    use quickwit_metastore::{
        CreateIndexRequestExt, FileBackedMetastore, ListSplitsQuery, ListSplitsRequestExt,
        MetastoreServiceStreamSplitsExt, SplitMetadata, SplitState, StageSplitsRequestExt,
    };
    use quickwit_proto::metastore::{
        CreateIndexRequest, ListSplitsRequest, MetastoreService, PublishSplitsRequest,
        SplitRecoveryMetadata, StageSplitsRequest,
    };
    use quickwit_proto::types::SplitId;
    use quickwit_storage::{BundleStorage, RamStorage, Storage};

    use super::TestSandbox;

    async fn recover_and_publish_split(
        test_sandbox: &TestSandbox,
        index_id: &str,
        original_metadata: &SplitMetadata,
    ) -> anyhow::Result<Vec<SplitId>> {
        // Load the split bundle from object storage and read its embedded recovery entry.
        let split_filename = quickwit_common::split_file(original_metadata.split_id());
        let split_path = Path::new(&split_filename);
        let storage = test_sandbox.storage();
        let (_hotcache, split_bundle, footer_offsets) = BundleStorage::open_from_storage(
            storage,
            split_path.to_path_buf(),
        )
        .await?;
        let serialized_recovery_metadata = split_bundle
            .get_all(Path::new(SPLIT_RECOVERY_METADATA_FILE_NAME))
            .await?;
        let recovery_metadata =
            SplitRecoveryMetadata::deserialize(serialized_recovery_metadata.as_ref())?;
        let (mut recovered_metadata, parent_split_ids) =
            SplitMetadata::try_from_recovery_metadata(recovery_metadata, footer_offsets)?;

        let mut expected_metadata = original_metadata.clone();
        expected_metadata.node_id.clear();
        assert_eq!(recovered_metadata, expected_metadata);

        // Simulate a lost metastore by creating the index in a new one. Index creation assigns a
        // new incarnation UID, so the importer explicitly remaps the recovered split to it.
        let fresh_metastore =
            FileBackedMetastore::try_new(Arc::new(RamStorage::default()), None).await?;
        let index_config = IndexConfig::for_test(index_id, "ram:///recovered-index");
        let recovered_index_uid = fresh_metastore
            .create_index(CreateIndexRequest::try_from_index_config(&index_config)?)
            .await?
            .index_uid()
            .clone();
        recovered_metadata.index_uid = recovered_index_uid.clone();

        fresh_metastore
            .stage_splits(StageSplitsRequest::try_from_split_metadata(
                recovered_index_uid.clone(),
                &recovered_metadata,
            )?)
            .await?;
        fresh_metastore
            .publish_splits(PublishSplitsRequest {
                index_uid: Some(recovered_index_uid.clone()),
                staged_split_ids: vec![recovered_metadata.split_id.to_string()],
                ..Default::default()
            })
            .await?;

        let published_splits = fresh_metastore
            .list_splits(ListSplitsRequest::try_from_index_uid(recovered_index_uid)?)
            .await?
            .collect_splits()
            .await?;
        assert_eq!(published_splits.len(), 1);
        assert_eq!(published_splits[0].split_state, SplitState::Published);
        assert_eq!(published_splits[0].split_metadata, recovered_metadata);

        Ok(parent_split_ids)
    }

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

    #[tokio::test]
    async fn test_recover_split_from_bundle_and_publish_it() -> anyhow::Result<()> {
        let index_id = quickwit_common::rand::append_random_suffix("split-recovery");
        let test_sandbox = TestSandbox::create(
            &index_id,
            r#"
                timestamp_field: timestamp
                tag_fields:
                  - tenant
                field_mappings:
                  - name: timestamp
                    type: datetime
                    fast: true
                  - name: body
                    type: text
                  - name: tenant
                    type: text
                    tokenizer: raw
            "#,
            "{}",
            &["body"],
        )
        .await?;
        test_sandbox
            .add_documents([serde_json::json!({
                "timestamp": 1_700_000_000,
                "body": "recover me",
                "tenant": "acme"
            })])
            .await?;

        let original_splits = test_sandbox
            .metastore()
            .list_splits(ListSplitsRequest::try_from_index_uid(
                test_sandbox.index_uid(),
            )?)
            .await?
            .collect_splits()
            .await?;
        assert_eq!(original_splits.len(), 1);
        let original_metadata = &original_splits[0].split_metadata;
        assert_eq!(
            original_metadata
                .tags
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["tenant!", "tenant:acme"]
        );

        let parent_split_ids =
            recover_and_publish_split(&test_sandbox, &index_id, original_metadata).await?;
        assert!(parent_split_ids.is_empty());

        test_sandbox.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_recover_merged_split_from_bundle_and_publish_it() -> anyhow::Result<()> {
        let index_id = quickwit_common::rand::append_random_suffix("merged-split-recovery");
        let test_sandbox = TestSandbox::create(
            &index_id,
            r#"
                timestamp_field: timestamp
                field_mappings:
                  - name: timestamp
                    type: datetime
                    fast: true
                  - name: body
                    type: text
            "#,
            r#"
                split_num_docs_target: 1000
                merge_policy:
                  type: stable_log
                  merge_factor: 2
                  max_merge_factor: 2
            "#,
            &["body"],
        )
        .await?;
        test_sandbox
            .add_documents([serde_json::json!({
                "timestamp": 1_700_000_000,
                "body": "first parent"
            })])
            .await?;
        test_sandbox
            .add_documents([serde_json::json!({
                "timestamp": 1_700_000_001,
                "body": "second parent"
            })])
            .await?;

        wait_until_predicate(
            || {
                let metastore = test_sandbox.metastore();
                let index_uid = test_sandbox.index_uid();
                async move {
                    let query = ListSplitsQuery::for_index(index_uid)
                        .with_split_state(SplitState::Published);
                    let Ok(request) = ListSplitsRequest::try_from_list_splits_query(&query) else {
                        return false;
                    };
                    let Ok(split_stream) = metastore.list_splits(request).await else {
                        return false;
                    };
                    let Ok(splits) = split_stream.collect_splits().await else {
                        return false;
                    };
                    splits.len() == 1 && splits[0].split_metadata.num_merge_ops == 1
                }
            },
            Duration::from_secs(10),
            Duration::from_millis(25),
        )
        .await?;

        let published_query = ListSplitsQuery::for_index(test_sandbox.index_uid())
            .with_split_state(SplitState::Published);
        let published_splits = test_sandbox
            .metastore()
            .list_splits(ListSplitsRequest::try_from_list_splits_query(
                &published_query,
            )?)
            .await?
            .collect_splits()
            .await?;
        assert_eq!(published_splits.len(), 1);
        let merged_metadata = &published_splits[0].split_metadata;
        assert_eq!(merged_metadata.num_docs, 2);
        assert_eq!(merged_metadata.num_merge_ops, 1);

        let replaced_query = ListSplitsQuery::for_index(test_sandbox.index_uid())
            .with_split_state(SplitState::MarkedForDeletion);
        let replaced_splits = test_sandbox
            .metastore()
            .list_splits(ListSplitsRequest::try_from_list_splits_query(
                &replaced_query,
            )?)
            .await?
            .collect_splits()
            .await?;
        assert_eq!(replaced_splits.len(), 2);
        let mut expected_parent_ids: Vec<SplitId> = replaced_splits
            .iter()
            .map(|split| split.split_metadata.split_id.clone())
            .collect();
        expected_parent_ids.sort();

        let mut recovered_parent_ids =
            recover_and_publish_split(&test_sandbox, &index_id, merged_metadata).await?;
        recovered_parent_ids.sort();
        assert_eq!(recovered_parent_ids, expected_parent_ids);

        test_sandbox.assert_quit().await;
        Ok(())
    }
}
