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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use quickwit_actors::Universe;
use quickwit_cluster::{ChitchatTransport, create_cluster_for_test};
use quickwit_common::io::IoControls;
use quickwit_common::rand::append_random_suffix;
use quickwit_config::{SourceInputFormat, VecSourceParams};
use quickwit_metastore::{
    CreateIndexRequestExt, ListSplitsRequestExt, MetastoreServiceStreamSplitsExt,
    metastore_for_test,
};
use quickwit_proto::metastore::CreateIndexRequest;
use tantivy::schema::Value;
use tantivy::{DocAddress, Index, TantivyDocument};

use super::*;
use crate::DocEnricher;

#[tokio::test]
async fn test_doc_enricher_factory_selects_pipelines_and_propagates_errors() -> anyhow::Result<()> {
    let transport = ChitchatTransport::default();
    let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true).await?;
    let metastore = metastore_for_test();
    let index_id = append_random_suffix("enrichment");
    let index_config = IndexConfig::for_test(&index_id, &format!("ram:///indexes/{index_id}"));
    let sources = ["selected", "untouched", "invalid"].map(|source_id| SourceConfig {
        source_id: source_id.to_string(),
        num_pipelines: NonZeroUsize::MIN,
        enabled: true,
        source_params: SourceParams::Vec(VecSourceParams {
            docs: vec![bytes::Bytes::from_static(br#"{"timestamp":1600000000}"#)],
            batch_num_docs: 1,
            partition: "0".to_string(),
        }),
        transform_config: None,
        input_format: SourceInputFormat::Json,
    });
    let create_request =
        CreateIndexRequest::try_from_index_and_source_configs(&index_config, &sources)?;
    let index_uid = metastore
        .create_index(create_request)
        .await?
        .index_uid()
        .clone();
    let universe = Universe::new();
    let data_dir = tempfile::tempdir()?;
    let storage_resolver = StorageResolver::for_test();
    let storage = storage_resolver.resolve(&index_config.index_uri).await?;
    let callback_calls = Arc::new(AtomicUsize::new(0));
    let factory_calls = Arc::new(AtomicUsize::new(0));
    let factory: DocEnricherFactory = {
        let callback_calls = callback_calls.clone();
        let factory_calls = factory_calls.clone();
        let expected_index_uid = index_uid.clone();
        Arc::new(move |pipeline_id, mapper| {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(pipeline_id.index_uid, expected_index_uid);
            anyhow::ensure!(pipeline_id.source_id != "invalid", "incompatible mapping");
            if pipeline_id.source_id != "selected" {
                return Ok(None);
            }
            let body = mapper.schema().get_field("body")?;
            let expected_pipeline_id = pipeline_id.clone();
            let expected_mapping_uid = mapper.doc_mapping_uid();
            let callback_calls = callback_calls.clone();
            let enricher: DocEnricher = Arc::new(move |context, doc| {
                assert_eq!(context.pipeline_id, &expected_pipeline_id);
                assert_eq!(context.doc_mapping_uid, expected_mapping_uid);
                anyhow::ensure!(doc.get_first(body).is_none(), "body field collision");
                doc.add_text(body, context.split_id.as_str());
                callback_calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            });
            Ok(Some(enricher))
        })
    };
    let service = IndexingService::new(
        NodeId::from_str("test-node"),
        data_dir.path().to_path_buf(),
        IndexerConfig::for_test()?,
        1,
        cluster,
        metastore.clone(),
        None,
        None,
        IngesterPool::default(),
        storage_resolver,
        EventBroker::default(),
        Arc::new(IndexingSplitCache::no_caching()),
        None,
    )
    .await?
    .with_doc_enricher_factory(factory);
    let (mailbox, _) = universe.spawn_builder().spawn(service);
    for source in &sources[..2] {
        let pipeline_id = mailbox
            .ask_for_res(SpawnPipeline {
                index_id: index_id.clone(),
                source_config: source.clone(),
                pipeline_uid: PipelineUid::default(),
            })
            .await?;
        // Detaching gives an explicit completion barrier rather than polling the service.
        let handle = mailbox
            .ask_for_res(DetachIndexingPipeline { pipeline_id })
            .await?;
        let (status, statistics) =
            tokio::time::timeout(Duration::from_secs(30), handle.join()).await?;
        assert!(status.is_success(), "{status:?}");
        assert_eq!(statistics.num_published_splits, 1);
        assert_eq!(statistics.num_docs, 1);
    }
    let error = mailbox
        .ask_for_res(SpawnPipeline {
            index_id,
            source_config: sources[2].clone(),
            pipeline_uid: PipelineUid::default(),
        })
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("incompatible mapping"));
    assert_eq!(factory_calls.load(Ordering::SeqCst), 3);
    assert_eq!(callback_calls.load(Ordering::SeqCst), 1);

    let query = ListSplitsQuery::for_index(index_uid).with_split_state(SplitState::Published);
    let splits = metastore
        .list_splits(ListSplitsRequest::try_from_list_splits_query(&query)?)
        .await?
        .collect_splits()
        .await?;
    assert_eq!(splits.len(), 2);
    let split_store = IndexingSplitStore::create_without_local_store_for_test(storage);
    let download_dir = tempfile::tempdir()?;
    for split in splits {
        let metadata = split.split_metadata;
        let directory = split_store
            .fetch_and_open_split(
                metadata.split_id.clone(),
                download_dir.path(),
                &IoControls::default(),
            )
            .await?;
        let index = Index::open(directory)?;
        let body = index.schema().get_field("body")?;
        let searcher = index.reader()?.searcher();
        let doc: TantivyDocument = searcher.doc(DocAddress::new(0, 0))?;
        match metadata.source_id.as_str() {
            "selected" => assert_eq!(
                doc.get_first(body).unwrap().as_str(),
                Some(metadata.split_id.as_str())
            ),
            "untouched" => assert!(doc.get_first(body).is_none()),
            unexpected => panic!("unexpected source: {unexpected}"),
        }
    }
    universe.assert_quit().await;
    Ok(())
}
