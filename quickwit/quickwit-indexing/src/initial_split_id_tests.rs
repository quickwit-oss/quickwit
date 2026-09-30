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

use std::collections::BTreeMap;
use std::sync::Arc;

use bytesize::ByteSize;
use quickwit_actors::{ActorExitStatus, Universe};
use quickwit_common::io::IoControls;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_config::{DocsClusteringConfig, IndexingSettings};
use quickwit_doc_mapper::{DocMapper, DocMapperBuilder};
use quickwit_metastore::checkpoint::SourceCheckpointDelta;
use quickwit_metastore::{ListSplitsRequestExt, MetastoreServiceStreamSplitsExt};
use quickwit_proto::indexing::{IndexingPipelineId, MergePipelineId};
use quickwit_proto::metastore::{
    DeleteQuery, DeleteTask, LastDeleteOpstampResponse, ListDeleteTasksResponse, ListSplitsRequest,
    MetastoreService, MetastoreServiceClient, MockMetastoreService,
};
use quickwit_proto::types::{IndexUid, NodeId, PipelineUid};
use quickwit_query::query_ast::query_ast_from_user_text;
use serde_json::{Value as JsonValue, json};
use tantivy::collector::Count;
use tantivy::{DocAddress, Document, Index, TantivyDocument};

use crate::actors::{Indexer, MergeExecutor};
use crate::docs_clustering::{Fingerprint, Fingerprinter};
use crate::merge_policy::{MergeOperation, MergePolicy, MergeSource, MergeTask, NopMergePolicy};
use crate::models::{
    IndexedSplit, IndexedSplitBatch, IndexedSplitBatchBuilder, MergeScratch, ProcessedDoc,
    ProcessedDocBatch, create_split_metadata,
};

fn doc_mapper() -> Arc<DocMapper> {
    Arc::new(
        serde_json::from_value(json!({
            "mode": "strict", "store_source": true, "index_field_presence": false,
            "max_num_partitions": 2,
            "field_mappings": [
                {"name": "seq", "type": "u64", "fast": true},
                {"name": "origin", "type": "initial_split_id"},
                {"name": "metadata", "type": "object", "field_mappings": [
                    {"name": "origin", "type": "initial_split_id", "stored": false}
                ]}
            ]
        }))
        .unwrap(),
    )
}

fn query_count(index: &Index, mapper: &DocMapper, query: &str) -> anyhow::Result<usize> {
    let ast = query_ast_from_user_text(query, None).parse_user_query(&[])?;
    let (query, _) = mapper.query(index.schema(), ast, true, None)?;
    Ok(index.reader()?.searcher().search(&query, &Count)?)
}

/// Verifies stored/fast agreement, single values, and the unchanged source after any remapping.
fn identities(split: &IndexedSplit, mapper: &DocMapper) -> anyhow::Result<BTreeMap<u64, String>> {
    let schema = split.index.schema();
    let reader = split.index.reader()?;
    let searcher = reader.searcher();
    let mut result = BTreeMap::new();
    for (segment_ord, segment) in searcher.segment_readers().iter().enumerate() {
        let sequence = segment.fast_fields().u64("seq")?;
        for doc_id in 0..segment.max_doc() {
            assert!(!segment.is_deleted(doc_id));
            let stored: TantivyDocument =
                searcher.doc(DocAddress::new(segment_ord as u32, doc_id))?;
            assert_eq!(stored.get_all(schema.get_field("origin")?).count(), 1);
            assert!(
                stored
                    .get_first(schema.get_field("metadata.origin")?)
                    .is_none()
            );
            let output = JsonValue::Object(mapper.doc_to_json(stored.to_named_doc(&schema).0)?);
            let seq = sequence.first(doc_id).unwrap();
            let origin = output["origin"].as_str().unwrap();
            assert_eq!(
                output,
                json!({"seq": seq, "origin": origin, "_source": {"seq": seq}})
            );
            for name in ["origin", "metadata.origin"] {
                let column = segment.fast_fields().str(name)?.unwrap();
                let ords: Vec<_> = column.term_ords(doc_id).collect();
                assert_eq!(ords.len(), 1);
                let mut value = String::new();
                assert!(column.ord_to_str(ords[0], &mut value)?);
                assert_eq!(value, origin);
            }
            assert!(result.insert(seq, origin.to_string()).is_none());
        }
    }
    assert_eq!(result.len() as u64, split.split_attrs.num_docs);
    Ok(result)
}

async fn merge_splits(
    universe: &Universe,
    mapper: Arc<DocMapper>,
    inputs: &[IndexedSplit],
    delete_query: Option<String>,
) -> anyhow::Result<IndexedSplit> {
    let attrs = &inputs[0].split_attrs;
    let pipeline_id = MergePipelineId {
        index_uid: attrs.index_uid.clone(),
        source_id: attrs.source_id.clone(),
        node_id: attrs.node_id.clone(),
    };
    let policy: Arc<dyn MergePolicy> = Arc::new(NopMergePolicy);
    let mut metadata = inputs
        .iter()
        .map(|split| {
            create_split_metadata(&policy, None, &split.split_attrs, Default::default(), 0..0)
        })
        .collect::<Vec<_>>();
    let mut metastore = MockMetastoreService::new();
    let operation = if let Some(query) = delete_query {
        assert_eq!(inputs.len(), 1);
        let index_uid = attrs.index_uid.clone();
        let previous_opstamp = attrs.delete_opstamp;
        metastore
            .expect_list_delete_tasks()
            .once()
            .return_once(move |request| {
                assert_eq!(request.index_uid, Some(index_uid.clone()));
                assert_eq!(request.opstamp_start, previous_opstamp);
                Ok(ListDeleteTasksResponse {
                    delete_tasks: vec![DeleteTask {
                        create_timestamp: 0,
                        opstamp: previous_opstamp + 1,
                        delete_query: Some(DeleteQuery {
                            index_uid: Some(index_uid),
                            start_timestamp: None,
                            end_timestamp: None,
                            query_ast: quickwit_query::query_ast::qast_json_helper(&query, &[]),
                        }),
                    }],
                })
            });
        MergeOperation::new_delete_and_merge_operation(metadata.pop().unwrap())
    } else {
        MergeOperation::new_merge_operation(metadata)
    };
    let destination_id = operation.merge_split_id.clone();
    let scratch = TempDirectory::for_test();
    let merge_scratch = MergeScratch {
        merge_source: MergeSource::Task(MergeTask::from_merge_operation_for_test(operation)),
        tantivy_dirs: inputs
            .iter()
            .map(|split| {
                Box::new(split.controlled_directory.clone()) as Box<dyn tantivy::Directory>
            })
            .collect(),
        downloaded_splits_directory: scratch.named_temp_child("inputs-")?,
        merge_scratch_directory: scratch,
    };
    let (packager_mailbox, packager_inbox) = universe.create_test_mailbox();
    let executor = MergeExecutor::new(
        pipeline_id,
        MetastoreServiceClient::from_mock(metastore),
        mapper,
        IoControls::default(),
        packager_mailbox,
        None,
    );
    let (mailbox, handle) = universe.spawn_builder().spawn(executor);
    mailbox.send_message(merge_scratch).await?;
    handle.process_pending_and_observe().await;
    let mut batches: Vec<IndexedSplitBatch> = packager_inbox.drain_for_test_typed();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].splits.len(), 1);
    let output = batches.pop().unwrap().splits.pop().unwrap();
    assert_eq!(output.split_id(), &destination_id);
    assert!(
        inputs
            .iter()
            .all(|input| input.split_id() != output.split_id())
    );
    let (status, _) = handle.quit().await;
    assert!(matches!(status, ActorExitStatus::Quit), "{status:?}");
    Ok(output)
}

#[tokio::test]
async fn test_initial_split_id_lifecycle() -> anyhow::Result<()> {
    let universe = Universe::new();
    let mapper = doc_mapper();
    let pipeline_id = IndexingPipelineId {
        index_uid: IndexUid::new_with_random_ulid("arbitrarily-named-index"),
        source_id: "test-source".to_string(),
        node_id: NodeId::from_str("test-node"),
        pipeline_uid: PipelineUid::default(),
    };
    let mut settings = IndexingSettings::for_test();
    settings.split_num_docs_target = 6;
    settings.resources.heap_size = ByteSize::mb(256);
    settings.commit_timeout_secs = 3600;
    let mut metastore = MockMetastoreService::new();
    metastore
        .expect_last_delete_opstamp()
        .returning(|_| Ok(LastDeleteOpstampResponse::new(0)));
    let clustering: DocsClusteringConfig = serde_json::from_value(json!([
        {"fingerprint": [{"kind": "raw", "path": "seq"}]}
    ]))?;
    let (serializer_mailbox, serializer_inbox) = universe.create_test_mailbox();
    let indexer = Indexer::new(
        pipeline_id,
        mapper.clone(),
        MetastoreServiceClient::from_mock(metastore),
        TempDirectory::for_test(),
        settings,
        None,
        serializer_mailbox,
        Some(Fingerprinter::new(&clustering)),
        None,
    );
    let (mailbox, handle) = universe.spawn_builder().spawn(indexer);
    for (batch_number, batch) in [
        [(10, 0), (20, 1), (30, 2)],
        [(30, 3), (40, 4), (10, 5)],
        [(10, 6), (10, 7), (10, 8)],
    ]
    .into_iter()
    .enumerate()
    {
        let mut docs = Vec::new();
        for (partition, seq) in batch {
            let input = json!({"seq": seq}).to_string();
            let (_, doc) = mapper.doc_from_json_str(&input)?;
            docs.push(ProcessedDoc {
                doc,
                partition,
                timestamp_opt: None,
                num_bytes: input.len(),
                // Synthetic fingerprints force non-identity document remapping.
                fingerprint_opt: Some(Fingerprint::new([seq % 2])),
            });
        }
        mailbox
            .send_message(ProcessedDocBatch::new(
                docs,
                SourceCheckpointDelta::from_range(batch_number as u64..batch_number as u64 + 1),
                false,
            ))
            .await?;
    }
    handle.process_pending_and_observe().await;
    let (status, _) = handle.quit().await;
    assert!(matches!(status, ActorExitStatus::Quit), "{status:?}");
    let batches: Vec<IndexedSplitBatchBuilder> = serializer_inbox.drain_for_test_typed();
    let mut splits = Vec::new();
    let mut expected = BTreeMap::new();
    for builder in batches.into_iter().flat_map(|batch| batch.splits) {
        let split = builder.finalize()?;
        let rows = identities(&split, &mapper)?;
        assert!(rows.values().all(|id| id == split.split_id().as_str()));
        expected.extend(rows);
        splits.push(split);
    }
    assert_eq!(splits.len(), 4);
    assert_eq!(expected.len(), 9);
    assert_eq!(expected[&0], expected[&5]); // Reuse a normal partition across batches.
    assert_eq!(expected[&2], expected[&3]); // Reuse OTHER for the same overflow partition.
    assert_eq!(expected[&2], expected[&4]); // Different overflow partitions share OTHER.
    assert_ne!(expected[&0], expected[&1]);
    assert_ne!(expected[&0], expected[&2]);
    assert_eq!(expected[&6], expected[&8]);
    assert_ne!(expected[&0], expected[&6]); // Rollover of the same requested partition.
    let overflow = splits
        .iter()
        .find(|split| split.split_id().as_str() == expected[&2])
        .unwrap();
    assert!(![10, 20, 30, 40].contains(&overflow.split_attrs.partition_id));
    let reader = overflow.index.reader()?;
    let searcher = reader.searcher();
    let sequence = searcher.segment_reader(0).fast_fields().u64("seq")?;
    assert_eq!(
        (0..3)
            .map(|doc_id| sequence.first(doc_id).unwrap())
            .collect::<Vec<_>>(),
        [2, 4, 3]
    );

    // Reuse these splits to check two merge generations, with reordered inputs.
    splits.reverse();
    let mut remaining = splits.split_off(2);
    let mut first_expected = BTreeMap::new();
    for split in &splits {
        first_expected.extend(identities(split, &mapper)?);
    }
    let first = merge_splits(&universe, mapper.clone(), &splits, None).await?;
    assert_eq!(identities(&first, &mapper)?, first_expected);
    assert_eq!(first.split_attrs.num_merge_ops, 1);
    remaining.push(first);
    let mut merged = merge_splits(&universe, mapper.clone(), &remaining, None).await?;
    assert_eq!(identities(&merged, &mapper)?, expected);
    assert_eq!(merged.split_attrs.num_merge_ops, 2);
    for seq in [1u64, 7u64] {
        for (number, origin) in &expected {
            assert_eq!(
                query_count(
                    &merged.index,
                    &mapper,
                    &format!("origin:{origin} AND seq:{number}")
                )?,
                1
            );
        }
        let query = format!("origin:{} AND seq:{seq}", expected[&seq]);
        let previous_opstamp = merged.split_attrs.delete_opstamp;
        merged = merge_splits(&universe, mapper.clone(), &[merged], Some(query.clone())).await?;
        expected.remove(&seq);
        assert_eq!(identities(&merged, &mapper)?, expected);
        assert_eq!(merged.split_attrs.delete_opstamp, previous_opstamp + 1);
        assert_eq!(merged.split_attrs.num_merge_ops, 2);
        assert_eq!(query_count(&merged.index, &mapper, &query)?, 0);
        assert_eq!(
            query_count(
                &merged.index,
                &mapper,
                &format!("origin:{}", merged.split_id())
            )?,
            0
        );
    }
    assert_eq!(expected.len(), 7);
    universe.assert_quit().await;
    Ok(())
}

#[tokio::test]
async fn test_initial_split_id_pipeline() -> anyhow::Result<()> {
    let mut builder = DocMapperBuilder::from(doc_mapper().as_ref().clone());
    builder.doc_mapping.tag_fields.insert("origin".to_string());
    let mapping = serde_json::to_string(&builder.doc_mapping)?;
    let sandbox = crate::TestSandbox::create("invoice-catalog", &mapping, "", &[]).await?;
    let stats = sandbox
        .add_documents([
            json!({"seq": 1}),
            json!({"seq": 2, "origin": null}),
            json!({"seq": 3, "metadata": {"origin": []}}),
            json!({"seq": 4, "origin": "spoofed"}),
            json!({"seq": 5}),
        ])
        .await?;
    assert_eq!(stats.num_invalid_docs, 3);
    let splits = sandbox
        .metastore()
        .list_splits(ListSplitsRequest::try_from_index_uid(sandbox.index_uid())?)
        .await?
        .collect_splits_metadata()
        .await?;
    assert_eq!(splits.len(), 1);
    let split = &splits[0];
    assert_eq!(split.num_docs, 2);
    assert!(split.tags.contains(&format!("origin:{}", split.split_id())));
    let download_dir = TempDirectory::for_test();
    let filename = quickwit_common::split_file(split.split_id());
    let path = download_dir.path().join(&filename);
    sandbox
        .storage()
        .copy_to_file(std::path::Path::new(&filename), &path)
        .await?;
    let index = Index::open(crate::get_tantivy_directory_from_split_bundle(&path)?)?;
    let mapper = sandbox.doc_mapper();
    for query in [
        format!("origin:{}", split.split_id()),
        "origin:*".to_string(),
        "metadata.origin:*".to_string(),
    ] {
        assert_eq!(query_count(&index, &mapper, &query)?, 2);
    }
    assert_eq!(
        query_count(
            &index,
            &mapper,
            &format!("origin:{}", &split.split_id().as_str()[..12])
        )?,
        0
    );
    let aggregations = serde_json::from_value(json!({
        "origins": {"terms": {"field": "origin"}},
        "unstored_origins": {"terms": {"field": "metadata.origin"}}
    }))?;
    let collector =
        tantivy::aggregation::AggregationCollector::from_aggs(aggregations, Default::default());
    let aggregated = serde_json::to_value(
        index
            .reader()?
            .searcher()
            .search(&tantivy::query::AllQuery, &collector)?,
    )?;
    for name in ["origins", "unstored_origins"] {
        assert_eq!(
            aggregated[name]["buckets"],
            json!([
                {"key": split.split_id().as_str(), "doc_count": 2}
            ])
        );
    }
    sandbox.assert_quit().await;
    Ok(())
}
