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

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};

use quickwit_actors::{ObservationType, Universe};
use quickwit_proto::metastore::{LastDeleteOpstampResponse, MockMetastoreService};
use quickwit_proto::types::{IndexUid, NodeId, PipelineUid};
use quickwit_query::query_ast::FieldPresenceQuery;
use tantivy::collector::Count;
use tantivy::directory::RamDirectory;
use tantivy::query::TermQuery;
use tantivy::schema::{IndexRecordOption, Value};
use tantivy::{DocAddress, TantivyDocument, Term};

use super::*;

fn test_pipeline_id() -> IndexingPipelineId {
    IndexingPipelineId {
        node_id: NodeId::from_str("test-node"),
        index_uid: IndexUid::for_test("arbitrary-index-name", 1),
        source_id: "test-source".to_string(),
        pipeline_uid: PipelineUid::for_test(42),
    }
}

fn test_mapper() -> Arc<DocMapper> {
    Arc::new(
        serde_json::from_value(serde_json::json!({
            "index_field_presence": true,
            "max_num_partitions": 2,
            "field_mappings": [
                { "name": "body", "type": "text" },
                { "name": "origin", "type": "text", "tokenizer": "raw", "fast": true },
                { "name": "destination_partition", "type": "u64", "fast": true },
                { "name": "extra", "type": "text", "tokenizer": "raw" }
            ]
        }))
        .unwrap(),
    )
}

fn processed_doc(mapper: &DocMapper, partition: u64) -> ProcessedDoc {
    let json = r#"{"body":"original document"}"#;
    let (_, doc) = mapper.doc_from_json_str(json).unwrap();
    ProcessedDoc {
        doc,
        fingerprint_opt: None,
        timestamp_opt: None,
        partition,
        num_bytes: json.len(),
    }
}

async fn index_batches(
    mapper: Arc<DocMapper>,
    enricher_opt: Option<DocEnricher>,
    batches: Vec<ProcessedDocBatch>,
) -> (Universe, ActorExitStatus, Vec<IndexedSplitBatchBuilder>) {
    let universe = Universe::new();
    let (serializer_mailbox, serializer_inbox) = universe.create_test_mailbox();
    let mut metastore = MockMetastoreService::new();
    metastore
        .expect_last_delete_opstamp()
        .returning(|_| Ok(LastDeleteOpstampResponse::new(10)));
    metastore.expect_publish_splits().never();
    // Leave room for concurrent partitions; these tests control commits with force_commit.
    let mut settings = IndexingSettings::for_test();
    settings.resources.heap_size = ByteSize::mb(256);
    let mut indexer = Indexer::new(
        test_pipeline_id(),
        mapper,
        MetastoreServiceClient::from_mock(metastore),
        TempDirectory::for_test(),
        settings,
        None,
        serializer_mailbox,
        None,
        None,
    );
    if let Some(enricher) = enricher_opt {
        indexer = indexer.with_doc_enricher(enricher);
    }
    let (mailbox, handle) = universe.spawn_builder().spawn(indexer);
    for batch in batches {
        mailbox.send_message(batch).await.unwrap();
    }
    // The last batch forces a commit; after an enrichment failure the actor has already exited.
    let observation = handle.process_pending_and_observe().await;
    if observation.obs_type == ObservationType::Alive {
        universe.send_exit_with_success(&mailbox).await.unwrap();
    }
    let (exit_status, _) = handle.join().await;
    let output = serializer_inbox.drain_for_test_typed();
    // Keep the universe's directory kill switch alive until the caller finalizes the splits.
    (universe, exit_status, output)
}

#[tokio::test]
async fn test_doc_enricher_destination_and_rollover() -> anyhow::Result<()> {
    let mapper = test_mapper();
    let schema = mapper.schema();
    let origin = schema.get_field("origin")?;
    let destination_partition = schema.get_field("destination_partition")?;
    let extra = schema.get_field("extra")?;
    let calls = Arc::new(AtomicUsize::new(0));
    let enricher: DocEnricher = {
        let calls = calls.clone();
        let expected_schema = schema.clone();
        let mapping_uid = mapper.doc_mapping_uid();
        Arc::new(move |context, doc| {
            assert_eq!(context.pipeline_id, &test_pipeline_id());
            assert_eq!(context.doc_mapping_uid, mapping_uid);
            assert_eq!(context.schema, &expected_schema);
            assert!(doc.get_first(origin).is_none());
            doc.add_text(origin, context.split_id.as_str());
            doc.add_u64(destination_partition, context.partition_id);
            doc.add_text(extra, "enriched");
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    };
    // Multiple batches share open splits; two distinct overflow inputs share the OTHER split.
    let partitions = [vec![1, 2, 3], vec![1, 4, 3], vec![1]];
    let mut offset = 0;
    let mut batches = Vec::new();
    for (batch_ord, partitions) in partitions.into_iter().enumerate() {
        let docs = partitions
            .into_iter()
            .map(|partition| processed_doc(&mapper, partition))
            .collect::<Vec<_>>();
        let end = offset + docs.len() as u64;
        batches.push(ProcessedDocBatch::new(
            docs,
            SourceCheckpointDelta::from_range(offset..end),
            batch_ord > 0,
        ));
        offset = end;
    }
    let (universe, exit_status, output) =
        index_batches(mapper.clone(), Some(enricher), batches).await;
    assert!(exit_status.is_success(), "{exit_status:?}");
    assert_eq!(output.len(), 2);
    assert_eq!(output[0].splits.len(), 3);
    assert_eq!(output[1].splits.len(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 7);
    let mut counts_by_partition = BTreeMap::new();
    let mut initial_split_ids = HashSet::new();
    let mut splits = Vec::new();
    for batch in output {
        for builder in batch.splits {
            let split_id = builder.split_id().clone();
            assert!(initial_split_ids.insert(split_id.clone()));
            let split = builder.finalize()?;
            *counts_by_partition
                .entry(split.split_attrs.partition_id)
                .or_insert(0) += split.split_attrs.num_docs;
            let reader = split.index.reader()?;
            let searcher = reader.searcher();
            let term = Term::from_field_text(origin, split_id.as_str());
            let query = TermQuery::new(term, IndexRecordOption::Basic);
            assert_eq!(
                searcher.search(&query, &Count)? as u64,
                split.split_attrs.num_docs
            );
            // Exercise Quickwit's query translation, including non-fast field presence.
            for field_name in ["origin", "extra", "body"] {
                let ast = FieldPresenceQuery {
                    field: field_name.to_string(),
                }
                .into();
                let (query, _) = mapper.query(searcher.schema().clone(), ast, true, None)?;
                assert_eq!(
                    searcher.search(query.as_ref(), &Count)? as u64,
                    split.split_attrs.num_docs
                );
            }
            let segment = &searcher.segment_readers()[0];
            let fast_origin = segment.fast_fields().str("origin")?.unwrap();
            let fast_partition = segment.fast_fields().u64("destination_partition")?;
            for doc_id in 0..segment.max_doc() {
                let stored: TantivyDocument = searcher.doc(DocAddress::new(0, doc_id))?;
                assert_eq!(
                    stored.get_first(origin).unwrap().as_str(),
                    Some(split_id.as_str())
                );
                assert_eq!(stored.get_first(extra).unwrap().as_str(), Some("enriched"));
                let mut fast_value = String::new();
                fast_origin.ord_to_str(
                    fast_origin.term_ords(doc_id).next().unwrap(),
                    &mut fast_value,
                )?;
                assert_eq!(fast_value, split_id.as_str());
                assert_eq!(
                    fast_partition.first(doc_id),
                    Some(split.split_attrs.partition_id)
                );
            }
            splits.push(split);
        }
    }
    assert_eq!(
        counts_by_partition,
        BTreeMap::from([(1, 3), (2, 1), (OTHER_PARTITION_ID, 3)])
    );
    // Merging ordinary indexes must preserve the initial values without any callback registration.
    let indexes = splits
        .iter()
        .map(|split| split.index.clone())
        .collect::<Vec<_>>();
    let merged = tantivy::indexer::merge_indices(&indexes, RamDirectory::default())?;
    let searcher = merged.reader()?.searcher();
    assert_eq!(searcher.num_docs(), 7);
    for segment_ord in 0..searcher.segment_readers().len() {
        for doc_id in 0..searcher.segment_readers()[segment_ord].max_doc() {
            let doc: TantivyDocument = searcher.doc(DocAddress::new(segment_ord as u32, doc_id))?;
            assert!(
                initial_split_ids
                    .iter()
                    .any(|id| Some(id.as_str()) == doc.get_first(origin).unwrap().as_str())
            );
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 7);
    universe.assert_quit().await;
    Ok(())
}

#[tokio::test]
async fn test_doc_enricher_is_disabled_by_default() -> anyhow::Result<()> {
    let mapper = test_mapper();
    let mut processed = processed_doc(&mapper, 0);
    let origin = mapper.schema().get_field("origin")?;
    // This field name is not reserved in a pipeline without enrichment.
    processed.doc.add_text(origin, "user-supplied");
    let batch = ProcessedDocBatch::new(
        vec![processed],
        SourceCheckpointDelta::from_range(0..1),
        true,
    );
    let (universe, exit_status, output) = index_batches(mapper, None, vec![batch]).await;
    assert!(exit_status.is_success(), "{exit_status:?}");
    let builder = output
        .into_iter()
        .next()
        .unwrap()
        .splits
        .into_iter()
        .next()
        .unwrap();
    let split = builder.finalize()?;
    let searcher = split.index.reader()?.searcher();
    let doc: TantivyDocument = searcher.doc(DocAddress::new(0, 0))?;
    assert_eq!(
        doc.get_first(origin).unwrap().as_str(),
        Some("user-supplied")
    );
    universe.assert_quit().await;
    Ok(())
}

#[tokio::test]
async fn test_doc_enricher_errors_fail_the_batch() -> anyhow::Result<()> {
    let mapper = test_mapper();
    let origin = mapper.schema().get_field("origin")?;
    let enricher: DocEnricher = Arc::new(move |context, doc| {
        anyhow::ensure!(doc.get_first(origin).is_none(), "origin field collision");
        doc.add_text(origin, context.split_id.as_str());
        Ok(())
    });
    let first = processed_doc(&mapper, 0);
    let mut second = processed_doc(&mapper, 0);
    second.doc.add_text(origin, "collision");
    let batch = ProcessedDocBatch::new(
        vec![first, second],
        SourceCheckpointDelta::from_range(0..2),
        true,
    );
    let (universe, exit_status, output) = index_batches(mapper, Some(enricher), vec![batch]).await;
    let ActorExitStatus::Failure(error) = exit_status else {
        panic!("expected an enrichment error, got {exit_status:?}");
    };
    assert!(format!("{error:#}").contains("origin field collision"));
    assert!(
        output.is_empty(),
        "a partially enriched batch must not be serialized"
    );
    universe.assert_quit().await;
    Ok(())
}
