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
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use quickwit_actors::Universe;
use quickwit_common::rand::append_random_suffix;
use quickwit_config::{
    INGEST_API_SOURCE_ID, IngestApiConfig, SourceConfig, SourceInputFormat, SourceParams,
};
use quickwit_doc_mapper::DocMapper;
use quickwit_ingest::{CommitType, DocBatchBuilder, IngestRequest, init_ingest_api};
use quickwit_proto::types::IndexUid;
use tantivy::collector::Count;
use tantivy::directory::RamDirectory;
use tantivy::query::TermQuery;
use tantivy::schema::{IndexRecordOption, Value};
use tantivy::store::{Compressor, ZstdCompressor};
use tantivy::{DocAddress, IndexBuilder, IndexSettings, TantivyDocument, Term};

use super::IngestApiSource;
use crate::actors::DocProcessor;
use crate::docs_clustering::Fingerprinter;
use crate::models::ProcessedDocBatch;
use crate::source::SourceActor;
use crate::source::tests::SourceRuntimeBuilder;

#[tokio::test]
async fn ingest_queue_to_actual_processor_and_finalized_stored_index() {
    let universe = Universe::with_accelerated_time();
    let index_id = append_random_suffix("borrowed-ingest");
    let temp_dir = tempfile::tempdir().unwrap();
    let service = init_ingest_api(&universe, temp_dir.path(), &IngestApiConfig::default())
        .await
        .unwrap();
    let mapper: Arc<DocMapper> = Arc::new(
        serde_json::from_value(serde_json::json!({
            "mode":"strict", "index_field_presence":false, "store_source":true,
            "field_mappings":[{"name":"body","type":"text"},{"name":"value","type":"f64"}]
        }))
        .unwrap(),
    );
    let printer = Fingerprinter::new(
        &serde_json::from_value(serde_json::json!([
            {"fingerprint":[{"kind":"raw","path":"value"}]},
            {"fingerprint":[{"kind":"tokenized","path":"body","max_tokens":3}]}
        ]))
        .unwrap(),
    );
    let (indexer_mailbox, inbox) = universe.create_test_mailbox();
    let processor = DocProcessor::try_new(
        index_id.clone(),
        INGEST_API_SOURCE_ID.into(),
        mapper.clone(),
        indexer_mailbox,
        None,
        SourceInputFormat::Json,
        Some(printer),
    )
    .unwrap();
    let (processor_mailbox, processor_handle) = universe.spawn_builder().spawn(processor);
    let config = SourceConfig {
        source_id: INGEST_API_SOURCE_ID.into(),
        num_pipelines: NonZeroUsize::MIN,
        enabled: true,
        source_params: SourceParams::IngestApi,
        transform_config: None,
        input_format: SourceInputFormat::Json,
    };
    let runtime = SourceRuntimeBuilder::new(IndexUid::new_with_random_ulid(&index_id), config)
        .with_queues_dir(temp_dir.path())
        .build();
    let source = IngestApiSource::try_new(runtime).await.unwrap();
    let (_, source_handle) = universe
        .spawn_builder()
        .spawn(SourceActor::new(Box::new(source), processor_mailbox));
    let inputs: &[&[u8]] = &[
        br#"{"body":"server 12","value":1.0}"#,
        br#"{"body":{},"b\u006fdy":"server 99","value":-0}"#,
        b"[",
    ];
    let mut builder = DocBatchBuilder::new(index_id);
    for input in inputs {
        builder.ingest_doc(*input);
    }
    service
        .ask_for_res(IngestRequest {
            doc_batches: vec![builder.build()],
            commit: CommitType::Auto.into(),
        })
        .await
        .unwrap();
    universe.sleep(Duration::from_secs(2)).await;
    let source_state = source_handle.process_pending_and_observe().await.state;
    assert_eq!(source_state["num_docs_processed"], 3);
    // Follow the source regression tests' shutdown order: finish source before processor.
    source_handle.quit().await;
    let counters = processor_handle.process_pending_and_observe().await.state;
    assert_eq!(counters.valid.num_docs.load(Ordering::Relaxed), 2);
    assert_eq!(
        counters.json_parse_errors.num_docs.load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        counters.doc_mapper_errors.num_docs.load(Ordering::Relaxed),
        0
    );
    assert_eq!(counters.num_processed_docs(), 3);
    assert_eq!(
        counters.num_bytes_total.load(Ordering::Relaxed),
        inputs.iter().map(|input| input.len() as u64).sum::<u64>()
    );
    let batches: Vec<ProcessedDocBatch> = inbox.drain_for_test_typed();
    let docs: Vec<_> = batches.into_iter().flat_map(|batch| batch.docs).collect();
    assert_eq!(docs.len(), 2);
    for (index, doc) in docs.iter().enumerate() {
        assert_eq!(doc.partition, 0);
        assert_eq!(doc.num_bytes, inputs[index].len());
        assert_eq!(doc.timestamp_opt, None);
        assert_eq!(
            &**doc.fingerprint_opt.as_ref().unwrap(),
            &[
                [9090247521291406000, 3523228072621034834][index],
                14895941878402920949
            ]
        );
    }
    universe.assert_quit().await;
    let schema = mapper.schema();
    let settings = IndexSettings {
        docstore_blocksize: 1_000_000,
        docstore_compression: Compressor::Zstd(ZstdCompressor {
            compression_level: Some(8),
        }),
        docstore_compress_dedicated_thread: true,
        ..Default::default()
    };
    let mut writer = IndexBuilder::new()
        .schema(schema.clone())
        .tokenizers(mapper.tokenizer_manager().tantivy_manager().clone())
        .settings(settings)
        .single_segment_index_writer::<TantivyDocument>(RamDirectory::create(), 15_000_000)
        .unwrap();
    for doc in docs {
        writer.add_document(doc.doc).unwrap();
    }
    let index = writer.finalize().unwrap();
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();
    assert_eq!(searcher.num_docs(), 2);
    assert_eq!(searcher.segment_readers().len(), 1);
    let body = schema.get_field("body").unwrap();
    let query = TermQuery::new(
        Term::from_field_text(body, "server"),
        IndexRecordOption::Basic,
    );
    assert_eq!(searcher.search(&query, &Count).unwrap(), 2);
    for (doc_id, expected) in ["server 12", "server 99"].into_iter().enumerate() {
        let stored: TantivyDocument = searcher.doc(DocAddress::new(0, doc_id as u32)).unwrap();
        assert_eq!(stored.get_first(body).unwrap().as_str(), Some(expected));
    }
}
