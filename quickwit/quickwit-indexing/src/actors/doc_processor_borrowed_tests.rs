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

use quickwit_actors::Universe;
use tantivy::collector::Count;
use tantivy::directory::RamDirectory;
use tantivy::query::TermQuery;
use tantivy::schema::{Document, IndexRecordOption};
use tantivy::store::{Compressor, ZstdCompressor};
use tantivy::{DocAddress, IndexBuilder, IndexSettings, Term};

use super::*;

fn mapper(mode: &str) -> Arc<DocMapper> {
    Arc::new(
        serde_json::from_value(serde_json::json!({
            "mode":mode, "store_source":true, "index_field_presence":false,
            "timestamp_field":"timestamp", "field_mappings":[
                {"name":"body","type":"text"},
                {"name":"timestamp","type":"datetime","fast":true},
                {"name":"value","type":"f64"}
            ]
        }))
        .unwrap(),
    )
}

fn fingerprinter() -> Fingerprinter {
    Fingerprinter::new(
        &serde_json::from_value(serde_json::json!([
            {"fingerprint":[{"kind":"raw","path":"value"}]},
            {"fingerprint":[{"kind":"tokenized","path":"body","max_tokens":3}]},
            {"fingerprint":[{"kind":"structure","exclude":["timestamp"]}]}
        ]))
        .unwrap(),
    )
}

#[tokio::test]
async fn raw_json_actor_counters_fingerprints_and_finalized_index() {
    for configured_fingerprints in [false, true] {
        let universe = Universe::with_accelerated_time();
        let mapper = mapper("strict");
        let (mailbox, inbox) = universe.create_test_mailbox();
        let processor = DocProcessor::try_new(
            "borrowed-actor".into(),
            "source".into(),
            mapper.clone(),
            mailbox,
            None,
            SourceInputFormat::Json,
            configured_fingerprints.then(fingerprinter),
        )
        .unwrap();
        let (mailbox, handle) = universe.spawn_builder().spawn(processor);
        let inputs: &[&[u8]] = &[
            br#"{"body":"server 12","timestamp":1628837062,"value":1.0}"#,
            br#"{"body":[],"b\u006fdy":"server 99","timestamp":{},"timestamp":1628837062,"value":-0}"#,
            b"[1]", b"{",
            br#"{"body":{},"timestamp":1628837062,"value":1}"#,
            br#"{"body":"missing timestamp","value":1}"#,
        ];
        mailbox
            .send_message(RawDocBatch::for_test(inputs, 0..6))
            .await
            .unwrap();
        let counters = handle.process_pending_and_observe().await.state;
        assert_eq!(counters.valid.get_num_docs(), 2);
        assert_eq!(counters.json_parse_errors.get_num_docs(), 2);
        assert_eq!(counters.doc_mapper_errors.get_num_docs(), 2);
        assert_eq!(counters.transform_errors.get_num_docs(), 0);
        assert_eq!(counters.otlp_parse_errors.get_num_docs(), 0);
        assert_eq!(counters.num_processed_docs(), 6);
        assert_eq!(counters.num_invalid_docs(), 4);
        assert_eq!(
            counters.num_bytes_total.load(Ordering::Relaxed),
            inputs.iter().map(|doc| doc.len() as u64).sum::<u64>()
        );
        let mut batches: Vec<ProcessedDocBatch> = inbox.drain_for_test_typed();
        assert_eq!(batches.len(), 1);
        let batch = batches.pop().unwrap();
        assert_eq!(batch.docs.len(), 2);
        for (index, processed) in batch.docs.iter().enumerate() {
            assert_eq!(processed.partition, 0);
            assert_eq!(processed.num_bytes, inputs[index].len());
            assert_eq!(
                processed.timestamp_opt,
                Some(DateTime::from_timestamp_secs(1628837062))
            );
            if configured_fingerprints {
                let raw_hash = [9090247521291406000, 3523228072621034834][index];
                assert_eq!(
                    &**processed.fingerprint_opt.as_ref().unwrap(),
                    &[raw_hash, 14895941878402920949, 16756079140178559348]
                );
            } else {
                assert!(processed.fingerprint_opt.is_none());
            }
        }
        // The actor has released every input Bytes before its owned output is indexed.
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
        for processed in batch.docs {
            writer.add_document(processed.doc).unwrap();
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
}

#[tokio::test]
async fn borrowed_parse_and_mapping_errors_keep_original_categories_and_text() {
    let universe = Universe::with_accelerated_time();
    let (mailbox, _) = universe.create_test_mailbox();
    let processor = DocProcessor::try_new(
        "borrowed-errors".into(),
        "source".into(),
        mapper("strict"),
        mailbox,
        None,
        SourceInputFormat::Json,
        None,
    )
    .unwrap();
    for input in [
        b"{".as_slice(),
        b"[1]",
        b"null",
        b"true",
        b"12",
        b"\"text\"",
        b"{\"a\":1,}",
        b"{\"a\":\"\xff\"}",
    ] {
        // Original parser remains the independent oracle for exact serde diagnostics.
        let expected = serde_json::from_slice::<JsonObject>(input)
            .unwrap_err()
            .to_string();
        let error = processor.process_json_bytes(input).unwrap_err();
        let DocProcessorError::JsonParsing(actual) = error else {
            panic!("wrong category: {error}")
        };
        assert_eq!(actual, expected);
    }
    assert_eq!(
        processor.process_json_bytes(b"{").unwrap_err().to_string(),
        "JSON parse error: EOF while parsing an object at line 1 column 1"
    );
    let error = processor
        .process_json_bytes(br#"{"body":{},"timestamp":1628837062}"#)
        .unwrap_err();
    assert!(matches!(error, DocProcessorError::DocMapperParsing(_)));
    assert_eq!(
        error.to_string(),
        "doc mapper parse error: the field `body` could not be parsed: expected string, got `{}`"
    );
}

#[cfg(feature = "vrl")]
#[tokio::test]
async fn configured_vrl_keeps_owned_transform_and_fingerprint_processing() {
    let universe = Universe::with_accelerated_time();
    let (mailbox, _) = universe.create_test_mailbox();
    let mut processor = DocProcessor::try_new(
        "vrl-control".into(),
        "source".into(),
        mapper("strict"),
        mailbox,
        Some(TransformConfig::for_test(".body = upcase(string!(.body))")),
        SourceInputFormat::Json,
        Some(fingerprinter()),
    )
    .unwrap();
    let raw = br#"{"body":"server 12","timestamp":1628837062,"value":1.0}"#;
    let mut docs = Vec::new();
    processor.process_raw_doc(Bytes::copy_from_slice(raw), &mut docs);
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].num_bytes, raw.len());
    let body = processor.doc_mapper.schema().get_field("body").unwrap();
    assert_eq!(
        docs[0].doc.get_first(body).unwrap().as_str(),
        Some("SERVER 12")
    );
    assert_eq!(
        &**docs[0].fingerprint_opt.as_ref().unwrap(),
        &[
            9090247521291406000,
            14895941878402920949,
            16756079140178559348
        ]
    );
    assert_eq!(processor.counters.valid.get_num_docs(), 1);
    assert_eq!(processor.counters.num_invalid_docs(), 0);
}

#[tokio::test]
async fn plain_text_stays_native_owned_with_fingerprints_and_utf8_errors() {
    let universe = Universe::with_accelerated_time();
    let (mailbox, _) = universe.create_test_mailbox();
    let mapper = Arc::new(
        serde_json::from_value(serde_json::json!({
            "mode":"strict", "index_field_presence":false,
            "field_mappings":[{"name":"plain_text","type":"text"}]
        }))
        .unwrap(),
    );
    let printer = Fingerprinter::new(
        &serde_json::from_value(serde_json::json!([
            {"fingerprint":[{"kind":"raw","path":"plain_text"}]},
            {"fingerprint":[{"kind":"tokenized","path":"plain_text","max_tokens":3}]}
        ]))
        .unwrap(),
    );
    let mut processor = DocProcessor::try_new(
        "plain-control".into(),
        "source".into(),
        mapper,
        mailbox,
        None,
        SourceInputFormat::PlainText,
        Some(printer.clone()),
    )
    .unwrap();
    let input = "server 12\nnot JSON";
    let mut docs = Vec::new();
    processor.process_raw_doc(Bytes::copy_from_slice(input.as_bytes()), &mut docs);
    processor.process_raw_doc(Bytes::from_static(b"\xff"), &mut docs);
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].num_bytes, input.len());
    assert_eq!(docs[0].partition, 0);
    assert_eq!(docs[0].timestamp_opt, None);
    assert_eq!(
        docs[0].fingerprint_opt,
        Some(printer.fingerprint(&serde_json::json!({"plain_text":input})))
    );
    let field = processor
        .doc_mapper
        .schema()
        .get_field("plain_text")
        .unwrap();
    assert_eq!(docs[0].doc.get_first(field).unwrap().as_str(), Some(input));
    assert_eq!(processor.counters.valid.get_num_docs(), 1);
    assert_eq!(processor.counters.json_parse_errors.get_num_docs(), 1);
    assert_eq!(processor.counters.doc_mapper_errors.get_num_docs(), 0);
    assert_eq!(
        processor.counters.num_bytes_total.load(Ordering::Relaxed),
        input.len() as u64 + 1
    );
}

#[tokio::test]
async fn borrowed_processing_matches_native_owned_modes_partition_and_first_error() {
    let universe = Universe::with_accelerated_time();
    for mode in ["strict", "lenient", "dynamic"] {
        let mut config = serde_json::to_value(&*mapper(mode)).unwrap();
        config["partition_key"] = serde_json::json!("body");
        let mapper = Arc::new(serde_json::from_value(config).unwrap());
        let (mailbox, _) = universe.create_test_mailbox();
        let processor = DocProcessor::try_new(
            "borrowed-parity".into(),
            "source".into(),
            mapper,
            mailbox,
            None,
            SourceInputFormat::Json,
            Some(fingerprinter()),
        )
        .unwrap();
        for raw in [
            r#"{"body":"server 12","timestamp":1628837062,"value":1.0}"#,
            r#"{"body":"server 99","timestamp":1628837062,"extra":{"date":"2024-01-02T03:04:05Z","a":[1,null]}}"#,
            r#"{"body":{},"timestamp":[],"value":false}"#,
            r#"{"body":"missing timestamp"}"#,
            r#"{"value":{},"body":null,"timestamp":false}"#,
        ] {
            let owned = processor
                .process_json_doc(JsonDoc::new(serde_json::from_str(raw).unwrap(), raw.len()));
            let borrowed = processor.process_json_bytes(raw.as_bytes());
            match (owned, borrowed) {
                (Ok(owned), Ok(borrowed)) => {
                    assert_eq!(owned.partition, borrowed.partition);
                    assert_eq!(owned.timestamp_opt, borrowed.timestamp_opt);
                    assert_eq!(owned.fingerprint_opt, borrowed.fingerprint_opt);
                    assert_eq!(owned.num_bytes, borrowed.num_bytes);
                    let schema = processor.doc_mapper.schema();
                    assert_eq!(
                        owned.doc.to_named_doc(&schema).0,
                        borrowed.doc.to_named_doc(&schema).0
                    );
                }
                (Err(owned), Err(borrowed)) => assert_eq!(owned.to_string(), borrowed.to_string()),
                _ => panic!("owned/borrowed outcome differs: {raw}"),
            }
        }
    }
}
