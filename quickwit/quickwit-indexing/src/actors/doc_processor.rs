// Copyright (C) 2022 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::sync::Arc;

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_common::runtimes::RuntimeType;
use quickwit_doc_mapper::{DocMapper, DocParsingError};
use tantivy::schema::{Field, Value};
use tokio::runtime::Handle;
use tracing::warn;

use crate::actors::Indexer;
use crate::models::{NewPublishLock, PreparedDoc, PreparedDocBatch, PublishLock, RawDocBatch};

enum PrepareDocumentError {
    ParsingError,
    MissingField,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DocProcessorCounters {
    /// Overall number of documents received, partitioned
    /// into 3 categories:
    /// - number docs that did not parse correctly.
    /// - number docs missing a timestamp (if the index has no timestamp,
    /// then this counter is 0)
    /// - number of valid docs.
    pub num_parse_errors: u64,
    pub num_docs_with_missing_fields: u64,
    pub num_valid_docs: u64,

    /// Number of bytes that went through the indexer
    /// during its entire lifetime.
    ///
    /// Includes both valid and invalid documents.
    pub overall_num_bytes: u64,
}

impl DocProcessorCounters {
    /// Returns the overall number of docs that went through the indexer (valid or not).
    pub fn num_processed_docs(&self) -> u64 {
        self.num_valid_docs + self.num_parse_errors + self.num_docs_with_missing_fields
    }

    /// Returns the overall number of docs that were sent to the indexer but were invalid.
    /// (For instance, because they were missing a required field or because their because
    /// their format was invalid)
    pub fn num_invalid_docs(&self) -> u64 {
        self.num_parse_errors + self.num_docs_with_missing_fields
    }

    pub fn record_parsing_error(&mut self, num_bytes: u64) {
        self.num_parse_errors += 1;
        self.overall_num_bytes += num_bytes;
        crate::metrics::INDEXER_METRICS
            .parsing_errors_num_docs_total
            .inc();
        crate::metrics::INDEXER_METRICS
            .parsing_errors_num_bytes_total
            .inc_by(num_bytes);
    }

    pub fn record_missing_field(&mut self, num_bytes: u64) {
        self.num_docs_with_missing_fields += 1;
        self.overall_num_bytes += num_bytes;
        crate::metrics::INDEXER_METRICS
            .missing_field_num_docs_total
            .inc();
        crate::metrics::INDEXER_METRICS
            .missing_field_num_bytes_total
            .inc_by(num_bytes);
    }

    pub fn record_valid(&mut self, num_bytes: u64) {
        self.num_valid_docs += 1;
        self.overall_num_bytes += num_bytes;
        crate::metrics::INDEXER_METRICS.valid_num_docs_total.inc();
        crate::metrics::INDEXER_METRICS
            .valid_num_bytes_total
            .inc_by(num_bytes);
    }
}

pub struct DocProcessor {
    doc_mapper: Arc<dyn DocMapper>,
    indexer_mailbox: Mailbox<Indexer>,
    timestamp_field_opt: Option<Field>,
    counters: DocProcessorCounters,
    publish_lock: PublishLock,
}

impl DocProcessor {
    pub fn new(doc_mapper: Arc<dyn DocMapper>, indexer_mailbox: Mailbox<Indexer>) -> Self {
        let schema = doc_mapper.schema();
        let timestamp_field_opt = doc_mapper.timestamp_field(&schema);
        Self {
            doc_mapper,
            indexer_mailbox,
            timestamp_field_opt,
            counters: Default::default(),
            publish_lock: PublishLock::default(),
        }
    }

    fn prepare_document(
        &self,
        doc_json: String,
        ctx: &ActorContext<Self>,
    ) -> Result<PreparedDoc, PrepareDocumentError> {
        // Parse the document
        let _protect_guard = ctx.protect_zone();
        let num_bytes = doc_json.len();
        let doc_parsing_result = self.doc_mapper.doc_from_json(doc_json);
        let (partition, doc) = doc_parsing_result.map_err(|doc_parsing_error| {
            warn!(err=?doc_parsing_error);
            match doc_parsing_error {
                DocParsingError::RequiredFastField(_) => PrepareDocumentError::MissingField,
                _ => PrepareDocumentError::ParsingError,
            }
        })?;
        // Extract timestamp if necessary
        let timestamp_field = if let Some(timestamp_field) = self.timestamp_field_opt {
            timestamp_field
        } else {
            // No need to check the timestamp, there are no timestamp.
            return Ok(PreparedDoc {
                doc,
                timestamp_opt: None,
                partition,
                num_bytes,
            });
        };
        let timestamp = doc
            .get_first(timestamp_field)
            .and_then(|value| match value {
                Value::Date(date_time) => Some(date_time.into_timestamp_secs()),
                value => value.as_i64(),
            })
            .ok_or(PrepareDocumentError::MissingField)?;
        Ok(PreparedDoc {
            doc,
            timestamp_opt: Some(timestamp),
            partition,
            num_bytes,
        })
    }
}

#[async_trait]
impl Actor for DocProcessor {
    type ObservableState = DocProcessorCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(10)
    }

    fn runtime_handle(&self) -> Handle {
        RuntimeType::Blocking.get_runtime_handle()
    }

    async fn finalize(
        &mut self,
        exit_status: &ActorExitStatus,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        match exit_status {
            ActorExitStatus::DownstreamClosed
            | ActorExitStatus::Killed
            | ActorExitStatus::Failure(_)
            | ActorExitStatus::Panicked => return Ok(()),
            ActorExitStatus::Quit | ActorExitStatus::Success => {
                ctx.send_exit_with_success(&self.indexer_mailbox).await?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<RawDocBatch> for DocProcessor {
    type Reply = ();

    async fn handle(
        &mut self,
        raw_doc_batch: RawDocBatch,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if self.publish_lock.is_dead() {
            return Ok(());
        }
        let mut prepared_docs: Vec<PreparedDoc> = Vec::with_capacity(raw_doc_batch.docs.len());
        for doc_json in raw_doc_batch.docs {
            let doc_json_num_bytes = doc_json.len() as u64;
            match self.prepare_document(doc_json, ctx) {
                Ok(document) => {
                    self.counters.record_valid(doc_json_num_bytes);
                    prepared_docs.push(document);
                }
                Err(PrepareDocumentError::ParsingError) => {
                    self.counters.record_parsing_error(doc_json_num_bytes);
                }
                Err(PrepareDocumentError::MissingField) => {
                    self.counters.record_missing_field(doc_json_num_bytes);
                }
            }
            ctx.record_progress();
        }
        let prepared_doc_batch = PreparedDocBatch {
            docs: prepared_docs,
            checkpoint_delta: raw_doc_batch.checkpoint_delta,
        };
        ctx.send_message(&self.indexer_mailbox, prepared_doc_batch)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<NewPublishLock> for DocProcessor {
    type Reply = ();

    async fn handle(
        &mut self,
        new_publish_lock: NewPublishLock,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.publish_lock = new_publish_lock.0.clone();
        ctx.send_message(&self.indexer_mailbox, new_publish_lock)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use quickwit_actors::{create_test_mailbox, Universe};
    use quickwit_doc_mapper::{default_doc_mapper_for_test, DefaultDocMapper};
    use quickwit_metastore::checkpoint::SourceCheckpointDelta;

    use super::*;
    use crate::models::{PublishLock, RawDocBatch};

    #[tokio::test]
    async fn test_doc_processor_simple() -> anyhow::Result<()> {
        let doc_mapper = Arc::new(default_doc_mapper_for_test());
        let (indexer_mailbox, indexer_inbox) = create_test_mailbox();
        let doc_processor = DocProcessor::new(doc_mapper.clone(), indexer_mailbox);
        let universe = Universe::new();
        let (doc_processor_mailbox, doc_processor_handle) =
            universe.spawn_builder().spawn(doc_processor);
        let checkpoint_delta = SourceCheckpointDelta::from(0..4);
        doc_processor_mailbox
            .send_message(RawDocBatch {
                docs: vec![
                        r#"{"body": "happy", "response_date": "2021-12-19T16:39:57+00:00", "response_time": 12, "response_payload": "YWJj"}"#.to_string(), // missing timestamp
                        r#"{"body": "happy", "timestamp": 1628837062, "response_date": "2021-12-19T16:39:59+00:00", "response_time": 2, "response_payload": "YWJj"}"#.to_string(), // ok
                        r#"{"body": "happy2", "timestamp": 1628837062, "response_date": "2021-12-19T16:40:57+00:00", "response_time": 13, "response_payload": "YWJj"}"#.to_string(), // ok
                        "{".to_string(),                    // invalid json
                    ],
                checkpoint_delta: checkpoint_delta.clone(),
            })
            .await?;
        let doc_processor_counters = doc_processor_handle
            .process_pending_and_observe()
            .await
            .state;
        assert_eq!(
            doc_processor_counters,
            DocProcessorCounters {
                num_parse_errors: 1,
                num_docs_with_missing_fields: 1,
                num_valid_docs: 2,
                overall_num_bytes: 387,
            }
        );
        let output_messages = indexer_inbox.drain_for_test();
        assert_eq!(output_messages.len(), 1);
        let batch = *(output_messages
            .into_iter()
            .next()
            .unwrap()
            .downcast::<PreparedDocBatch>()
            .unwrap());
        assert_eq!(batch.docs.len(), 2);
        assert_eq!(batch.checkpoint_delta, checkpoint_delta);

        let schema = doc_mapper.schema();
        let doc_json: serde_json::Value =
            serde_json::from_str(&schema.to_json(&batch.docs[0].doc)).unwrap();
        assert_eq!(
            doc_json,
            serde_json::json!({
                "_source": [{
                    "body": "happy",
                    "response_date": "2021-12-19T16:39:59+00:00",
                    "response_payload": "YWJj",
                    "response_time": 2,
                    "timestamp": 1628837062
                }],
                "body": ["happy"],
                "response_date": ["2021-12-19T16:39:59Z"],
                 "response_payload": [[97, 98, 99]],
                 "response_time": [2.0],
                 "timestamp": [1628837062]
            })
        );
        Ok(())
    }

    const DOCMAPPER_WITH_PARTITION_JSON: &str = r#"
        {
            "tag_fields": ["tenant"],
            "partition_key": "tenant",
            "field_mappings": [
                { "name": "tenant", "type": "text", "tokenizer": "raw", "indexed": true },
                { "name": "body", "type": "text" }
            ]
        }"#;

    #[tokio::test]
    async fn test_doc_processor_partitioning() -> anyhow::Result<()> {
        let doc_mapper: Arc<dyn DocMapper> = Arc::new(
            serde_json::from_str::<DefaultDocMapper>(DOCMAPPER_WITH_PARTITION_JSON).unwrap(),
        );
        let (indexer_mailbox, indexer_inbox) = create_test_mailbox();
        let doc_processor = DocProcessor::new(doc_mapper, indexer_mailbox);
        let universe = Universe::new();
        let (doc_processor_mailbox, doc_processor_handle) =
            universe.spawn_builder().spawn(doc_processor);
        doc_processor_mailbox
            .send_message(RawDocBatch {
                docs: vec![
                    r#"{"tenant": "tenant_1", "body": "first doc for tenant 1"}"#.to_string(),
                    r#"{"tenant": "tenant_2", "body": "first doc for tenant 2"}"#.to_string(),
                    r#"{"tenant": "tenant_1", "body": "second doc for tenant 1"}"#.to_string(),
                    r#"{"tenant": "tenant_2", "body": "second doc for tenant 2"}"#.to_string(),
                ],
                checkpoint_delta: SourceCheckpointDelta::from(0..2),
            })
            .await?;
        universe
            .send_exit_with_success(&doc_processor_mailbox)
            .await
            .unwrap();
        let (exit_status, _) = doc_processor_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Success));
        let prepared_doc_batches: Vec<PreparedDocBatch> = indexer_inbox.drain_for_test_typed();
        assert_eq!(prepared_doc_batches.len(), 1);
        let partition_ids: Vec<u64> = prepared_doc_batches[0]
            .docs
            .iter()
            .map(|doc| doc.partition)
            .collect();
        assert_eq!(partition_ids[0], partition_ids[2]);
        assert_eq!(partition_ids[1], partition_ids[3]);
        assert_ne!(partition_ids[0], partition_ids[1]);
        Ok(())
    }

    #[tokio::test]
    async fn test_doc_processor_forward_publish_lock() {
        let doc_mapper = Arc::new(default_doc_mapper_for_test());
        let (indexer_mailbox, indexer_inbox) = create_test_mailbox();
        let universe = Universe::new();
        let doc_processor = DocProcessor::new(doc_mapper, indexer_mailbox);
        let (doc_processor_mailbox, doc_processor_handle) =
            universe.spawn_builder().spawn(doc_processor);
        let publish_lock = PublishLock::default();
        doc_processor_mailbox
            .send_message(NewPublishLock(publish_lock.clone()))
            .await
            .unwrap();
        universe
            .send_exit_with_success(&doc_processor_mailbox)
            .await
            .unwrap();
        let (exit_status, _) = doc_processor_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Success));
        let publish_locks: Vec<NewPublishLock> = indexer_inbox.drain_for_test_typed();
        assert_eq!(&publish_locks, &[NewPublishLock(publish_lock)]);
    }

    #[tokio::test]
    async fn test_doc_processor_ignores_messages_when_publish_lock_is_dead() {
        let doc_mapper = Arc::new(default_doc_mapper_for_test());
        let (indexer_mailbox, indexer_inbox) = create_test_mailbox();
        let doc_processor = DocProcessor::new(doc_mapper, indexer_mailbox);
        let universe = Universe::new();
        let (doc_processor_mailbox, doc_processor_handle) =
            universe.spawn_builder().spawn(doc_processor);
        let publish_lock = PublishLock::default();
        doc_processor_mailbox
            .send_message(NewPublishLock(publish_lock.clone()))
            .await
            .unwrap();
        doc_processor_handle.process_pending_and_observe().await;
        publish_lock.kill().await;
        doc_processor_mailbox
            .send_message(RawDocBatch {
                docs: vec![
                        r#"{"body": "happy", "timestamp": 1628837062, "response_date": "2021-12-19T16:39:59+00:00", "response_time": 2, "response_payload": "YWJj"}"#.to_string(),
                    ],
                checkpoint_delta: SourceCheckpointDelta::from(0..1),
            })
            .await.unwrap();
        universe
            .send_exit_with_success(&doc_processor_mailbox)
            .await
            .unwrap();
        let (exit_status, _indexer_counters) = doc_processor_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Success));
        let indexer_messages: Vec<PreparedDocBatch> = indexer_inbox.drain_for_test_typed();
        assert!(indexer_messages.is_empty());
    }
}
