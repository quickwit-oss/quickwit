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

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_common::runtimes::RuntimeType;
use quickwit_config::TransformConfig;
use quickwit_doc_mapper::{DocMapper, DocParsingError};
use serde::Serialize;
use serde_json::Value as JsonValue;
use tantivy::schema::{Field, Value};
use tokio::runtime::Handle;
use tracing::warn;
use vrl::{Program, Runtime, TargetValueRef, Terminate, TimeZone};

use crate::actors::Indexer;
use crate::models::{NewPublishLock, PreparedDoc, PreparedDocBatch, PublishLock, RawDocBatch};

type VrlValue = ::value::Value;
type VrlSecrets = ::value::Secrets;

#[derive(Debug)]
pub enum PrepareDocumentError {
    ParsingError,
    MissingField,
    TransformError(Terminate),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DocProcessorCounters {
    index_id: String,
    source_id: String,
    /// Overall number of documents received, partitioned
    /// into 4 categories:
    /// - number of docs that could not be parsed.
    /// - number of docs that could not be transformed.
    /// - number of docs without a timestamp (if the index has no timestamp field,
    /// then this counter is equal to zero)
    /// - number of valid docs.
    pub num_parse_errors: u64,
    pub num_transform_errors: u64,
    pub num_docs_with_missing_fields: u64,
    pub num_valid_docs: u64,

    /// Number of bytes that went through the indexer
    /// during its entire lifetime.
    ///
    /// Includes both valid and invalid documents.
    pub overall_num_bytes: u64,
}

impl DocProcessorCounters {
    pub fn new(index_id: String, source_id: String) -> Self {
        Self {
            index_id,
            source_id,
            num_parse_errors: 0,
            num_transform_errors: 0,
            num_docs_with_missing_fields: 0,
            num_valid_docs: 0,
            overall_num_bytes: 0,
        }
    }

    /// Returns the overall number of docs that went through the indexer (valid or not).
    pub fn num_processed_docs(&self) -> u64 {
        self.num_valid_docs
            + self.num_parse_errors
            + self.num_docs_with_missing_fields
            + self.num_transform_errors
    }

    /// Returns the overall number of docs that were sent to the indexer but were invalid.
    /// (For instance, because they were missing a required field or because their because
    /// their format was invalid)
    pub fn num_invalid_docs(&self) -> u64 {
        self.num_parse_errors + self.num_docs_with_missing_fields + self.num_transform_errors
    }

    pub fn record_parsing_error(&mut self, num_bytes: u64) {
        self.num_parse_errors += 1;
        self.overall_num_bytes += num_bytes;
        crate::metrics::INDEXER_METRICS
            .processed_docs_total
            .with_label_values([
                self.index_id.as_str(),
                self.source_id.as_str(),
                "parsing_error",
            ])
            .inc();
        crate::metrics::INDEXER_METRICS
            .processed_bytes
            .with_label_values([
                self.index_id.as_str(),
                self.source_id.as_str(),
                "parsing_error",
            ])
            .inc_by(num_bytes);
    }

    pub fn record_transform_error(&mut self, num_bytes: u64) {
        self.num_transform_errors += 1;
        self.overall_num_bytes += num_bytes;
        crate::metrics::INDEXER_METRICS
            .processed_docs_total
            .with_label_values([
                self.index_id.as_str(),
                self.source_id.as_str(),
                "transform_error",
            ])
            .inc();
        crate::metrics::INDEXER_METRICS
            .processed_bytes
            .with_label_values([
                self.index_id.as_str(),
                self.source_id.as_str(),
                "transform_error",
            ])
            .inc_by(num_bytes);
    }

    pub fn record_missing_field(&mut self, num_bytes: u64) {
        self.num_docs_with_missing_fields += 1;
        self.overall_num_bytes += num_bytes;
        crate::metrics::INDEXER_METRICS
            .processed_docs_total
            .with_label_values([
                self.index_id.as_str(),
                self.source_id.as_str(),
                "missing_field",
            ])
            .inc();
        crate::metrics::INDEXER_METRICS
            .processed_bytes
            .with_label_values([
                self.index_id.as_str(),
                self.source_id.as_str(),
                "missing_field",
            ])
            .inc_by(num_bytes);
    }

    pub fn record_valid(&mut self, num_bytes: u64) {
        self.num_valid_docs += 1;
        self.overall_num_bytes += num_bytes;
        crate::metrics::INDEXER_METRICS
            .processed_docs_total
            .with_label_values([self.index_id.as_str(), self.source_id.as_str(), "valid"])
            .inc();
        crate::metrics::INDEXER_METRICS
            .processed_bytes
            .with_label_values([self.index_id.as_str(), self.source_id.as_str(), "valid"])
            .inc_by(num_bytes);
    }
}

pub struct DocProcessor {
    doc_mapper: Arc<dyn DocMapper>,
    indexer_mailbox: Mailbox<Indexer>,
    timestamp_field_opt: Option<Field>,
    counters: DocProcessorCounters,
    publish_lock: PublishLock,
    transform_opt: Option<VrlProgram>,
}

impl DocProcessor {
    pub fn try_new(
        index_id: String,
        source_id: String,
        doc_mapper: Arc<dyn DocMapper>,
        indexer_mailbox: Mailbox<Indexer>,
        transform_config_opt: Option<TransformConfig>,
    ) -> anyhow::Result<Self> {
        let schema = doc_mapper.schema();
        let timestamp_field_opt = doc_mapper.timestamp_field(&schema);
        let transform_opt = transform_config_opt
            .map(VrlProgram::try_from_transform_config)
            .transpose()?;

        let doc_processor = Self {
            doc_mapper,
            indexer_mailbox,
            timestamp_field_opt,
            counters: DocProcessorCounters::new(index_id, source_id),
            publish_lock: PublishLock::default(),
            transform_opt,
        };
        Ok(doc_processor)
    }

    fn prepare_document(
        &mut self,
        json_doc: &str,
        ctx: &ActorContext<Self>,
    ) -> Result<PreparedDoc, PrepareDocumentError> {
        let _protect_guard = ctx.protect_zone();

        // Transform and parse the document
        let doc_parsing_result = if let Some(vrl_program) = self.transform_opt.as_mut() {
            let vrl_value = vrl_program.transform_doc(json_doc)?;
            let json_obj = match serde_json::to_value(vrl_value) {
                Ok(JsonValue::Object(json_obj)) => json_obj,
                _ => return Err(PrepareDocumentError::ParsingError),
            };
            self.doc_mapper.doc_from_json_obj(json_obj)
        } else {
            self.doc_mapper.doc_from_json_str(json_doc)
        };
        let (partition, doc) = doc_parsing_result.map_err(|doc_parsing_error| {
            warn!(err=?doc_parsing_error);
            match doc_parsing_error {
                DocParsingError::RequiredFastField(_) => PrepareDocumentError::MissingField,
                _ => PrepareDocumentError::ParsingError,
            }
        })?;
        // Extract timestamp if necessary
        let Some(timestamp_field) = self.timestamp_field_opt else {
            // No need to check the timestamp, there are no timestamp.
            return Ok(PreparedDoc {
                doc,
                timestamp_opt: None,
                partition,
                num_bytes: json_doc.len(),
            });
        };
        let timestamp = doc
            .get_first(timestamp_field)
            .and_then(|value| match value {
                Value::Date(date_time) => Some(date_time.into_timestamp_secs()),
                _ => None,
            })
            .ok_or(PrepareDocumentError::MissingField)?;
        Ok(PreparedDoc {
            doc,
            timestamp_opt: Some(timestamp),
            partition,
            num_bytes: json_doc.len(),
        })
    }
}

#[async_trait]
impl Actor for DocProcessor {
    type ObservableState = DocProcessorCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(10)
    }

    fn runtime_handle(&self) -> Handle {
        RuntimeType::Blocking.get_runtime_handle()
    }

    #[inline]
    fn yield_after_each_message(&self) -> bool {
        false
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
        for json_doc in raw_doc_batch.docs {
            let json_doc_num_bytes = json_doc.len() as u64;
            match self.prepare_document(&json_doc, ctx) {
                Ok(document) => {
                    self.counters.record_valid(json_doc_num_bytes);
                    prepared_docs.push(document);
                }
                Err(PrepareDocumentError::ParsingError) => {
                    self.counters.record_parsing_error(json_doc_num_bytes);
                }
                Err(PrepareDocumentError::TransformError(_)) => {
                    self.counters.record_transform_error(json_doc_num_bytes);
                }
                Err(PrepareDocumentError::MissingField) => {
                    self.counters.record_missing_field(json_doc_num_bytes);
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

struct VrlProgram {
    runtime: Runtime,
    program: Program,
    timezone: TimeZone,
}

impl VrlProgram {
    fn transform_doc(&mut self, json_doc: &str) -> Result<VrlValue, PrepareDocumentError> {
        let mut value = match serde_json::from_str::<VrlValue>(json_doc) {
            Ok(value) if value.is_object() => value,
            _ => return Err(PrepareDocumentError::ParsingError),
        };
        let mut metadata = VrlValue::Object(BTreeMap::new());
        let mut secrets = VrlSecrets::new();
        let mut target = TargetValueRef {
            value: &mut value,
            metadata: &mut metadata,
            secrets: &mut secrets,
        };
        let runtime_res = self
            .runtime
            .resolve(&mut target, &self.program, &self.timezone)
            .map_err(PrepareDocumentError::TransformError);

        self.runtime.clear();

        runtime_res
    }

    fn try_from_transform_config(transform_config: TransformConfig) -> anyhow::Result<Self> {
        let (program, timezone) = transform_config.compile_vrl_script()?;
        let state = vrl::state::Runtime::default();
        let runtime = Runtime::new(state);

        Ok(VrlProgram {
            program,
            runtime,
            timezone,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use quickwit_actors::Universe;
    use quickwit_doc_mapper::{default_doc_mapper_for_test, DefaultDocMapper};
    use quickwit_metastore::checkpoint::SourceCheckpointDelta;
    use serde_json::Value as JsonValue;
    use tantivy::schema::NamedFieldDocument;

    use super::*;
    use crate::models::{PublishLock, RawDocBatch};

    #[tokio::test]
    async fn test_doc_processor_simple() -> anyhow::Result<()> {
        let index_id = "my-index";
        let source_id = "my-source";
        let universe = Universe::with_accelerated_time();
        let doc_mapper = Arc::new(default_doc_mapper_for_test());
        let (indexer_mailbox, indexer_inbox) = universe.create_test_mailbox();
        let doc_processor = DocProcessor::try_new(
            index_id.to_string(),
            source_id.to_string(),
            doc_mapper.clone(),
            indexer_mailbox,
            None,
        )
        .unwrap();
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
                index_id: index_id.to_string(),
                source_id: source_id.to_string(),
                num_parse_errors: 1,
                num_transform_errors: 0,
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
        let NamedFieldDocument(named_field_doc_map) = schema.to_named_doc(&batch.docs[0].doc);
        let doc_json = JsonValue::Object(doc_mapper.doc_to_json(named_field_doc_map)?);
        assert_eq!(
            doc_json,
            serde_json::json!({
                "_source": {
                    "body": "happy",
                    "response_date": "2021-12-19T16:39:59+00:00",
                    "response_payload": "YWJj",
                    "response_time": 2,
                    "timestamp": 1628837062
                },
                "body": "happy",
                "response_date": "2021-12-19T16:39:59Z",
                 "response_payload": "YWJj",
                 "response_time": 2.0,
                 "timestamp": 1628837062
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
        let universe = Universe::with_accelerated_time();
        let (indexer_mailbox, indexer_inbox) = universe.create_test_mailbox();
        let doc_processor = DocProcessor::try_new(
            "my-index".to_string(),
            "my-source".to_string(),
            doc_mapper,
            indexer_mailbox,
            None,
        )
        .unwrap();
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
        let universe = Universe::with_accelerated_time();
        let (indexer_mailbox, indexer_inbox) = universe.create_test_mailbox();
        let doc_processor = DocProcessor::try_new(
            "my-index".to_string(),
            "my-source".to_string(),
            doc_mapper,
            indexer_mailbox,
            None,
        )
        .unwrap();
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
        let universe = Universe::with_accelerated_time();
        let (indexer_mailbox, indexer_inbox) = universe.create_test_mailbox();
        let doc_mapper = Arc::new(default_doc_mapper_for_test());
        let doc_processor = DocProcessor::try_new(
            "my-index".to_string(),
            "my-source".to_string(),
            doc_mapper,
            indexer_mailbox,
            None,
        )
        .unwrap();
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

    #[tokio::test]
    async fn test_doc_processor_simple_vrl() -> anyhow::Result<()> {
        let index_id = "my-index";
        let source_id = "my-source";
        let universe = Universe::with_accelerated_time();
        let (indexer_mailbox, indexer_inbox) = universe.create_test_mailbox();
        let doc_mapper = Arc::new(default_doc_mapper_for_test());
        let transform_config = TransformConfig::for_test(".body = upcase(string!(.body))");
        let doc_processor = DocProcessor::try_new(
            index_id.to_string(),
            source_id.to_string(),
            doc_mapper.clone(),
            indexer_mailbox,
            Some(transform_config),
        )
        .unwrap();
        let (doc_processor_mailbox, doc_processor_handle) =
            universe.spawn_builder().spawn(doc_processor);
        let checkpoint_delta = SourceCheckpointDelta::from(0..4);
        doc_processor_mailbox
            .send_message(RawDocBatch {
                docs: vec![
                        r#"{"body": "happy", "response_date": "2021-12-19T16:39:57+00:00", "response_time": 12, "response_payload": "YWJj"}"#.to_string(), // missing timestamp
                        r#"{"body": "happy using VRL", "timestamp": 1628837062, "response_date": "2021-12-19T16:39:59+00:00", "response_time": 2, "response_payload": "YWJj"}"#.to_string(), // ok
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
                index_id: index_id.to_string(),
                source_id: source_id.to_string(),
                num_parse_errors: 1,
                num_transform_errors: 0,
                num_docs_with_missing_fields: 1,
                num_valid_docs: 2,
                overall_num_bytes: 397,
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
        let NamedFieldDocument(named_field_doc_map) = schema.to_named_doc(&batch.docs[0].doc);
        let doc_json = JsonValue::Object(doc_mapper.doc_to_json(named_field_doc_map)?);
        assert_eq!(
            doc_json,
            serde_json::json!({
                "_source": {
                    "body": "HAPPY USING VRL",
                    "response_date": "2021-12-19T16:39:59+00:00",
                    "response_payload": "YWJj",
                    "response_time": 2,
                    "timestamp": 1628837062
                },
                "body": "HAPPY USING VRL",
                "response_date": "2021-12-19T16:39:59Z",
                 "response_payload": "YWJj",
                 "response_time": 2.0,
                 "timestamp": 1628837062
            })
        );
        Ok(())
    }
}
