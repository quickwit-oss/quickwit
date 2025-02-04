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

use std::string::FromUtf8Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{bail, Context};
use async_trait::async_trait;
use bytes::Bytes;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_common::metrics::IntCounter;
use quickwit_common::rate_limited_tracing::rate_limited_warn;
use quickwit_common::runtimes::RuntimeType;
use quickwit_config::{SourceInputFormat, TransformConfig};
use quickwit_doc_mapper::{DocMapper, DocParsingError, JsonObject};
use quickwit_opentelemetry::otlp::{
    parse_otlp_logs_json, parse_otlp_logs_protobuf, parse_otlp_spans_json,
    parse_otlp_spans_protobuf, JsonLogIterator, JsonSpanIterator, OtlpLogsError, OtlpTracesError,
};
use quickwit_proto::types::{IndexId, SourceId};
use serde::Serialize;
use serde_json::Value as JsonValue;
use tantivy::schema::{Field, Value};
use tantivy::{DateTime, TantivyDocument};
use thiserror::Error;
use tokio::runtime::Handle;

#[cfg(feature = "vrl")]
use super::vrl_processing::*;
use crate::actors::Indexer;
use crate::models::{
    NewPublishLock, NewPublishToken, ProcessedDoc, ProcessedDocBatch, PublishLock, RawDocBatch,
};

const PLAIN_TEXT: &str = "plain_text";

pub(super) struct JsonDoc {
    json_obj: JsonObject,
    num_bytes: usize,
}

impl JsonDoc {
    pub fn new(json_obj: JsonObject, num_bytes: usize) -> Self {
        Self {
            json_obj,
            num_bytes,
        }
    }

    pub fn try_from_json_value(
        json_value: JsonValue,
        num_bytes: usize,
    ) -> Result<Self, DocProcessorError> {
        match json_value {
            JsonValue::Object(json_obj) => Ok(Self::new(json_obj, num_bytes)),
            _ => Err(DocProcessorError::JsonParsing(
                "document is not an object".to_string(),
            )),
        }
    }

    #[cfg(feature = "vrl")]
    pub fn try_from_vrl_doc(vrl_doc: VrlDoc) -> Result<Self, DocProcessorError> {
        let json_value = serde_json::to_value(vrl_doc.vrl_value)?;
        Self::try_from_json_value(json_value, vrl_doc.num_bytes)
    }
}

#[allow(clippy::enum_variant_names)]
#[derive(Error, Debug)]
pub enum DocProcessorError {
    #[error("doc mapper parse error: {0}")]
    DocMapperParsing(DocParsingError),
    #[error("JSON parse error: {0}")]
    JsonParsing(String),
    #[error("OLTP log records parse error: {0}")]
    OltpLogsParsing(OtlpLogsError),
    #[error("OLTP traces parse error: {0}")]
    OltpTracesParsing(OtlpTracesError),
    #[cfg(feature = "vrl")]
    #[error("VRL transform error: {0}")]
    Transform(VrlTerminate),
}

impl From<OtlpLogsError> for DocProcessorError {
    fn from(error: OtlpLogsError) -> Self {
        Self::OltpLogsParsing(error)
    }
}

impl From<OtlpTracesError> for DocProcessorError {
    fn from(error: OtlpTracesError) -> Self {
        Self::OltpTracesParsing(error)
    }
}

impl From<DocParsingError> for DocProcessorError {
    fn from(error: DocParsingError) -> Self {
        Self::DocMapperParsing(error)
    }
}

impl From<serde_json::Error> for DocProcessorError {
    fn from(error: serde_json::Error) -> Self {
        Self::JsonParsing(error.to_string())
    }
}

impl From<FromUtf8Error> for DocProcessorError {
    fn from(error: FromUtf8Error) -> Self {
        Self::JsonParsing(error.to_string())
    }
}

#[cfg(feature = "vrl")]
fn try_into_vrl_doc(
    input_format: SourceInputFormat,
    raw_doc: Bytes,
    num_bytes: usize,
) -> Result<VrlDoc, DocProcessorError> {
    let vrl_value = match input_format {
        SourceInputFormat::Json => serde_json::from_slice::<VrlValue>(&raw_doc)?,
        SourceInputFormat::PlainText => {
            let mut map = std::collections::BTreeMap::new();
            let key = vrl::value::KeyString::from(PLAIN_TEXT);
            let value = VrlValue::Bytes(raw_doc);
            map.insert(key, value);
            VrlValue::Object(map)
        }
        SourceInputFormat::OtlpLogsJson
        | SourceInputFormat::OtlpLogsProtobuf
        | SourceInputFormat::OtlpTracesJson
        | SourceInputFormat::OtlpTracesProtobuf => {
            panic!("OTP logs or traces do not support VRL transforms")
        }
    };
    let vrl_doc = VrlDoc::new(vrl_value, num_bytes);
    Ok(vrl_doc)
}

fn try_into_json_docs(
    input_format: SourceInputFormat,
    raw_doc: Bytes,
    num_bytes: usize,
) -> JsonDocIterator {
    match input_format {
        SourceInputFormat::Json => {
            let json_doc_result = serde_json::from_slice::<JsonObject>(&raw_doc)
                .map(|json_obj| JsonDoc::new(json_obj, num_bytes));
            JsonDocIterator::from(json_doc_result)
        }
        SourceInputFormat::OtlpLogsJson => {
            let logs = parse_otlp_logs_json(&raw_doc);
            JsonDocIterator::from(logs)
        }
        SourceInputFormat::OtlpLogsProtobuf => {
            let logs = parse_otlp_logs_protobuf(&raw_doc);
            JsonDocIterator::from(logs)
        }
        SourceInputFormat::OtlpTracesJson => {
            let spans = parse_otlp_spans_json(&raw_doc);
            JsonDocIterator::from(spans)
        }
        SourceInputFormat::OtlpTracesProtobuf => {
            let spans = parse_otlp_spans_protobuf(&raw_doc);
            JsonDocIterator::from(spans)
        }
        SourceInputFormat::PlainText => {
            let json_doc_result = String::from_utf8(raw_doc.to_vec()).map(|value| {
                let mut json_obj = serde_json::Map::with_capacity(1);
                let key = PLAIN_TEXT.to_string();
                json_obj.insert(key, JsonValue::String(value));
                JsonDoc::new(json_obj, num_bytes)
            });
            JsonDocIterator::from(json_doc_result)
        }
    }
}

#[cfg(feature = "vrl")]
fn parse_raw_doc(
    input_format: SourceInputFormat,
    raw_doc: Bytes,
    num_bytes: usize,
    vrl_program_opt: Option<&mut VrlProgram>,
) -> JsonDocIterator {
    let Some(vrl_program) = vrl_program_opt else {
        return try_into_json_docs(input_format, raw_doc, num_bytes);
    };
    let json_doc_result = try_into_vrl_doc(input_format, raw_doc, num_bytes)
        .and_then(|vrl_doc| vrl_program.transform_doc(vrl_doc))
        .and_then(|vrl_doc_opt| vrl_doc_opt.map(JsonDoc::try_from_vrl_doc).transpose());

    JsonDocIterator::from(json_doc_result)
}

#[cfg(not(feature = "vrl"))]
fn parse_raw_doc(
    input_format: SourceInputFormat,
    raw_doc: Bytes,
    num_bytes: usize,
    _vrl_program_opt: Option<&mut VrlProgram>,
) -> JsonDocIterator {
    try_into_json_docs(input_format, raw_doc, num_bytes)
}

enum JsonDocIterator {
    One(Option<Result<JsonDoc, DocProcessorError>>),
    Logs(JsonLogIterator),
    Spans(JsonSpanIterator),
}

impl Iterator for JsonDocIterator {
    type Item = Result<JsonDoc, DocProcessorError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::One(opt) => opt.take(),
            Self::Logs(logs) => logs
                .next()
                .map(|(json_value, num_bytes)| JsonDoc::try_from_json_value(json_value, num_bytes)),
            Self::Spans(spans) => spans
                .next()
                .map(|(json_value, num_bytes)| JsonDoc::try_from_json_value(json_value, num_bytes)),
        }
    }
}

impl<E> From<Result<JsonDoc, E>> for JsonDocIterator
where E: Into<DocProcessorError>
{
    fn from(result: Result<JsonDoc, E>) -> Self {
        match result {
            Ok(json_doc) => Self::One(Some(Ok(json_doc))),
            Err(error) => Self::One(Some(Err(error.into()))),
        }
    }
}

impl<E> From<Result<Option<JsonDoc>, E>> for JsonDocIterator
where E: Into<DocProcessorError>,
{
    fn from(result: Result<Option<JsonDoc>, E>) -> Self {
        match result {
            Ok(None) => Self::One(None),
            Ok(Some(json_doc)) => Self::One(Some(Ok(json_doc))),
            Err(error) => Self::One(Some(Err(error.into()))),
        }
    }
}

impl From<Result<JsonLogIterator, OtlpLogsError>> for JsonDocIterator {
    fn from(result: Result<JsonLogIterator, OtlpLogsError>) -> Self {
        match result {
            Ok(logs) => Self::Logs(logs),
            Err(error) => Self::One(Some(Err(DocProcessorError::from(error)))),
        }
    }
}

impl From<Result<JsonSpanIterator, OtlpTracesError>> for JsonDocIterator {
    fn from(result: Result<JsonSpanIterator, OtlpTracesError>) -> Self {
        match result {
            Ok(spans) => Self::Spans(spans),
            Err(error) => Self::One(Some(Err(DocProcessorError::from(error)))),
        }
    }
}

#[derive(Debug)]
pub struct DocProcessorCounter {
    pub num_docs: AtomicU64,
    pub num_docs_metric: IntCounter,
    pub num_bytes_metric: IntCounter,
}

impl Serialize for DocProcessorCounter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        serializer.serialize_u64(self.get_num_docs())
    }
}

impl DocProcessorCounter {
    fn for_index_and_doc_processor_outcome(index: &str, outcome: &str) -> DocProcessorCounter {
        let index_label = quickwit_common::metrics::index_label(index);
        let labels = [index_label, outcome];
        DocProcessorCounter {
            num_docs: Default::default(),
            num_docs_metric: crate::metrics::INDEXER_METRICS
                .processed_docs_total
                .with_label_values(labels),
            num_bytes_metric: crate::metrics::INDEXER_METRICS
                .processed_bytes
                .with_label_values(labels),
        }
    }

    #[inline(always)]
    fn get_num_docs(&self) -> u64 {
        self.num_docs.load(Ordering::Relaxed)
    }

    fn record_doc(&self, num_bytes: u64) {
        self.num_docs.fetch_add(1, Ordering::Relaxed);
        self.num_docs_metric.inc();
        self.num_bytes_metric.inc_by(num_bytes);
    }
}

#[derive(Debug, Serialize)]
pub struct DocProcessorCounters {
    index_id: IndexId,
    source_id: SourceId,

    /// Overall number of documents received, partitioned
    /// into 5 categories:
    /// - valid documents
    /// - number of docs that could not be parsed.
    /// - number of docs that were not valid json.
    /// - number of docs that could not be transformed.
    /// - number of docs for which the doc mapper returned an error.
    /// - number of valid docs.
    pub valid: DocProcessorCounter,
    pub doc_mapper_errors: DocProcessorCounter,
    pub transform_errors: DocProcessorCounter,
    pub json_parse_errors: DocProcessorCounter,
    pub otlp_parse_errors: DocProcessorCounter,

    /// Number of bytes that went through the indexer
    /// during its entire lifetime.
    ///
    /// Includes both valid and invalid documents.
    pub num_bytes_total: AtomicU64,
}

impl DocProcessorCounters {
    pub fn new(index_id: IndexId, source_id: SourceId) -> Self {
        let valid_docs =
            DocProcessorCounter::for_index_and_doc_processor_outcome(&index_id, "valid");
        let doc_mapper_errors =
            DocProcessorCounter::for_index_and_doc_processor_outcome(&index_id, "doc_mapper_error");
        let transform_errors =
            DocProcessorCounter::for_index_and_doc_processor_outcome(&index_id, "transform_error");
        let json_parse_errors =
            DocProcessorCounter::for_index_and_doc_processor_outcome(&index_id, "json_parse_error");
        let otlp_parse_errors =
            DocProcessorCounter::for_index_and_doc_processor_outcome(&index_id, "otlp_parse_error");
        DocProcessorCounters {
            index_id,
            source_id,

            valid: valid_docs,
            doc_mapper_errors,
            transform_errors,
            json_parse_errors,
            otlp_parse_errors,
            num_bytes_total: Default::default(),
        }
    }

    /// Returns the overall number of docs that went through the indexer (valid or not).
    pub fn num_processed_docs(&self) -> u64 {
        self.valid.get_num_docs()
            + self.doc_mapper_errors.get_num_docs()
            + self.json_parse_errors.get_num_docs()
            + self.otlp_parse_errors.get_num_docs()
            + self.transform_errors.get_num_docs()
    }

    /// Returns the overall number of docs that were sent to the indexer but were invalid.
    /// (For instance, because they were missing a required field or because their because
    /// their format was invalid)
    pub fn num_invalid_docs(&self) -> u64 {
        self.doc_mapper_errors.get_num_docs()
            + self.json_parse_errors.get_num_docs()
            + self.otlp_parse_errors.get_num_docs()
            + self.transform_errors.get_num_docs()
    }

    pub fn record_valid(&self, num_bytes: u64) {
        self.num_bytes_total.fetch_add(num_bytes, Ordering::Relaxed);
        self.valid.record_doc(num_bytes);
    }

    pub fn record_error(&self, error: DocProcessorError, num_bytes: u64) {
        self.num_bytes_total.fetch_add(num_bytes, Ordering::Relaxed);
        match error {
            DocProcessorError::DocMapperParsing(_) => {
                self.doc_mapper_errors.record_doc(num_bytes);
            }
            DocProcessorError::JsonParsing(_) => {
                self.json_parse_errors.record_doc(num_bytes);
            }
            DocProcessorError::OltpLogsParsing(_) | DocProcessorError::OltpTracesParsing(_) => {
                self.otlp_parse_errors.record_doc(num_bytes);
            }
            #[cfg(feature = "vrl")]
            DocProcessorError::Transform(_) => {
                self.transform_errors.record_doc(num_bytes);
            }
        };
    }
}

pub struct DocProcessor {
    doc_mapper: Arc<DocMapper>,
    indexer_mailbox: Mailbox<Indexer>,
    timestamp_field_opt: Option<Field>,
    counters: Arc<DocProcessorCounters>,
    publish_lock: PublishLock,
    #[cfg(feature = "vrl")]
    transform_opt: Option<VrlProgram>,
    input_format: SourceInputFormat,
}

impl DocProcessor {
    pub fn try_new(
        index_id: IndexId,
        source_id: SourceId,
        doc_mapper: Arc<DocMapper>,
        indexer_mailbox: Mailbox<Indexer>,
        transform_config_opt: Option<TransformConfig>,
        input_format: SourceInputFormat,
    ) -> anyhow::Result<Self> {
        let timestamp_field_opt = extract_timestamp_field(&doc_mapper)?;
        if cfg!(not(feature = "vrl")) && transform_config_opt.is_some() {
            bail!("VRL is not enabled: please recompile with the `vrl` feature")
        }
        Ok(DocProcessor {
            doc_mapper,
            indexer_mailbox,
            timestamp_field_opt,
            counters: Arc::new(DocProcessorCounters::new(index_id, source_id)),
            publish_lock: PublishLock::default(),
            #[cfg(feature = "vrl")]
            transform_opt: transform_config_opt
                .map(VrlProgram::try_from_transform_config)
                .transpose()?,
            input_format,
        })
    }

    // Extract a timestamp from a tantivy document.
    //
    // If the timestamp is set up in the docmapper and the timestamp is missing,
    // returns an PrepareDocumentError::MissingField error.
    fn extract_timestamp(
        &self,
        doc: &TantivyDocument,
    ) -> Result<Option<DateTime>, DocProcessorError> {
        let Some(timestamp_field) = self.timestamp_field_opt else {
            return Ok(None);
        };
        let timestamp = doc
            .get_first(timestamp_field)
            .and_then(|val| val.as_datetime())
            .ok_or(DocProcessorError::from(DocParsingError::RequiredField(
                "timestamp field is required".to_string(),
            )))?;
        Ok(Some(timestamp))
    }

    fn process_raw_doc(&mut self, raw_doc: Bytes, processed_docs: &mut Vec<ProcessedDoc>) {
        let num_bytes = raw_doc.len();

        #[cfg(feature = "vrl")]
        let transform_opt = self.transform_opt.as_mut();
        #[cfg(not(feature = "vrl"))]
        let transform_opt: Option<&mut VrlProgram> = None;

        for json_doc_result in parse_raw_doc(self.input_format, raw_doc, num_bytes, transform_opt) {
            let processed_doc_result =
                json_doc_result.and_then(|json_doc| self.process_json_doc(json_doc));

            match processed_doc_result {
                Ok(processed_doc) => {
                    self.counters.record_valid(processed_doc.num_bytes as u64);
                    processed_docs.push(processed_doc);
                }
                Err(error) => {
                    rate_limited_warn!(
                        limit_per_min = 10,
                        index_id = self.counters.index_id,
                        source_id = self.counters.source_id,
                        "{error}",
                    );
                    self.counters.record_error(error, num_bytes as u64);
                }
            }
        }
    }

    fn process_json_doc(&self, json_doc: JsonDoc) -> Result<ProcessedDoc, DocProcessorError> {
        let num_bytes = json_doc.num_bytes;

        let (partition, doc) = self
            .doc_mapper
            .doc_from_json_obj(json_doc.json_obj, json_doc.num_bytes as u64)?;
        let timestamp_opt = self.extract_timestamp(&doc)?;
        Ok(ProcessedDoc {
            doc,
            timestamp_opt,
            partition,
            num_bytes,
        })
    }
}

fn extract_timestamp_field(doc_mapper: &DocMapper) -> anyhow::Result<Option<Field>> {
    let schema = doc_mapper.schema();
    let Some(timestamp_field_name) = doc_mapper.timestamp_field_name() else {
        return Ok(None);
    };
    let timestamp_field = schema
        .get_field(timestamp_field_name)
        .context("failed to find timestamp field in schema")?;
    Ok(Some(timestamp_field))
}

#[cfg(not(feature = "vrl"))]
struct VrlProgram {}

#[async_trait]
impl Actor for DocProcessor {
    type ObservableState = Arc<DocProcessorCounters>;

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
                let _ = ctx.send_exit_with_success(&self.indexer_mailbox).await;
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
        let mut processed_docs: Vec<ProcessedDoc> = Vec::with_capacity(raw_doc_batch.docs.len());

        for raw_doc in raw_doc_batch.docs {
            let _protected_zone_guard = ctx.protect_zone();
            self.process_raw_doc(raw_doc, &mut processed_docs);
            ctx.record_progress();
        }
        let processed_doc_batch = ProcessedDocBatch::new(
            processed_docs,
            raw_doc_batch.checkpoint_delta,
            raw_doc_batch.force_commit,
        );
        ctx.send_message(&self.indexer_mailbox, processed_doc_batch)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<NewPublishLock> for DocProcessor {
    type Reply = ();

    async fn handle(
        &mut self,
        message: NewPublishLock,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let NewPublishLock(publish_lock) = &message;
        self.publish_lock = publish_lock.clone();
        ctx.send_message(&self.indexer_mailbox, message).await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<NewPublishToken> for DocProcessor {
    type Reply = ();

    async fn handle(
        &mut self,
        message: NewPublishToken,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        ctx.send_message(&self.indexer_mailbox, message).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use prost::Message;
    use quickwit_actors::Universe;
    use quickwit_common::uri::Uri;
    use quickwit_config::{build_doc_mapper, SearchSettings};
    use quickwit_doc_mapper::{default_doc_mapper_for_test, DocMapper};
    use quickwit_metastore::checkpoint::SourceCheckpointDelta;
    use quickwit_opentelemetry::otlp::{OtlpGrpcLogsService, OtlpGrpcTracesService};
    use quickwit_proto::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
    use quickwit_proto::opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest;
    use quickwit_proto::opentelemetry::proto::common::v1::any_value::Value as OtlpAnyValueValue;
    use quickwit_proto::opentelemetry::proto::common::v1::AnyValue as OtlpAnyValue;
    use quickwit_proto::opentelemetry::proto::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use quickwit_proto::opentelemetry::proto::trace::v1::{ResourceSpans, ScopeSpans, Span};
    use serde_json::Value as JsonValue;
    use tantivy::schema::NamedFieldDocument;
    use tantivy::Document;

    use super::*;
    use crate::models::{PublishLock, RawDocBatch};

    #[tokio::test]
    async fn test_doc_processor_simple() {
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
            SourceInputFormat::Json,
        )
        .unwrap();
        let (doc_processor_mailbox, doc_processor_handle) =
            universe.spawn_builder().spawn(doc_processor);
        let checkpoint_delta = SourceCheckpointDelta::from_range(0..4);
        doc_processor_mailbox
            .send_message(RawDocBatch::for_test(
                &[
                    br#"{"body": "happy", "response_date": "2021-12-19T16:39:57+00:00", "response_time": 12, "response_payload": "YWJj"}"#, // missing timestamp
                    br#"{"body": "happy", "timestamp": 1628837062, "response_date": "2021-12-19T16:39:59+00:00", "response_time": 2, "response_payload": "YWJj"}"#, // ok
                    br#"{"body": "happy2", "timestamp": 1628837062, "response_date": "2021-12-19T16:40:57+00:00", "response_time": 13, "response_payload": "YWJj"}"#, // ok
                    b"{", // invalid json
                ],
                0..4,
            ))
            .await.unwrap();

        let counters = doc_processor_handle
            .process_pending_and_observe()
            .await
            .state;
        assert_eq!(counters.index_id, index_id);
        assert_eq!(counters.source_id, source_id);
        assert_eq!(counters.doc_mapper_errors.get_num_docs(), 1);
        assert_eq!(counters.json_parse_errors.get_num_docs(), 1);
        assert_eq!(counters.transform_errors.get_num_docs(), 0);
        assert_eq!(counters.otlp_parse_errors.get_num_docs(), 0);
        assert_eq!(counters.valid.get_num_docs(), 2);
        assert_eq!(counters.num_bytes_total.load(Ordering::Relaxed), 387);

        let output_messages = indexer_inbox.drain_for_test();
        assert_eq!(output_messages.len(), 1);
        let batch = *(output_messages
            .into_iter()
            .next()
            .unwrap()
            .downcast::<ProcessedDocBatch>()
            .unwrap());
        assert_eq!(batch.docs.len(), 2);
        assert_eq!(batch.checkpoint_delta, checkpoint_delta);

        let schema = doc_mapper.schema();
        let NamedFieldDocument(named_field_doc_map) = batch.docs[0].doc.to_named_doc(&schema);
        let doc_json = JsonValue::Object(doc_mapper.doc_to_json(named_field_doc_map).unwrap());
        assert_eq!(
            doc_json,
            serde_json::json!({
                "_source": {
                    "body": "happy",
                    "response_date": "2021-12-19T16:39:59Z",
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
        universe.assert_quit().await;
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
    async fn test_doc_processor_partitioning() {
        let doc_mapper: Arc<DocMapper> =
            Arc::new(serde_json::from_str::<DocMapper>(DOCMAPPER_WITH_PARTITION_JSON).unwrap());
        let universe = Universe::with_accelerated_time();
        let (indexer_mailbox, indexer_inbox) = universe.create_test_mailbox();
        let doc_processor = DocProcessor::try_new(
            "my-index".to_string(),
            "my-source".to_string(),
            doc_mapper,
            indexer_mailbox,
            None,
            SourceInputFormat::Json,
        )
        .unwrap();
        let (doc_processor_mailbox, doc_processor_handle) =
            universe.spawn_builder().spawn(doc_processor);
        doc_processor_mailbox
            .send_message(RawDocBatch::for_test(
                &[
                    br#"{"tenant": "tenant_1", "body": "first doc for tenant 1"}"#,
                    br#"{"tenant": "tenant_2", "body": "first doc for tenant 2"}"#,
                    br#"{"tenant": "tenant_1", "body": "second doc for tenant 1"}"#,
                    br#"{"tenant": "tenant_2", "body": "second doc for tenant 2"}"#,
                ],
                0..2,
            ))
            .await
            .unwrap();

        universe
            .send_exit_with_success(&doc_processor_mailbox)
            .await
            .unwrap();
        let (exit_status, _) = doc_processor_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Success));
        let processed_doc_batches: Vec<ProcessedDocBatch> = indexer_inbox.drain_for_test_typed();
        assert_eq!(processed_doc_batches.len(), 1);
        let partition_ids: Vec<u64> = processed_doc_batches[0]
            .docs
            .iter()
            .map(|doc| doc.partition)
            .collect();
        assert_eq!(partition_ids[0], partition_ids[2]);
        assert_eq!(partition_ids[1], partition_ids[3]);
        assert_ne!(partition_ids[0], partition_ids[1]);
        universe.assert_quit().await;
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
            SourceInputFormat::Json,
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
        universe.assert_quit().await;
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
            SourceInputFormat::Json,
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
            .send_message(RawDocBatch::for_test(
                &[
                    br#"{"body": "happy", "timestamp": 1628837062, "response_date": "2021-12-19T16:39:59+00:00", "response_time": 2, "response_payload": "YWJj"}"#,
                ],
                0..1,
            ))
            .await.unwrap();
        universe
            .send_exit_with_success(&doc_processor_mailbox)
            .await
            .unwrap();
        let (exit_status, _indexer_counters) = doc_processor_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Success));
        let indexer_messages: Vec<ProcessedDocBatch> = indexer_inbox.drain_for_test_typed();
        assert!(indexer_messages.is_empty());
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_doc_processor_otlp_logs_json() {
        let root_uri = Uri::for_test("ram:///indexes");
        let index_config = OtlpGrpcLogsService::index_config(&root_uri).unwrap();
        let doc_mapper =
            build_doc_mapper(&index_config.doc_mapping, &SearchSettings::default()).unwrap();

        let universe = Universe::with_accelerated_time();
        let (indexer_mailbox, indexer_inbox) = universe.create_test_mailbox();
        let doc_processor = DocProcessor::try_new(
            "my-index".to_string(),
            "my-source".to_string(),
            doc_mapper,
            indexer_mailbox,
            None,
            SourceInputFormat::OtlpLogsJson,
        )
        .unwrap();

        let (doc_processor_mailbox, doc_processor_handle) =
            universe.spawn_builder().spawn(doc_processor);

        let scope_logs = vec![ScopeLogs {
            log_records: vec![
                LogRecord {
                    time_unix_nano: 1_000_000_000,
                    body: Some(OtlpAnyValue {
                        value: Some(OtlpAnyValueValue::StringValue(
                            "foo log message".to_string(),
                        )),
                    }),
                    ..Default::default()
                },
                LogRecord {
                    time_unix_nano: 1_000_000_001,
                    body: Some(OtlpAnyValue {
                        value: Some(OtlpAnyValueValue::StringValue(
                            "bar log message".to_string(),
                        )),
                    }),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }];
        let resource_logs = vec![ResourceLogs {
            scope_logs,
            ..Default::default()
        }];
        let request = ExportLogsServiceRequest { resource_logs };
        let raw_doc_json = serde_json::to_vec(&request).unwrap();
        let raw_doc_batch = RawDocBatch::for_test(&[&raw_doc_json], 0..2);
        doc_processor_mailbox
            .send_message(raw_doc_batch)
            .await
            .unwrap();

        universe
            .send_exit_with_success(&doc_processor_mailbox)
            .await
            .unwrap();

        let counters = doc_processor_handle
            .process_pending_and_observe()
            .await
            .state;
        assert_eq!(counters.valid.get_num_docs(), 2);

        let batch = indexer_inbox.drain_for_test_typed::<ProcessedDocBatch>();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].docs.len(), 2);

        let (exit_status, _) = doc_processor_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Success));
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_doc_processor_otlp_logs_proto() {
        let root_uri = Uri::for_test("ram:///indexes");
        let index_config = OtlpGrpcLogsService::index_config(&root_uri).unwrap();
        let doc_mapper =
            build_doc_mapper(&index_config.doc_mapping, &SearchSettings::default()).unwrap();

        let universe = Universe::with_accelerated_time();
        let (indexer_mailbox, indexer_inbox) = universe.create_test_mailbox();
        let doc_processor = DocProcessor::try_new(
            "my-index".to_string(),
            "my-source".to_string(),
            doc_mapper,
            indexer_mailbox,
            None,
            SourceInputFormat::OtlpLogsProtobuf,
        )
        .unwrap();

        let (doc_processor_mailbox, doc_processor_handle) =
            universe.spawn_builder().spawn(doc_processor);

        let scope_logs = vec![ScopeLogs {
            log_records: vec![
                LogRecord {
                    time_unix_nano: 1_000_000_000,
                    body: Some(OtlpAnyValue {
                        value: Some(OtlpAnyValueValue::StringValue(
                            "foo log message".to_string(),
                        )),
                    }),
                    ..Default::default()
                },
                LogRecord {
                    time_unix_nano: 1_000_000_001,
                    body: Some(OtlpAnyValue {
                        value: Some(OtlpAnyValueValue::StringValue(
                            "bar log message".to_string(),
                        )),
                    }),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }];
        let resource_logs = vec![ResourceLogs {
            scope_logs,
            ..Default::default()
        }];
        let request = ExportLogsServiceRequest { resource_logs };
        let mut raw_doc_buffer = Vec::new();
        request.encode(&mut raw_doc_buffer).unwrap();

        let raw_doc_batch = RawDocBatch::for_test(&[&raw_doc_buffer], 0..2);
        doc_processor_mailbox
            .send_message(raw_doc_batch)
            .await
            .unwrap();

        universe
            .send_exit_with_success(&doc_processor_mailbox)
            .await
            .unwrap();

        let counters = doc_processor_handle
            .process_pending_and_observe()
            .await
            .state;
        assert_eq!(counters.valid.get_num_docs(), 2);

        let batch = indexer_inbox.drain_for_test_typed::<ProcessedDocBatch>();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].docs.len(), 2);

        let (exit_status, _) = doc_processor_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Success));
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_doc_processor_otlp_traces_json() {
        let root_uri = Uri::for_test("ram:///indexes");
        let index_config = OtlpGrpcTracesService::index_config(&root_uri).unwrap();
        let doc_mapper =
            build_doc_mapper(&index_config.doc_mapping, &SearchSettings::default()).unwrap();

        let universe = Universe::with_accelerated_time();
        let (indexer_mailbox, indexer_inbox) = universe.create_test_mailbox();
        let doc_processor = DocProcessor::try_new(
            "my-index".to_string(),
            "my-source".to_string(),
            doc_mapper,
            indexer_mailbox,
            None,
            SourceInputFormat::OtlpTracesJson,
        )
        .unwrap();

        let (doc_processor_mailbox, doc_processor_handle) =
            universe.spawn_builder().spawn(doc_processor);

        let scope_spans = vec![ScopeSpans {
            spans: vec![
                Span {
                    trace_id: vec![1; 16],
                    span_id: vec![2; 8],
                    start_time_unix_nano: 1_000_000_001,
                    end_time_unix_nano: 1_000_000_002,
                    ..Default::default()
                },
                Span {
                    trace_id: vec![3; 16],
                    span_id: vec![4; 8],
                    start_time_unix_nano: 2_000_000_001,
                    end_time_unix_nano: 2_000_000_002,
                    ..Default::default()
                },
            ],
            ..Default::default()
        }];
        let resource_spans = vec![ResourceSpans {
            scope_spans,
            ..Default::default()
        }];
        let request = ExportTraceServiceRequest { resource_spans };
        let raw_doc_json = serde_json::to_vec(&request).unwrap();
        let raw_doc_batch = RawDocBatch::for_test(&[&raw_doc_json], 0..2);
        doc_processor_mailbox
            .send_message(raw_doc_batch)
            .await
            .unwrap();

        universe
            .send_exit_with_success(&doc_processor_mailbox)
            .await
            .unwrap();

        let counters = doc_processor_handle
            .process_pending_and_observe()
            .await
            .state;
        assert_eq!(counters.valid.get_num_docs(), 2);

        let batch = indexer_inbox.drain_for_test_typed::<ProcessedDocBatch>();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].docs.len(), 2);

        let (exit_status, _) = doc_processor_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Success));
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_doc_processor_otlp_traces_proto() {
        let root_uri = Uri::for_test("ram:///indexes");
        let index_config = OtlpGrpcTracesService::index_config(&root_uri).unwrap();
        let doc_mapper =
            build_doc_mapper(&index_config.doc_mapping, &SearchSettings::default()).unwrap();

        let universe = Universe::with_accelerated_time();
        let (indexer_mailbox, indexer_inbox) = universe.create_test_mailbox();
        let doc_processor = DocProcessor::try_new(
            "my-index".to_string(),
            "my-source".to_string(),
            doc_mapper,
            indexer_mailbox,
            None,
            SourceInputFormat::OtlpTracesProtobuf,
        )
        .unwrap();

        let (doc_processor_mailbox, doc_processor_handle) =
            universe.spawn_builder().spawn(doc_processor);

        let scope_spans = vec![ScopeSpans {
            spans: vec![
                Span {
                    trace_id: vec![1; 16],
                    span_id: vec![2; 8],
                    start_time_unix_nano: 1_000_000_001,
                    end_time_unix_nano: 1_000_000_002,
                    ..Default::default()
                },
                Span {
                    trace_id: vec![3; 16],
                    span_id: vec![4; 8],
                    start_time_unix_nano: 2_000_000_001,
                    end_time_unix_nano: 2_000_000_002,
                    ..Default::default()
                },
            ],
            ..Default::default()
        }];
        let resource_spans = vec![ResourceSpans {
            scope_spans,
            ..Default::default()
        }];
        let request = ExportTraceServiceRequest { resource_spans };
        let mut raw_doc_buffer = Vec::new();
        request.encode(&mut raw_doc_buffer).unwrap();

        let raw_doc_batch = RawDocBatch::for_test(&[&raw_doc_buffer], 0..2);
        doc_processor_mailbox
            .send_message(raw_doc_batch)
            .await
            .unwrap();

        universe
            .send_exit_with_success(&doc_processor_mailbox)
            .await
            .unwrap();

        let counters = doc_processor_handle
            .process_pending_and_observe()
            .await
            .state;
        assert_eq!(counters.valid.get_num_docs(), 2);

        let batch = indexer_inbox.drain_for_test_typed::<ProcessedDocBatch>();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].docs.len(), 2);

        let (exit_status, _) = doc_processor_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Success));
        universe.assert_quit().await;
    }
}

#[cfg(feature = "vrl")]
#[cfg(test)]
mod tests_vrl {
    use quickwit_actors::Universe;
    use quickwit_doc_mapper::default_doc_mapper_for_test;
    use quickwit_metastore::checkpoint::SourceCheckpointDelta;
    use tantivy::schema::NamedFieldDocument;
    use tantivy::Document;

    use super::*;

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
            SourceInputFormat::Json,
        )
        .unwrap();
        let (doc_processor_mailbox, doc_processor_handle) =
            universe.spawn_builder().spawn(doc_processor);
        doc_processor_mailbox
            .send_message(RawDocBatch::for_test(
                &[
                    br#"{"body": "happy", "response_date": "2021-12-19T16:39:57+00:00", "response_time": 12, "response_payload": "YWJj"}"#, // missing timestamp
                    br#"{"body": "happy using VRL", "timestamp": 1628837062, "response_date": "2021-12-19T16:39:59+00:00", "response_time": 2, "response_payload": "YWJj"}"#, // ok
                    br#"{"body": "happy2", "timestamp": 1628837062, "response_date": "2021-12-19T16:40:57+00:00", "response_time": 13, "response_payload": "YWJj"}"#, // ok
                    b"{", // invalid json
                ],
                0..4,
            ))
            .await?;
        let counters = doc_processor_handle
            .process_pending_and_observe()
            .await
            .state;
        assert_eq!(counters.index_id, index_id.to_string());
        assert_eq!(counters.source_id, source_id.to_string());
        assert_eq!(counters.doc_mapper_errors.get_num_docs(), 1);
        assert_eq!(counters.json_parse_errors.get_num_docs(), 1);
        assert_eq!(counters.transform_errors.get_num_docs(), 0);
        assert_eq!(counters.otlp_parse_errors.get_num_docs(), 0);
        assert_eq!(counters.valid.get_num_docs(), 2);
        assert_eq!(counters.num_bytes_total.load(Ordering::Relaxed), 397);

        let output_messages = indexer_inbox.drain_for_test();
        assert_eq!(output_messages.len(), 1);
        let batch = *(output_messages
            .into_iter()
            .next()
            .unwrap()
            .downcast::<ProcessedDocBatch>()
            .unwrap());
        assert_eq!(batch.docs.len(), 2);
        assert_eq!(
            batch.checkpoint_delta,
            SourceCheckpointDelta::from_range(0..4)
        );

        let schema = doc_mapper.schema();
        let NamedFieldDocument(named_field_doc_map) = batch.docs[0].doc.to_named_doc(&schema);
        let doc_json = JsonValue::Object(doc_mapper.doc_to_json(named_field_doc_map)?);
        assert_eq!(
            doc_json,
            serde_json::json!({
                "_source": {
                    "body": "HAPPY USING VRL",
                    "response_date": "2021-12-19T16:39:59Z",
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
        universe.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_doc_processor_with_plain_text_input() {
        let index_id = "my-index";
        let source_id = "my-source";
        let universe = Universe::with_accelerated_time();
        let (indexer_mailbox, indexer_inbox) = universe.create_test_mailbox();
        let doc_mapper = Arc::new(default_doc_mapper_for_test());
        let vrl_script = r#"
            values = parse_csv!(.plain_text)
            .body = upcase(string!(values[0]))
            .timestamp = to_int!(values[1])
            .response_date = values[2]
            .response_time = to_int!(values[3])
            .response_payload = values[4]
            del(.plain_text)
        "#;

        let transform_config = TransformConfig::for_test(vrl_script);
        let doc_processor = DocProcessor::try_new(
            index_id.to_string(),
            source_id.to_string(),
            doc_mapper.clone(),
            indexer_mailbox,
            Some(transform_config),
            SourceInputFormat::PlainText,
        )
        .unwrap();
        let (doc_processor_mailbox, doc_processor_handle) =
            universe.spawn_builder().spawn(doc_processor);
        doc_processor_mailbox
            .send_message(RawDocBatch::for_test(
                &[
                    // body,timestamp,response_date,response_time,response_payload
                    br#""happy using VRL",1628837062,"2021-12-19T16:39:59+00:00",2,"YWJj""#,
                    br#""happy2",1628837062,"2021-12-19T16:40:57+00:00",13,"YWJj""#,
                    br#""happy2",1628837062,"2021-12-19T16:40:57+00:00","invalid-response_time","YWJj""#,
                ],
                0..4,
            ))
            .await.unwrap();
        let counters = doc_processor_handle
            .process_pending_and_observe()
            .await
            .state;
        assert_eq!(counters.index_id, index_id);
        assert_eq!(counters.source_id, source_id);
        assert_eq!(counters.doc_mapper_errors.get_num_docs(), 0,);
        assert_eq!(counters.transform_errors.get_num_docs(), 1,);
        assert_eq!(counters.otlp_parse_errors.get_num_docs(), 0,);
        assert_eq!(counters.valid.get_num_docs(), 2,);
        assert_eq!(counters.num_bytes_total.load(Ordering::Relaxed), 200,);

        let output_messages = indexer_inbox.drain_for_test();
        assert_eq!(output_messages.len(), 1);
        let batch = *(output_messages
            .into_iter()
            .next()
            .unwrap()
            .downcast::<ProcessedDocBatch>()
            .unwrap());
        assert_eq!(batch.docs.len(), 2);
        assert_eq!(
            batch.checkpoint_delta,
            SourceCheckpointDelta::from_range(0..4)
        );

        let schema = doc_mapper.schema();
        let NamedFieldDocument(named_field_doc_map) = batch.docs[0].doc.to_named_doc(&schema);
        let doc_json = JsonValue::Object(doc_mapper.doc_to_json(named_field_doc_map).unwrap());
        assert_eq!(
            doc_json,
            serde_json::json!({
                "_source": {
                    "body": "HAPPY USING VRL",
                    "response_date": "2021-12-19T16:39:59Z",
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
        universe.assert_quit().await;
    }
}
