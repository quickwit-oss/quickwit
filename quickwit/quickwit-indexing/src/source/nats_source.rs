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

//! A source consuming a NATS JetStream stream through a pre-provisioned
//! durable consumer.
//!
//! The consumer is only ever fetched so its lifecycle, subject filters,
//! deliver policy, and ack tuning (`ack_wait`, `max_ack_pending`) belong to
//! whoever provisioned it.
//!
//! Several indexing pipelines can share the consumer: NATS load-balances the
//! messages across them, so scaling is a plain `num_pipelines` update. Being
//! durable, the consumer is observable through NATS's own monitoring even
//! while the pipelines are down.
//!
//! When a message carries a W3C `traceparent` header, the source stitches the
//! processing of the message into the publisher's distributed trace.

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::{Context as _, anyhow, bail, ensure};
use async_nats::header::{HeaderMap, HeaderName};
use async_nats::jetstream::consumer::pull::{
    MessagesError, MessagesErrorKind, Stream as DurableMessageStream,
};
use async_nats::jetstream::consumer::{AckPolicy, PullConsumer};
use async_nats::jetstream::message::AckKind;
use async_nats::{ConnectOptions, Subject, jetstream};
use async_trait::async_trait;
use futures::{FutureExt, StreamExt};
use opentelemetry::propagation::Extractor;
use opentelemetry::trace::TraceContextExt;
use opentelemetry::{Context as OtelContext, global};
use quickwit_actors::ActorExitStatus;
use quickwit_common::rand::append_random_suffix;
use quickwit_config::{NatsSourceAuth, NatsSourceParams};
use quickwit_metastore::checkpoint::{PartitionId, SourceCheckpoint};
use quickwit_proto::metastore::SourceType;
use quickwit_proto::types::Position;
use serde_json::{Value as JsonValue, json};
use tokio::time;
use tracing::{Span, debug, info, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::source::{
    BATCH_NUM_BYTES_LIMIT, BatchBuilder, EMIT_BATCHES_TIMEOUT, Source, SourceContext,
    SourceRuntime, SourceSink, TypedSourceFactory,
};

pub struct NatsSourceFactory;

#[async_trait]
impl TypedSourceFactory for NatsSourceFactory {
    type Source = NatsSource;
    type Params = NatsSourceParams;

    async fn typed_create_source(
        source_runtime: SourceRuntime,
        source_params: NatsSourceParams,
    ) -> anyhow::Result<Self::Source> {
        NatsSource::try_new(source_runtime, source_params).await
    }
}

#[derive(Default, Debug)]
pub struct NatsSourceState {
    /// Number of bytes processed by the source.
    pub num_bytes_processed: u64,
    /// Number of messages processed by the source (including invalid messages).
    pub num_messages_processed: u64,
    /// Number of invalid messages, i.e., that were empty.
    pub num_invalid_messages: u64,
}

pub struct NatsSource {
    source_runtime: SourceRuntime,
    source_params: NatsSourceParams,
    nats_client: async_nats::Client,
    message_stream: DurableMessageStream,
    consumer_name: String,
    partition_id: PartitionId,
    delivery_counter: u64,
    /// Ack subjects of the messages delivered but not published yet, keyed by
    /// delivery counter. Bounded by the consumer's `max_ack_pending`: the
    /// server stops delivering when too many messages are unacknowledged.
    pending_acks: BTreeMap<u64, Subject>,
    state: NatsSourceState,
}

impl fmt::Debug for NatsSource {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("NatsSource")
            .field("index_uid", self.source_runtime.index_uid())
            .field("source_id", &self.source_runtime.source_id())
            .field("stream", &self.source_params.stream)
            .field("consumer_name", &self.consumer_name)
            .finish()
    }
}

impl NatsSource {
    pub async fn try_new(
        source_runtime: SourceRuntime,
        source_params: NatsSourceParams,
    ) -> anyhow::Result<Self> {
        let consumer_name = source_params.consumer.clone();

        info!(
            index_id=%source_runtime.index_id(),
            source_id=%source_runtime.source_id(),
            stream=%source_params.stream,
            %consumer_name,
            "starting NATS source"
        );

        let (nats_client, consumer) = connect_and_fetch_consumer(&source_params).await?;
        let message_stream = consumer
            .messages()
            .await
            .context("failed to subscribe to NATS consumer messages")?;

        // A fresh partition per pipeline: durable-mode positions are delivery
        // counters local to this instance, and pipelines sharing the consumer
        // must not collide on a partition.
        let partition_id =
            PartitionId::from(append_random_suffix(&format!("nats-{consumer_name}")));

        Ok(NatsSource {
            source_runtime,
            source_params,
            nats_client,
            message_stream,
            consumer_name,
            partition_id,
            delivery_counter: 0,
            pending_acks: BTreeMap::new(),
            state: NatsSourceState::default(),
        })
    }

    fn process_message(
        &mut self,
        message: jetstream::Message,
        batch: &mut BatchBuilder,
    ) -> anyhow::Result<()> {
        let stream_sequence = message
            .info()
            .map_err(|error| anyhow!("failed to parse NATS message metadata: {error}"))?
            .stream_sequence;
        let _span_guard = remote_parented_span(&message, stream_sequence, &self.source_runtime)
            .map(Span::entered);
        let Some(ack_subject) = message.message.reply else {
            bail!("NATS message carries no reply subject to acknowledge it on");
        };
        let doc = message.message.payload;
        let num_bytes = doc.len() as u64;

        if doc.is_empty() {
            warn!("message received from NATS was empty");
            self.state.num_invalid_messages += 1;
        } else {
            batch.add_doc(doc);
        }
        let from_position = if self.delivery_counter == 0 {
            Position::Beginning
        } else {
            Position::offset(self.delivery_counter)
        };
        self.delivery_counter += 1;
        batch
            .checkpoint_delta
            .record_partition_delta(
                self.partition_id.clone(),
                from_position,
                Position::offset(self.delivery_counter),
            )
            .context("failed to record partition delta")?;
        self.pending_acks.insert(self.delivery_counter, ack_subject);

        self.state.num_bytes_processed += num_bytes;
        self.state.num_messages_processed += 1;

        Ok(())
    }
}

#[async_trait]
impl Source for NatsSource {
    async fn emit_batches(
        &mut self,
        source_sink: &SourceSink,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        let now = Instant::now();
        let mut batch_builder = BatchBuilder::new(SourceType::Nats);
        let deadline = time::sleep(*EMIT_BATCHES_TIMEOUT);
        tokio::pin!(deadline);
        let mut wait_before_next_batch = Duration::default();

        loop {
            tokio::select! {
                message_res_opt = self.message_stream.next() => {
                    let message_res = message_res_opt
                        .ok_or_else(|| ActorExitStatus::from(anyhow!("NATS message stream ended unexpectedly")))?;
                    let message = match message_res {
                        Ok(message) => message,
                        Err(error) if is_transient_stream_error(&error) => {
                            warn!(%error, "transient NATS message stream error");
                            wait_before_next_batch = TRANSIENT_STREAM_ERROR_BACKOFF;
                            break;
                        }
                        Err(error) => {
                            return Err(ActorExitStatus::from(anyhow!(
                                "failed to pull message from NATS consumer: {error}"
                            )));
                        }
                    };
                    self.process_message(message, &mut batch_builder).map_err(ActorExitStatus::from)?;

                    if batch_builder.num_bytes >= BATCH_NUM_BYTES_LIMIT {
                        break;
                    }
                }
                _ = &mut deadline => {
                    break;
                }
            }
            ctx.record_progress();
        }

        if !batch_builder.checkpoint_delta.is_empty() {
            debug!(
                num_docs=%batch_builder.docs.len(),
                num_bytes=%batch_builder.num_bytes,
                num_millis=%now.elapsed().as_millis(),
                "sending doc batch to indexer"
            );
            let message = batch_builder.build();
            source_sink.send_raw_doc_batch(message, ctx).await?;
        }
        Ok(wait_before_next_batch)
    }

    async fn suggest_truncate(
        &mut self,
        checkpoint: SourceCheckpoint,
        ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        let Some(position) = checkpoint.position_for_partition(&self.partition_id) else {
            return Ok(());
        };
        let Some(published_up_to) = position.as_u64() else {
            return Ok(());
        };
        ctx.protect_future(ack_up_to(
            &self.nats_client,
            &mut self.pending_acks,
            published_up_to,
        ))
        .await;
        Ok(())
    }

    fn should_be_drained(&self) -> bool {
        true
    }

    fn is_drained(&self) -> bool {
        // Every delivered message is pending from `process_message` until the
        // server confirmed its ack: an empty map means everything delivered so
        // far has been published and acknowledged.
        self.pending_acks.is_empty()
    }

    fn name(&self) -> String {
        format!("{self:?}")
    }

    async fn finalize(
        &mut self,
        _exit_status: &ActorExitStatus,
        _ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        // The durable consumer itself is left untouched. Any ack still pending
        // at this point was not settled by a drain: its message is redelivered
        // after the consumer's `ack_wait`.
        //
        // Messages the client prefetched but that were never processed are
        // negatively acknowledged so the server redelivers them right after
        // this connection is gone, instead of after `ack_wait`. Only what is
        // already buffered is NAK'd: the loop stops at the first pending poll,
        // so a refill in flight cannot keep it alive.
        while let Some(Some(message_res)) = self.message_stream.next().now_or_never() {
            let Ok(message) = message_res else {
                continue;
            };
            let Some(ack_subject) = message.message.reply else {
                continue;
            };
            let _ = self
                .nats_client
                .publish(ack_subject, AckKind::Nak(Some(NAK_REDELIVERY_DELAY)).into())
                .await;
        }
        let _ = self.nats_client.drain().await;
        Ok(())
    }

    fn observable_state(&self) -> JsonValue {
        let num_pending_acks = self.pending_acks.len();
        json!({
            "index_id": self.source_runtime.index_id(),
            "source_id": self.source_runtime.source_id(),
            "stream": self.source_params.stream,
            "consumer_name": self.consumer_name,
            "num_bytes_processed": self.state.num_bytes_processed,
            "num_messages_processed": self.state.num_messages_processed,
            "num_invalid_messages": self.state.num_invalid_messages,
            "num_pending_acks": num_pending_acks,
        })
    }
}

/// Bounds the concurrent server-confirmed ack requests: enough to hide the
/// round-trip latency without flooding the connection.
const MAX_CONCURRENT_ACK_REQUESTS: usize = 128;

/// Pause before pulling again after a transient stream error, to avoid a hot
/// error loop while the connection recovers. `SuggestTruncate` is still
/// processed in the meantime.
const TRANSIENT_STREAM_ERROR_BACKOFF: Duration = Duration::from_secs(1);

/// Delay before the server redelivers a negatively acknowledged message: long
/// enough for this connection to be gone, so the redelivery goes to a
/// surviving pipeline instead of bouncing back into this one's outstanding
/// pull request.
const NAK_REDELIVERY_DELAY: Duration = Duration::from_secs(1);

/// Errors the pull stream recovers from on its own: the subscription stays
/// usable and polling simply resumes. `MissingHeartbeat` in particular also
/// fires when the stream was not polled for a while (e.g. under downstream
/// backpressure), not only when the server is unreachable.
fn is_transient_stream_error(error: &MessagesError) -> bool {
    matches!(
        error.kind(),
        MessagesErrorKind::MissingHeartbeat
            | MessagesErrorKind::Pull
            | MessagesErrorKind::NoResponders
    )
}

/// Sends server-confirmed acknowledgments ("double acks") for the messages up
/// to `published_up_to`.
///
/// Entries only leave the pending map once the server confirmed their ack, so
/// `is_drained` never reports progress that could still be lost: a
/// fire-and-forget ack can be dropped by a reconnection even after a
/// successful client-side flush. A retried ack for a message the server
/// already saw acknowledged is simply confirmed again. Whatever could not be
/// acknowledged is redelivered after the consumer's `ack_wait`.
async fn ack_up_to(
    nats_client: &async_nats::Client,
    pending_acks: &mut BTreeMap<u64, Subject>,
    published_up_to: u64,
) {
    let acks: Vec<(u64, Subject)> = pending_acks
        .range(..=published_up_to)
        .map(|(delivery_counter, ack_subject)| (*delivery_counter, ack_subject.clone()))
        .collect();
    if acks.is_empty() {
        return;
    }
    let mut ack_results = futures::stream::iter(acks)
        .map(|(delivery_counter, ack_subject)| async move {
            // Mirrors `jetstream::Message::double_ack()`: a request instead of
            // a plain publish, so the server's reply confirms the ack.
            let ack_result = nats_client.request(ack_subject, AckKind::Ack.into()).await;
            (delivery_counter, ack_result)
        })
        .buffer_unordered(MAX_CONCURRENT_ACK_REQUESTS);
    let mut num_acks = 0usize;
    let mut num_failed_acks = 0usize;
    let mut last_ack_error = None;
    while let Some((delivery_counter, ack_result)) = ack_results.next().await {
        match ack_result {
            Ok(_ack_reply) => {
                pending_acks.remove(&delivery_counter);
                num_acks += 1;
            }
            Err(error) => {
                num_failed_acks += 1;
                last_ack_error = Some(error);
            }
        }
    }
    if let Some(error) = last_ack_error {
        warn!(%error, num_failed_acks, "failed to ack NATS messages");
    }
    debug!(num_acks, "acked published messages");
}

/// Fetches the pre-provisioned durable consumer.
async fn fetch_durable_consumer(
    jetstream_stream: &jetstream::stream::Stream,
    consumer_name: &str,
) -> anyhow::Result<PullConsumer> {
    let consumer: PullConsumer = jetstream_stream
        .get_consumer(consumer_name)
        .await
        .map_err(|error| anyhow!("failed to find NATS consumer `{consumer_name}`: {error}"))?;
    let ack_policy = consumer.cached_info().config.ack_policy;
    ensure!(
        ack_policy == AckPolicy::Explicit,
        "NATS consumer `{consumer_name}` must use the explicit ack policy, got `{ack_policy:?}`"
    );
    Ok(consumer)
}

/// W3C trace context extraction from NATS message headers, following the
/// pattern of `quickwit_common::tracing_utils` for gRPC metadata.
struct NatsHeaderExtractor<'a>(&'a HeaderMap);

impl Extractor for NatsHeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        let header_name = HeaderName::from_str(key).ok()?;
        self.0.get(header_name).map(|value| value.as_str())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.iter().map(|(key, _)| key.as_ref()).collect()
    }
}

fn extract_remote_context(headers: &HeaderMap) -> OtelContext {
    global::get_text_map_propagator(|propagator| propagator.extract(&NatsHeaderExtractor(headers)))
}

/// Builds a span parented on the publisher's trace when the message carries
/// a W3C `traceparent` header, stitching the processing of the message into
/// the publisher's distributed trace. Messages without a propagated context
/// cost nothing: no span is created.
fn remote_parented_span(
    message: &jetstream::Message,
    stream_sequence: u64,
    source_runtime: &SourceRuntime,
) -> Option<Span> {
    let headers = message.headers.as_ref()?;
    let parent_context = extract_remote_context(headers);
    if !parent_context.span().span_context().is_valid() {
        return None;
    }
    let span = tracing::info_span!(
        "process_nats_message",
        index_id = %source_runtime.index_id(),
        source_id = %source_runtime.source_id(),
        subject = %message.subject,
        stream_sequence,
    );
    let _ = span.set_parent(parent_context);
    Some(span)
}

async fn connect_nats(params: &NatsSourceParams) -> anyhow::Result<async_nats::Client> {
    let mut connect_options = ConnectOptions::new();
    match params.authentication.clone() {
        None => {}
        Some(NatsSourceAuth::UserPassword { user, password }) => {
            connect_options = connect_options.user_and_password(user, password);
        }
        Some(NatsSourceAuth::Token(token)) => {
            connect_options = connect_options.token(token);
        }
    }
    if let Some(tls) = &params.tls {
        if let Some(ca_certificates_path) = &tls.ca_certificates_path {
            connect_options =
                connect_options.add_root_certificates(PathBuf::from(ca_certificates_path));
        }
        if let (Some(certificate_path), Some(key_path)) =
            (&tls.client_certificate_path, &tls.client_key_path)
        {
            connect_options = connect_options
                .add_client_certificate(PathBuf::from(certificate_path), PathBuf::from(key_path));
        }
    }
    let client = async_nats::connect_with_options(&params.uris, connect_options)
        .await
        .with_context(|| {
            format!(
                "failed to connect to NATS servers `{}`",
                params.uris.join(", ")
            )
        })?;
    Ok(client)
}

/// Connects to the NATS servers and binds to the JetStream stream's
/// pre-provisioned durable consumer.
async fn connect_and_fetch_consumer(
    params: &NatsSourceParams,
) -> anyhow::Result<(async_nats::Client, PullConsumer)> {
    let nats_client = connect_nats(params).await?;
    let jetstream_ctx = jetstream::new(nats_client.clone());
    let jetstream_stream = jetstream_ctx
        .get_stream(&params.stream)
        .await
        .with_context(|| format!("failed to find NATS JetStream stream `{}`", params.stream))?;
    let consumer = fetch_durable_consumer(&jetstream_stream, &params.consumer).await?;
    Ok((nats_client, consumer))
}

/// Checks whether we can connect to the NATS servers and find the JetStream
/// stream and the pre-provisioned consumer.
pub(crate) async fn check_connectivity(params: &NatsSourceParams) -> anyhow::Result<()> {
    connect_and_fetch_consumer(params).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_trace_context_from_headers() {
        use opentelemetry::propagation::TextMapPropagator;
        use opentelemetry_sdk::propagation::TraceContextPropagator;

        // The global propagator is not installed in tests, so the extractor
        // is exercised against an explicit W3C propagator.
        let propagator = TraceContextPropagator::new();

        let mut headers = HeaderMap::new();
        headers.insert(
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        );
        let span_context = propagator
            .extract(&NatsHeaderExtractor(&headers))
            .span()
            .span_context()
            .clone();
        assert!(span_context.is_valid());
        assert_eq!(
            span_context.trace_id().to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
        assert_eq!(span_context.span_id().to_string(), "00f067aa0ba902b7");

        let empty_headers = HeaderMap::new();
        let span_context = propagator
            .extract(&NatsHeaderExtractor(&empty_headers))
            .span()
            .span_context()
            .clone();
        assert!(!span_context.is_valid());
    }
}

#[cfg(all(test, feature = "nats-broker-tests"))]
mod nats_broker_tests {
    use std::num::NonZeroUsize;
    use std::ops::Range;
    use std::sync::Arc;

    use bytes::Bytes;
    use quickwit_actors::{ActorHandle, Inbox, Universe};
    use quickwit_config::{SourceConfig, SourceInputFormat, SourceParams};
    use quickwit_metastore::metastore_for_test;
    use quickwit_proto::metastore::MetastoreServiceClient;
    use quickwit_proto::types::IndexUid;

    use super::*;
    use crate::actors::DocProcessor;
    use crate::models::RawDocBatch;
    use crate::source::test_setup_helper::setup_index;
    use crate::source::tests::SourceRuntimeBuilder;
    use crate::source::{SourceActor, SuggestTruncate, quickwit_supported_sources};

    static NATS_URI: &str = "nats://localhost:4222";

    async fn setup_nats_stream(stream_name: &str) -> jetstream::Context {
        let client = async_nats::connect(NATS_URI).await.unwrap();
        let jetstream_ctx = jetstream::new(client);
        jetstream_ctx
            .create_stream(jetstream::stream::Config {
                name: stream_name.to_string(),
                subjects: vec![format!("{stream_name}.>")],
                ..Default::default()
            })
            .await
            .unwrap();
        jetstream_ctx
    }

    /// Publishes one JSON doc per ID on the subject and waits for each
    /// publish ack, so stream sequences are assigned in `ids` order. Messages
    /// carry a W3C `traceparent` header to exercise the trace propagation
    /// path.
    async fn publish_docs(
        jetstream_ctx: &jetstream::Context,
        subject: &str,
        ids: Range<usize>,
    ) -> Vec<String> {
        let mut headers = HeaderMap::new();
        headers.insert(
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        );
        let mut docs = Vec::with_capacity(ids.len());
        for id in ids {
            let doc = json!({ "id": id, "subject": subject }).to_string();
            jetstream_ctx
                .publish_with_headers(subject.to_string(), headers.clone(), doc.clone().into())
                .await
                .unwrap()
                .await
                .unwrap();
            docs.push(doc);
        }
        docs
    }

    async fn create_source_actor(
        universe: &Universe,
        metastore: MetastoreServiceClient,
        index_uid: IndexUid,
        source_config: SourceConfig,
    ) -> (ActorHandle<SourceActor>, Inbox<DocProcessor>) {
        let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config)
            .with_metastore(metastore)
            .build();
        let source = quickwit_supported_sources()
            .load_source(source_runtime)
            .await
            .unwrap();
        let (doc_processor_mailbox, doc_processor_inbox) = universe.create_test_mailbox();
        let source_actor = SourceActor::new(source, doc_processor_mailbox);
        let (_source_mailbox, source_handle) = universe.spawn_builder().spawn(source_actor);
        (source_handle, doc_processor_inbox)
    }

    async fn wait_for_processed_messages(
        source_handle: &ActorHandle<SourceActor>,
        num_expected: u64,
    ) {
        loop {
            let observation = source_handle.observe().await;
            let num_messages_processed = observation
                .state
                .get("num_messages_processed")
                .unwrap()
                .as_u64()
                .unwrap();
            if num_messages_processed >= num_expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn merge_doc_batches(batches: Vec<RawDocBatch>) -> RawDocBatch {
        let mut merged_batch = RawDocBatch::default();
        for batch in batches {
            merged_batch.docs.extend(batch.docs);
            merged_batch
                .checkpoint_delta
                .extend(batch.checkpoint_delta)
                .unwrap();
        }
        merged_batch.docs.sort();
        merged_batch
    }

    fn get_durable_source_config(stream: &str, consumer: &str) -> SourceConfig {
        let source_id = append_random_suffix("test-nats-source--durable-source");
        SourceConfig {
            source_id,
            num_pipelines: NonZeroUsize::MIN,
            enabled: true,
            source_params: SourceParams::Nats(NatsSourceParams {
                uris: vec![NATS_URI.to_string()],
                stream: stream.to_string(),
                consumer: consumer.to_string(),
                tls: None,
                authentication: None,
            }),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        }
    }

    /// Provisions the durable consumer the way an operator would: the source
    /// itself only ever fetches it.
    async fn provision_durable_consumer(
        jetstream_ctx: &jetstream::Context,
        stream: &str,
        consumer_name: &str,
    ) {
        jetstream_ctx
            .create_consumer_on_stream(
                jetstream::consumer::pull::Config {
                    name: Some(consumer_name.to_string()),
                    durable_name: Some(consumer_name.to_string()),
                    ack_policy: AckPolicy::Explicit,
                    // Long enough for the tests to never hit a redelivery.
                    ack_wait: Duration::from_secs(300),
                    ..Default::default()
                },
                stream,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_durable_mode_ingestion_and_ack() {
        let universe = Universe::with_accelerated_time();
        let metastore = metastore_for_test();
        let stream = append_random_suffix("test-nats-source--durable--stream");
        let jetstream_ctx = setup_nats_stream(&stream).await;
        let consumer_name = "durable-ack-consumer";
        provision_durable_consumer(&jetstream_ctx, &stream, consumer_name).await;

        let subject = format!("{stream}.logs");
        let expected_docs = publish_docs(&jetstream_ctx, &subject, 0..10).await;

        let index_id = append_random_suffix("test-nats-source--durable--index");
        let source_config = get_durable_source_config(&stream, consumer_name);
        let index_uid = setup_index(metastore.clone(), &index_id, &source_config, &[]).await;

        let (source_handle, doc_processor_inbox) =
            create_source_actor(&universe, metastore, index_uid, source_config).await;

        wait_for_processed_messages(&source_handle, 10).await;

        let batches: Vec<RawDocBatch> = doc_processor_inbox.drain_for_test_typed();
        let batch = merge_doc_batches(batches);
        assert_eq!(batch.docs, expected_docs);
        // Positions are synthetic delivery counters on a per-pipeline
        // partition, not stream sequences.
        assert_eq!(batch.checkpoint_delta.num_partitions(), 1);

        // A `SuggestTruncate` simulates the split publication notification:
        // it must release the acks of the published messages.
        let checkpoint = batch.checkpoint_delta.get_source_checkpoint();
        source_handle
            .mailbox()
            .send_message(SuggestTruncate(checkpoint))
            .await
            .unwrap();

        let mut consumer: PullConsumer = jetstream_ctx
            .get_consumer_from_stream(consumer_name, stream.as_str())
            .await
            .unwrap();
        let mut acked = false;
        for _ in 0..100 {
            let consumer_info = consumer.info().await.unwrap();
            if consumer_info.num_ack_pending == 0 {
                assert_eq!(consumer_info.ack_floor.stream_sequence, 10);
                acked = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(acked, "messages should be acked after truncation");

        source_handle.quit().await;
        jetstream_ctx.delete_stream(&stream).await.unwrap();
        universe.assert_quit().await;
    }

    /// End-to-end drain: a full indexing pipeline is asked to drain, which
    /// must publish the in-flight batches and flush their acks BEFORE the
    /// drain replies — the exactly-once guarantee on planned teardowns.
    #[tokio::test]
    async fn test_durable_mode_graceful_drain_acks_before_teardown() {
        use quickwit_actors::Universe;
        use quickwit_common::temp_dir::TempDirectory;
        use quickwit_config::IndexingSettings;
        use quickwit_doc_mapper::default_doc_mapper_for_test;
        use quickwit_ingest::IngesterPool;
        use quickwit_proto::indexing::IndexingPipelineId;
        use quickwit_proto::types::{NodeId, PipelineUid};
        use quickwit_storage::{RamStorage, StorageResolver};

        use crate::actors::pipeline_shared::DrainPipeline;
        use crate::merge_policy::default_merge_policy;
        use crate::{IndexingPipeline, IndexingPipelineParams, IndexingSplitStore};

        let universe = Universe::with_accelerated_time();
        let metastore = metastore_for_test();
        let stream = append_random_suffix("test-nats-source--durable-drain--stream");
        let jetstream_ctx = setup_nats_stream(&stream).await;
        let consumer_name = "durable-drain-consumer";
        provision_durable_consumer(&jetstream_ctx, &stream, consumer_name).await;

        let index_id = append_random_suffix("test-nats-source--durable-drain--index");
        let source_config = get_durable_source_config(&stream, consumer_name);
        let index_uid = setup_index(metastore.clone(), &index_id, &source_config, &[]).await;

        // Docs valid for the test doc mapper, so the pipeline indexes them.
        let subject = format!("{stream}.logs");
        for id in 0..10 {
            let doc = json!({"timestamp": 1_700_000_000 + id, "body": format!("drain test {id}")})
                .to_string();
            jetstream_ctx
                .publish(subject.clone(), doc.into())
                .await
                .unwrap()
                .await
                .unwrap();
        }

        let pipeline_id = IndexingPipelineId {
            node_id: NodeId::from_str("test-node"),
            index_uid,
            source_id: source_config.source_id.clone(),
            pipeline_uid: PipelineUid::for_test(0u128),
        };
        let storage = Arc::new(RamStorage::default());
        let split_store = IndexingSplitStore::create_without_local_store_for_test(storage.clone());
        let (merge_planner_mailbox, _merge_planner_inbox) = universe.create_test_mailbox();
        let pipeline_params = IndexingPipelineParams {
            pipeline_id,
            doc_mapper: Arc::new(default_doc_mapper_for_test()),
            source_config,
            source_storage_resolver: StorageResolver::for_test(),
            indexing_directory: TempDirectory::for_test(),
            indexing_settings: IndexingSettings::for_test(),
            fingerprinter_opt: None,
            ingester_pool: IngesterPool::default(),
            metastore,
            queues_dir_path: PathBuf::from("./queues"),
            storage,
            split_store,
            merge_policy: default_merge_policy(),
            retention_policy: None,
            max_concurrent_split_uploads_index: 4,
            max_concurrent_split_uploads_merge: 5,
            cooperative_indexing_permits: None,
            merge_planner_mailbox_opt: Some(merge_planner_mailbox),
            event_broker: Default::default(),
            params_fingerprint: 42u64,
        };
        let pipeline = IndexingPipeline::new(pipeline_params);
        let (pipeline_mailbox, pipeline_handle) = universe.spawn_builder().spawn(pipeline);

        // Wait for the source to have delivered all the messages. The commit
        // timeout has not elapsed: without the drain, nothing would have been
        // published nor acked yet.
        loop {
            let observation = pipeline_handle.observe().await;
            if observation.num_docs >= 10 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        pipeline_mailbox
            .send_message(DrainPipeline {
                drain_timeout: Duration::from_secs(60),
            })
            .await
            .unwrap();
        // The pipeline exits on its own once the in-flight batches are
        // published and their acks flushed.
        let (exit_status, _statistics) = pipeline_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Success));

        // The pipeline exit implies the acks are already flushed: no polling,
        // no grace period.
        let mut consumer: PullConsumer = jetstream_ctx
            .get_consumer_from_stream(consumer_name, stream.as_str())
            .await
            .unwrap();
        let consumer_info = consumer.info().await.unwrap();
        assert_eq!(consumer_info.num_ack_pending, 0);
        assert_eq!(consumer_info.ack_floor.stream_sequence, 10);

        jetstream_ctx.delete_stream(&stream).await.unwrap();
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_durable_mode_load_balancing() {
        let universe = Universe::with_accelerated_time();
        let metastore = metastore_for_test();
        let stream = append_random_suffix("test-nats-source--durable-lb--stream");
        let jetstream_ctx = setup_nats_stream(&stream).await;
        let consumer_name = "durable-lb-consumer";
        provision_durable_consumer(&jetstream_ctx, &stream, consumer_name).await;

        let index_id = append_random_suffix("test-nats-source--durable-lb--index");
        let source_config = get_durable_source_config(&stream, consumer_name);
        let index_uid = setup_index(metastore.clone(), &index_id, &source_config, &[]).await;

        let (source_handle_1, doc_processor_inbox_1) = create_source_actor(
            &universe,
            metastore.clone(),
            index_uid.clone(),
            source_config.clone(),
        )
        .await;
        let (source_handle_2, doc_processor_inbox_2) =
            create_source_actor(&universe, metastore, index_uid, source_config).await;

        let subject = format!("{stream}.logs");
        let mut expected_docs = publish_docs(&jetstream_ctx, &subject, 0..20).await;
        expected_docs.sort();

        // Work-queue delivery splits the messages between the two pipelines
        // in some arbitrary way; together they must cover all of them
        // exactly once.
        loop {
            let num_processed_1 = source_handle_1
                .observe()
                .await
                .state
                .get("num_messages_processed")
                .unwrap()
                .as_u64()
                .unwrap();
            let num_processed_2 = source_handle_2
                .observe()
                .await
                .state
                .get("num_messages_processed")
                .unwrap()
                .as_u64()
                .unwrap();
            if num_processed_1 + num_processed_2 >= 20 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        source_handle_1.quit().await;
        source_handle_2.quit().await;

        let mut all_docs: Vec<Bytes> = Vec::new();
        for batch in doc_processor_inbox_1
            .drain_for_test_typed::<RawDocBatch>()
            .into_iter()
            .chain(doc_processor_inbox_2.drain_for_test_typed::<RawDocBatch>())
        {
            all_docs.extend(batch.docs);
        }
        all_docs.sort();
        assert_eq!(all_docs, expected_docs);

        jetstream_ctx.delete_stream(&stream).await.unwrap();
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_durable_mode_missing_consumer() {
        let metastore = metastore_for_test();
        let stream = append_random_suffix("test-nats-source--durable-missing--stream");
        let jetstream_ctx = setup_nats_stream(&stream).await;

        let index_id = append_random_suffix("test-nats-source--durable-missing--index");
        let source_config = get_durable_source_config(&stream, "does-not-exist");
        let index_uid = setup_index(metastore.clone(), &index_id, &source_config, &[]).await;

        let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config)
            .with_metastore(metastore)
            .build();
        let load_source_result = quickwit_supported_sources()
            .load_source(source_runtime)
            .await;
        assert!(
            load_source_result.is_err(),
            "binding to a missing durable consumer should fail"
        );

        jetstream_ctx.delete_stream(&stream).await.unwrap();
    }
}
