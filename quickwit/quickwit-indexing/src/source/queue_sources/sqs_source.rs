// Copyright (C) 2024 Quickwit, Inc.
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

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use quickwit_actors::{ActorContext, ActorExitStatus, Mailbox};
use quickwit_config::SqsSourceParams;
use quickwit_metastore::checkpoint::SourceCheckpoint;
use serde_json::Value as JsonValue;

use super::processor::QueueProcessor;
use super::sqs_queue::{get_sqs_client, SqsQueue};
use crate::actors::DocProcessor;
use crate::source::{Source, SourceActor, SourceContext, SourceRuntime, TypedSourceFactory};

pub struct SqsSourceFactory;

#[async_trait]
impl TypedSourceFactory for SqsSourceFactory {
    type Source = SqsSource;
    type Params = SqsSourceParams;

    async fn typed_create_source(
        source_runtime: SourceRuntime,
        source_params: SqsSourceParams,
    ) -> anyhow::Result<Self::Source> {
        SqsSource::try_new(source_runtime, source_params).await
    }
}

#[derive(Debug)]
pub struct SqsSource {
    processor: QueueProcessor,
}

impl SqsSource {
    pub async fn try_new(
        source_runtime: SourceRuntime,
        source_params: SqsSourceParams,
    ) -> anyhow::Result<Self> {
        let queue =
            SqsQueue::try_new(source_params.queue_url, source_params.wait_time_seconds).await?;
        let processor =
            QueueProcessor::new(source_runtime, Arc::new(queue), source_params.queue_params);
        Ok(SqsSource { processor })
    }
}

#[async_trait]
impl Source for SqsSource {
    async fn initialize(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<(), ActorExitStatus> {
        self.processor.initialize(doc_processor_mailbox, ctx).await
    }

    async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        self.processor
            .emit_batches(doc_processor_mailbox, ctx)
            .await
    }

    async fn suggest_truncate(
        &mut self,
        checkpoint: SourceCheckpoint,
        ctx: &ActorContext<SourceActor>,
    ) -> anyhow::Result<()> {
        self.processor.suggest_truncate(checkpoint, ctx).await
    }

    fn name(&self) -> String {
        format!("{:?}", self)
    }

    fn observable_state(&self) -> JsonValue {
        self.processor.observable_state()
    }
}

/// Checks whether we can establish a connection to the SQS service and we can
/// access the provided queue_url
pub(crate) async fn check_connectivity(params: &SqsSourceParams) -> anyhow::Result<()> {
    let client = get_sqs_client(&params.queue_url).await?;
    client
        .get_queue_attributes()
        .queue_url(params.queue_url.clone())
        .send()
        .await?;

    Ok(())
}

#[cfg(all(test, feature = "sqs-localstack-tests"))]
mod localstack_tests {
    use std::str::FromStr;

    use quickwit_actors::Universe;
    use quickwit_common::rand::append_random_suffix;
    use quickwit_common::uri::Uri;
    use quickwit_config::{
        QueueMessageType, QueueParams, SourceConfig, SourceParams, SqsSourceParams,
    };
    use quickwit_metastore::metastore_for_test;

    use super::*;
    use crate::models::RawDocBatch;
    use crate::source::doc_file_reader::file_test_helpers::generate_dummy_doc_file;
    use crate::source::queue_sources::sqs_queue::test_helpers::{
        create_queue, get_localstack_sqs_client, send_message,
    };
    use crate::source::test_setup_helper::setup_index;
    use crate::source::tests::SourceRuntimeBuilder;

    #[tokio::test]
    async fn test_check_connectivity() {
        let sqs_client = get_localstack_sqs_client().await.unwrap();
        let queue_url = create_queue(&sqs_client, "check-connectivity").await;
        let src_params = SqsSourceParams {
            queue_url,
            wait_time_seconds: 1,
            queue_params: QueueParams {
                message_type: QueueMessageType::S3Notification,
            },
        };
        check_connectivity(&src_params).await.unwrap();
    }

    #[tokio::test]
    async fn test_sqs_source() {
        // queue setup
        let sqs_client = get_localstack_sqs_client().await.unwrap();
        let queue_url = create_queue(&sqs_client, "check-connectivity").await;
        let dummy_doc_file = generate_dummy_doc_file(false, 10).await;
        let test_uri = Uri::from_str(dummy_doc_file.path().to_str().unwrap()).unwrap();
        send_message(&sqs_client, &queue_url, test_uri.as_str()).await;

        // source setup
        let sqs_params = SqsSourceParams {
            queue_url,
            wait_time_seconds: 1,
            queue_params: QueueParams {
                message_type: QueueMessageType::RawUri,
            },
        };
        let source_params = SourceParams::Sqs(sqs_params.clone());
        let source_config = SourceConfig::for_test("test-sqs-source", source_params);
        let metastore = metastore_for_test();
        let index_id = append_random_suffix("test-sqs-index");
        let index_uid = setup_index(metastore.clone(), &index_id, &source_config, &[]).await;
        let source_runtime = SourceRuntimeBuilder::new(index_uid, source_config)
            .with_metastore(metastore)
            .build();
        let sqs_source = SqsSource::try_new(source_runtime, sqs_params)
            .await
            .unwrap();

        // actor setup
        let universe = Universe::with_accelerated_time();
        let (doc_processor_mailbox, doc_processor_inbox) = universe.create_test_mailbox();
        {
            let actor = SourceActor {
                source: Box::new(sqs_source),
                doc_processor_mailbox: doc_processor_mailbox.clone(),
            };
            let (_mailbox, handle) = universe.spawn_builder().spawn(actor);

            // run the source actor for a while
            tokio::time::timeout(Duration::from_millis(500), handle.join())
                .await
                .unwrap_err();

            let next_message = doc_processor_inbox
                .drain_for_test()
                .into_iter()
                .flat_map(|box_any| box_any.downcast::<RawDocBatch>().ok())
                .map(|box_raw_doc_batch| *box_raw_doc_batch)
                .next()
                .unwrap();
            assert_eq!(next_message.docs.len(), 10);
        }
        universe.assert_quit().await;
    }
}
