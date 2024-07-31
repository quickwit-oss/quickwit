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

use anyhow::bail;
use async_trait::async_trait;
use quickwit_actors::{ActorContext, ActorExitStatus, Mailbox};
use quickwit_config::SqsSourceParams;
use quickwit_metastore::checkpoint::SourceCheckpoint;
use serde_json::Value as JsonValue;

use super::coordinator::QueueCoordinator;
use super::message::MessageType;
use super::sqs_queue::SqsQueue;
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
    coordinator: QueueCoordinator,
}

impl SqsSource {
    pub async fn try_new(
        source_runtime: SourceRuntime,
        source_params: SqsSourceParams,
    ) -> anyhow::Result<Self> {
        let queue =
            SqsQueue::try_new(source_params.queue_url, source_params.wait_time_seconds).await?;
        let _processor =
            QueueCoordinator::new(source_runtime, Arc::new(queue), MessageType::RawData);
        // TODO: implement the `MessageType::RawData` processing for the SQS source to work
        bail!("The SQS source is not fully implemented yet")
    }
}

#[async_trait]
impl Source for SqsSource {
    async fn initialize(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<(), ActorExitStatus> {
        self.coordinator
            .initialize(doc_processor_mailbox, ctx)
            .await
    }

    async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        self.coordinator
            .emit_batches(doc_processor_mailbox, ctx)
            .await
    }

    async fn suggest_truncate(
        &mut self,
        checkpoint: SourceCheckpoint,
        ctx: &ActorContext<SourceActor>,
    ) -> anyhow::Result<()> {
        self.coordinator.suggest_truncate(checkpoint, ctx).await
    }

    fn name(&self) -> String {
        format!("{:?}", self)
    }

    fn observable_state(&self) -> JsonValue {
        serde_json::to_value(&self.coordinator.observable_state()).unwrap()
    }
}
