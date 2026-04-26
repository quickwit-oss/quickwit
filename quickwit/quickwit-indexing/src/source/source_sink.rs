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

//! Type-erased mailbox for sending messages to any doc processor actor.
//!
//! This decouples `Source` implementations from the specific processor actor
//! type (e.g. `DocProcessor` for logs, `ParquetDocProcessor` for metrics).

use std::sync::Arc;

use async_trait::async_trait;
use quickwit_actors::{Actor, Command, DeferableReplyHandler, Mailbox, SendError};

use super::SourceContext;
use crate::models::{NewPublishLock, NewPublishToken, RawDocBatch};

/// Internal trait used to type-erase the concrete `Mailbox<T>`.
#[async_trait]
trait SourceSinkTrait: Send + Sync + 'static {
    async fn send_raw_doc_batch(&self, batch: RawDocBatch) -> Result<(), SendError>;
    async fn send_publish_lock(&self, lock: NewPublishLock) -> Result<(), SendError>;
    async fn send_publish_token(&self, token: NewPublishToken) -> Result<(), SendError>;
    async fn send_exit_with_success(&self) -> Result<(), SendError>;
}

#[async_trait]
impl<A> SourceSinkTrait for Mailbox<A>
where A: Actor
        + DeferableReplyHandler<RawDocBatch>
        + DeferableReplyHandler<NewPublishLock>
        + DeferableReplyHandler<NewPublishToken>
{
    async fn send_raw_doc_batch(&self, batch: RawDocBatch) -> Result<(), SendError> {
        self.send_message(batch).await?;
        Ok(())
    }

    async fn send_publish_lock(&self, lock: NewPublishLock) -> Result<(), SendError> {
        self.send_message(lock).await?;
        Ok(())
    }

    async fn send_publish_token(&self, token: NewPublishToken) -> Result<(), SendError> {
        self.send_message(token).await?;
        Ok(())
    }

    async fn send_exit_with_success(&self) -> Result<(), SendError> {
        self.send_message(Command::ExitWithSuccess).await?;
        Ok(())
    }
}

/// Output of source, usually the entrypoint of metrics and logs index pipeline,
/// and more specifically a Processor's Mailbox.
///
/// This decouples `Source` implementations from pipelines.
#[derive(Clone)]
pub struct SourceSink {
    inner: Arc<dyn SourceSinkTrait>,
}

impl<T: SourceSinkTrait> From<T> for SourceSink {
    fn from(source_sink: T) -> Self {
        Self {
            inner: Arc::new(source_sink),
        }
    }
}

impl SourceSink {
    /// Send a `RawDocBatch` to the processor.
    ///
    /// The source context's protect zone is held while the send is in flight,
    /// so the supervisor does not consider the source actor stuck while waiting
    /// on backpressure from the processor mailbox.
    pub async fn send_raw_doc_batch(
        &self,
        batch: RawDocBatch,
        ctx: &SourceContext,
    ) -> Result<(), SendError> {
        let _guard = ctx.protect_zone();
        self.inner.send_raw_doc_batch(batch).await
    }

    pub async fn send_publish_lock(
        &self,
        lock: NewPublishLock,
        ctx: &SourceContext,
    ) -> Result<(), SendError> {
        let _guard = ctx.protect_zone();
        self.inner.send_publish_lock(lock).await
    }

    pub async fn send_publish_token(
        &self,
        token: NewPublishToken,
        ctx: &SourceContext,
    ) -> Result<(), SendError> {
        let _guard = ctx.protect_zone();
        self.inner.send_publish_token(token).await
    }

    pub async fn send_exit_with_success(&self, ctx: &SourceContext) -> Result<(), SendError> {
        let _guard = ctx.protect_zone();
        self.inner.send_exit_with_success().await
    }
}
