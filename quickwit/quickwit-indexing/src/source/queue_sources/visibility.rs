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

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, ActorState, Handler, Mailbox,
};
use serde_json::{json, Value as JsonValue};

use super::Queue;
use crate::source::SourceContext;

#[derive(Debug, Clone)]
pub(super) struct VisibilitySettings {
    /// The original deadline asked from the queue when polling the messages
    pub deadline_for_receive: Duration,
    /// The last deadline extension when the message reading is completed
    pub deadline_for_last_extension: Duration,
    /// The extension applied why the VisibilityTask to maintain the message visibility
    pub deadline_for_default_extension: Duration,
    /// Rhe timeout for the visibility extension request
    pub request_timeout: Duration,
    /// an extra margin that is substracted from the expected deadline when
    /// asserting whether we are still in time to extend the visibility
    pub request_margin: Duration,
}

impl VisibilitySettings {
    /// The commit timeout gives us a first estimate on how long the processing
    /// will take for the messages. We could include other factors such as the
    /// message size.
    pub(super) fn from_commit_timeout(commit_timeout_secs: usize) -> Self {
        let commit_timeout = Duration::from_secs(commit_timeout_secs as u64);
        Self {
            deadline_for_receive: Duration::from_secs(120) + commit_timeout,
            deadline_for_last_extension: 2 * commit_timeout,
            deadline_for_default_extension: Duration::from_secs(60),
            request_timeout: Duration::from_secs(3),
            request_margin: Duration::from_secs(1),
        }
    }
}

#[derive(Debug)]
struct VisibilityTask {
    queue: Arc<dyn Queue>,
    ack_id: String,
    extension_count: u64,
    current_deadline: Instant,
    stop_extension_loop: bool,
    visibility_settings: VisibilitySettings,
}

// A handle to the visibility actor. When dropped, the actor exits and the
// visibility isn't maintained anymore.
pub(super) struct VisibilityTaskHandle {
    mailbox: Mailbox<VisibilityTask>,
    actor_handle: ActorHandle<VisibilityTask>,
    ack_id: String,
}

/// Spawns actor that ensures that the visibility of a given message
/// (represented by its ack_id) is extended when required. We prefer applying
/// ample margins in the extension process to avoid missing deadlines while also
/// keeping the number of extension requests (and associated cost) small.
pub(super) fn spawn_visibility_task(
    ctx: &SourceContext,
    queue: Arc<dyn Queue>,
    ack_id: String,
    current_deadline: Instant,
    visibility_settings: VisibilitySettings,
) -> VisibilityTaskHandle {
    let task = VisibilityTask {
        queue,
        ack_id: ack_id.clone(),
        extension_count: 0,
        current_deadline,
        stop_extension_loop: false,
        visibility_settings,
    };
    let (_mailbox, _actor_handle) = ctx.spawn_actor().spawn(task);
    VisibilityTaskHandle {
        mailbox: _mailbox,
        actor_handle: _actor_handle,
        ack_id,
    }
}

impl VisibilityTask {
    async fn extend_visibility(
        &mut self,
        ctx: &ActorContext<Self>,
        extension: Duration,
    ) -> anyhow::Result<()> {
        let _zone = ctx.protect_zone();
        self.current_deadline = tokio::time::timeout(
            self.visibility_settings.request_timeout,
            self.queue.modify_deadlines(&self.ack_id, extension),
        )
        .await
        .context("deadline extension timed out")??;
        self.extension_count += 1;
        Ok(())
    }

    fn next_extension(&self) -> Duration {
        (self.current_deadline - Instant::now())
            - self.visibility_settings.request_timeout
            - self.visibility_settings.request_margin
    }
}

impl VisibilityTaskHandle {
    pub fn extension_failed(&self) -> bool {
        self.actor_handle.state() == ActorState::Failure
    }

    pub fn ack_id(&self) -> &str {
        &self.ack_id
    }

    pub async fn request_last_extension(self, extension: Duration) -> anyhow::Result<()> {
        self.mailbox
            .ask_for_res(RequestLastExtension { extension })
            .await
            .map_err(|e| anyhow!(e))?;
        Ok(())
    }
}

#[async_trait]
impl Actor for VisibilityTask {
    type ObservableState = JsonValue;

    fn name(&self) -> String {
        "QueueVisibilityTask".to_string()
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        let first_extension = self.next_extension();
        if first_extension.is_zero() {
            return Err(anyhow!("initial visibility deadline insufficient").into());
        }
        ctx.schedule_self_msg(first_extension, Loop);
        Ok(())
    }

    fn yield_after_each_message(&self) -> bool {
        false
    }

    fn observable_state(&self) -> Self::ObservableState {
        json!({
            "ack_id": self.ack_id,
            "extension_count": self.extension_count,
        })
    }
}

#[derive(Debug)]
struct Loop;

#[async_trait]
impl Handler<Loop> for VisibilityTask {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: Loop,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if self.stop_extension_loop {
            return Ok(());
        }
        self.extend_visibility(ctx, self.visibility_settings.deadline_for_default_extension)
            .await?;
        ctx.schedule_self_msg(self.next_extension(), Loop);
        Ok(())
    }
}

/// Ensures that the visibility of the message is extended until the given
/// deadline and then stops the extension loop.
#[derive(Debug)]
struct RequestLastExtension {
    extension: Duration,
}

#[async_trait]
impl Handler<RequestLastExtension> for VisibilityTask {
    type Reply = anyhow::Result<()>;

    async fn handle(
        &mut self,
        message: RequestLastExtension,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let last_deadline = Instant::now() + message.extension;
        self.stop_extension_loop = true;
        if last_deadline > self.current_deadline {
            Ok(self.extend_visibility(ctx, message.extension).await)
        } else {
            Ok(Ok(()))
        }
    }
}

#[cfg(test)]
mod tests {
    use quickwit_actors::Universe;
    use tokio::sync::watch;

    use super::*;
    use crate::source::queue_sources::memory_queue::MemoryQueueForTests;

    #[tokio::test]
    async fn test_visibility_task_request_last_extension() {
        // actor context
        let universe = Universe::with_accelerated_time();
        let (source_mailbox, _source_inbox) = universe.create_test_mailbox();
        let (observable_state_tx, _observable_state_rx) = watch::channel(serde_json::Value::Null);
        let ctx: SourceContext =
            ActorContext::for_test(&universe, source_mailbox, observable_state_tx);
        // queue with test message
        let ack_id = "ack_id".to_string();
        let queue = MemoryQueueForTests::new();
        queue.send_message("test message".to_string(), &ack_id);
        let initial_deadline = queue.receive(1, Duration::from_secs(1)).await.unwrap()[0]
            .metadata
            .initial_deadline;
        // spawn task
        let visibility_settings = VisibilitySettings {
            deadline_for_default_extension: Duration::from_secs(1),
            deadline_for_last_extension: Duration::from_secs(20),
            deadline_for_receive: Duration::from_secs(1),
            request_timeout: Duration::from_millis(100),
            request_margin: Duration::from_millis(100),
        };
        let handle = spawn_visibility_task(
            &ctx,
            Arc::new(queue.clone()),
            ack_id.clone(),
            initial_deadline,
            visibility_settings.clone(),
        );
        // assert that the background task performs extensions
        assert!(!handle.extension_failed());
        tokio::time::sleep_until(initial_deadline.into()).await;
        assert!(initial_deadline < queue.next_visibility_deadline(&ack_id).unwrap());
        assert!(!handle.extension_failed());
        // request last extension
        handle
            .request_last_extension(Duration::from_secs(5))
            .await
            .unwrap();
        assert!(
            Instant::now() + Duration::from_secs(4)
                < queue.next_visibility_deadline(&ack_id).unwrap()
        );
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_visibility_task_stop_on_drop() {
        // actor context
        let universe = Universe::with_accelerated_time();
        let (source_mailbox, _source_inbox) = universe.create_test_mailbox();
        let (observable_state_tx, _observable_state_rx) = watch::channel(serde_json::Value::Null);
        let ctx: SourceContext =
            ActorContext::for_test(&universe, source_mailbox, observable_state_tx);
        // queue with test message
        let ack_id = "ack_id".to_string();
        let queue = MemoryQueueForTests::new();
        queue.send_message("test message".to_string(), &ack_id);
        let initial_deadline = queue.receive(1, Duration::from_secs(1)).await.unwrap()[0]
            .metadata
            .initial_deadline;
        // spawn task
        let visibility_settings = VisibilitySettings {
            deadline_for_default_extension: Duration::from_secs(1),
            deadline_for_last_extension: Duration::from_secs(20),
            deadline_for_receive: Duration::from_secs(1),
            request_timeout: Duration::from_millis(100),
            request_margin: Duration::from_millis(100),
        };
        let handle = spawn_visibility_task(
            &ctx,
            Arc::new(queue.clone()),
            ack_id.clone(),
            initial_deadline,
            visibility_settings.clone(),
        );
        // assert that visibility is not extended after drop
        drop(handle);
        tokio::time::sleep_until(initial_deadline.into()).await;
        assert_eq!(queue.next_visibility_deadline(&ack_id), None);
        universe.assert_quit().await;
    }
}
