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

use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, ActorState, Handler, Mailbox,
};
use serde_json::{Value as JsonValue, json};

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
    /// an extra margin that is subtracted from the expected deadline when
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
    last_extension_requested: bool,
    visibility_settings: VisibilitySettings,
    ref_count: Weak<()>,
}

// A handle to the visibility actor. When dropped, the actor exits and the
// visibility isn't maintained anymore.
pub(super) struct VisibilityTaskHandle {
    mailbox: Mailbox<VisibilityTask>,
    actor_handle: ActorHandle<VisibilityTask>,
    ack_id: String,
    _ref_count: Arc<()>,
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
    let ref_count = Arc::new(());
    let weak_ref = Arc::downgrade(&ref_count);
    let task = VisibilityTask {
        queue,
        ack_id: ack_id.clone(),
        extension_count: 0,
        current_deadline,
        last_extension_requested: false,
        visibility_settings,
        ref_count: weak_ref,
    };
    let (mailbox, actor_handle) = ctx.spawn_actor().spawn(task);
    VisibilityTaskHandle {
        mailbox,
        actor_handle,
        ack_id,
        _ref_count: ref_count,
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

    pub async fn request_last_extension(self) -> anyhow::Result<()> {
        self.mailbox
            .ask_for_res(RequestLastExtension)
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
        if self.ref_count.strong_count() == 0 {
            return Ok(());
        }
        if self.last_extension_requested {
            return Ok(());
        }
        self.extend_visibility(ctx, self.visibility_settings.deadline_for_default_extension)
            .await?;
        ctx.schedule_self_msg(self.next_extension(), Loop);
        Ok(())
    }
}

/// Ensures that the visibility of the message is extended using
/// deadline_for_last_extension and then stops the extension loop.
#[derive(Debug)]
struct RequestLastExtension;

#[async_trait]
impl Handler<RequestLastExtension> for VisibilityTask {
    type Reply = anyhow::Result<()>;

    async fn handle(
        &mut self,
        _message: RequestLastExtension,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let deadline_for_last_extension = self.visibility_settings.deadline_for_last_extension;
        let last_deadline = Instant::now() + deadline_for_last_extension;
        self.last_extension_requested = true;
        if last_deadline > self.current_deadline {
            Ok(self
                .extend_visibility(ctx, deadline_for_last_extension)
                .await)
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
        let queue = Arc::new(MemoryQueueForTests::new());
        queue.send_message("test message".to_string(), &ack_id);
        let initial_deadline = queue
            .clone()
            .receive(1, Duration::from_secs(1))
            .await
            .unwrap()[0]
            .metadata
            .initial_deadline;
        // spawn task
        let visibility_settings = VisibilitySettings {
            deadline_for_default_extension: Duration::from_secs(1),
            deadline_for_last_extension: Duration::from_secs(5),
            deadline_for_receive: Duration::from_secs(1),
            request_timeout: Duration::from_millis(100),
            request_margin: Duration::from_millis(100),
        };
        let handle = spawn_visibility_task(
            &ctx,
            queue.clone(),
            ack_id.clone(),
            initial_deadline,
            visibility_settings.clone(),
        );
        // assert that the background task performs extensions
        assert!(!handle.extension_failed());
        tokio::time::sleep_until(initial_deadline.into()).await;
        let next_deadline = queue.next_visibility_deadline(&ack_id).unwrap();
        assert!(initial_deadline < next_deadline);
        assert!(!handle.extension_failed());
        handle.request_last_extension().await.unwrap();
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
        let queue = Arc::new(MemoryQueueForTests::new());
        queue.send_message("test message".to_string(), &ack_id);
        let initial_deadline = queue
            .clone()
            .receive(1, Duration::from_secs(1))
            .await
            .unwrap()[0]
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
            queue.clone(),
            ack_id.clone(),
            initial_deadline,
            visibility_settings.clone(),
        );
        // assert that visibility is not extended after drop
        drop(handle);
        tokio::time::sleep_until(initial_deadline.into()).await;
        // the message is either already expired or about to expire
        if let Some(next_deadline) = queue.next_visibility_deadline(&ack_id) {
            assert_eq!(next_deadline, initial_deadline);
        }
        // assert_eq!(q, None);
        universe.assert_quit().await;
    }
}
