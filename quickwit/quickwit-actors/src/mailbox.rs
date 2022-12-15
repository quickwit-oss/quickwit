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

use std::any::Any;
use std::convert::Infallible;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Instant;

use async_trait::async_trait;
use quickwit_common::metrics::IntCounter;
use tokio::sync::oneshot;

use crate::channel_with_priority::{Receiver, Sender, TrySendError};
use crate::envelope::{wrap_in_envelope, Envelope};
use crate::{
    Actor, ActorContext, ActorExitStatus, AskError, Handler, QueueCapacity, RecvError, SendError,
};

/// A mailbox is the object that makes it possible to send a message
/// to an actor.
///
/// It is lightweight to clone.
///
/// The actor holds its `Inbox` counterpart.
///
/// The mailbox can receive high priority and low priority messages.
/// Commands are typically sent as high priority messages, whereas regular
/// actor messages are sent to the low priority channel.
///
/// Whenever a high priority message is available, it is processed
/// before low priority messages.
///
/// If all mailboxes are dropped, the actor will process all of the pending messages
/// and gracefully exit with [`crate::actor::ActorExitStatus::Success`].
pub struct Mailbox<A: Actor> {
    inner: Arc<Inner<A>>,
    // We do not rely on the `Arc:strong_count` here to avoid an intricate
    // race condition. We want to make sure the processing of the `Nudge`
    // message happens AFTER we decrement the refcount.
    ref_count: Arc<AtomicUsize>,
}

impl<A: Actor> Mailbox<A> {
    pub fn downgrade(&self) -> WeakMailbox<A> {
        WeakMailbox {
            inner: Arc::downgrade(&self.inner),
            ref_count: Arc::downgrade(&self.ref_count),
        }
    }
}

impl<A: Actor> Drop for Mailbox<A> {
    fn drop(&mut self) {
        let old_val = self.ref_count.fetch_sub(1, Ordering::SeqCst);
        if old_val == 2 {
            // This was the last mailbox.
            // `ref_count == 1` means that only the mailbox in the ActorContext
            // is remaining.
            let _ = self.send_message_with_high_priority(LastMailbox);
        }
    }
}

#[derive(Debug)]
struct LastMailbox;

#[async_trait]
impl<A: Actor> Handler<LastMailbox> for A {
    type Reply = ();

    async fn handle(
        &mut self,
        _: LastMailbox,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        // Being the last mailbox does not necessarily mean that we
        // want to stop the processing.
        //
        // There could be pending message in the queue that will
        // spawn actors which will get a new copy of the mailbox
        // etc.
        //
        // For that reason, the logic that really detects
        // the last mailbox happens when all message have been drained.
        //
        // The `LastMailbox` message is just here to make sure the actor
        // loop does not get stuck waiting for a message that does
        // will never come.
        Ok(())
    }
}

impl<A: Actor> Clone for Mailbox<A> {
    fn clone(&self) -> Self {
        self.ref_count.fetch_add(1, Ordering::SeqCst);
        Mailbox {
            inner: self.inner.clone(),
            ref_count: self.ref_count.clone(),
        }
    }
}

impl<A: Actor> Mailbox<A> {
    pub(crate) fn is_last_mailbox(&self) -> bool {
        self.ref_count.load(Ordering::SeqCst) == 1
    }

    pub fn id(&self) -> &str {
        &self.inner.instance_id
    }
}

pub(crate) struct Inner<A: Actor> {
    pub(crate) tx: Sender<Envelope<A>>,
    instance_id: String,
}

impl<A: Actor> fmt::Debug for Mailbox<A> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("Mailbox")
            .field(&self.actor_instance_id())
            .finish()
    }
}

impl<A: Actor> Mailbox<A> {
    pub fn actor_instance_id(&self) -> &str {
        &self.inner.instance_id
    }

    pub fn is_disconnected(&self) -> bool {
        self.inner.tx.is_disconnected()
    }

    /// Sends a message to the actor owning the associated inbox.
    ///
    /// From an actor context, use the `ActorContext::send_message` method instead.
    ///
    /// SendError is returned if the actor has already exited.
    pub async fn send_message<M>(
        &self,
        message: M,
    ) -> Result<oneshot::Receiver<A::Reply>, SendError>
    where
        A: Handler<M>,
        M: 'static + Send + Sync + fmt::Debug,
    {
        self.send_message_with_backpressure_counter(message, None)
            .await
    }

    /// Attempts to queue a message in the low priority channel of the mailbox.
    ///
    /// If sending the message would block, the method simply returns `TrySendError::Full(message)`.
    pub fn try_send_message<M>(
        &self,
        message: M,
    ) -> Result<oneshot::Receiver<A::Reply>, TrySendError<M>>
    where
        A: Handler<M>,
        M: 'static + Send + Sync + fmt::Debug,
    {
        let (envelope, response_rx) = wrap_in_envelope(message);
        self.inner
            .tx
            .try_send_low_priority(envelope)
            .map_err(|err| {
                match err {
                    TrySendError::Disconnected => TrySendError::Disconnected,
                    TrySendError::Full(mut envelope) => {
                        // We need to un pack the envelope.
                        let message: M = envelope.message_typed().unwrap();
                        TrySendError::Full(message)
                    }
                }
            })?;
        Ok(response_rx)
    }

    /// Sends a message to the actor owning the associated inbox.
    ///
    /// If the actor experiences some backpressure, then
    /// `backpressure_micros` will be increased by the amount of
    /// microseconds of backpressure experienced.
    pub async fn send_message_with_backpressure_counter<M>(
        &self,
        message: M,
        backpressure_micros_counter_opt: Option<&IntCounter>,
    ) -> Result<oneshot::Receiver<A::Reply>, SendError>
    where
        A: Handler<M>,
        M: 'static + Send + Sync + fmt::Debug,
    {
        let (envelope, response_rx) = wrap_in_envelope(message);
        if let Some(backpressure_micros_counter) = backpressure_micros_counter_opt {
            match self.inner.tx.try_send_low_priority(envelope) {
                Ok(()) => Ok(response_rx),
                Err(TrySendError::Full(msg)) => {
                    let now = Instant::now();
                    self.inner.tx.send_low_priority(msg).await?;
                    let elapsed = now.elapsed();
                    backpressure_micros_counter.inc_by(elapsed.as_micros() as u64);
                    Ok(response_rx)
                }
                Err(TrySendError::Disconnected) => Err(SendError::Disconnected),
            }
        } else {
            self.inner.tx.send_low_priority(envelope).await?;
            Ok(response_rx)
        }
    }

    pub(crate) fn send_message_with_high_priority<M>(&self, message: M) -> Result<(), SendError>
    where
        A: Handler<M>,
        M: 'static + Send + Sync + fmt::Debug,
    {
        let (envelope, _response_rx) = wrap_in_envelope(message);
        self.inner.tx.send_high_priority(envelope)
    }

    /// Similar to `send_message`, except this method
    /// waits asynchronously for the actor reply.
    ///
    /// From an actor context, use the `ActorContext::ask` method instead.
    pub async fn ask<M, T>(&self, message: M) -> Result<T, AskError<Infallible>>
    where
        A: Handler<M, Reply = T>,
        M: 'static + Send + Sync + fmt::Debug,
    {
        self.ask_with_backpressure_counter(message, None).await
    }

    /// Similar to `ask`, but if a backpressure counter is passed,
    /// it increments the amount of time spent in the backpressure.
    ///
    /// The backpressure duration only includes the amount of time
    /// it took to `queue` the request into the actor pipeline.
    ///
    /// It does not include
    /// - the amount spent waiting in the queue,
    /// - the amount spent processing the message.
    ///
    /// From an actor context, use the `ActorContext::ask` method instead.
    pub async fn ask_with_backpressure_counter<M, T>(
        &self,
        message: M,
        backpressure_micros_counter_opt: Option<&IntCounter>,
    ) -> Result<T, AskError<Infallible>>
    where
        A: Handler<M, Reply = T>,
        M: 'static + Send + Sync + fmt::Debug,
    {
        let resp = self
            .send_message_with_backpressure_counter(message, backpressure_micros_counter_opt)
            .await;
        resp.map_err(|_send_error| AskError::MessageNotDelivered)?
            .await
            .map_err(|_| AskError::ProcessMessageError)
    }

    /// Similar to `send_message`, except this method
    /// waits asynchronously for the actor reply.
    ///
    /// From an actor context, use the `ActorContext::ask` method instead.
    pub async fn ask_for_res<M, T, E: fmt::Debug>(&self, message: M) -> Result<T, AskError<E>>
    where
        A: Handler<M, Reply = Result<T, E>>,
        M: 'static + Send + Sync + fmt::Debug,
    {
        self.send_message(message)
            .await
            .map_err(|_send_error| AskError::MessageNotDelivered)?
            .await
            .map_err(|_| AskError::ProcessMessageError)?
            .map_err(AskError::from)
    }
}

pub struct Inbox<A: Actor> {
    rx: Arc<Receiver<Envelope<A>>>,
}

impl<A: Actor> Clone for Inbox<A> {
    fn clone(&self) -> Self {
        Inbox {
            rx: self.rx.clone(),
        }
    }
}

impl<A: Actor> Inbox<A> {
    pub(crate) async fn recv(&self) -> Result<Envelope<A>, RecvError> {
        self.rx.recv().await
    }

    pub(crate) async fn recv_cmd_and_scheduled_msg_only(&self) -> Envelope<A> {
        self.rx.recv_high_priority().await
    }

    #[allow(dead_code)] // temporary
    pub(crate) fn try_recv(&self) -> Result<Envelope<A>, RecvError> {
        self.rx.try_recv()
    }

    #[allow(dead_code)] // temporary
    pub(crate) fn try_recv_cmd_and_scheduled_msg_only(&self) -> Result<Envelope<A>, RecvError> {
        self.rx.try_recv_high_priority_message()
    }

    /// Destroys the inbox and returns the list of pending messages or commands
    /// in the low priority channel.
    ///
    /// Warning this iterator might never be exhausted if there is a living
    /// mailbox associated to it.
    pub fn drain_for_test(&self) -> Vec<Box<dyn Any>> {
        self.rx
            .drain_low_priority()
            .into_iter()
            .map(|mut envelope| envelope.message())
            .collect()
    }

    /// Destroys the inbox and returns the list of pending messages or commands
    /// in the low priority channel.
    ///
    /// Warning this iterator might never be exhausted if there is a living
    /// mailbox associated to it.
    pub fn drain_for_test_typed<M: 'static>(&self) -> Vec<M> {
        self.rx
            .drain_low_priority()
            .into_iter()
            .flat_map(|mut envelope| envelope.message_typed())
            .collect()
    }
}

pub fn create_mailbox<A: Actor>(
    actor_name: String,
    queue_capacity: QueueCapacity,
) -> (Mailbox<A>, Inbox<A>) {
    let (tx, rx) = crate::channel_with_priority::channel(queue_capacity);
    let ref_count = Arc::new(AtomicUsize::new(1));
    let mailbox = Mailbox {
        inner: Arc::new(Inner {
            tx,
            instance_id: quickwit_common::new_coolid(&actor_name),
        }),
        ref_count,
    };
    let inbox = Inbox { rx: Arc::new(rx) };
    (mailbox, inbox)
}

pub fn create_test_mailbox<A: Actor>() -> (Mailbox<A>, Inbox<A>) {
    create_mailbox("test-mailbox".to_string(), QueueCapacity::Unbounded)
}

pub struct WeakMailbox<A: Actor> {
    inner: Weak<Inner<A>>,
    ref_count: Weak<AtomicUsize>,
}

impl<A: Actor> WeakMailbox<A> {
    pub fn upgrade(&self) -> Option<Mailbox<A>> {
        let inner = self.inner.upgrade()?;
        let ref_count = self.ref_count.upgrade()?;
        Some(Mailbox { inner, ref_count })
    }
}

#[cfg(test)]
mod tests {
    use std::mem;
    use std::time::Duration;

    use super::*;
    use crate::tests::{Ping, PingReceiverActor};
    use crate::Universe;

    #[test]
    fn test_weak_mailbox_downgrade_upgrade() {
        let (mailbox, _inbox) = create_test_mailbox::<PingReceiverActor>();
        let weak_mailbox = mailbox.downgrade();
        assert!(weak_mailbox.upgrade().is_some());
    }

    #[test]
    fn test_weak_mailbox_failing_upgrade() {
        let (mailbox, _inbox) = create_test_mailbox::<PingReceiverActor>();
        let weak_mailbox = mailbox.downgrade();
        drop(mailbox);
        assert!(weak_mailbox.upgrade().is_none());
    }

    struct BackPressureActor;

    impl Actor for BackPressureActor {
        type ObservableState = ();

        fn observable_state(&self) -> Self::ObservableState {}

        fn queue_capacity(&self) -> QueueCapacity {
            QueueCapacity::Bounded(0)
        }

        fn yield_after_each_message(&self) -> bool {
            false
        }
    }

    use async_trait::async_trait;

    #[async_trait]
    impl Handler<Duration> for BackPressureActor {
        type Reply = ();

        async fn handle(
            &mut self,
            sleep_duration: Duration,
            _ctx: &ActorContext<Self>,
        ) -> Result<(), ActorExitStatus> {
            if !sleep_duration.is_zero() {
                tokio::time::sleep(sleep_duration).await;
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_mailbox_send_with_backpressure_counter_no_backpressure_cleansheet() {
        let universe = Universe::new();
        let back_pressure_actor = BackPressureActor;
        let (mailbox, _handle) = universe.spawn_builder().spawn(back_pressure_actor);
        // We send a first message to make sure the actor has been properly spawned and is listening
        // for new messages.
        mailbox
            .ask_with_backpressure_counter(Duration::default(), None)
            .await
            .unwrap();
        // At this point the actor was started and even processed a message entirely.
        let backpressure_micros_counter =
            IntCounter::new("test_counter", "help for test_counter").unwrap();
        let wait_duration = Duration::from_millis(1);
        let processed = mailbox
            .send_message_with_backpressure_counter(
                wait_duration,
                Some(&backpressure_micros_counter),
            )
            .await
            .unwrap();
        assert_eq!(backpressure_micros_counter.get(), 0u64);
        processed.await.unwrap();
        assert_eq!(backpressure_micros_counter.get(), 0u64);
    }

    #[tokio::test]
    async fn test_mailbox_send_with_backpressure_counter_backpressure() {
        let universe = Universe::new();
        let back_pressure_actor = BackPressureActor;
        let (mailbox, _handle) = universe.spawn_builder().spawn(back_pressure_actor);
        // We send a first message to make sure the actor has been properly spawned and is listening
        // for new messages.
        mailbox
            .ask_with_backpressure_counter(Duration::default(), None)
            .await
            .unwrap();
        let backpressure_micros_counter =
            IntCounter::new("test_counter", "help for test_counter").unwrap();
        let wait_duration = Duration::from_millis(1);
        mailbox
            .send_message_with_backpressure_counter(
                wait_duration,
                Some(&backpressure_micros_counter),
            )
            .await
            .unwrap();
        // That second message will present some backpressure, since the capacity is 0 and
        // the first message willl take 1000 micros to be processed.
        mailbox
            .send_message_with_backpressure_counter(
                Duration::default(),
                Some(&backpressure_micros_counter),
            )
            .await
            .unwrap();
        assert!(backpressure_micros_counter.get() > 1_000u64);
    }

    #[tokio::test]
    async fn test_mailbox_waiting_for_processing_does_not_counter_as_backpressure() {
        let universe = Universe::new();
        let back_pressure_actor = BackPressureActor;
        let (mailbox, _handle) = universe.spawn_builder().spawn(back_pressure_actor);
        mailbox
            .ask_with_backpressure_counter(Duration::default(), None)
            .await
            .unwrap();
        let backpressure_micros_counter =
            IntCounter::new("test_counter", "help for test_counter").unwrap();
        let start = Instant::now();
        mailbox
            .ask_with_backpressure_counter(Duration::from_millis(1), None)
            .await
            .unwrap();
        let elapsed = start.elapsed();
        assert!(elapsed.as_micros() > 1000);
        assert_eq!(backpressure_micros_counter.get(), 0);
    }

    #[test]
    fn test_try_send() {
        let (mailbox, _inbox) = super::create_mailbox::<PingReceiverActor>(
            "hello".to_string(),
            QueueCapacity::Bounded(1),
        );
        assert!(mailbox.try_send_message(Ping).is_ok());
        assert!(matches!(
            mailbox.try_send_message(Ping).unwrap_err(),
            TrySendError::Full(Ping)
        ));
    }

    #[test]
    fn test_try_send_disconnect() {
        let (mailbox, inbox) = super::create_mailbox::<PingReceiverActor>(
            "hello".to_string(),
            QueueCapacity::Bounded(1),
        );
        assert!(mailbox.try_send_message(Ping).is_ok());
        mem::drop(inbox);
        assert!(matches!(
            mailbox.try_send_message(Ping).unwrap_err(),
            TrySendError::Disconnected
        ));
    }
}
