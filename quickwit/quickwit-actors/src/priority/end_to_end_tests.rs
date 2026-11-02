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

//! End-to-end tests of actor priority: real actors on a runtime with a single
//! worker thread.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use crate::{Actor, ActorContext, ActorExitStatus, ActorPriority, Handler, Universe};

#[derive(Debug)]
struct BusyLoop;

/// Keeps its own mailbox non-empty, so its actor loop never returns
/// `Pending` on its own.
struct BusyActor {
    runtime_handle: tokio::runtime::Handle,
    num_loops: Arc<AtomicUsize>,
}

#[async_trait]
impl Actor for BusyActor {
    type ObservableState = ();

    fn observable_state(&self) {}

    fn runtime_handle(&self) -> tokio::runtime::Handle {
        self.runtime_handle.clone()
    }

    fn yield_after_each_message(&self) -> bool {
        false
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        ctx.send_self_message(BusyLoop).await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<BusyLoop> for BusyActor {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: BusyLoop,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        // Simulates CPU intensive work: it holds the worker thread.
        std::thread::sleep(Duration::from_millis(1));
        self.num_loops.fetch_add(1, Ordering::SeqCst);
        ctx.try_send_self_message(BusyLoop)
            .expect("the mailbox should be unbounded");
        Ok(())
    }
}

#[derive(Debug)]
struct Ping;

struct HighPriorityActor {
    runtime_handle: tokio::runtime::Handle,
}

impl Actor for HighPriorityActor {
    type ObservableState = ();

    fn observable_state(&self) {}

    fn runtime_handle(&self) -> tokio::runtime::Handle {
        self.runtime_handle.clone()
    }

    fn priority(&self) -> ActorPriority {
        ActorPriority::High
    }

    fn yield_after_each_message(&self) -> bool {
        false
    }
}

#[async_trait]
impl Handler<Ping> for HighPriorityActor {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: Ping,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        Ok(())
    }
}

#[test]
fn test_high_priority_actor_not_starved_by_busy_actor() {
    // Both actors run on a runtime with a single worker thread. Without
    // priority, the busy actor would hold that thread forever.
    let actor_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let test_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    test_runtime.block_on(async {
        let universe = Universe::new();
        let high_priority_actor = HighPriorityActor {
            runtime_handle: actor_runtime.handle().clone(),
        };
        let (high_priority_mailbox, _high_priority_handle) =
            universe.spawn_builder().spawn(high_priority_actor);
        // After this, the high priority actor is idle, waiting for messages.
        high_priority_mailbox.ask(Ping).await.unwrap();

        let num_loops = Arc::new(AtomicUsize::new(0));
        let busy_actor = BusyActor {
            runtime_handle: actor_runtime.handle().clone(),
            num_loops: num_loops.clone(),
        };
        let (_busy_mailbox, _busy_handle) = universe.spawn_builder().spawn(busy_actor);
        while num_loops.load(Ordering::SeqCst) < 10 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        tokio::time::timeout(Duration::from_secs(5), high_priority_mailbox.ask(Ping))
            .await
            .expect("the high priority actor should not be starved")
            .unwrap();
        // The busy actor resumes once the high priority actor has run.
        let num_loops_after_ping = num_loops.load(Ordering::SeqCst);
        while num_loops.load(Ordering::SeqCst) <= num_loops_after_ping {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        universe.assert_quit().await;
    });
}

#[derive(Debug)]
struct RecordHandling;

struct RecordingActor {
    name: &'static str,
    priority: ActorPriority,
    runtime_handle: tokio::runtime::Handle,
    num_initialized_actors: Arc<AtomicUsize>,
    handling_order: Arc<Mutex<Vec<&'static str>>>,
}

#[async_trait]
impl Actor for RecordingActor {
    type ObservableState = ();

    fn observable_state(&self) {}

    fn runtime_handle(&self) -> tokio::runtime::Handle {
        self.runtime_handle.clone()
    }

    fn priority(&self) -> ActorPriority {
        self.priority
    }

    // Like `Indexer` or `DocProcessor`. Otherwise, the low priority actor would yield
    // after taking its message out of the mailbox, and let the other actor run first
    // regardless of priority.
    fn yield_after_each_message(&self) -> bool {
        false
    }

    // An actor goes from `initialize` to waiting in its receive within the same poll.
    async fn initialize(&mut self, _ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.num_initialized_actors.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl Handler<RecordHandling> for RecordingActor {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: RecordHandling,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.handling_order.lock().unwrap().push(self.name);
        Ok(())
    }
}

#[test]
fn test_normal_actor_does_not_receive_before_woken_high_priority_actor() {
    let actor_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let test_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let num_initialized_actors = Arc::new(AtomicUsize::new(0));
    let handling_order: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
    test_runtime.block_on(async {
        let universe = Universe::new();
        let normal_actor = RecordingActor {
            name: "normal",
            priority: ActorPriority::Low,
            runtime_handle: actor_runtime.handle().clone(),
            num_initialized_actors: num_initialized_actors.clone(),
            handling_order: handling_order.clone(),
        };
        let (normal_mailbox, _normal_handle) = universe.spawn_builder().spawn(normal_actor);
        let high_priority_actor = RecordingActor {
            name: "high",
            priority: ActorPriority::High,
            runtime_handle: actor_runtime.handle().clone(),
            num_initialized_actors: num_initialized_actors.clone(),
            handling_order: handling_order.clone(),
        };
        let (high_priority_mailbox, _high_priority_handle) =
            universe.spawn_builder().spawn(high_priority_actor);
        // We do not send them a first message: after handling a message, a low priority
        // actor yields once before waiting in its receive.
        while num_initialized_actors.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        // Blocks the only worker thread while both messages arrive.
        let is_blocking = Arc::new(AtomicBool::new(false));
        let is_blocking_clone = is_blocking.clone();
        actor_runtime.spawn(async move {
            is_blocking_clone.store(true, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(200));
        });
        // Once the blocking task runs, both actors are waiting in their receive.
        while !is_blocking.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // The normal actor is woken up first, so tokio polls it first.
        let normal_reply_rx = normal_mailbox.send_message(RecordHandling).await.unwrap();
        let high_priority_reply_rx = high_priority_mailbox
            .send_message(RecordHandling)
            .await
            .unwrap();
        normal_reply_rx.await.unwrap();
        high_priority_reply_rx.await.unwrap();
        universe.assert_quit().await;
    });
    assert_eq!(*handling_order.lock().unwrap(), vec!["high", "normal"]);
}

#[derive(Debug)]
struct SendDownstream;

/// High priority actor that forwards a `Ping` to a downstream mailbox on each
/// `SendDownstream` message.
struct SendingActor {
    runtime_handle: tokio::runtime::Handle,
    downstream_mailbox: crate::Mailbox<crate::tests::PingReceiverActor>,
    is_initialized: Arc<AtomicBool>,
}

#[async_trait]
impl Actor for SendingActor {
    type ObservableState = ();

    fn observable_state(&self) {}

    fn runtime_handle(&self) -> tokio::runtime::Handle {
        self.runtime_handle.clone()
    }

    fn priority(&self) -> ActorPriority {
        ActorPriority::High
    }

    async fn initialize(&mut self, _ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.is_initialized.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl Handler<SendDownstream> for SendingActor {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: SendDownstream,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        ctx.send_message(&self.downstream_mailbox, crate::tests::Ping)
            .await?;
        Ok(())
    }
}

#[test]
fn test_high_priority_actor_send_unblocked_despite_busy_actor() {
    // Both actors run on a runtime with a single worker thread. The busy actor never
    // returns `Pending` on its own.
    let actor_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let test_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    test_runtime.block_on(async {
        let universe = Universe::new();
        // A capacity of 0 blocks the send until the test receives the message.
        let (downstream_mailbox, downstream_inbox) = universe
            .create_mailbox::<crate::tests::PingReceiverActor>(
                "Downstream",
                crate::QueueCapacity::Bounded(0),
            );
        let is_initialized = Arc::new(AtomicBool::new(false));
        let sending_actor = SendingActor {
            runtime_handle: actor_runtime.handle().clone(),
            downstream_mailbox,
            is_initialized: is_initialized.clone(),
        };
        let (sending_mailbox, _sending_handle) = universe.spawn_builder().spawn(sending_actor);
        // After this, the high priority actor is idle, waiting for messages.
        while !is_initialized.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        let num_loops = Arc::new(AtomicUsize::new(0));
        let busy_actor = BusyActor {
            runtime_handle: actor_runtime.handle().clone(),
            num_loops: num_loops.clone(),
        };
        let (_busy_mailbox, _busy_handle) = universe.spawn_builder().spawn(busy_actor);
        while num_loops.load(Ordering::SeqCst) < 10 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        // The high priority actor receives the message and blocks on its send.
        let reply_rx = sending_mailbox.send_message(SendDownstream).await.unwrap();
        // Receiving takes the message out of the blocked send, and wakes the high priority
        // actor up. That wake-up must be boosted for the actor to get the worker back.
        downstream_inbox
            .recv_typed_message::<crate::tests::Ping>()
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), reply_rx)
            .await
            .expect("the high priority actor should complete its send")
            .unwrap();
        universe.assert_quit().await;
    });
}
