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

//! `quickwit_actor_duration_seconds{actor,operation}` measures elapsed actor durations.
//! The actor label uses the name captured at mailbox creation, never an instance identifier.
//! Message handlers and work performed when the queue becomes empty each contribute a separate
//! `work` observation. Spawned task lifetimes are not actor work observations.
//!
//! Work and send futures are observed on completion, including errors; cancellation and panic do
//! not produce totals. `wake_to_poll` records individual intervals after each task poll returns.
//! These durations overlap: sends can happen inside work, and send-to-receive includes capacity
//! waiting and queue residence. Indexing-operation durations also overlap actor work. Do not add
//! these categories together or subtract independently computed histogram quantiles.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use quickwit_metrics::{Histogram, LazyHistogram, histogram, lazy_histogram};
use tokio::time::Instant;

static ACTOR_DURATION_SECONDS: LazyHistogram = lazy_histogram!(
    name: "duration_seconds",
    description: "Elapsed seconds for actor work, sends, message delivery, and wake-to-poll intervals",
    subsystem: "actor",
    buckets: vec![
        0.000_001, 0.000_01, 0.000_1, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0,
        10.0, 20.0, 30.0, 60.0, 120.0, 300.0, 600.0,
    ],
);

#[derive(Clone, Copy, Debug)]
pub(crate) enum ActorOperation {
    /// Work performed in a message handler or when the message queue becomes empty.
    Work,
    /// Send a message, including waiting for channel capacity.
    Send,
    /// Interval from starting a send until the destination receives the message.
    SendToReceive,
    /// Interval from receipt until handling starts, including priority parking and actor-loop
    /// yields.
    ReceiveToHandler,
    /// Interval from first wake to next poll, including remaining execution if woken during a poll.
    WakeToPoll,
}

impl ActorOperation {
    fn as_str(self) -> &'static str {
        match self {
            Self::Work => "work",
            Self::Send => "send",
            Self::SendToReceive => "send_to_receive",
            Self::ReceiveToHandler => "receive_to_handler",
            Self::WakeToPoll => "wake_to_poll",
        }
    }
}

/// Measure a future's duration on completion, including errors but not cancellation or panic.
/// Callers control actor opt-in. Unlike `HistogramTimer`, this does not record on drop.
pub(crate) async fn measure_future_duration<F: Future>(
    actor_name: &str,
    operation: ActorOperation,
    future: F,
) -> F::Output {
    let started_at = Instant::now();
    let output = future.await;
    add_duration_sample(actor_name, operation, started_at.elapsed());
    output
}

/// Add one elapsed-duration sample for an actor operation.
pub(crate) fn add_duration_sample(actor_name: &str, operation: ActorOperation, duration: Duration) {
    duration_histogram(actor_name, operation).observe(duration.as_secs_f64());
}

/// Get the shared histogram for this actor name and operation.
pub(crate) fn duration_histogram(actor_name: &str, operation: ActorOperation) -> Histogram {
    histogram!(parent: ACTOR_DURATION_SECONDS, "actor" => actor_name.to_owned(), "operation" => operation.as_str())
}

/// Send and receive timestamps for an actor message, with its destination actor's name.
/// The send timestamp precedes waiting for channel capacity; both timestamps survive parking.
pub(crate) struct ActorMessageTimestamps {
    destination_actor_name: Arc<str>,
    send_started_at: Instant,
    received_at: Option<Instant>,
}

impl ActorMessageTimestamps {
    pub(crate) fn new(destination_actor_name: Arc<str>) -> Self {
        Self {
            destination_actor_name,
            send_started_at: Instant::now(),
            received_at: None,
        }
    }

    pub(crate) fn measure_send_to_receive(&mut self) {
        let received_at = Instant::now();
        assert!(
            self.received_at.is_none(),
            "an envelope can only be received once"
        );
        self.received_at = Some(received_at);
        add_duration_sample(
            &self.destination_actor_name,
            ActorOperation::SendToReceive,
            received_at - self.send_started_at,
        );
    }

    pub(crate) fn measure_receive_to_handler(&self, handler_started_at: Instant) {
        let received_at = self
            .received_at
            .expect("an envelope must be received before handling");
        add_duration_sample(
            &self.destination_actor_name,
            ActorOperation::ReceiveToHandler,
            handler_started_at - received_at,
        );
    }
}
