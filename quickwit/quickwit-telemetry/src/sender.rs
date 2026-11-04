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

use std::mem;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::payload::{
    ClientInfo, EventWithTimestamp, QuickwitTelemetryInfo, TELEMETRY_SCHEMA_VERSION,
    TelemetryEvent, TelemetryPayload,
};
use crate::sink::Sink;

/// At most 1 request per minute, except for the last request on termination.
const TELEMETRY_PUSH_COOLDOWN: Duration = Duration::from_secs(60);

/// Interval at which to send telemetry `Running` event.
const TELEMETRY_RUNNING_EVENT_INTERVAL: Duration = Duration::from_secs(60 * 60 * 12);

/// Upon termination of the program, we send one last telemetry request with pending events.
/// This duration is the amount of time we wait for at most to send that last telemetry request.
const LAST_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);

/// The relay rejects requests with more events.
const MAX_NUM_EVENTS_IN_QUEUE: usize = 10;

#[derive(Default)]
struct EventsState {
    events: Vec<EventWithTimestamp>,
    num_dropped_events: usize,
}

impl EventsState {
    /// Returns true if the event was queued into an empty queue.
    fn push_event(&mut self, mut event: TelemetryEvent) -> bool {
        if self.events.len() >= MAX_NUM_EVENTS_IN_QUEUE {
            self.num_dropped_events += 1;
            return false;
        }
        event.add_usage();
        let events_was_empty = self.events.is_empty();
        self.events.push(EventWithTimestamp::from(event));
        events_was_empty
    }
}

struct Inner {
    sink: Box<dyn Sink>,
    client_info: ClientInfo,
    quickwit_info: QuickwitTelemetryInfo,
    events_state: Mutex<EventsState>,
    /// Wakes the telemetry loop up when an event is queued into an empty queue.
    events_available: Notify,
}

impl Inner {
    fn push_event(&self, event: TelemetryEvent) {
        let is_first_event = self.events_state.lock().unwrap().push_event(event);
        if is_first_event {
            self.events_available.notify_one();
        }
    }

    /// Sends the queued events, if any.
    async fn send_queued_events(&self) {
        let events_state = mem::take(&mut *self.events_state.lock().unwrap());
        if events_state.events.is_empty() {
            return;
        }
        let payload = TelemetryPayload {
            schema_version: TELEMETRY_SCHEMA_VERSION,
            client_info: self.client_info.clone(),
            quickwit_info: self.quickwit_info.clone(),
            events: events_state.events,
            num_dropped_events: events_state.num_dropped_events,
        };
        self.sink.send_payload(payload).await;
    }
}

pub struct TelemetrySender {
    inner: Arc<Inner>,
}

pub struct TelemetryLoopHandle {
    join_handle: JoinHandle<()>,
    terminate_command_tx: oneshot::Sender<()>,
}

impl TelemetryLoopHandle {
    /// Stops the telemetry loop, which sends the queued events without waiting for the cooldown,
    /// and waits for it at most `LAST_REQUEST_TIMEOUT`.
    pub async fn terminate_telemetry(self) {
        let _ = self.terminate_command_tx.send(());
        let _ = tokio::time::timeout(LAST_REQUEST_TIMEOUT, self.join_handle).await;
    }
}

impl TelemetrySender {
    pub(crate) fn new(quickwit_info: QuickwitTelemetryInfo, sink: impl Sink) -> Self {
        Self {
            inner: Arc::new(Inner {
                sink: Box::new(sink),
                client_info: ClientInfo::default(),
                quickwit_info,
                events_state: Mutex::default(),
                events_available: Notify::new(),
            }),
        }
    }

    /// Starts the loop that sends the queued events. Must be called at most once.
    pub(crate) fn start_loop(&self) -> TelemetryLoopHandle {
        let (terminate_command_tx, mut terminate_command_rx) = oneshot::channel();
        let inner = self.inner.clone();
        start_monitor_if_server_running_task(inner.clone());
        let join_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut terminate_command_rx => break,
                    _ = inner.events_available.notified() => {}
                }
                inner.send_queued_events().await;
                tokio::select! {
                    _ = &mut terminate_command_rx => break,
                    _ = tokio::time::sleep(TELEMETRY_PUSH_COOLDOWN) => {}
                }
            }
            inner.send_queued_events().await;
        });
        TelemetryLoopHandle {
            join_handle,
            terminate_command_tx,
        }
    }

    pub fn send(&self, event: TelemetryEvent) {
        self.inner.push_event(event);
    }
}

fn start_monitor_if_server_running_task(inner: Arc<Inner>) {
    let mut interval = tokio::time::interval_at(
        Instant::now() + TELEMETRY_RUNNING_EVENT_INTERVAL,
        TELEMETRY_RUNNING_EVENT_INTERVAL,
    );
    tokio::spawn(async move {
        loop {
            interval.tick().await;
            inner.push_event(TelemetryEvent::running());
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_event_drops_events_when_queue_is_full() {
        let mut events_state = EventsState::default();
        for _ in 0..=MAX_NUM_EVENTS_IN_QUEUE {
            events_state.push_event(TelemetryEvent::RunCommand);
        }
        assert_eq!(events_state.events.len(), MAX_NUM_EVENTS_IN_QUEUE);
        assert_eq!(events_state.num_dropped_events, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn test_telemetry_uptime_events() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let telemetry_sender = TelemetrySender::new(QuickwitTelemetryInfo::default(), tx);
        let loop_handle = telemetry_sender.start_loop();
        let start = Instant::now();
        let payload = rx.recv().await.unwrap();
        assert!(start.elapsed() >= TELEMETRY_RUNNING_EVENT_INTERVAL);
        assert_eq!(payload.events.len(), 1);
        assert!(matches!(
            payload.events[0].event,
            TelemetryEvent::Running { .. }
        ));
        loop_handle.terminate_telemetry().await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_telemetry_cooldown_observed() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let telemetry_sender = TelemetrySender::new(QuickwitTelemetryInfo::default(), tx);
        let loop_handle = telemetry_sender.start_loop();
        telemetry_sender.send(TelemetryEvent::RunCommand);
        let payload = rx.recv().await.unwrap();
        assert_eq!(payload.events.len(), 1);

        let first_request_instant = Instant::now();
        telemetry_sender.send(TelemetryEvent::RunCommand);
        telemetry_sender.send(TelemetryEvent::RunCommand);
        let payload = rx.recv().await.unwrap();
        assert!(first_request_instant.elapsed() >= TELEMETRY_PUSH_COOLDOWN);
        assert_eq!(payload.events.len(), 2);
        loop_handle.terminate_telemetry().await;
    }

    #[tokio::test]
    async fn test_terminate_telemetry_sends_pending_events() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let telemetry_sender = TelemetrySender::new(QuickwitTelemetryInfo::default(), tx);
        let loop_handle = telemetry_sender.start_loop();
        telemetry_sender.send(TelemetryEvent::RunCommand);
        let payload = rx.recv().await.unwrap();
        assert_eq!(payload.events.len(), 1);
        telemetry_sender.send(TelemetryEvent::end_command(2i32));
        loop_handle.terminate_telemetry().await;
        let payload = rx.try_recv().unwrap();
        assert_eq!(payload.events.len(), 1);
        assert!(matches!(
            &payload.events[0].event,
            &TelemetryEvent::EndCommand { .. }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn test_terminate_telemetry_does_not_wait_without_pending_events() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let telemetry_sender = TelemetrySender::new(QuickwitTelemetryInfo::default(), tx);
        let loop_handle = telemetry_sender.start_loop();
        telemetry_sender.send(TelemetryEvent::end_command(0i32));
        rx.recv().await.unwrap();
        tokio::time::sleep(TELEMETRY_PUSH_COOLDOWN).await;
        tokio::task::yield_now().await;

        let terminate_start = Instant::now();
        loop_handle.terminate_telemetry().await;
        assert!(terminate_start.elapsed() < LAST_REQUEST_TIMEOUT);
    }
}
