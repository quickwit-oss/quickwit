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
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{RwLock, oneshot};
use tokio::task::JoinHandle;

use crate::clock::Clock;
use crate::payload::{
    ClientInfo, EventWithTimestamp, QuickwitTelemetryInfo, TELEMETRY_SCHEMA_VERSION,
    TelemetryEvent, TelemetryPayload,
};
use crate::sink::{HttpClient, Sink};

/// At most 1 request per minute, except for the last request on termination.
const TELEMETRY_PUSH_COOLDOWN: Duration = Duration::from_secs(60);

/// Interval at which to send telemetry `Running` event.
const TELEMETRY_RUNNING_EVENT_INTERVAL: Duration =
    Duration::from_secs(if cfg!(test) { 3 } else { 60 * 60 * 12 });

/// Upon termination of the program, we send one last telemetry request with pending events.
/// This duration is the amount of time we wait for at most to send that last telemetry request.
const LAST_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);

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
            // UI page loads are the only events that can fill the queue. Any other event, such as
            // a summary carrying the API counts, replaces the latest one instead of being dropped.
            if matches!(event, TelemetryEvent::UiIndexPageLoad) {
                return false;
            }
            let Some(ui_page_load_idx) = self
                .events
                .iter()
                .rposition(|queued| matches!(queued.event, TelemetryEvent::UiIndexPageLoad))
            else {
                return false;
            };
            self.events.remove(ui_page_load_idx);
        }
        event.add_usage();
        let events_was_empty = self.events.is_empty();
        self.events.push(EventWithTimestamp::from(event));
        events_was_empty
    }
}

struct Events {
    state: RwLock<EventsState>,
    /// Wakes the telemetry loop up when an event is queued into an empty queue.
    items_available_tx: Sender<()>,
    items_available_rx: RwLock<Receiver<()>>,
}

impl Default for Events {
    fn default() -> Self {
        let (items_available_tx, items_available_rx) = tokio::sync::mpsc::channel(1);
        Events {
            state: RwLock::new(EventsState::default()),
            items_available_tx,
            items_available_rx: RwLock::new(items_available_rx),
        }
    }
}

impl Events {
    async fn wait_for_events(&self) {
        self.items_available_rx.write().await.recv().await;
    }

    async fn take_events(&self) -> EventsState {
        mem::take(&mut *self.state.write().await)
    }

    async fn push_event(&self, event: TelemetryEvent) {
        let is_first_event = self.state.write().await.push_event(event);
        if is_first_event {
            // A full channel means that a wake-up is already pending.
            let _ = self.items_available_tx.try_send(());
        }
    }
}

struct Inner {
    sink: Box<dyn Sink>,
    client_info: ClientInfo,
    quickwit_info: QuickwitTelemetryInfo,
    events: Events,
    clock: Clock,
}

impl Inner {
    /// Sends the queued events, if any.
    async fn send_queued_events(&self) {
        let events_state = self.events.take_events().await;
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
    pub(crate) fn with_http_client(
        quickwit_info: QuickwitTelemetryInfo,
        http_client: HttpClient,
    ) -> Self {
        Self::new(
            quickwit_info,
            http_client,
            Clock::Cooldown(TELEMETRY_PUSH_COOLDOWN),
        )
    }

    fn new(quickwit_info: QuickwitTelemetryInfo, sink: impl Sink, clock: Clock) -> Self {
        Self {
            inner: Arc::new(Inner {
                sink: Box::new(sink),
                client_info: ClientInfo::default(),
                quickwit_info,
                events: Events::default(),
                clock,
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
                let quit_loop = tokio::select! {
                    _ = &mut terminate_command_rx => true,
                    _ = inner.events.wait_for_events() => false,
                };
                inner.send_queued_events().await;
                if quit_loop {
                    break;
                }
                let quit_loop = tokio::select! {
                    _ = &mut terminate_command_rx => true,
                    _ = inner.clock.tick() => false,
                };
                if quit_loop {
                    inner.send_queued_events().await;
                    break;
                }
            }
        });
        TelemetryLoopHandle {
            join_handle,
            terminate_command_tx,
        }
    }

    pub async fn send(&self, event: TelemetryEvent) {
        self.inner.events.push_event(event).await;
    }
}

fn start_monitor_if_server_running_task(inner: Arc<Inner>) {
    let mut clock = tokio::time::interval(TELEMETRY_RUNNING_EVENT_INTERVAL);
    tokio::spawn(async move {
        // Drop the first immediate tick.
        clock.tick().await;
        loop {
            clock.tick().await;
            inner.events.push_event(TelemetryEvent::running()).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_event_replaces_ui_page_load_when_queue_is_full() {
        let mut events_state = EventsState::default();
        for _ in 0..=MAX_NUM_EVENTS_IN_QUEUE {
            events_state.push_event(TelemetryEvent::UiIndexPageLoad);
        }
        events_state.push_event(TelemetryEvent::end_command(0));

        assert_eq!(events_state.events.len(), MAX_NUM_EVENTS_IN_QUEUE);
        assert_eq!(events_state.num_dropped_events, 2);
        assert!(matches!(
            events_state.events.last().unwrap().event,
            TelemetryEvent::EndCommand { .. }
        ));
    }

    #[tokio::test]
    async fn test_telemetry_two_events() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let (clock_btn, clock) = Clock::manual();
        let telemetry_sender = TelemetrySender::new(QuickwitTelemetryInfo::default(), tx, clock);
        let loop_handler = telemetry_sender.start_loop();
        telemetry_sender.send(TelemetryEvent::UiIndexPageLoad).await;
        {
            let payload = rx.recv().await.unwrap();
            assert_eq!(payload.events.len(), 1);
        }
        clock_btn.tick().await;
        telemetry_sender.send(TelemetryEvent::UiIndexPageLoad).await;
        {
            let payload = rx.recv().await.unwrap();
            assert_eq!(payload.events.len(), 1);
        }
        loop_handler.terminate_telemetry().await;
    }

    #[tokio::test]
    async fn test_telemetry_uptime_events() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let (clock_btn, clock) = Clock::manual();
        let telemetry_sender = TelemetrySender::new(QuickwitTelemetryInfo::default(), tx, clock);
        let loop_handler = telemetry_sender.start_loop();
        telemetry_sender.send(TelemetryEvent::UiIndexPageLoad).await;
        {
            let payload = rx.recv().await.unwrap();
            assert_eq!(payload.events.len(), 1);
        }
        clock_btn.tick().await;
        tokio::time::sleep(TELEMETRY_RUNNING_EVENT_INTERVAL + Duration::from_secs(1)).await;
        {
            let payload = rx.recv().await.unwrap();
            assert_eq!(payload.events.len(), 1);
            assert!(matches!(
                payload.events[0].event,
                TelemetryEvent::Running { .. }
            ));
        }
        loop_handler.terminate_telemetry().await;
    }

    #[tokio::test]
    async fn test_telemetry_cooldown_observed() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let (clock_btn, clock) = Clock::manual();
        let telemetry_sender = TelemetrySender::new(QuickwitTelemetryInfo::default(), tx, clock);
        let loop_handler = telemetry_sender.start_loop();
        telemetry_sender.send(TelemetryEvent::UiIndexPageLoad).await;
        {
            let payload = rx.recv().await.unwrap();
            assert_eq!(payload.events.len(), 1);
        }
        tokio::task::yield_now().await;
        telemetry_sender.send(TelemetryEvent::UiIndexPageLoad).await;

        let timeout_res = tokio::time::timeout(Duration::from_millis(1), rx.recv()).await;
        assert!(timeout_res.is_err());

        telemetry_sender.send(TelemetryEvent::UiIndexPageLoad).await;
        clock_btn.tick().await;
        {
            let payload = rx.recv().await.unwrap();
            assert_eq!(payload.events.len(), 2);
        }
        loop_handler.terminate_telemetry().await;
    }

    #[tokio::test]
    async fn test_terminate_telemetry_sends_pending_events() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let (_clock_btn, clock) = Clock::manual();
        let telemetry_sender = TelemetrySender::new(QuickwitTelemetryInfo::default(), tx, clock);
        let loop_handler = telemetry_sender.start_loop();
        telemetry_sender.send(TelemetryEvent::UiIndexPageLoad).await;
        let payload = rx.recv().await.unwrap();
        assert_eq!(payload.events.len(), 1);
        telemetry_sender
            .send(TelemetryEvent::end_command(2i32))
            .await;
        loop_handler.terminate_telemetry().await;
        let payload = rx.recv().await.unwrap();
        assert_eq!(payload.events.len(), 1);
        assert!(matches!(
            &payload.events[0].event,
            &TelemetryEvent::EndCommand { .. }
        ));
        tokio::time::timeout(
            Duration::from_secs(1),
            telemetry_sender.send(TelemetryEvent::UiIndexPageLoad),
        )
        .await
        .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_terminate_telemetry_does_not_wait_without_pending_events() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let (clock_btn, clock) = Clock::manual();
        let telemetry_sender = TelemetrySender::new(QuickwitTelemetryInfo::default(), tx, clock);
        let loop_handler = telemetry_sender.start_loop();
        telemetry_sender
            .send(TelemetryEvent::end_command(0i32))
            .await;
        rx.recv().await.unwrap();
        clock_btn.tick().await;
        tokio::task::yield_now().await;

        let terminate_start = tokio::time::Instant::now();
        loop_handler.terminate_telemetry().await;
        assert!(terminate_start.elapsed() < LAST_REQUEST_TIMEOUT);
    }
}
