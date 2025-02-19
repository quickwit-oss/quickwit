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

#![allow(clippy::bool_assert_comparison)]
#![deny(clippy::disallowed_methods)]

pub mod payload;
/// This crate contains  the code responsible for sending usage data to Quickwit inc's server.
mod sender;
pub(crate) mod sink;

use once_cell::sync::OnceCell;
use payload::QuickwitTelemetryInfo;
use tracing::info;

use crate::payload::TelemetryEvent;
pub use crate::sender::is_telemetry_disabled;
use crate::sender::{TelemetryLoopHandle, TelemetrySender};

static TELEMETRY_SENDER: OnceCell<TelemetrySender> = OnceCell::new();

/// Returns a `TelemetryLoopHandle` if the telemetry loop is not yet started.
pub fn start_telemetry_loop(quickwit_info: QuickwitTelemetryInfo) -> Option<TelemetryLoopHandle> {
    let telemetry_sender =
        TELEMETRY_SENDER.get_or_init(|| TelemetrySender::from_quickwit_info(quickwit_info));
    // This should not happen... unless telemetry is enabled and you are running tests in parallel
    // in the same process.
    if telemetry_sender.loop_started() {
        info!("telemetry loop already started. please disable telemetry during tests");
        return None;
    }
    Some(telemetry_sender.start_loop())
}

/// Sends a telemetry event to Quickwit's server via HTTP.
///
/// Telemetry guarantees to send at most 1 request per minute.
/// Each requests can ship at most 10 messages.
///
/// If this methods is called too often, some events will be dropped.
///
/// If the http requests fail, the error will be silent.
///
/// We voluntarily use an enum here to make it easier for reader
/// to audit the type of information that is send home.
pub async fn send_telemetry_event(event: TelemetryEvent) {
    if let Some(telemetry_sender) = TELEMETRY_SENDER.get() {
        telemetry_sender.send(event).await;
    }
}

/// This environment variable can be set to disable sending telemetry events.
pub const DISABLE_TELEMETRY_ENV_KEY: &str = "QW_DISABLE_TELEMETRY";
