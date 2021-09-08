// Copyright (C) 2021 Quickwit, Inc.
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

#![allow(clippy::bool_assert_comparison)]
pub mod payload;
/// This crate contains  the code responsible for sending usage data to Quickwit inc's server.
mod sender;
pub(crate) mod sink;

use once_cell::sync::OnceCell;

use crate::payload::TelemetryEvent;
pub use crate::sender::is_telemetry_enabled;
use crate::sender::{TelemetryLoopHandle, TelemetrySender};

pub fn start_telemetry_loop() -> TelemetryLoopHandle {
    get_telemetry_sender_singleton().start_loop()
}

fn get_telemetry_sender_singleton() -> &'static TelemetrySender {
    static INSTANCE: OnceCell<TelemetrySender> = OnceCell::new();
    INSTANCE.get_or_init(TelemetrySender::default)
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
    get_telemetry_sender_singleton().send(event).await
}

/// This environment variable can be set to disable sending telemetry events.
pub const DISABLE_TELEMETRY_ENV_KEY: &str = "QUICKWIT_DISABLE_TELEMETRY";
