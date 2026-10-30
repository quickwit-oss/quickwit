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

mod clock;
mod install;
mod machine;
pub mod payload;
/// This crate contains the code responsible for sending anonymous usage data to the telemetry
/// relay. See `docs/telemetry.md` for the data that is sent.
mod sender;
pub(crate) mod sink;
mod usage;

use std::ffi::OsStr;
use std::sync::OnceLock;

use payload::QuickwitTelemetryInfo;
use tracing::info;

pub use crate::install::load_or_create_install_id;
use crate::payload::TelemetryEvent;
use crate::sender::{TelemetryLoopHandle, TelemetrySender};
pub use crate::usage::{ApiKind, record_api_request, set_cluster_summary};

static TELEMETRY_SENDER: OnceLock<TelemetrySender> = OnceLock::new();

/// Returns a `TelemetryLoopHandle` if the telemetry loop is not yet started.
pub fn start_telemetry_loop(quickwit_info: QuickwitTelemetryInfo) -> Option<TelemetryLoopHandle> {
    let telemetry_sender =
        TELEMETRY_SENDER.get_or_init(|| TelemetrySender::from_quickwit_info(quickwit_info));
    let telemetry_loop_handle_opt = telemetry_sender.start_loop();
    // This should not happen... unless telemetry is enabled and you are running tests in parallel
    // in the same process.
    if telemetry_loop_handle_opt.is_none() {
        info!("telemetry loop already started. please disable telemetry during tests");
    }
    telemetry_loop_handle_opt
}

/// Sends a telemetry event to the telemetry relay via HTTP.
///
/// Telemetry guarantees to send at most 1 request per minute, except for one last request with
/// the pending events when the node shuts down.
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

/// telemetry is disabled in tests.
#[cfg(test)]
pub fn is_telemetry_disabled() -> bool {
    true
}
/// Check to see if telemetry is enabled.
#[cfg(not(test))]
pub fn is_telemetry_disabled() -> bool {
    let env_value_opt = std::env::var_os(crate::DISABLE_TELEMETRY_ENV_KEY);
    is_telemetry_disabled_by_env_value(env_value_opt.as_deref())
}

/// Telemetry is disabled when `QW_DISABLE_TELEMETRY` is set to any non-empty value other than
/// `false`, `no` or `0`. Unrecognized values disable telemetry so that a mistyped opt-out never
/// leaves telemetry on.
fn is_telemetry_disabled_by_env_value(env_value_opt: Option<&OsStr>) -> bool {
    let Some(env_value) = env_value_opt else {
        return false;
    };
    let Some(env_value_str) = env_value.to_str() else {
        return true;
    };
    if env_value_str.is_empty() {
        return false;
    }
    quickwit_common::parse_bool_lenient(env_value_str) != Some(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_telemetry_disabled_by_env_value() {
        assert!(!is_telemetry_disabled_by_env_value(None));
        assert!(!is_telemetry_disabled_by_env_value(Some(OsStr::new(""))));
        for enabled_value in ["false", "FALSE", "no", "0"] {
            assert!(!is_telemetry_disabled_by_env_value(Some(OsStr::new(
                enabled_value
            ))));
        }
        for disabled_value in [" ", "1", "true", "yes", "on", "disabled", "anything"] {
            assert!(is_telemetry_disabled_by_env_value(Some(OsStr::new(
                disabled_value
            ))));
        }
    }
}
