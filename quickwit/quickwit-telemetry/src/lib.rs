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

//! This crate contains the code responsible for sending anonymous usage data to the telemetry
//! relay. See `docs/telemetry.md` for the data that is sent.

#![deny(clippy::disallowed_methods)]

mod clock;
mod install;
mod machine;
pub mod payload;
mod sender;
pub(crate) mod sink;
mod usage;

use std::ffi::OsStr;
use std::path::Path;
use std::sync::OnceLock;

use payload::QuickwitTelemetryInfo;
use tracing::{info, warn};

use crate::install::load_or_create_install_id;
use crate::payload::TelemetryEvent;
use crate::sender::{TelemetryLoopHandle, TelemetrySender};
use crate::sink::HttpClient;
pub use crate::usage::{ApiKind, record_api_request};

static TELEMETRY_SENDER: OnceLock<TelemetrySender> = OnceLock::new();

/// Starts the telemetry loop, and queues the `install` event if the install ID stored in
/// `data_dir` was just created. Returns `None` if telemetry is disabled, if the HTTP client can't
/// be created, or if the loop is already started.
pub async fn start_telemetry_loop(
    mut quickwit_info: QuickwitTelemetryInfo,
    data_dir: &Path,
) -> Option<TelemetryLoopHandle> {
    if is_telemetry_disabled() {
        info!("telemetry is disabled");
        return None;
    }
    let http_client = match HttpClient::try_new() {
        Ok(http_client) => http_client,
        Err(error) => {
            warn!(
                ?error,
                "failed to create the telemetry HTTP client, telemetry is disabled"
            );
            return None;
        }
    };
    let mut is_new_install_opt = None;
    let telemetry_sender = TELEMETRY_SENDER.get_or_init(|| {
        // Only created once telemetry can be sent, so that a node with telemetry off never writes
        // the install ID.
        let is_new_install = match load_or_create_install_id(data_dir) {
            Ok((install_uuid, is_new_install)) => {
                quickwit_info.install_uuid = Some(install_uuid);
                is_new_install
            }
            Err(error) => {
                warn!(%error, "failed to load or create the telemetry install ID");
                false
            }
        };
        is_new_install_opt = Some(is_new_install);
        info!("telemetry to {} is enabled", http_client.endpoint());
        TelemetrySender::with_http_client(quickwit_info, http_client)
    });
    let Some(is_new_install) = is_new_install_opt else {
        // This should not happen... unless telemetry is enabled and you are running tests in
        // parallel in the same process.
        info!("telemetry loop already started. please disable telemetry during tests");
        return None;
    };
    let telemetry_loop_handle = telemetry_sender.start_loop();
    if is_new_install {
        telemetry_sender.send(TelemetryEvent::Install).await;
    }
    Some(telemetry_loop_handle)
}

/// Queues a telemetry event for the telemetry loop to send to the telemetry relay. Does nothing
/// unless `start_telemetry_loop` started the loop.
///
/// Telemetry sends at most 1 request per minute, except for one last request with the pending
/// events when the node shuts down. Each request holds at most 10 events, so some events are
/// dropped if this function is called too often.
///
/// If a request fails, the error is only logged at the debug level.
///
/// The event is an enum so that readers can audit what information is sent.
pub async fn send_telemetry_event(event: TelemetryEvent) {
    if let Some(telemetry_sender) = TELEMETRY_SENDER.get() {
        telemetry_sender.send(event).await;
    }
}

/// Returns true once `start_telemetry_loop` has started the telemetry loop. It is false before
/// that, so check it on each use instead of caching it.
pub fn is_telemetry_enabled() -> bool {
    TELEMETRY_SENDER.get().is_some()
}

/// This environment variable can be set to disable sending telemetry events.
pub const DISABLE_TELEMETRY_ENV_KEY: &str = "QW_DISABLE_TELEMETRY";

/// Returns true if `QW_DISABLE_TELEMETRY` disables telemetry.
pub fn is_telemetry_disabled() -> bool {
    let env_value_opt = std::env::var_os(DISABLE_TELEMETRY_ENV_KEY);
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
