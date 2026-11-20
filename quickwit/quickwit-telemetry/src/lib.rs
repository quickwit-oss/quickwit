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

//! Anonymous usage telemetry. See `docs/telemetry.md` for the payload and opt-out.

#![deny(clippy::disallowed_methods)]

mod install;
mod machine;
pub mod payload;
mod sender;
pub(crate) mod sink;
mod usage;

use std::path::Path;
use std::sync::OnceLock;

use payload::QuickwitTelemetryInfo;
use tracing::{info, warn};

use crate::install::load_or_create_install_id;
use crate::payload::TelemetryEvent;
use crate::sender::{TelemetryLoopHandle, TelemetrySender};
use crate::sink::HttpClient;
pub use crate::usage::{ApiKind, IndexingSourceKind, record_api_request, record_indexed_batch};

static TELEMETRY_SENDER: OnceLock<Option<TelemetrySender>> = OnceLock::new();

/// Starts telemetry and queues `install` when a new install ID is created.
/// Returns `None` when disabled, initialization fails, or startup was already attempted.
/// Must be called from a Tokio runtime.
pub fn start_telemetry_loop(
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
        // Do not create an install ID until telemetry can be sent.
        let is_new_install = match load_or_create_install_id(data_dir) {
            Ok((install_uuid, is_new_install)) => {
                quickwit_info.install_uuid = Some(install_uuid);
                is_new_install
            }
            Err(error) => {
                warn!(%error, "failed to load or create the telemetry install ID, telemetry is disabled");
                return None;
            }
        };
        is_new_install_opt = Some(is_new_install);
        info!("telemetry to {} is enabled", http_client.endpoint_origin());
        Some(TelemetrySender::new(quickwit_info, http_client))
    }).as_ref()?;
    let Some(is_new_install) = is_new_install_opt else {
        info!("telemetry loop already started. please disable telemetry during tests");
        return None;
    };
    let telemetry_loop_handle = telemetry_sender.start_loop();
    if is_new_install {
        telemetry_sender.send(TelemetryEvent::Install);
    }
    Some(telemetry_loop_handle)
}

/// Queues an event if telemetry has started. Events are dropped when the queue is full.
/// Sends at most 10 events per request and one request per minute, plus a final flush on shutdown.
pub fn send_telemetry_event(event: TelemetryEvent) {
    if let Some(telemetry_sender) = TELEMETRY_SENDER.get().and_then(Option::as_ref) {
        telemetry_sender.send(event);
    }
}

/// Returns whether telemetry has started. Do not cache before startup.
pub fn is_telemetry_enabled() -> bool {
    TELEMETRY_SENDER.get().and_then(Option::as_ref).is_some()
}

/// Setting this environment variable to any value, even empty or `false`, disables telemetry, as
/// in releases 0.6.0 to 0.8.x.
pub const DISABLE_TELEMETRY_ENV_KEY: &str = "QW_DISABLE_TELEMETRY";

pub fn is_telemetry_disabled() -> bool {
    std::env::var_os(DISABLE_TELEMETRY_ENV_KEY).is_some()
}
