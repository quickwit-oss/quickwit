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

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::OnceLock;

use anyhow::{Context, bail};
use biscuit_auth::PublicKey;
use tracing::info;

use crate::AuthorizationToken;
use crate::authorizer::RequestFamily;
use crate::token::validate_datalog_string;

/// Authorization settings, built from the `authorization` section of the node config.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizationSettings {
    pub root_public_key_hex: String,
    pub node_token_base64: String,
    /// Role name -> request families granted to the role.
    pub roles: BTreeMap<String, Vec<String>>,
}

pub(crate) struct EnabledState {
    pub root_public_key: PublicKey,
    pub node_token: AuthorizationToken,
    /// `role_right("<role>", "<request family>");` facts, added to every authorizer.
    pub role_rights_datalog: String,
}

enum AuthorizationState {
    Disabled,
    Enabled {
        settings: AuthorizationSettings,
        state: Box<EnabledState>,
    },
}

static AUTHORIZATION_STATE: OnceLock<AuthorizationState> = OnceLock::new();

/// Initializes authorization for the whole process.
///
/// Must be called once at node startup, before any service is started. Several nodes may run in
/// the same process (integration tests): calling this function again with the same settings is a
/// no-op, while calling it with different settings is an error.
///
/// Hidden contract: if this function is never called, authorization is disabled. Production
/// startup (`quickwit_serve::serve_quickwit`) always calls it.
pub fn init_authorization(settings_opt: Option<AuthorizationSettings>) -> anyhow::Result<()> {
    let new_state = match settings_opt.clone() {
        None => AuthorizationState::Disabled,
        Some(settings) => {
            let state = Box::new(build_enabled_state(&settings)?);
            AuthorizationState::Enabled { settings, state }
        }
    };
    let enabled = matches!(new_state, AuthorizationState::Enabled { .. });
    let current_state = AUTHORIZATION_STATE.get_or_init(|| new_state);
    let current_settings_opt = match current_state {
        AuthorizationState::Disabled => None,
        AuthorizationState::Enabled { settings, .. } => Some(settings),
    };
    if current_settings_opt != settings_opt.as_ref() {
        bail!("authorization was already initialized with different settings");
    }
    info!(enabled, "authorization initialized");
    Ok(())
}

fn build_enabled_state(settings: &AuthorizationSettings) -> anyhow::Result<EnabledState> {
    let root_public_key = PublicKey::from_bytes_hex(&settings.root_public_key_hex)
        .context("failed to parse authorization root public key")?;
    let node_token = AuthorizationToken::parse(&settings.node_token_base64, root_public_key)
        .context("failed to parse authorization node token")?;
    let mut role_rights_datalog = String::new();
    for (role, request_families) in &settings.roles {
        let role = validate_datalog_string(role)?;
        for request_family_str in request_families {
            let request_family = RequestFamily::from_str(request_family_str)
                .with_context(|| format!("invalid right for role `{role}`"))?;
            role_rights_datalog.push_str(&format!(
                "role_right(\"{role}\", \"{}\");\n",
                request_family.as_str()
            ));
        }
    }
    Ok(EnabledState {
        root_public_key,
        node_token,
        role_rights_datalog,
    })
}

#[cfg(test)]
pub(crate) fn build_enabled_state_for_tests(settings: &AuthorizationSettings) -> EnabledState {
    build_enabled_state(settings).unwrap()
}

/// Returns `None` when authorization is disabled.
pub(crate) fn enabled_state() -> Option<&'static EnabledState> {
    match AUTHORIZATION_STATE.get()? {
        AuthorizationState::Disabled => None,
        AuthorizationState::Enabled { state, .. } => Some(state),
    }
}

pub fn is_authorization_enabled() -> bool {
    enabled_state().is_some()
}
