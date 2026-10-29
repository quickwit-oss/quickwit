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

use std::collections::{BTreeMap, HashMap};

use anyhow::ensure;
use serde::{Deserialize, Serialize};

const QW_AUTH_ROOT_PUBLIC_KEY: &str = "QW_AUTH_ROOT_PUBLIC_KEY";
const QW_AUTH_NODE_TOKEN: &str = "QW_AUTH_NODE_TOKEN";

/// The `authorization` section of the node config. Authorization is disabled when it is absent.
///
/// The keys and roles are validated when authorization is initialized at node startup.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationConfig {
    /// Hex-encoded public key of the root key pair that signs every token.
    /// Can be overridden with the `QW_AUTH_ROOT_PUBLIC_KEY` environment variable.
    #[serde(default)]
    pub root_public_key: String,
    /// Base64-encoded token used by this node for its own gRPC calls.
    /// Can be overridden with the `QW_AUTH_NODE_TOKEN` environment variable.
    #[serde(default)]
    pub node_token: String,
    /// Role name -> rights granted to the role (`index:read`, `index:write`, `index:admin`,
    /// `cluster`).
    #[serde(default)]
    pub roles: BTreeMap<String, Vec<String>>,
}

impl AuthorizationConfig {
    pub(crate) fn resolve(mut self, env_vars: &HashMap<String, String>) -> anyhow::Result<Self> {
        if let Some(root_public_key) = env_vars.get(QW_AUTH_ROOT_PUBLIC_KEY) {
            self.root_public_key = root_public_key.clone();
        }
        if let Some(node_token) = env_vars.get(QW_AUTH_NODE_TOKEN) {
            self.node_token = node_token.clone();
        }
        ensure!(
            !self.root_public_key.is_empty(),
            "`authorization.root_public_key` must be set when authorization is enabled"
        );
        ensure!(
            !self.node_token.is_empty(),
            "`authorization.node_token` must be set when authorization is enabled"
        );
        Ok(self)
    }

    pub(crate) fn redact(&mut self) {
        self.node_token = "***redacted***".to_string();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_authorization_config_from_yaml() {
        let yaml = r#"
            root_public_key: abcd
            node_token: efgh
            roles:
              reader: [index:read]
        "#;
        let config: AuthorizationConfig = serde_yaml::from_str(yaml).unwrap();
        let config = config.resolve(&HashMap::new()).unwrap();
        assert_eq!(config.root_public_key, "abcd");
        assert_eq!(config.roles["reader"], vec!["index:read".to_string()]);
    }

    #[test]
    fn test_authorization_config_env_override() {
        let config = AuthorizationConfig::default();
        let env_vars = HashMap::from([
            (QW_AUTH_ROOT_PUBLIC_KEY.to_string(), "abcd".to_string()),
            (QW_AUTH_NODE_TOKEN.to_string(), "efgh".to_string()),
        ]);
        let config = config.resolve(&env_vars).unwrap();
        assert_eq!(config.root_public_key, "abcd");
        assert_eq!(config.node_token, "efgh");
    }

    #[test]
    fn test_authorization_config_requires_keys() {
        AuthorizationConfig::default()
            .resolve(&HashMap::new())
            .unwrap_err();
    }

    #[test]
    fn test_authorization_config_rejects_unknown_fields() {
        serde_yaml::from_str::<AuthorizationConfig>("license: foo").unwrap_err();
    }
}
