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

use std::collections::HashMap;
use std::str::FromStr;
use std::{any, fmt};

use anyhow::{self, Context};
use serde::{Deserialize, Deserializer};

use crate::qw_env_vars::{QW_ENV_VARS, QW_NONE};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct ConfigValue<T, const E: usize> {
    /// Value provided by the user in a config file.
    provided: Option<T>,
    /// Value provided by Quickwit as default.
    default: Option<T>,
}

impl<T, const E: usize> ConfigValue<T, E>
where
    T: FromStr,
    <T as FromStr>::Err: fmt::Debug,
{
    pub(crate) fn with_default(value: T) -> Self {
        Self {
            provided: None,
            default: Some(value),
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test(value: T) -> Self {
        Self {
            provided: Some(value),
            default: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn none() -> Self {
        Self {
            provided: None,
            default: None,
        }
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub(crate) fn unwrap(self) -> T {
        self.provided.or(self.default).unwrap()
    }

    pub(crate) fn resolve_optional(
        self,
        env_vars: &HashMap<String, String>,
    ) -> anyhow::Result<Option<T>> {
        // QW env vars take precedence over the config file values.
        if E > QW_NONE
            && let Some(env_var_key) = QW_ENV_VARS.get(&E)
            && let Some(env_var_value) = env_vars.get(*env_var_key)
        {
            let value = env_var_value.parse::<T>().map_err(|error| {
                anyhow::anyhow!(
                    "failed to convert value `{env_var_value}` read from environment variable \
                     `{env_var_key}` to type `{}`: {error:?}",
                    any::type_name::<T>(),
                )
            })?;
            return Ok(Some(value));
        }
        Ok(self.provided.or(self.default))
    }

    pub(crate) fn resolve(self, env_vars: &HashMap<String, String>) -> anyhow::Result<T> {
        self.resolve_optional(env_vars)?.context(
            "failed to resolve field value: no value was provided via environment variable or \
             config file, and the field has no default",
        )
    }
}

impl<T, const E: usize> Default for ConfigValue<T, E>
where T: Default
{
    fn default() -> Self {
        Self {
            provided: None,
            default: Some(T::default()),
        }
    }
}

impl<'de, T, const E: usize> Deserialize<'de> for ConfigValue<T, E>
where T: Deserialize<'de>
{
    fn deserialize<D>(deserializer: D) -> Result<ConfigValue<T, E>, D::Error>
    where D: Deserializer<'de> {
        let value: Option<T> = Deserialize::deserialize(deserializer)?;
        Ok(ConfigValue {
            provided: value,
            default: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::qw_env_vars::{
        QW_CLUSTER_ID, QW_GOSSIP_LISTEN_PORT, QW_NODE_ID, QW_REST_LISTEN_PORT,
    };

    #[test]
    fn test_config_value_resolve_optional() {
        {
            let env_vars = HashMap::new();
            let rest_listen_port = ConfigValue::<usize, QW_REST_LISTEN_PORT>::none();
            assert!(
                rest_listen_port
                    .resolve_optional(&env_vars)
                    .unwrap()
                    .is_none()
            );
        }
        {
            let env_vars = HashMap::new();
            let rest_listen_port = ConfigValue::<usize, QW_REST_LISTEN_PORT>::with_default(7280);
            assert_eq!(
                rest_listen_port
                    .resolve_optional(&env_vars)
                    .unwrap()
                    .unwrap(),
                7280
            );
        }
        {
            let env_vars = HashMap::new();
            let rest_listen_port = ConfigValue::<usize, QW_REST_LISTEN_PORT> {
                provided: Some(5678),
                default: Some(7820),
            };
            assert_eq!(
                rest_listen_port
                    .resolve_optional(&env_vars)
                    .unwrap()
                    .unwrap(),
                5678
            );
        }
        {
            let mut env_vars = HashMap::new();
            env_vars.insert("QW_REST_LISTEN_PORT".to_string(), "foobar".to_string());
            let rest_listen_port = ConfigValue::<usize, QW_REST_LISTEN_PORT> {
                provided: Some(5678),
                default: Some(7820),
            };
            rest_listen_port.resolve_optional(&env_vars).unwrap_err();
        }
        {
            let mut env_vars = HashMap::new();
            env_vars.insert("QW_REST_LISTEN_PORT".to_string(), "1234".to_string());
            let rest_listen_port = ConfigValue::<usize, QW_REST_LISTEN_PORT> {
                provided: Some(5678),
                default: Some(7820),
            };
            assert_eq!(
                rest_listen_port
                    .resolve_optional(&env_vars)
                    .unwrap()
                    .unwrap(),
                1234
            );
        }
    }

    #[test]
    fn test_config_value_resolve() {
        let env_vars = HashMap::new();
        let rest_listen_port = ConfigValue::<usize, QW_REST_LISTEN_PORT>::none();
        rest_listen_port.resolve(&env_vars).unwrap_err();
    }

    #[test]
    fn test_config_value_deserialize() {
        fn default_cluster_id() -> ConfigValue<String, QW_CLUSTER_ID> {
            ConfigValue::with_default("default-cluster".to_string())
        }

        fn default_node_id() -> ConfigValue<String, QW_NODE_ID> {
            ConfigValue::with_default("default-node".to_string())
        }

        fn default_rest_listen_port() -> ConfigValue<usize, QW_REST_LISTEN_PORT> {
            ConfigValue::with_default(7280)
        }

        #[derive(Deserialize)]
        struct Config {
            #[serde(default)]
            version: ConfigValue<usize, QW_NONE>,
            #[serde(default = "default_cluster_id")]
            cluster_id: ConfigValue<String, QW_CLUSTER_ID>,
            #[serde(default = "default_node_id")]
            node_id: ConfigValue<String, QW_NODE_ID>,
            #[serde(default = "default_rest_listen_port")]
            rest_listen_port: ConfigValue<usize, QW_REST_LISTEN_PORT>,
            gossip_listen_port: ConfigValue<String, QW_GOSSIP_LISTEN_PORT>,
        }
        let config = serde_yaml::from_str::<Config>(
            r#"
            cluster_id: qw-cluster
            "#,
        )
        .unwrap();

        let mut env_vars = HashMap::new();
        env_vars.insert("QW_REST_LISTEN_PORT".to_string(), "1234".to_string());

        assert_eq!(config.version.resolve(&env_vars).unwrap(), 0);
        assert_eq!(config.cluster_id.resolve(&env_vars).unwrap(), "qw-cluster");
        assert_eq!(config.node_id.resolve(&env_vars).unwrap(), "default-node");
        assert_eq!(config.rest_listen_port.resolve(&env_vars).unwrap(), 1234);
        assert!(
            config
                .gossip_listen_port
                .resolve_optional(&env_vars)
                .unwrap()
                .is_none()
        );
    }
}
