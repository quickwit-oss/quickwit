// Copyright (C) 2022 Quickwit, Inc.
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

use std::cmp::PartialEq;
use std::collections::HashMap;
use std::ops::Deref;
use std::str::FromStr;
use std::{any, fmt};

use anyhow::{anyhow, bail};
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::qw_env_vars::{QW_ENV_VARS, QW_NONE};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigValueSource {
    EnvVar(String),
    EnvVarDefault(String),
    UserProvided,
    Default,
}

#[derive(Clone, Debug)]
pub struct ConfigValue<T> {
    pub value: T,
    pub source: ConfigValueSource,
}

impl<T> ConfigValue<T> {
    pub fn with_default(value: T) -> Self {
        ConfigValue {
            value,
            source: ConfigValueSource::Default,
        }
    }

    pub fn map<U, F>(self, f: F) -> ConfigValue<U>
    where F: FnOnce(T) -> U {
        ConfigValue {
            value: f(self.value),
            source: self.source,
        }
    }

    pub fn try_map<U, F, E>(self, f: F) -> Result<ConfigValue<U>, E>
    where F: FnOnce(T) -> Result<U, E> {
        f(self.value).map(|value| ConfigValue {
            value,
            source: self.source,
        })
    }
}

impl<T> ConfigValue<T>
where T: Clone
{
    pub fn cloned(&self) -> T {
        self.value.clone()
    }
}

impl<T> fmt::Display for ConfigValue<T>
where T: fmt::Display
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.value.fmt(f)
    }
}

impl<T> Default for ConfigValue<T>
where T: Default
{
    fn default() -> Self {
        Self {
            value: T::default(),
            source: ConfigValueSource::Default,
        }
    }
}

impl<T> Deref for ConfigValue<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T, U> PartialEq<U> for ConfigValue<T>
where T: PartialEq<U>
{
    fn eq(&self, other: &U) -> bool {
        self.value.eq(other)
    }
}

impl<T> Serialize for ConfigValue<T>
where T: Serialize
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        self.value.serialize(serializer)
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct ConfigValueBuilder<T, const E: usize> {
    env_var_key: Option<String>,
    env_var_default: Option<String>,
    provided: Option<T>,
    default_value: Option<T>,
    defaultify: bool,
}

impl<T, const E: usize> ConfigValueBuilder<T, E>
where
    T: Default + FromStr,
    <T as FromStr>::Err: fmt::Debug,
{
    /// Resolves the config value in the following order:
    /// 1. environment variable override
    /// 2. environment variable override default
    /// 3. QW environment variable
    /// 4. user provided value
    /// 5. default value
    pub fn build(self, env_vars: &HashMap<String, String>) -> anyhow::Result<ConfigValue<T>> {
        // 1. environment variable override
        if let Some(env_var_key) = self.env_var_key {
            if let Some(env_var_value) = env_vars.get(&env_var_key) {
                let value = env_var_value.parse::<T>().map_err(|error| {
                    anyhow!(
                        "Failed to convert value `{env_var_value}` of environment variable \
                         `{env_var_key}` to type `{}`: {error:?}",
                        any::type_name::<T>(),
                    )
                })?;
                return Ok(ConfigValue {
                    value,
                    source: ConfigValueSource::EnvVar(env_var_key),
                });
            // 2. environment variable override default
            } else if let Some(env_var_default) = self.env_var_default {
                let value = env_var_default.parse::<T>().map_err(|error| {
                    anyhow!(
                        "Failed to convert default value `{env_var_default}` of environment \
                         variable `{env_var_key}` to type `{}`: {error:?}",
                        any::type_name::<T>(),
                    )
                })?;
                return Ok(ConfigValue {
                    value,
                    source: ConfigValueSource::EnvVarDefault(env_var_key),
                });
            }
        }
        // 3. QW environment variable
        if E > QW_NONE {
            if let Some(env_var_key) = QW_ENV_VARS.get(&E) {
                if let Some(env_var_value) = env_vars.get(*env_var_key) {
                    let value = env_var_value.parse::<T>().map_err(|error| {
                        anyhow!(
                            "Failed to convert value `{env_var_value}` of environment variable \
                             `{env_var_key}` to type `{}`: {error:?}",
                            any::type_name::<T>(),
                        )
                    })?;
                    return Ok(ConfigValue {
                        value,
                        source: ConfigValueSource::EnvVar(env_var_key.to_string()),
                    });
                }
            }
        }
        // 4. user provided value
        if let Some(value) = self.provided {
            return Ok(ConfigValue {
                value,
                source: ConfigValueSource::UserProvided,
            });
        }
        // 5. default value
        if let Some(value) = self.default_value {
            return Ok(ConfigValue {
                value,
                source: ConfigValueSource::Default,
            });
        }
        if self.defaultify {
            let value = T::default();
            return Ok(ConfigValue {
                value,
                source: ConfigValueSource::Default,
            });
        }
        bail!("Failed to build config value. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.")
    }

    pub fn with_default(value: T) -> Self {
        Self {
            default_value: Some(value),
            defaultify: false,
            ..Default::default()
        }
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test(self) -> ConfigValue<T> {
        let env_vars = HashMap::new();
        self.build(&env_vars).unwrap()
    }
}

impl<T, const E: usize> Default for ConfigValueBuilder<T, E> {
    fn default() -> Self {
        Self {
            env_var_key: None,
            env_var_default: None,
            provided: None,
            default_value: None,
            defaultify: true,
        }
    }
}

/// A helper struct for deserializing a string or any other type into a `ConfigValueBuilder`.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum StringOrAny<T> {
    Str(String),
    Any(T),
}

impl<'de, T, const E: usize> Deserialize<'de> for ConfigValueBuilder<T, E>
where
    T: Deserialize<'de> + FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    fn deserialize<D>(deserializer: D) -> Result<ConfigValueBuilder<T, E>, D::Error>
    where D: Deserializer<'de> {
        let maybe_override = match StringOrAny::deserialize(deserializer)? {
            StringOrAny::Str(maybe_override) => maybe_override,
            StringOrAny::Any(value) => {
                return Ok(ConfigValueBuilder {
                    provided: Some(value),
                    defaultify: false,
                    ..Default::default()
                })
            }
        };
        if let Some((env_var_key, env_var_default)) =
            parse_env_var_override(&maybe_override).map_err(D::Error::custom)?
        {
            return Ok(ConfigValueBuilder {
                env_var_key: Some(env_var_key),
                env_var_default,
                defaultify: false,
                ..Default::default()
            });
        }
        // Cast the `String` back into a `T`... If anyboby knows a better way to do this, please let
        // us know!
        let value = maybe_override.parse::<T>().map_err(D::Error::custom)?;
        Ok(ConfigValueBuilder {
            provided: Some(value),
            defaultify: false,
            ..Default::default()
        })
    }
}

/// Attempts to parse an environment variable override of the form `${ENV_VAR}` or
/// `${ENV_VAR:-default}`. Returns `Ok(None)` if the value is simply a string literal.
fn parse_env_var_override(
    maybe_override: &str,
) -> anyhow::Result<Option<(String, Option<String>)>> {
    let maybe_trimmed_override = maybe_override.trim();
    if !maybe_trimmed_override.starts_with("${") || !maybe_trimmed_override.ends_with('}') {
        return Ok(None);
    }
    let env_var_override = &maybe_trimmed_override[2..maybe_trimmed_override.len() - 1];

    let (env_var_key, env_var_default) =
        if let Some((env_var_key, env_var_default)) = env_var_override.split_once(":-") {
            let trimmed_env_var_default = env_var_default.trim();
            if trimmed_env_var_default.is_empty() {
                bail!(
                    "Failed to parse environment variable override `{maybe_override}`: default \
                     value is empty.",
                );
            }
            (env_var_key.trim(), Some(trimmed_env_var_default))
        } else {
            (env_var_override.trim(), None)
        };
    if env_var_key.is_empty() {
        bail!(
            "Failed to parse environment variable override `{maybe_override}`: environment \
             variable name is empty.",
        );
    }
    if env_var_key.chars().any(|char| char == '-' || char == ':') {
        bail!(
            "Failed to parse environment variable override `{maybe_override}`: correct syntax is \
             `${{ENV_VAR}}` or `${{ENV_VAR:-default}}`.",
        );
    }
    Ok(Some((
        env_var_key.to_string(),
        env_var_default.map(|evd| evd.to_string()),
    )))
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::qw_env_vars::{
        QW_CLUSTER_ID, QW_LISTEN_ADDRESS, QW_NODE_ID, QW_NONE, QW_REST_LISTEN_PORT,
    };

    #[test]
    fn test_config_value_display() {
        let config_value: ConfigValue<usize> = ConfigValue::default();
        assert_eq!(format!("{config_value}"), "0");
    }

    #[test]
    fn test_config_value_default() {
        let config_value: ConfigValue<usize> = ConfigValue::default();
        assert_eq!(config_value.value, 0);
        assert_eq!(config_value.source, ConfigValueSource::Default);
    }

    #[test]
    fn test_config_value_builder_build() {
        let mut env_vars = HashMap::new();
        env_vars.insert("CLUSTER_ID".to_string(), "test-cluster".to_string());
        env_vars.insert("QW_CLUSTER_ID".to_string(), "qw-test-cluster".to_string());
        {
            let config_value_builder: ConfigValueBuilder<String, QW_CLUSTER_ID> =
                ConfigValueBuilder {
                    env_var_key: Some("CLUSTER_ID".to_string()),
                    ..Default::default()
                };
            let config_value = config_value_builder.build(&env_vars).unwrap();
            assert_eq!(config_value.value, "test-cluster");
            assert_eq!(
                config_value.source,
                ConfigValueSource::EnvVar("CLUSTER_ID".to_string())
            );
        }
        {
            let config_value_builder: ConfigValueBuilder<String, QW_CLUSTER_ID> =
                ConfigValueBuilder {
                    env_var_key: Some("CLUSTER".to_string()),
                    env_var_default: Some("default-cluster".to_string()),
                    ..Default::default()
                };
            let config_value = config_value_builder.build(&env_vars).unwrap();
            assert_eq!(config_value.value, "default-cluster");
            assert_eq!(
                config_value.source,
                ConfigValueSource::EnvVarDefault("CLUSTER".to_string())
            );
        }
        {
            let config_value_builder: ConfigValueBuilder<String, QW_CLUSTER_ID> =
                ConfigValueBuilder {
                    env_var_key: Some("CLUSTER".to_string()),
                    ..Default::default()
                };
            let config_value = config_value_builder.build(&env_vars).unwrap();
            assert_eq!(config_value.value, "qw-test-cluster");
            assert_eq!(
                config_value.source,
                ConfigValueSource::EnvVar("QW_CLUSTER_ID".to_string())
            );
        }
        {
            let config_value_builder: ConfigValueBuilder<String, QW_NODE_ID> = ConfigValueBuilder {
                provided: Some("test-node".to_string()),
                ..Default::default()
            };
            let config_value = config_value_builder.build(&env_vars).unwrap();
            assert_eq!(config_value.value, "test-node");
            assert_eq!(config_value.source, ConfigValueSource::UserProvided);
        }
        {
            let config_value_builder: ConfigValueBuilder<String, QW_NODE_ID> = ConfigValueBuilder {
                default_value: Some("default-test-node".to_string()),
                ..Default::default()
            };
            let config_value = config_value_builder.build(&env_vars).unwrap();
            assert_eq!(config_value.value, "default-test-node");
            assert_eq!(config_value.source, ConfigValueSource::Default);
        }
    }

    #[test]
    fn test_config_value_builder_deser() {
        #[derive(Debug, Deserialize)]
        struct MyConfigBuilder {
            #[serde(default)]
            version: ConfigValueBuilder<usize, QW_NONE>,
            #[serde(default = "default_cluster_id")]
            cluster_id: ConfigValueBuilder<String, QW_CLUSTER_ID>,
            node_id: ConfigValueBuilder<String, QW_NODE_ID>,
            listen_address: ConfigValueBuilder<String, QW_LISTEN_ADDRESS>,
            rest_listen_port: ConfigValueBuilder<usize, QW_REST_LISTEN_PORT>,
        }

        fn default_cluster_id() -> ConfigValueBuilder<String, 1> {
            ConfigValueBuilder::with_default("test-cluster".to_string())
        }

        let config_yaml = r#"
            node_id: test-node
            listen_address: ${LISTEN_ADDRESS}
            rest_listen_port: ${REST_LISTEN_PORT:-7280}
        "#;
        let config_builder = serde_yaml::from_str::<MyConfigBuilder>(config_yaml).unwrap();
        assert_eq!(
            config_builder.version,
            ConfigValueBuilder {
                defaultify: true,
                ..Default::default()
            }
        );
        assert_eq!(
            config_builder.cluster_id,
            ConfigValueBuilder {
                default_value: Some("test-cluster".to_string()),
                defaultify: false,
                ..Default::default()
            }
        );
        assert_eq!(
            config_builder.node_id,
            ConfigValueBuilder {
                provided: Some("test-node".to_string()),
                defaultify: false,
                ..Default::default()
            }
        );
        assert_eq!(
            config_builder.listen_address,
            ConfigValueBuilder {
                env_var_key: Some("LISTEN_ADDRESS".to_string()),
                defaultify: false,
                ..Default::default()
            }
        );
        assert_eq!(
            config_builder.rest_listen_port,
            ConfigValueBuilder {
                env_var_key: Some("REST_LISTEN_PORT".to_string()),
                env_var_default: Some("7280".to_string()),
                defaultify: false,
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_parse_env_var_override() {
        parse_env_var_override("${}").unwrap_err();
        parse_env_var_override("${:}").unwrap_err();
        parse_env_var_override("${:-}").unwrap_err();
        parse_env_var_override("${ENV_VAR:}").unwrap_err();
        parse_env_var_override("${ENV_VAR:-}").unwrap_err();
        parse_env_var_override("${ENV_VAR:default}").unwrap_err();
        assert_eq!(
            parse_env_var_override("${ENV_VAR}").unwrap(),
            Some(("ENV_VAR".to_string(), None))
        );
        assert_eq!(
            parse_env_var_override("${ENV_VAR:-default}").unwrap(),
            Some(("ENV_VAR".to_string(), Some("default".to_string())))
        );
        assert!(parse_env_var_override("${ENV_VAR").unwrap().is_none());
        assert!(parse_env_var_override("{ENV_VAR}").unwrap().is_none());
    }
}
