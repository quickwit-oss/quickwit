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

#![deny(clippy::disallowed_methods)]

use std::collections::HashSet;
use std::str::FromStr;

use anyhow::Context;
use clap::{arg, Arg, ArgMatches};
use dialoguer::theme::ColorfulTheme;
use dialoguer::Confirm;
use quickwit_common::runtimes::RuntimesConfig;
use quickwit_common::uri::Uri;
use quickwit_config::service::QuickwitService;
use quickwit_config::{
    ConfigFormat, MetastoreConfigs, NodeConfig, SourceConfig, StorageConfigs,
    DEFAULT_QW_CONFIG_PATH,
};
use quickwit_indexing::check_source_connectivity;
use quickwit_metastore::{IndexMetadataResponseExt, MetastoreResolver};
use quickwit_proto::metastore::{IndexMetadataRequest, MetastoreService, MetastoreServiceClient};
use quickwit_rest_client::models::Timeout;
use quickwit_rest_client::rest_client::{QuickwitClient, QuickwitClientBuilder, DEFAULT_BASE_URL};
use quickwit_storage::{load_file, StorageResolver};
use reqwest::Url;
use tabled::settings::object::Rows;
use tabled::settings::panel::Header;
use tabled::settings::{Alignment, Modify, Style};
use tabled::{Table, Tabled};
use tracing::info;

use crate::checklist::run_checklist;

pub mod checklist;
pub mod cli;
pub mod index;
#[cfg(feature = "jemalloc")]
pub mod jemalloc;
pub mod logger;
pub mod metrics;
pub mod service;
pub mod source;
pub mod split;
pub mod stats;
pub mod tool;

/// Throughput calculation window size.
const THROUGHPUT_WINDOW_SIZE: usize = 5;

pub const QW_ENABLE_TOKIO_CONSOLE_ENV_KEY: &str = "QW_ENABLE_TOKIO_CONSOLE";

pub const QW_ENABLE_OPENTELEMETRY_OTLP_EXPORTER_ENV_KEY: &str =
    "QW_ENABLE_OPENTELEMETRY_OTLP_EXPORTER";

fn config_cli_arg() -> Arg {
    Arg::new("config")
        .long("config")
        .help("Config file location")
        .env("QW_CONFIG")
        .default_value(DEFAULT_QW_CONFIG_PATH)
        .global(true)
        .display_order(1)
}

fn client_args() -> Vec<Arg> {
    vec![
        arg!(--"endpoint" <QW_CLUSTER_ENDPOINT> "Quickwit cluster endpoint.")
            .default_value("http://127.0.0.1:7280")
            .env("QW_CLUSTER_ENDPOINT")
            .required(false)
            .display_order(1)
            .global(true),
        Arg::new("timeout")
            .long("timeout")
            .help("Duration of the timeout.")
            .required(false)
            .global(true)
            .display_order(2),
        Arg::new("connect-timeout")
            .long("connect-timeout")
            .help("Duration of the connect timeout.")
            .required(false)
            .global(true)
            .display_order(3),
    ]
}

#[derive(Debug, Eq, PartialEq)]
pub struct ClientArgs {
    pub cluster_endpoint: Url,
    pub connect_timeout: Option<Timeout>,
    pub timeout: Option<Timeout>,
    pub commit_timeout: Option<Timeout>,
}

impl Default for ClientArgs {
    fn default() -> Self {
        Self {
            cluster_endpoint: Url::parse(DEFAULT_BASE_URL).unwrap(),
            connect_timeout: None,
            timeout: None,
            commit_timeout: None,
        }
    }
}

impl ClientArgs {
    pub fn client_builder(self) -> QuickwitClientBuilder {
        let mut builder = QuickwitClientBuilder::new(self.cluster_endpoint);
        if let Some(connect_timeout) = self.connect_timeout {
            builder = builder.connect_timeout(connect_timeout);
        }
        if let Some(timeout) = self.timeout {
            builder = builder.timeout(timeout);
            builder = builder.search_timeout(timeout);
            builder = builder.ingest_timeout(timeout);
        }
        if let Some(commit_timeout) = self.commit_timeout {
            builder = builder.commit_timeout(commit_timeout);
        }
        builder
    }

    pub fn client(self) -> QuickwitClient {
        self.client_builder().build()
    }

    pub fn parse_for_ingest(matches: &mut ArgMatches) -> anyhow::Result<Self> {
        Self::parse_inner(matches, true)
    }

    pub fn parse(matches: &mut ArgMatches) -> anyhow::Result<Self> {
        Self::parse_inner(matches, false)
    }

    fn parse_inner(matches: &mut ArgMatches, process_ingest: bool) -> anyhow::Result<Self> {
        let cluster_endpoint = matches
            .remove_one::<String>("endpoint")
            .map(|endpoint_str| Url::from_str(&endpoint_str))
            .expect("`endpoint` should be a required arg.")?;
        let connect_timeout =
            if let Some(duration) = matches.remove_one::<String>("connect-timeout") {
                Some(parse_duration_or_none(&duration)?)
            } else {
                None
            };
        let timeout = if let Some(duration) = matches.remove_one::<String>("timeout") {
            Some(parse_duration_or_none(&duration)?)
        } else {
            None
        };
        let commit_timeout = if process_ingest {
            if let Some(duration) = matches.remove_one::<String>("commit-timeout") {
                Some(parse_duration_or_none(&duration)?)
            } else {
                None
            }
        } else {
            None
        };
        Ok(Self {
            cluster_endpoint,
            connect_timeout,
            timeout,
            commit_timeout,
        })
    }
}

pub fn parse_duration_or_none(duration_with_unit_str: &str) -> anyhow::Result<Timeout> {
    if duration_with_unit_str == "none" {
        Ok(Timeout::none())
    } else {
        humantime::parse_duration(duration_with_unit_str)
            .map(Timeout::new)
            .context("failed to parse timeout")
    }
}

pub fn start_actor_runtimes(
    runtimes_config: RuntimesConfig,
    services: &HashSet<QuickwitService>,
) -> anyhow::Result<()> {
    if services.contains(&QuickwitService::Indexer)
        || services.contains(&QuickwitService::Janitor)
        || services.contains(&QuickwitService::ControlPlane)
    {
        quickwit_common::runtimes::initialize_runtimes(runtimes_config)
            .context("failed to start actor runtimes")?;
    }
    Ok(())
}

/// Loads a node config located at `config_uri` with the default storage configuration.
async fn load_node_config(config_uri: &Uri) -> anyhow::Result<NodeConfig> {
    let config_content = load_file(&StorageResolver::unconfigured(), config_uri)
        .await
        .context(r#"
            Could not find Quickwit config in quickwit/quickwit.yaml.
            Use the `--config` parameter to specify location or see https://quickwit.io/docs/get-started/installation#install-script for expected directory structure."#)?;
    let config_format = ConfigFormat::sniff_from_uri(config_uri)?;
    let config = NodeConfig::load(config_format, config_content.as_slice())
        .await
        .with_context(|| format!("failed to parse node config `{config_uri}`"))?;
    info!(config_uri=%config_uri, config=?config, "loaded node config");
    Ok(config)
}

fn get_resolvers(
    storage_configs: &StorageConfigs,
    metastore_configs: &MetastoreConfigs,
) -> (StorageResolver, MetastoreResolver) {
    // The CLI tests rely on the unconfigured singleton resolvers, so it's better to return them if
    // the storage and metastore configs are not set.
    if storage_configs.is_empty() && metastore_configs.is_empty() {
        return (
            StorageResolver::unconfigured(),
            MetastoreResolver::unconfigured(),
        );
    }
    let storage_resolver = StorageResolver::configured(storage_configs);
    let metastore_resolver =
        MetastoreResolver::configured(storage_resolver.clone(), metastore_configs);
    (storage_resolver, metastore_resolver)
}

/// Runs connectivity checks for a given `metastore_uri` and `index_id`.
/// Optionally, it takes a `SourceConfig` that will be checked instead
/// of the index's sources.
pub async fn run_index_checklist(
    metastore: &mut MetastoreServiceClient,
    storage_resolver: &StorageResolver,
    index_id: &str,
    source_config_opt: Option<&SourceConfig>,
) -> anyhow::Result<()> {
    let mut checks: Vec<(&str, anyhow::Result<()>)> = Vec::new();
    for metastore_endpoint in metastore.endpoints() {
        // If it's not a database, the metastore is file-backed. To display a nicer message to the
        // user, we check the metastore storage connectivity before the mestastore check
        // connectivity which will check the storage anyway.
        if !metastore_endpoint.protocol().is_database() {
            let metastore_storage = storage_resolver.resolve(&metastore_endpoint).await?;
            checks.push((
                "metastore storage",
                metastore_storage.check_connectivity().await,
            ));
        }
    }
    checks.push(("metastore", metastore.check_connectivity().await));
    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await?
        .deserialize_index_metadata()?;
    let index_storage = storage_resolver.resolve(index_metadata.index_uri()).await?;
    checks.push(("index storage", index_storage.check_connectivity().await));

    if let Some(source_config) = source_config_opt {
        checks.push((
            source_config.source_id.as_str(),
            check_source_connectivity(storage_resolver, source_config).await,
        ));
    } else {
        for source_config in index_metadata.sources.values() {
            checks.push((
                source_config.source_id.as_str(),
                check_source_connectivity(storage_resolver, source_config).await,
            ));
        }
    }
    run_checklist(checks)?;
    Ok(())
}

/// Constructs a table for display.
pub fn make_table<T: Tabled>(
    header: &str,
    rows: impl IntoIterator<Item = T>,
    transpose: bool,
) -> Table {
    let mut table = if transpose {
        let index_builder = Table::builder(rows).index();
        index_builder.column(0).transpose().build()
    } else {
        Table::builder(rows).build()
    };

    table
        .with(Modify::new(Rows::new(1..)).with(Alignment::left()))
        .with(Style::ascii())
        .with(Header::new(header))
        .with(Modify::new(Rows::single(0)).with(Alignment::center()));

    table
}

/// Prompts user for confirmation.
fn prompt_confirmation(prompt: &str, default: bool) -> bool {
    if Confirm::with_theme(&ColorfulTheme::default())
        .with_prompt(prompt)
        .default(default)
        .interact()
        .unwrap()
    {
        true
    } else {
        println!("Aborting.");
        false
    }
}

pub mod busy_detector {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::Instant;

    use once_cell::sync::Lazy;
    use tracing::debug;

    use crate::metrics::CLI_METRICS;

    // we need that time reference to use an atomic and not a mutex for LAST_UNPARK
    static TIME_REF: Lazy<Instant> = Lazy::new(Instant::now);
    static ENABLED: AtomicBool = AtomicBool::new(false);

    const ALLOWED_DELAY_MICROS: u64 = 5000;
    const DEBUG_SUPPRESSION_MICROS: u64 = 30_000_000;

    // LAST_UNPARK_TIMESTAMP and NEXT_DEBUG_TIMESTAMP are semantically micro-second
    // precision timestamps, but we use atomics to allow accessing them without locks.
    thread_local!(static LAST_UNPARK_TIMESTAMP: AtomicU64 = const { AtomicU64::new(0) });
    static NEXT_DEBUG_TIMESTAMP: AtomicU64 = AtomicU64::new(0);
    static SUPPRESSED_DEBUG_COUNT: AtomicU64 = AtomicU64::new(0);

    pub fn set_enabled(enabled: bool) {
        ENABLED.store(enabled, Ordering::Relaxed);
    }

    pub fn thread_unpark() {
        LAST_UNPARK_TIMESTAMP.with(|time| {
            let now = Instant::now()
                .checked_duration_since(*TIME_REF)
                .unwrap_or_default();
            time.store(now.as_micros() as u64, Ordering::Relaxed);
        })
    }

    pub fn thread_park() {
        if !ENABLED.load(Ordering::Relaxed) {
            return;
        }

        LAST_UNPARK_TIMESTAMP.with(|time| {
            let now = Instant::now()
                .checked_duration_since(*TIME_REF)
                .unwrap_or_default();
            let now = now.as_micros() as u64;
            let delta = now - time.load(Ordering::Relaxed);
            CLI_METRICS
                .thread_unpark_duration_microseconds
                .with_label_values([])
                .observe(delta as f64);
            if delta > ALLOWED_DELAY_MICROS {
                emit_debug(delta, now);
            }
        })
    }

    fn emit_debug(delta: u64, now: u64) {
        if NEXT_DEBUG_TIMESTAMP
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next_debug| {
                if next_debug < now {
                    Some(now + DEBUG_SUPPRESSION_MICROS)
                } else {
                    None
                }
            })
            .is_err()
        {
            // a debug was emitted recently, don't emit log for this one
            SUPPRESSED_DEBUG_COUNT.fetch_add(1, Ordering::Relaxed);
            return;
        }

        let suppressed = SUPPRESSED_DEBUG_COUNT.swap(0, Ordering::Relaxed);
        if suppressed == 0 {
            debug!("thread wasn't parked for {delta}µs, is the runtime too busy?");
        } else {
            debug!(
                "thread wasn't parked for {delta}µs, is the runtime too busy? ({suppressed} \
                 similar messages suppressed)"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use quickwit_config::{S3StorageConfig, StorageConfigs};
    use quickwit_rest_client::models::Timeout;

    use super::*;
    use crate::parse_duration_or_none;

    #[test]
    fn test_parse_duration_or_none() -> anyhow::Result<()> {
        assert_eq!(parse_duration_or_none("1s")?, Timeout::from_secs(1));
        assert_eq!(parse_duration_or_none("2m")?, Timeout::from_mins(2));
        assert_eq!(parse_duration_or_none("3h")?, Timeout::from_hours(3));
        assert_eq!(parse_duration_or_none("4d")?, Timeout::from_days(4));
        assert_eq!(parse_duration_or_none("none")?, Timeout::none());
        assert!(parse_duration_or_none("something").is_err());
        Ok(())
    }

    #[test]
    fn test_get_resolvers() {
        let s3_storage_config = S3StorageConfig {
            force_path_style_access: true,
            ..Default::default()
        };
        let storage_configs = StorageConfigs::new(vec![s3_storage_config.into()]);
        let metastore_configs = MetastoreConfigs::default();
        let (_storage_resolver, _metastore_resolver) =
            get_resolvers(&storage_configs, &metastore_configs);
    }
}
