// Copyright (C) 2023 Quickwit, Inc.
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

#![deny(clippy::disallowed_methods)]

use std::collections::HashSet;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{bail, Context};
use clap::{arg, Arg, ArgMatches};
use dialoguer::theme::ColorfulTheme;
use dialoguer::Confirm;
use once_cell::sync::Lazy;
use quickwit_common::run_checklist;
use quickwit_common::runtimes::RuntimesConfiguration;
use quickwit_common::uri::Uri;
use quickwit_config::service::QuickwitService;
use quickwit_config::{ConfigFormat, QuickwitConfig, SourceConfig, DEFAULT_QW_CONFIG_PATH};
use quickwit_indexing::check_source_connectivity;
use quickwit_metastore::quickwit_metastore_uri_resolver;
use quickwit_rest_client::rest_client::{QuickwitClient, QuickwitClientBuilder, DEFAULT_BASE_URL};
use quickwit_storage::{load_file, quickwit_storage_uri_resolver};
use regex::Regex;
use reqwest::Url;
use tabled::object::Rows;
use tabled::{Alignment, Header, Modify, Style, Table, Tabled};
use tracing::info;

pub mod cli;
pub mod index;
#[cfg(feature = "jemalloc")]
pub mod jemalloc;
pub mod metrics;
pub mod service;
pub mod source;
pub mod split;
pub mod stats;
pub mod tool;

/// Throughput calculation window size.
const THROUGHPUT_WINDOW_SIZE: usize = 5;

pub const QW_ENABLE_JAEGER_EXPORTER_ENV_KEY: &str = "QW_ENABLE_JAEGER_EXPORTER";

pub const QW_ENABLE_TOKIO_CONSOLE_ENV_KEY: &str = "QW_ENABLE_TOKIO_CONSOLE";

pub const QW_ENABLE_OPENTELEMETRY_OTLP_EXPORTER_ENV_KEY: &str =
    "QW_ENABLE_OPENTELEMETRY_OTLP_EXPORTER";

/// Regular expression representing a valid duration with unit.
pub const DURATION_WITH_UNIT_PATTERN: &str = r#"^(\d{1,3})(s|m|h|d)$"#;

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
    /// The outer option represents the presense of the argument,
    /// the inner option represents the presence of timeout.
    pub connect_timeout: Option<Option<Duration>>,
    pub timeout: Option<Option<Duration>>,
    pub commit_timeout: Option<Option<Duration>>,
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
    pub fn client(self) -> QuickwitClient {
        let mut builder = QuickwitClientBuilder::new(self.cluster_endpoint);
        if let Some(connect_timeout) = self.connect_timeout {
            builder = builder.connect_timeout(connect_timeout);
        }
        if let Some(timeout) = self.timeout {
            builder = builder.timeout(timeout);
        }
        builder.build()
    }

    pub fn search_client(self) -> QuickwitClient {
        let mut builder = QuickwitClientBuilder::new(self.cluster_endpoint);
        if let Some(connect_timeout) = self.connect_timeout {
            builder = builder.connect_timeout(connect_timeout);
        }
        if let Some(timeout) = self.timeout {
            builder = builder.search_timeout(timeout);
        }
        builder.build()
    }

    pub fn ingest_client(self) -> QuickwitClient {
        let mut builder = QuickwitClientBuilder::new(self.cluster_endpoint);
        if let Some(connect_timeout) = self.connect_timeout {
            builder = builder.connect_timeout(connect_timeout);
        }
        if let Some(timeout) = self.timeout {
            builder = builder.ingest_timeout(timeout);
        }
        if let Some(commit_timeout) = self.commit_timeout {
            builder = builder.commit_timeout(commit_timeout);
        }
        builder.build()
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

/// Parse duration with unit like `1s`, `2m`, `3h`, `5d`.
pub fn parse_duration_with_unit(duration_with_unit_str: &str) -> anyhow::Result<Duration> {
    static DURATION_WITH_UNIT_RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(DURATION_WITH_UNIT_PATTERN).unwrap());
    let captures = DURATION_WITH_UNIT_RE
        .captures(duration_with_unit_str)
        .ok_or_else(|| anyhow::anyhow!("Invalid duration format: `[0-9]+[smhd]`"))?;
    let value = captures.get(1).unwrap().as_str().parse::<u64>().unwrap();
    let unit = captures.get(2).unwrap().as_str();

    match unit {
        "s" => Ok(Duration::from_secs(value)),
        "m" => Ok(Duration::from_secs(value * 60)),
        "h" => Ok(Duration::from_secs(value * 60 * 60)),
        "d" => Ok(Duration::from_secs(value * 60 * 60 * 24)),
        _ => bail!("Invalid duration format: `[0-9]+[smhd]`"),
    }
}

pub fn parse_duration_or_none(duration_with_unit_str: &str) -> anyhow::Result<Option<Duration>> {
    if duration_with_unit_str == "none" {
        Ok(None)
    } else {
        parse_duration_with_unit(duration_with_unit_str)
            .map(Some)
            .map_err(|_| anyhow::anyhow!("Invalid duration format: `[0-9]+[smhd]|none`"))
    }
}

pub fn start_actor_runtimes(services: &HashSet<QuickwitService>) -> anyhow::Result<()> {
    if services.contains(&QuickwitService::Indexer)
        || services.contains(&QuickwitService::Janitor)
        || services.contains(&QuickwitService::ControlPlane)
    {
        let runtime_configuration = RuntimesConfiguration::default();
        quickwit_common::runtimes::initialize_runtimes(runtime_configuration)
            .context("Failed to start actor runtimes.")?;
    }
    Ok(())
}

async fn load_quickwit_config(config_uri: &Uri) -> anyhow::Result<QuickwitConfig> {
    let config_content = load_file(config_uri)
        .await
        .context("Failed to load quickwit config.")?;
    let config_format = ConfigFormat::sniff_from_uri(config_uri)?;
    let config = QuickwitConfig::load(config_format, config_content.as_slice())
        .await
        .with_context(|| format!("Failed to deserialize quickwit config `{config_uri}`."))?;
    info!(config_uri=%config_uri, config=?config, "Loaded Quickwit config.");
    Ok(config)
}

/// Runs connectivity checks for a given `metastore_uri` and `index_id`.
/// Optionally, it takes a `SourceConfig` that will be checked instead
/// of the index's sources.
pub async fn run_index_checklist(
    metastore_uri: &Uri,
    index_id: &str,
    source_to_check: Option<&SourceConfig>,
) -> anyhow::Result<()> {
    let mut checks: Vec<(&str, anyhow::Result<()>)> = Vec::new();
    let metastore_uri_resolver = quickwit_metastore_uri_resolver();
    let metastore = metastore_uri_resolver.resolve(metastore_uri).await?;
    checks.push(("metastore", metastore.check_connectivity().await));

    let index_metadata = metastore.index_metadata(index_id).await?;
    let storage_uri_resolver = quickwit_storage_uri_resolver();
    let storage = storage_uri_resolver
        .resolve(index_metadata.index_uri())
        .await?;
    checks.push(("storage", storage.check_connectivity().await));

    if let Some(source_config) = source_to_check {
        checks.push((
            source_config.source_id.as_str(),
            check_source_connectivity(source_config).await,
        ));
    } else {
        for source_config in index_metadata.sources.values() {
            checks.push((
                source_config.source_id.as_str(),
                check_source_connectivity(source_config).await,
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
    let table = if transpose {
        let mut index_builder = Table::builder(rows).index();
        index_builder.set_index(0);
        index_builder.transpose();
        index_builder.build()
    } else {
        Table::builder(rows).build()
    };

    table
        .with(Modify::new(Rows::new(1..)).with(Alignment::left()))
        .with(Style::ascii())
        .with(Header(header))
        .with(Modify::new(Rows::single(0)).with(Alignment::center()))
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
    thread_local!(static LAST_UNPARK_TIMESTAMP: AtomicU64 = AtomicU64::new(0));
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
            // a debug was emited recently, don't emit log for this one
            SUPPRESSED_DEBUG_COUNT.fetch_add(1, Ordering::Relaxed);
            return;
        }

        let suppressed = SUPPRESSED_DEBUG_COUNT.swap(0, Ordering::Relaxed);
        if suppressed == 0 {
            debug!("Thread wasn't parked for {delta}µs, is the runtime too busy?");
        } else {
            debug!(
                "Thread wasn't parked for {delta}µs, is the runtime too busy? ({suppressed} \
                 similar messages suppressed)"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{parse_duration_or_none, parse_duration_with_unit};

    #[test]
    fn test_parse_duration_with_unit() -> anyhow::Result<()> {
        assert_eq!(parse_duration_with_unit("8s")?, Duration::from_secs(8));
        assert_eq!(parse_duration_with_unit("5m")?, Duration::from_secs(5 * 60));
        assert_eq!(
            parse_duration_with_unit("2h")?,
            Duration::from_secs(2 * 60 * 60)
        );
        assert_eq!(
            parse_duration_with_unit("3d")?,
            Duration::from_secs(3 * 60 * 60 * 24)
        );

        assert!(parse_duration_with_unit("").is_err());
        assert!(parse_duration_with_unit("a2d").is_err());
        assert!(parse_duration_with_unit("3 d").is_err());
        assert!(parse_duration_with_unit("3").is_err());
        assert!(parse_duration_with_unit("1h30").is_err());
        Ok(())
    }

    #[test]
    fn test_parse_duration_or_none() -> anyhow::Result<()> {
        assert_eq!(parse_duration_or_none("8s")?, Some(Duration::from_secs(8)));
        assert_eq!(parse_duration_or_none("none")?, None);
        assert!(parse_duration_or_none("something").is_err());
        Ok(())
    }
}
