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

use std::path::PathBuf;
use std::time::Duration;

use anyhow::bail;
use once_cell::sync::Lazy;
use quickwit_common::run_checklist;
use quickwit_common::uri::Uri;
use quickwit_config::{QuickwitConfig, SourceConfig};
use quickwit_indexing::check_source_connectivity;
use quickwit_metastore::quickwit_metastore_uri_resolver;
use quickwit_storage::{load_file, quickwit_storage_uri_resolver};
use regex::Regex;
use tabled::{Alignment, Header, Modify, Row, Style, Table, Tabled};
use tracing::info;

pub mod cli;
pub mod index;
pub mod service;
pub mod source;
pub mod split;
pub mod stats;

/// Throughput calculation window size.
const THROUGHPUT_WINDOW_SIZE: usize = 5;

/// This environment variable can be set to send telemetry events to a jaeger instance.
pub const QW_JAEGER_ENABLED_ENV_KEY: &str = "QW_JAEGER_ENABLED";

/// This environment variable can be set to send data to tokio console.
pub const QW_TOKIO_CONSOLE_ENABLED_ENV_KEY: &str = "QW_TOKIO_CONSOLE_ENABLED";

/// Regular expression representing a valid duration with unit.
pub const DURATION_WITH_UNIT_PATTERN: &str = r#"^(\d{1,3})(s|m|h|d)$"#;

/// Parse duration with unit like `1s`, `2m`, `3h`, `5d`.
pub fn parse_duration_with_unit(duration_with_unit_str: &str) -> anyhow::Result<Duration> {
    static DURATION_WITH_UNIT_RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(DURATION_WITH_UNIT_PATTERN).unwrap());
    let captures = DURATION_WITH_UNIT_RE
        .captures(duration_with_unit_str)
        .ok_or_else(|| anyhow::anyhow!("Invalid duration format: `[0-9]+[smhd]`"))?;
    let value = captures.get(1).unwrap().as_str().parse::<u64>().unwrap();
    let unit = captures.get(2).unwrap().as_str();
    return match unit {
        "s" => Ok(Duration::from_secs(value)),
        "m" => Ok(Duration::from_secs(value * 60)),
        "h" => Ok(Duration::from_secs(value * 60 * 60)),
        "d" => Ok(Duration::from_secs(value * 60 * 60 * 24)),
        _ => bail!("Invalid duration format: `[0-9]+[smhd]`"),
    };
}

async fn load_quickwit_config(
    uri: &Uri,
    data_dir: Option<PathBuf>,
) -> anyhow::Result<QuickwitConfig> {
    let config_content = load_file(uri).await?;
    let config = QuickwitConfig::load(uri, config_content.as_slice(), data_dir).await?;
    info!(config_uri = %uri, config = ?config, "Loaded Quickwit config.");
    Ok(config)
}

/// Runs connectivity checks for a given `metastore_uri` and `index_id`.
/// Optionaly, it takes a `SourceConfig` that will be checked instead
/// of the index's sources.
pub async fn run_index_checklist(
    metastore_uri: &str,
    index_id: &str,
    source_to_check: Option<&SourceConfig>,
) -> anyhow::Result<()> {
    let mut checks: Vec<(&str, anyhow::Result<()>)> = Vec::new();
    let metastore_uri_resolver = quickwit_metastore_uri_resolver();
    let metastore = metastore_uri_resolver.resolve(metastore_uri).await?;
    checks.push(("metastore", metastore.check_connectivity().await));

    let index_metadata = metastore.index_metadata(index_id).await?;
    let storage_uri_resolver = quickwit_storage_uri_resolver();
    let storage = storage_uri_resolver.resolve(&index_metadata.index_uri)?;
    checks.push(("storage", storage.check().await));

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

    run_checklist(checks);

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::parse_duration_with_unit;

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
}

/// Construct a table for display.
pub fn make_table<T: Tabled>(header: &str, rows: impl IntoIterator<Item = T>) -> Table {
    Table::new(rows)
        .with(Header(header))
        .with(Modify::new(Row(2..)).with(Alignment::left()))
        .with(Style::PSQL)
}
