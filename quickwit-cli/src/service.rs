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

use anyhow::{bail, Context};
use clap::ArgMatches;
use quickwit_common::run_checklist;
use quickwit_common::uri::normalize_uri;
use quickwit_config::ServerConfig;
use quickwit_indexing::index_data;
use quickwit_metastore::MetastoreUriResolver;
use quickwit_serve::run_searcher;
use quickwit_storage::quickwit_storage_uri_resolver;
use quickwit_telemetry::payload::TelemetryEvent;
use tracing::debug;

use crate::run_index_checklist;

#[derive(Debug, PartialEq)]
pub struct RunIndexerArgs {
    pub server_config_uri: String,
    pub index_id: String,
}

#[derive(Debug, PartialEq)]
pub struct RunSearcherArgs {
    pub server_config_uri: String,
}

#[derive(Debug, PartialEq)]
pub enum ServiceCliCommand {
    RunSearcher(RunSearcherArgs),
    RunIndexer(RunIndexerArgs),
}

impl ServiceCliCommand {
    pub fn parse_cli_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches) = matches
            .subcommand()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse sub-matches."))?;
        match subcommand {
            "run" => Self::parse_run_args(submatches),
            _ => bail!("Service subcommand `{}` is not implemented.", subcommand),
        }
    }

    fn parse_run_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches) = matches
            .subcommand()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse sub-matches."))?;
        match subcommand {
            "searcher" => Self::parse_searcher_args(submatches),
            "indexer" => Self::parse_indexer_args(submatches),
            _ => bail!(
                "Service `{}` is not implemented. Available services are `indexer` and `searcher`.",
                subcommand
            ),
        }
    }

    fn parse_searcher_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let server_config_uri = matches
            .value_of("server-config-uri")
            .map(normalize_uri)
            .expect("`server-config-uri` is a required arg.")?;
        Ok(ServiceCliCommand::RunSearcher(RunSearcherArgs {
            server_config_uri,
        }))
    }

    fn parse_indexer_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let server_config_uri = matches
            .value_of("server-config-uri")
            .map(normalize_uri)
            .expect("`server-config-uri` is a required arg.")?;
        let index_id = matches
            .value_of("index-id")
            .map(String::from)
            .expect("`index-id` is a required arg.");
        Ok(ServiceCliCommand::RunIndexer(RunIndexerArgs {
            server_config_uri,
            index_id,
        }))
    }

    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            Self::RunSearcher(args) => run_searcher_cli(args).await,
            Self::RunIndexer(args) => run_indexer_cli(args).await,
        }
    }
}

async fn run_indexer_cli(args: RunIndexerArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "run-indexer");
    let telemetry_event = TelemetryEvent::RunService("indexer".to_string());
    quickwit_telemetry::send_telemetry_event(telemetry_event).await;
    let server_config = ServerConfig::from_file(&args.server_config_uri).await?;
    run_index_checklist(&server_config.metastore_uri, &args.index_id, None).await?;
    let indexer_config = server_config
        .indexer_config
        .context("Indexer config is empty.")?;
    let metastore_uri_resolver = MetastoreUriResolver::default();
    let metastore = metastore_uri_resolver
        .resolve(&server_config.metastore_uri)
        .await?;
    let index_metadata = metastore.index_metadata(&args.index_id).await?;
    let storage_uri_resolver = quickwit_storage_uri_resolver();
    let storage = storage_uri_resolver.resolve(&index_metadata.index_uri)?;
    index_data(index_metadata, indexer_config, metastore, storage).await?;
    Ok(())
}

async fn run_searcher_cli(args: RunSearcherArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "run-searcher");
    let telemetry_event = TelemetryEvent::RunService("searcher".to_string());
    quickwit_telemetry::send_telemetry_event(telemetry_event).await;

    let server_config = ServerConfig::from_file(&args.server_config_uri).await?;
    let searcher_config = server_config
        .searcher_config
        .context("Searcher config is empty.")?;
    let metastore_uri_resolver = MetastoreUriResolver::default();
    let metastore = metastore_uri_resolver
        .resolve(&server_config.metastore_uri)
        .await?;
    run_checklist(vec![("metastore", metastore.check_connectivity().await)]);
    run_searcher(searcher_config, metastore).await?;
    Ok(())
}
