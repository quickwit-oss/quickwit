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
use quickwit_config::{IndexerConfig, SearcherConfig, ServerConfig};
use quickwit_indexing::{check_source_connectivity, index_data};
use quickwit_metastore::MetastoreUriResolver;
use quickwit_serve::run_searcher;
use quickwit_storage::quickwit_storage_uri_resolver;
use quickwit_telemetry::payload::TelemetryEvent;
use tracing::debug;

#[derive(Debug, Eq, PartialEq)]
pub struct RunServiceArgs {
    pub service_name: String,
    pub server_config_uri: String,
    pub index_id: Option<String>,
}

#[derive(Debug)]
pub struct RunIndexerArgs {
    pub metastore_uri: String,
    pub indexer_config: IndexerConfig,
    pub index_id: String,
}

#[derive(Debug)]
pub struct RunSearcherArgs {
    pub metastore_uri: String,
    pub searcher_config: SearcherConfig,
}

#[derive(Debug, PartialEq)]
pub enum ServiceCliCommand {
    Run(RunServiceArgs),
}

impl ServiceCliCommand {
    pub fn parse_cli_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches) = matches
            .subcommand()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse sub-matches."))?;
        match subcommand {
            "run" => Self::parse_run_args(submatches),
            _ => bail!("Service subcommand '{}' is not implemented", subcommand),
        }
    }

    fn parse_run_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let service_name = matches
            .value_of("service-name")
            .expect("`service-name` should be a required arg.")
            .to_string();
        let server_config_uri = matches
            .value_of("server-config-uri")
            .map(normalize_uri)
            .expect("`server-config-uri` should be a required arg.")?;
        let index_id = matches.value_of("index-id").map(String::from);
        Ok(ServiceCliCommand::Run(RunServiceArgs {
            service_name,
            server_config_uri,
            index_id,
        }))
    }

    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            Self::Run(args) => run_service(args).await,
        }
    }
}

// FIXME: Implement `indexer` and `searcher` as subcommands.
pub async fn run_service(args: RunServiceArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "run-service");
    let server_config = ServerConfig::from_file(&args.server_config_uri).await?;

    match args.service_name.as_ref() {
        "indexer" => {
            run_indexer_cli(RunIndexerArgs {
                metastore_uri: server_config.metastore_uri,
                indexer_config: server_config
                    .indexer_config
                    .context("Indexer config is empty.")?,
                index_id: args.index_id.expect("`index-id` should be a required arg."),
            })
            .await?
        }
        "searcher" => {
            run_searcher_cli(RunSearcherArgs {
                metastore_uri: server_config.metastore_uri,
                searcher_config: server_config
                    .searcher_config
                    .context("Searcher config is empty.")?,
            })
            .await?
        }
        _ => bail!(
            "Service `{}` is not implemented. Available services are `indexer` and `searcher`.",
            args.service_name
        ),
    }
    Ok(())
}

async fn run_indexer_cli(args: RunIndexerArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "run-indexer");
    let telemetry_event = TelemetryEvent::RunService("indexer".to_string());
    quickwit_telemetry::send_telemetry_event(telemetry_event).await;

    let metastore_uri_resolver = MetastoreUriResolver::default();
    let metastore = metastore_uri_resolver.resolve(&args.metastore_uri).await?;
    let index_metadata = metastore.index_metadata(&args.index_id).await?;
    let storage_uri_resolver = quickwit_storage_uri_resolver();
    let storage = storage_uri_resolver.resolve(&index_metadata.index_uri)?;
    let mut checks = vec![("metastore", Ok(())), ("storage", storage.check().await)];
    for source_config in index_metadata.sources.iter() {
        checks.push(("source", check_source_connectivity(source_config).await));
    }
    run_checklist(checks);
    index_data(index_metadata, args.indexer_config, metastore, storage).await?;
    Ok(())
}

async fn run_searcher_cli(args: RunSearcherArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "run-searcher");
    let telemetry_event = TelemetryEvent::RunService("searcher".to_string());
    quickwit_telemetry::send_telemetry_event(telemetry_event).await;

    let metastore_uri_resolver = MetastoreUriResolver::default();
    let metastore = metastore_uri_resolver.resolve(&args.metastore_uri).await?;
    run_checklist(vec![("metastore", metastore.check_connectivity().await)]);
    run_searcher(args.searcher_config, metastore).await?;
    Ok(())
}
