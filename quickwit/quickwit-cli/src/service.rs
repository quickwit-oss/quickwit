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

use std::collections::HashSet;
use std::path::PathBuf;
use std::str::FromStr;

use clap::{arg, ArgMatches, Command};
use itertools::Itertools;
use quickwit_common::uri::Uri;
use quickwit_config::service::QuickwitService;
use quickwit_serve::serve_quickwit;
use quickwit_telemetry::payload::TelemetryEvent;
use tracing::debug;

use crate::{load_quickwit_config, start_actor_runtimes};

pub fn build_run_command<'a>() -> Command<'a> {
    Command::new("run")
        .about("Runs quickwit services. By default, `metastore`, `indexer` and `searcher` are started.")
        .args(&[
            arg!(--"data-dir" <DATA_DIR> "Where data is persisted. Override data-dir defined in config file, default is `./qwdata`.")
                .env("QW_DATA_DIR")
                .required(false),
            arg!(--"service" <SERVICE> "Services (searcher|indexer|metastore) to run. If unspecified all services `metastore`, `searcher` and `indexer` are started.")
                .multiple_occurrences(true)
                .required(false),
            arg!(--"metastore-uri" <METASTORE_URI> "Metastore URI. Override the `metastore_uri` parameter defined in the config file. Defaults to file-backed, but could be Amazon S3 or PostgreSQL.")
                .env("QW_METASTORE_URI")
                .required(false),
            arg!(--"cluster-id" <CLUSTER_ID> "ID of the cluster to connect to.")
                .env("QW_CLUSTER_ID")
                .required(false),
            arg!(--"node-id" <NODE_ID> "Node ID identifying uniquely the node in the cluster.")
                .env("QW_NODE_ID")
                .required(false),
            arg!(--"peer-seeds" <PEER_SEEDS> "Comma-separated list of peer seeds to connect to in order to join a cluster.")
                .env("QW_PEER_SEEDS")
                .required(false),
        ])
}

#[derive(Debug, Eq, PartialEq)]
pub struct RunCliCommand {
    pub config_uri: Uri,
    pub data_dir_path: Option<PathBuf>,
    pub services: Option<HashSet<QuickwitService>>,
    pub metastore_uri: Option<Uri>,
    pub cluster_id: Option<String>,
    pub node_id: Option<String>,
    pub peer_seeds: Option<Vec<String>>,
}

impl RunCliCommand {
    pub fn parse_cli_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let config_uri = matches
            .value_of("config")
            .map(Uri::try_new)
            .expect("`config` is a required arg.")?;
        let data_dir_path = matches.value_of("data-dir").map(PathBuf::from);
        let services = matches
            .values_of("service")
            .map(|values| {
                let services: Result<HashSet<_>, _> =
                    values.into_iter().map(QuickwitService::from_str).collect();
                services
            })
            .transpose()?;
        let metastore_uri = matches
            .value_of("metastore-uri")
            .map(Uri::try_new)
            .transpose()?;
        let cluster_id = matches.value_of("cluster-id").map(String::from);
        let node_id = matches.value_of("node-id").map(String::from);
        let peer_seeds = matches
            .value_of("peer-seeds")
            .map(|peer_seeds_str| peer_seeds_str.split(',').map(String::from).collect());
        Ok(RunCliCommand {
            config_uri,
            data_dir_path,
            services,
            metastore_uri,
            cluster_id,
            node_id,
            peer_seeds,
        })
    }

    pub async fn execute(&self) -> anyhow::Result<()> {
        debug!(args = ?self, "run-service");
        let service_str = self
            .services
            .iter()
            .map(|service| format!("{service:?}"))
            .join(",");
        let telemetry_event = TelemetryEvent::RunService(service_str);
        quickwit_telemetry::send_telemetry_event(telemetry_event).await;

        let mut config = load_quickwit_config(&self.config_uri, self.data_dir_path.clone()).await?;

        // TODO: Remove these overrides when #1011 lands.
        if let Some(metastore_uri) = &self.metastore_uri {
            tracing::info!(metastore_uri = %metastore_uri, "Setting metastore URI from override.");
            config.metastore_uri = metastore_uri.clone();
        }
        if let Some(cluster_id) = &self.cluster_id {
            tracing::info!(cluster_id = %cluster_id, "Setting cluster ID from override.");
            config.cluster_id = cluster_id.clone();
        }
        if let Some(node_id) = &self.node_id {
            tracing::info!(node_id = %node_id, "Setting node ID from override.");
            config.node_id = node_id.clone();
        }
        if let Some(peer_seeds) = &self.peer_seeds {
            tracing::info!(peer_seeds = %peer_seeds.join(", "), "Setting peer seeds from override.");
            config.peer_seeds = peer_seeds.clone();
        }
        if let Some(services) = &self.services {
            tracing::info!(services = %services.iter().join(", "), "Setting services from override.");
            config.enabled_services = services.clone();
        }
        // Revalidate config because of overrides.
        config.validate()?;
        // TODO move in serve quickwit?
        start_actor_runtimes(&config.enabled_services)?;
        serve_quickwit(config).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::cli::{build_cli, CliCommand};

    #[test]
    fn test_parse_service_run_args_all_services() -> anyhow::Result<()> {
        let command = build_cli().no_binary_name(true);
        let matches = command.try_get_matches_from(vec!["run", "--config", "/config.yaml"])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        let expected_config_uri = Uri::try_new("file:///config.yaml").unwrap();
        assert!(matches!(
            command,
            CliCommand::Run(RunCliCommand {
                config_uri,
                data_dir_path: None,
                services,
                ..
            })
            if config_uri == expected_config_uri && services.is_none()
        ));
        Ok(())
    }

    #[test]
    fn test_parse_service_run_args_indexer_only() -> anyhow::Result<()> {
        let command = build_cli().no_binary_name(true);
        let matches = command.try_get_matches_from(vec![
            "run",
            "--service",
            "indexer",
            "--config",
            "/config.yaml",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        let expected_config_uri = Uri::try_new("file:///config.yaml").unwrap();
        assert!(matches!(
            command,
            CliCommand::Run(RunCliCommand {
                config_uri,
                data_dir_path: None,
                services,
                ..
            })
            if config_uri == expected_config_uri && services.as_ref().unwrap().len() == 1 && services.as_ref().unwrap().iter().cloned().next().unwrap() == QuickwitService::Indexer
        ));
        Ok(())
    }

    #[test]
    fn test_parse_service_run_args_searcher_and_metastore() -> anyhow::Result<()> {
        let command = build_cli().no_binary_name(true);
        let matches = command.try_get_matches_from(vec![
            "run",
            "--service",
            "searcher",
            "--service",
            "metastore",
            "--config",
            "/config.yaml",
        ])?;
        let command = CliCommand::parse_cli_args(&matches).unwrap();
        let expected_config_uri = Uri::try_new("file:///config.yaml").unwrap();
        let expected_services =
            HashSet::from_iter([QuickwitService::Metastore, QuickwitService::Searcher]);
        assert!(matches!(
            command,
            CliCommand::Run(RunCliCommand {
                config_uri,
                data_dir_path: None,
                services,
                ..
            })
            if config_uri == expected_config_uri && services.as_ref().unwrap().len() == 2 && services.as_ref().unwrap() == &expected_services
        ));
        Ok(())
    }

    #[test]
    fn test_parse_service_run_indexer_only_args() -> anyhow::Result<()> {
        let command = build_cli().no_binary_name(true);
        let matches = command.try_get_matches_from(vec![
            "run",
            "--service",
            "indexer",
            "--config",
            "/config.yaml",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        let expected_config_uri = Uri::try_new("file:///config.yaml").unwrap();
        assert!(matches!(
            command,
            CliCommand::Run(RunCliCommand {
                config_uri,
                data_dir_path: None,
                services,
                ..
            })
            if config_uri == expected_config_uri && services.as_ref().unwrap().len() == 1 && services.as_ref().unwrap().contains(&QuickwitService::Indexer)
        ));
        Ok(())
    }
}
