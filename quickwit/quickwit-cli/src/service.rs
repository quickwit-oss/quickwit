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
        .about("Runs quickwit services. By default, `metastore`, `indexer`, `searcher` and `janitor` are started.")
        .args(&[
            arg!(--"service" <SERVICE> "Services (indexer|searcher|janitor|metastore) to run. If unspecified, all the supported services are started.")
                .multiple_occurrences(true)
                .required(false),
        ])
}

#[derive(Debug, Eq, PartialEq)]
pub struct RunCliCommand {
    pub config_uri: Uri,
    pub services: Option<HashSet<QuickwitService>>,
}

impl RunCliCommand {
    pub fn parse_cli_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let config_uri = matches
            .value_of("config")
            .map(Uri::from_str)
            .expect("`config` is a required arg.")?;
        let services = matches
            .values_of("service")
            .map(|values| {
                let services: Result<HashSet<_>, _> =
                    values.into_iter().map(QuickwitService::from_str).collect();
                services
            })
            .transpose()?;
        Ok(RunCliCommand {
            config_uri,
            services,
        })
    }

    pub async fn execute(&self) -> anyhow::Result<()> {
        debug!(args = ?self, "run-service");
        let mut config = load_quickwit_config(&self.config_uri).await?;

        if let Some(services) = &self.services {
            tracing::info!(services = %services.iter().join(", "), "Setting services from override.");
            config.enabled_services = services.clone();
        }
        let telemetry_event = TelemetryEvent::RunService(config.enabled_services.iter().join(","));
        quickwit_telemetry::send_telemetry_event(telemetry_event).await;
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
        let expected_config_uri = Uri::from_str("file:///config.yaml").unwrap();
        assert!(matches!(
            command,
            CliCommand::Run(RunCliCommand {
                config_uri,
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
            "--config",
            "/config.yaml",
            "--service",
            "indexer",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        let expected_config_uri = Uri::from_str("file:///config.yaml").unwrap();
        assert!(matches!(
            command,
            CliCommand::Run(RunCliCommand {
                config_uri,
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
            "--config",
            "/config.yaml",
            "--service",
            "searcher",
            "--service",
            "metastore",
        ])?;
        let command = CliCommand::parse_cli_args(&matches).unwrap();
        let expected_config_uri = Uri::from_str("file:///config.yaml").unwrap();
        let expected_services =
            HashSet::from_iter([QuickwitService::Metastore, QuickwitService::Searcher]);
        assert!(matches!(
            command,
            CliCommand::Run(RunCliCommand {
                config_uri,
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
            "--config",
            "/config.yaml",
            "--service",
            "indexer",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        let expected_config_uri = Uri::from_str("file:///config.yaml").unwrap();
        assert!(matches!(
            command,
            CliCommand::Run(RunCliCommand {
                config_uri,
                services,
                ..
            })
            if config_uri == expected_config_uri && services.as_ref().unwrap().len() == 1 && services.as_ref().unwrap().contains(&QuickwitService::Indexer)
        ));
        Ok(())
    }
}
