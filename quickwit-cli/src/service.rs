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

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use clap::ArgMatches;
use quickwit_common::net::socket_addr_from_str;
use quickwit_common::uri::normalize_uri;
use quickwit_serve::{serve_cli, ServeArgs};
use tracing::debug;

#[derive(Debug, Eq, PartialEq)]
pub struct StartServiceArgs {
    pub service_name: String,
    pub rest_socket_addr: SocketAddr,
    pub host_key_path: PathBuf,
    pub peer_socket_addrs: Vec<SocketAddr>,
    pub metastore_uri: String,
    pub index_ids: Vec<String>,
}

#[derive(Debug, PartialEq)]
pub enum ServiceCliCommand {
    Start(StartServiceArgs),
}

impl ServiceCliCommand {
    pub fn parse_cli_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches) = matches
            .subcommand()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse sub-matches."))?;
        match subcommand {
            "start" => Self::parse_start_args(submatches),
            _ => bail!("Service subcommand '{}' is not implemented", subcommand),
        }
    }

    fn parse_start_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let name = matches
            .value_of("name")
            .map(|service_name_str| service_name_str.to_string())
            .context("'service-name' is a required arg")?;
        let metastore_uri = matches
            .value_of("metastore-uri")
            .context("'metastore-uri' is a required arg")
            .map(normalize_uri)??;
        let host = matches
            .value_of("host")
            .context("'host' has a default value")?
            .to_string();
        let port = matches.value_of_t::<u16>("port")?;
        let rest_addr = format!("{}:{}", host, port);
        let rest_socket_addr = socket_addr_from_str(&rest_addr)?;
        let host_key_path_prefix = matches
            .value_of("host-key-path-prefix")
            .context("'host-key-path-prefix' has a default  value")?
            .to_string();
        let host_key_path =
            Path::new(format!("{}-{}-{}", host_key_path_prefix, host, port.to_string()).as_str())
                .to_path_buf();
        let mut peer_socket_addrs: Vec<SocketAddr> = Vec::new();
        if matches.is_present("peer-seed") {
            if let Some(values) = matches.values_of("peer-seed") {
                for value in values {
                    peer_socket_addrs.push(socket_addr_from_str(value)?);
                }
            }
        }
        Ok(ServiceCliCommand::Start(StartServiceArgs {
            service_name: name,
            rest_socket_addr,
            host_key_path,
            peer_socket_addrs,
            metastore_uri,
            index_ids: Vec::new(),
        }))
    }

    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            Self::Start(args) => start_service(args).await,
        }
    }
}

pub async fn start_service(args: StartServiceArgs) -> anyhow::Result<()> {
    debug!(args = ?args, "start-service");

    match args.service_name.as_str() {
        "searcher" => {
            serve_cli(ServeArgs {
                host_key_path: args.host_key_path,
                peer_socket_addrs: args.peer_socket_addrs,
                metastore_uri: args.metastore_uri,
                rest_socket_addr: args.rest_socket_addr,
            })
            .await?
        }
        // TODO: add indexer.
        _ => bail!("Service '{}' is not implemented", args.service_name),
    }

    Ok(())
}
