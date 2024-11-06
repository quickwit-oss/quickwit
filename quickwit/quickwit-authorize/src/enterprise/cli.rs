// Copyright (C) 2024 Quickwit, Inc.
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

use std::str::FromStr;

use anyhow::Context;
use biscuit_auth::builder::{fact, string};
use biscuit_auth::KeyPair;
use quickwit_common::QuickwitService;

use super::AuthorizationToken;

#[derive(Debug, Eq, PartialEq)]
pub struct GenerateAuthTokensArgs {
    pub root_private_key: Option<String>,
    pub services: Vec<String>,
}

impl GenerateAuthTokensArgs {
    fn list_services(self) -> anyhow::Result<Vec<Vec<QuickwitService>>> {
        if self.services.is_empty() {
            let mut default_services_set: Vec<Vec<QuickwitService>> =
                vec![QuickwitService::supported_services().into_iter().collect()];
            for individual_service in QuickwitService::supported_services() {
                default_services_set.push(vec![individual_service]);
            }
            return Ok(default_services_set);
        } else {
            let mut services_set: Vec<Vec<QuickwitService>> = vec![];
            for services_str in self.services {
                let services = services_str
                    .split(",")
                    .map(QuickwitService::from_str)
                    .collect::<Result<Vec<_>, _>>()
                    .context("failed to parse quickwit service name")?;
                services_set.push(services);
            }
            return Ok(services_set);
        }
    }
}

fn generate_token_for_services(
    key_pair: &KeyPair,
    services: &[QuickwitService],
) -> anyhow::Result<AuthorizationToken> {
    let mut biscuit_builder = biscuit_auth::Biscuit::builder();
    for service in services {
        biscuit_builder.add_fact(fact("service", &[string(service.as_str())]))?;
    }
    let biscuit = biscuit_builder
        .build(&key_pair)
        .context("failed ot generate token")?;
    Ok(AuthorizationToken::from(biscuit))
}

pub async fn generate_auth_tokens_cli(args: GenerateAuthTokensArgs) -> anyhow::Result<()> {
    let key_pair = if let Some(private_key_hex) = &args.root_private_key {
        let private_key = biscuit_auth::PrivateKey::from_bytes_hex(private_key_hex)
            .context("invalid root private key")?;
        biscuit_auth::KeyPair::from(&private_key)
    } else {
        println!("generating keys");
        biscuit_auth::KeyPair::new()
    };
    println!("Private root key: {}", key_pair.private().to_bytes_hex());
    println!("Public root key: {}", key_pair.public().to_bytes_hex());
    for services in args.list_services()? {
        use itertools::Itertools;
        let token = generate_token_for_services(&key_pair, &services)?;
        let services_str = services.iter().map(QuickwitService::as_str).join(",");
        println!("--\nService token for {services_str}\n{token}");
    }

    Ok(())
}
