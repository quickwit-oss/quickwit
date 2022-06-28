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

use std::time::Duration;

use anyhow::Context;
use hyper_rustls::HttpsConnectorBuilder;
use once_cell::sync::OnceCell;
use rusoto_core::credential::{AutoRefreshingProvider, ChainProvider, ProvideAwsCredentials};
use rusoto_core::{HttpClient, HttpConfig};

pub mod error;
pub mod retry;

/// A timeout for idle sockets being kept-alive.
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(10);

/// A credential timeout.
const CREDENTIAL_TIMEOUT: Duration = Duration::from_secs(5);

/// Returns a hyper http client.
pub fn get_http_client() -> HttpClient {
    let mut http_config: HttpConfig = HttpConfig::default();
    // We experience an issue similar to https://github.com/hyperium/hyper/issues/2312.
    // It seems like the setting below solved it.
    http_config.pool_idle_timeout(POOL_IDLE_TIMEOUT);
    let builder = HttpsConnectorBuilder::new();
    let builder = builder.with_native_roots();
    let connector = builder
        .https_or_http()
        // We do not enable HTTP2.
        // It is not enabled on S3 and it does not seem to work with Google Cloud Storage at
        // this point. https://github.com/quickwit-oss/quickwit/issues/1584
        //
        // (Besides, HTTP2 would be awesome but rusoto does not leverage
        // multiplexing anyway.)
        .enable_http1()
        .build();
    HttpClient::from_connector_with_config(connector, http_config)
}

/// Returns a singleton credentials provider.
pub fn get_credentials_provider() -> anyhow::Result<impl ProvideAwsCredentials + Send + Sync> {
    static CREDENTIALS_PROVIDER_SINGLETON: OnceCell<AutoRefreshingProvider<ChainProvider>> =
        OnceCell::new();
    CREDENTIALS_PROVIDER_SINGLETON
        .get_or_try_init(move || {
            let mut chain_provider = ChainProvider::new();
            chain_provider.set_timeout(CREDENTIAL_TIMEOUT);
            let credentials_provider = AutoRefreshingProvider::new(chain_provider)
                .with_context(|| "Failed to instantiate AWS credentials provider.")?;
            Ok(credentials_provider)
        })
        .cloned()
}
