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

use std::time::{Duration, SystemTime};

use aws_credential_types::provider::ProvideCredentials;
use aws_sigv4::http_request::{SignableBody, SignableRequest, SignatureLocation, SigningSettings};
use aws_sigv4::sign::v4;
use quickwit_proto::metastore::{MetastoreError, MetastoreResult};
use sqlx::MySql;
use sqlx::mysql::MySqlConnectOptions;

use super::pool::TrackedPool;

/// Spawns a background task that refreshes the IAM auth token every 10 minutes.
/// RDS IAM tokens are valid for 15 minutes, so this provides a 5-minute buffer.
pub(super) fn spawn_token_refresh_task(
    pool: TrackedPool<MySql>,
    base_connect_options: MySqlConnectOptions,
    host: String,
    port: u16,
    user: String,
    region: String,
    aws_config: aws_config::SdkConfig,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(600));
        interval.tick().await; // skip immediate first tick

        loop {
            interval.tick().await;

            match generate_rds_iam_token(&host, port, &user, &region, &aws_config).await {
                Ok(token) => {
                    let new_options = base_connect_options
                        .clone()
                        .password(&token)
                        .enable_cleartext_plugin(true);
                    pool.set_connect_options(new_options);
                    tracing::debug!("refreshed RDS IAM auth token");
                }
                Err(error) => {
                    quickwit_common::rate_limited_error!(
                        limit_per_min = 6,
                        error = ?error,
                        "failed to refresh RDS IAM auth token"
                    );
                }
            }
        }
    });
}

/// Generates a SigV4-signed IAM authentication token for Aurora MySQL.
///
/// The token is a presigned URL (with `https://` stripped) that Aurora
/// accepts as a password via the `mysql_clear_password` auth plugin.
/// Tokens are valid for 15 minutes.
pub(super) async fn generate_rds_iam_token(
    host: &str,
    port: u16,
    user: &str,
    region: &str,
    aws_config: &aws_config::SdkConfig,
) -> MetastoreResult<String> {
    let credentials_provider =
        aws_config
            .credentials_provider()
            .ok_or_else(|| MetastoreError::Internal {
                message: "failed to generate RDS IAM token".to_string(),
                cause: "no AWS credentials provider configured; ensure AWS credentials are \
                        available via environment variables, instance profile, or IRSA"
                    .to_string(),
            })?;

    let credentials = credentials_provider
        .provide_credentials()
        .await
        .map_err(|err| MetastoreError::Internal {
            message: "failed to resolve AWS credentials for RDS IAM token".to_string(),
            cause: err.to_string(),
        })?;

    let identity = credentials.into();

    let mut settings = SigningSettings::default();
    settings.expires_in = Some(Duration::from_secs(900));
    settings.signature_location = SignatureLocation::QueryParams;

    let signing_params = v4::SigningParams::builder()
        .identity(&identity)
        .region(region)
        .name("rds-db")
        .time(SystemTime::now())
        .settings(settings)
        .build()
        .map_err(|err| MetastoreError::Internal {
            message: "failed to build SigV4 signing params".to_string(),
            cause: err.to_string(),
        })?;

    let url = format!("https://{host}:{port}/?Action=connect&DBUser={user}");
    let signable = SignableRequest::new("GET", &url, std::iter::empty(), SignableBody::Bytes(&[]))
        .map_err(|err| MetastoreError::Internal {
            message: "failed to create signable request for RDS IAM token".to_string(),
            cause: err.to_string(),
        })?;

    let (instructions, _signature) =
        aws_sigv4::http_request::sign(signable, &signing_params.into())
            .map_err(|err| MetastoreError::Internal {
                message: "failed to sign RDS IAM token".to_string(),
                cause: err.to_string(),
            })?
            .into_parts();

    let mut signed_url = url::Url::parse(&url).expect("URL was already validated during signing");
    for (name, value) in instructions.params() {
        signed_url.query_pairs_mut().append_pair(name, value);
    }

    // Strip the "https://" prefix — the token is the host:port/?query… portion.
    let token = signed_url
        .as_str()
        .strip_prefix("https://")
        .unwrap_or(signed_url.as_str());
    Ok(token.to_string())
}
