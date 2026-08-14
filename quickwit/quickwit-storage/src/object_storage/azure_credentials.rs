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

//! Chooses how Quickwit authenticates to Azure Blob Storage, and builds the container client.
//!
//! `azure_identity` 1.0 removed `DefaultAzureCredential` and `create_credential()`, so the
//! choice the SDK used to make by inspecting the environment is made here instead. The
//! nearest remaining type, `DeveloperToolsCredential`, chains the Azure CLI and the
//! Developer CLI only, which does not cover a pod.

use std::env;
use std::sync::Arc;

use azure_core::credentials::TokenCredential;
use azure_core::http::policies::Policy;
use azure_core::http::{ClientOptions, Url};
use azure_identity::{ManagedIdentityCredential, WorkloadIdentityCredential};
use azure_storage_blob::{BlobContainerClient, BlobContainerClientOptions};
use quickwit_config::AzureStorageConfig;
use tracing::info;

use crate::StorageResolverError;
use crate::object_storage::azure_shared_key::SharedKeyAuthorizationPolicy;

/// Environment variables the workload identity webhook injects into a pod.
const AZURE_CLIENT_ID: &str = "AZURE_CLIENT_ID";
const AZURE_TENANT_ID: &str = "AZURE_TENANT_ID";
const AZURE_FEDERATED_TOKEN_FILE: &str = "AZURE_FEDERATED_TOKEN_FILE";
/// Lets an operator pin the provider instead of relying on the detection below.
const AZURE_CREDENTIAL_KIND: &str = "AZURE_CREDENTIAL_KIND";

/// How requests to the blob service are authorized.
pub(crate) enum AzureCredential {
    /// Storage account key, signed by Quickwit because the 1.0 SDK dropped shared key.
    SharedKey(String),
    /// Entra ID token, signed by the SDK's bearer policy.
    Token(Arc<dyn TokenCredential>),
}

/// Resolves the credential from the storage config and the environment.
///
/// An explicit `access_key` wins, matching the previous behaviour: it is the only credential
/// an operator states in the config file, so treating it as a preference is the least
/// surprising reading.
pub(crate) fn resolve_credential(
    azure_storage_config: &AzureStorageConfig,
) -> Result<AzureCredential, StorageResolverError> {
    if let Some(access_key) = azure_storage_config.resolve_access_key() {
        return Ok(AzureCredential::SharedKey(access_key));
    }
    let token_credential = resolve_token_credential().map_err(|error| {
        StorageResolverError::InvalidConfig(format!(
            "could not build an Azure token credential: {error}. Set an access key, or run with \
             workload identity or managed identity configured"
        ))
    })?;
    Ok(AzureCredential::Token(token_credential))
}

/// Builds the token credential the environment describes.
fn resolve_token_credential() -> azure_core::Result<Arc<dyn TokenCredential>> {
    let credential_kind = env::var(AZURE_CREDENTIAL_KIND)
        .map(|kind| kind.trim().to_lowercase())
        .unwrap_or_default();

    match credential_kind.as_str() {
        "workloadidentity" => Ok(WorkloadIdentityCredential::new(None)?),
        "managedidentity" => Ok(ManagedIdentityCredential::new(None)?),
        // An empty or unrecognized value falls through to detection. The workload identity
        // variables are injected by a mutating webhook rather than by an operator, so their
        // presence is the signal that the pod runs under workload identity.
        _ => {
            if workload_identity_env_is_complete() {
                info!("using azure workload identity credential");
                return Ok(WorkloadIdentityCredential::new(None)?);
            }
            info!("using azure managed identity credential");
            Ok(ManagedIdentityCredential::new(None)?)
        }
    }
}

/// Returns `true` when all three variables a `WorkloadIdentityCredential` needs are present.
///
/// A partial set means the webhook did not inject a usable identity, and building the
/// credential would fail at the first request rather than here.
fn workload_identity_env_is_complete() -> bool {
    [
        AZURE_CLIENT_ID,
        AZURE_TENANT_ID,
        AZURE_FEDERATED_TOKEN_FILE,
    ]
    .iter()
    .all(|variable| match env::var(variable) {
        Ok(value) => !value.trim().is_empty(),
        Err(_) => false,
    })
}

/// Builds the container client.
///
/// The 1.0 clients take the container URL directly rather than an account name plus a cloud
/// location, so a custom sovereign endpoint needs no special case: it is the base of the URL.
pub(crate) fn build_container_client(
    storage_account_name: &str,
    credential: AzureCredential,
    blob_service_uri: Option<String>,
    container_name: &str,
) -> Result<BlobContainerClient, StorageResolverError> {
    let blob_service_uri = match blob_service_uri {
        Some(uri) => {
            info!(
                endpoint = %uri,
                "using Azure blob storage endpoint defined in storage config or environment \
                 variable"
            );
            uri
        }
        None => format!("https://{storage_account_name}.blob.core.windows.net"),
    };
    let container_url = join_container(&blob_service_uri, container_name)?;

    let mut client_options = BlobContainerClientOptions::default();
    let token_credential = match credential {
        AzureCredential::SharedKey(access_key) => {
            // Signing happens per retry so that every attempt carries a fresh `x-ms-date`.
            let policy: Arc<dyn Policy> = Arc::new(SharedKeyAuthorizationPolicy::new(
                storage_account_name.to_owned(),
                access_key,
            ));
            client_options.client_options = ClientOptions {
                per_try_policies: vec![policy],
                ..Default::default()
            };
            None
        }
        AzureCredential::Token(token_credential) => Some(token_credential),
    };

    BlobContainerClient::new(container_url, token_credential, Some(client_options)).map_err(
        |error| {
            StorageResolverError::InvalidConfig(format!(
                "could not build an Azure container client: {error}"
            ))
        },
    )
}

/// Appends the container to the blob service URL.
fn join_container(
    blob_service_uri: &str,
    container_name: &str,
) -> Result<Url, StorageResolverError> {
    let mut container_url = Url::parse(blob_service_uri.trim_end_matches('/')).map_err(|error| {
        StorageResolverError::InvalidConfig(format!(
            "`{blob_service_uri}` is not a valid Azure blob service URL: {error}"
        ))
    })?;
    container_url
        .path_segments_mut()
        .map_err(|_| {
            StorageResolverError::InvalidConfig(format!(
                "`{blob_service_uri}` cannot be a base URL"
            ))
        })?
        .pop_if_empty()
        .push(container_name);
    Ok(container_url)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_join_container_appends_the_container() {
        let url = join_container("https://acct.blob.core.windows.net", "my-container").unwrap();
        assert_eq!(url.as_str(), "https://acct.blob.core.windows.net/my-container");
    }

    #[test]
    fn test_join_container_tolerates_a_trailing_slash() {
        let url = join_container("https://acct.blob.core.windows.net/", "my-container").unwrap();
        assert_eq!(url.as_str(), "https://acct.blob.core.windows.net/my-container");
    }

    #[test]
    fn test_join_container_keeps_a_sovereign_host() {
        let url = join_container("https://acct.blob.core.usgovcloudapi.net", "c").unwrap();
        assert_eq!(url.as_str(), "https://acct.blob.core.usgovcloudapi.net/c");
    }

    #[test]
    fn test_join_container_keeps_an_emulator_path_prefix() {
        // Azurite addresses accounts by path rather than by subdomain, so the account
        // segment has to survive.
        let url = join_container("http://127.0.0.1:10000/devstoreaccount1", "c").unwrap();
        assert_eq!(url.as_str(), "http://127.0.0.1:10000/devstoreaccount1/c");
    }

    #[test]
    fn test_join_container_rejects_a_non_url() {
        assert!(join_container("not a url", "c").is_err());
    }
}
