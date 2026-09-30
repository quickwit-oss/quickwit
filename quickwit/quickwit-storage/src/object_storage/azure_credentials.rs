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

use std::sync::Arc;
use std::{env, fmt};

use azure_core::credentials::TokenCredential;
use azure_core::http::policies::Policy;
use azure_core::http::{ClientOptions, Url};
use azure_identity::{
    AzureCliCredential, ClientSecretCredential, ManagedIdentityCredential,
    ManagedIdentityCredentialOptions, UserAssignedId, WorkloadIdentityCredential,
};
use azure_storage_blob::{BlobContainerClient, BlobContainerClientOptions};
use quickwit_config::AzureStorageConfig;
use tracing::info;

use crate::StorageResolverError;
use crate::object_storage::azure_shared_key::SharedKeyAuthorizationPolicy;

/// Environment variables the workload identity webhook injects into a pod.
const AZURE_CLIENT_ID: &str = "AZURE_CLIENT_ID";
const AZURE_TENANT_ID: &str = "AZURE_TENANT_ID";
const AZURE_FEDERATED_TOKEN_FILE: &str = "AZURE_FEDERATED_TOKEN_FILE";
/// Set when a service principal authenticates with a secret rather than a federated token.
const AZURE_CLIENT_SECRET: &str = "AZURE_CLIENT_SECRET";
/// Lets an operator pin the provider instead of relying on the detection below.
const AZURE_CREDENTIAL_KIND: &str = "AZURE_CREDENTIAL_KIND";

/// Which token credential the environment describes.
///
/// Kept separate from construction so the precedence can be tested without mutating process
/// environment, which no test can do safely while others run.
#[derive(Eq, PartialEq)]
enum TokenCredentialKind {
    /// Service principal with a client secret, the `EnvironmentCredential` of the old chain.
    ClientSecret {
        tenant_id: String,
        client_id: String,
        secret: String,
    },
    /// Federated token file, injected into a pod by the workload identity webhook.
    WorkloadIdentity,
    /// IMDS. `user_assigned_client_id` selects a user-assigned identity, and `None` means the
    /// system-assigned one.
    ManagedIdentity {
        user_assigned_client_id: Option<String>,
    },
    /// The identity `az login` cached, for a developer machine or a CI runner rather than a pod.
    /// Only reachable by pinning `AZURE_CREDENTIAL_KIND`, never by detection: no environment
    /// variable describes it.
    AzureCli,
}

impl fmt::Debug for TokenCredentialKind {
    /// Hand written so the client secret cannot reach a log line or a panic message. A
    /// derived implementation would print it, and test failures print this type.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClientSecret {
                tenant_id,
                client_id,
                ..
            } => formatter
                .debug_struct("ClientSecret")
                .field("tenant_id", tenant_id)
                .field("client_id", client_id)
                .finish_non_exhaustive(),
            Self::WorkloadIdentity => formatter.write_str("WorkloadIdentity"),
            Self::ManagedIdentity {
                user_assigned_client_id,
            } => formatter
                .debug_struct("ManagedIdentity")
                .field("user_assigned_client_id", user_assigned_client_id)
                .finish(),
            Self::AzureCli => formatter.write_str("AzureCli"),
        }
    }
}

/// Decides which credential the environment describes.
///
/// Precedence follows the chain `azure_identity::create_credential()` used to walk, where an
/// environment credential came before managed identity. Dropping that ordering silently
/// breaks every deployment that authenticates with a service principal, because the client
/// secret is ignored and IMDS is contacted instead.
fn select_token_credential_kind(var_fn: impl Fn(&str) -> Option<String>) -> TokenCredentialKind {
    let non_empty = |name: &str| match var_fn(name) {
        Some(value) if !value.trim().is_empty() => Some(value.trim().to_string()),
        _ => None,
    };
    let client_id = non_empty(AZURE_CLIENT_ID);

    // A secret and a federated token file are mutually exclusive in practice. The secret is
    // checked first because the webhook injects the token file, so its presence says less
    // about operator intent than a secret does.
    if let (Some(tenant_id), Some(client_id), Some(secret)) = (
        non_empty(AZURE_TENANT_ID),
        client_id.clone(),
        non_empty(AZURE_CLIENT_SECRET),
    ) {
        return TokenCredentialKind::ClientSecret {
            tenant_id,
            client_id,
            secret,
        };
    }
    if non_empty(AZURE_TENANT_ID).is_some()
        && client_id.is_some()
        && non_empty(AZURE_FEDERATED_TOKEN_FILE).is_some()
    {
        return TokenCredentialKind::WorkloadIdentity;
    }
    // `AZURE_CLIENT_ID` on its own names a user-assigned identity. Ignoring it asks IMDS for
    // the system-assigned identity, which either does not exist or is the wrong principal.
    TokenCredentialKind::ManagedIdentity {
        user_assigned_client_id: client_id,
    }
}

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
             workload identity, managed identity, or the Azure CLI configured"
        ))
    })?;
    Ok(AzureCredential::Token(token_credential))
}

/// Maps an explicit `AZURE_CREDENTIAL_KIND` to the provider it names.
///
/// `None` when the variable is unset, empty, or names no provider, in which case the caller
/// detects from the environment instead.
///
/// `azure_identity` 0.21 matched this variable after `replace(' ', "").to_lowercase()`, so
/// `Azure CLI` and `azurecli` were the same value. Normalizing the same way keeps a deployment
/// that pinned the variable working across the upgrade.
fn select_pinned_credential_kind(
    credential_kind: &str,
    var_fn: impl Fn(&str) -> Option<String>,
) -> Option<TokenCredentialKind> {
    let credential_kind = credential_kind.trim().replace(' ', "").to_lowercase();

    match credential_kind.as_str() {
        "workloadidentity" => Some(TokenCredentialKind::WorkloadIdentity),
        "managedidentity" => Some(TokenCredentialKind::ManagedIdentity {
            user_assigned_client_id: var_fn(AZURE_CLIENT_ID)
                .map(|client_id| client_id.trim().to_string())
                .filter(|client_id| !client_id.is_empty()),
        }),
        "azurecli" => Some(TokenCredentialKind::AzureCli),
        _ => None,
    }
}

/// Builds the token credential the environment describes.
fn resolve_token_credential() -> azure_core::Result<Arc<dyn TokenCredential>> {
    let credential_kind = env::var(AZURE_CREDENTIAL_KIND).unwrap_or_default();
    let var_fn = |name: &str| env::var(name).ok();
    // An empty or unrecognized value falls through to detection.
    let kind = select_pinned_credential_kind(&credential_kind, var_fn)
        .unwrap_or_else(|| select_token_credential_kind(var_fn));
    build_token_credential(kind)
}

/// Builds the credential for a kind already chosen.
fn build_token_credential(
    kind: TokenCredentialKind,
) -> azure_core::Result<Arc<dyn TokenCredential>> {
    match kind {
        TokenCredentialKind::ClientSecret {
            tenant_id,
            client_id,
            secret,
        } => {
            info!("using azure client secret credential");
            Ok(ClientSecretCredential::new(
                &tenant_id,
                client_id,
                secret.into(),
                None,
            )?)
        }
        TokenCredentialKind::WorkloadIdentity => {
            info!("using azure workload identity credential");
            Ok(WorkloadIdentityCredential::new(None)?)
        }
        TokenCredentialKind::ManagedIdentity {
            user_assigned_client_id,
        } => {
            let options = match user_assigned_client_id {
                Some(client_id) => {
                    info!(%client_id, "using azure user-assigned managed identity credential");
                    Some(ManagedIdentityCredentialOptions {
                        user_assigned_id: Some(UserAssignedId::ClientId(client_id)),
                        ..Default::default()
                    })
                }
                None => {
                    info!("using azure system-assigned managed identity credential");
                    None
                }
            };
            Ok(ManagedIdentityCredential::new(options)?)
        }
        TokenCredentialKind::AzureCli => {
            info!("using azure cli credential");
            Ok(AzureCliCredential::new(None)?)
        }
    }
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
    let mut container_url =
        Url::parse(blob_service_uri.trim_end_matches('/')).map_err(|error| {
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
    use std::collections::HashMap;

    use super::*;

    fn env_from(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect();
        move |name: &str| map.get(name).cloned()
    }

    /// A service principal authenticates with a secret. The old chain tried an environment
    /// credential before managed identity, and losing that ordering sends the request to
    /// IMDS with the secret ignored, so the deployment loses access entirely.
    #[test]
    fn test_client_secret_wins_over_managed_identity() {
        let kind = select_token_credential_kind(env_from(&[
            (AZURE_TENANT_ID, "tenant"),
            (AZURE_CLIENT_ID, "client"),
            (AZURE_CLIENT_SECRET, "secret"),
        ]));
        assert_eq!(
            kind,
            TokenCredentialKind::ClientSecret {
                tenant_id: "tenant".to_owned(),
                client_id: "client".to_owned(),
                secret: "secret".to_owned(),
            }
        );
    }

    /// Whitespace around a variable survives most shell and manifest plumbing. Accepting the
    /// value but keeping the padding sends it to Entra as part of the tenant or client id.
    #[test]
    fn test_surrounding_whitespace_is_stripped() {
        let kind = select_token_credential_kind(env_from(&[
            (AZURE_TENANT_ID, "  tenant\n"),
            (AZURE_CLIENT_ID, "\tclient  "),
            (AZURE_CLIENT_SECRET, " secret "),
        ]));
        assert_eq!(
            kind,
            TokenCredentialKind::ClientSecret {
                tenant_id: "tenant".to_owned(),
                client_id: "client".to_owned(),
                secret: "secret".to_owned(),
            }
        );
    }

    /// The webhook injects the token file, so a secret is the stronger statement of intent.
    #[test]
    fn test_client_secret_wins_over_workload_identity() {
        let kind = select_token_credential_kind(env_from(&[
            (AZURE_TENANT_ID, "tenant"),
            (AZURE_CLIENT_ID, "client"),
            (AZURE_CLIENT_SECRET, "secret"),
            (AZURE_FEDERATED_TOKEN_FILE, "/var/run/token"),
        ]));
        assert!(matches!(kind, TokenCredentialKind::ClientSecret { .. }));
    }

    #[test]
    fn test_workload_identity_when_the_three_variables_are_present() {
        let kind = select_token_credential_kind(env_from(&[
            (AZURE_TENANT_ID, "tenant"),
            (AZURE_CLIENT_ID, "client"),
            (AZURE_FEDERATED_TOKEN_FILE, "/var/run/token"),
        ]));
        assert_eq!(kind, TokenCredentialKind::WorkloadIdentity);
    }

    /// `AZURE_CLIENT_ID` alone names a user-assigned identity. Dropping it asks IMDS for the
    /// system-assigned one, which is either absent or the wrong principal.
    #[test]
    fn test_client_id_alone_selects_a_user_assigned_identity() {
        let kind = select_token_credential_kind(env_from(&[(AZURE_CLIENT_ID, "client")]));
        assert_eq!(
            kind,
            TokenCredentialKind::ManagedIdentity {
                user_assigned_client_id: Some("client".to_owned()),
            }
        );
    }

    #[test]
    fn test_empty_environment_selects_the_system_assigned_identity() {
        let kind = select_token_credential_kind(env_from(&[]));
        assert_eq!(
            kind,
            TokenCredentialKind::ManagedIdentity {
                user_assigned_client_id: None,
            }
        );
    }

    /// A half configured service principal must not be read as one, and a blank variable is
    /// the same as an unset one.
    #[test]
    fn test_blank_and_partial_values_do_not_select_a_service_principal() {
        let partial = select_token_credential_kind(env_from(&[
            (AZURE_TENANT_ID, "tenant"),
            (AZURE_CLIENT_ID, "client"),
        ]));
        assert_eq!(
            partial,
            TokenCredentialKind::ManagedIdentity {
                user_assigned_client_id: Some("client".to_owned()),
            }
        );

        let blank = select_token_credential_kind(env_from(&[
            (AZURE_TENANT_ID, "tenant"),
            (AZURE_CLIENT_ID, "client"),
            (AZURE_CLIENT_SECRET, "   "),
        ]));
        assert_eq!(
            blank,
            TokenCredentialKind::ManagedIdentity {
                user_assigned_client_id: Some("client".to_owned()),
            }
        );
    }

    #[test]
    fn test_debug_does_not_leak_the_client_secret() {
        let kind = select_token_credential_kind(env_from(&[
            (AZURE_TENANT_ID, "tenant"),
            (AZURE_CLIENT_ID, "client"),
            (AZURE_CLIENT_SECRET, "super-secret-value"),
        ]));
        let rendered = format!("{kind:?}");
        assert!(rendered.contains("client"));
        assert!(!rendered.contains("super-secret-value"));
    }

    /// A partial workload identity set is not workload identity.
    #[test]
    fn test_token_file_without_tenant_is_not_workload_identity() {
        let kind = select_token_credential_kind(env_from(&[
            (AZURE_CLIENT_ID, "client"),
            (AZURE_FEDERATED_TOKEN_FILE, "/var/run/token"),
        ]));
        assert_eq!(
            kind,
            TokenCredentialKind::ManagedIdentity {
                user_assigned_client_id: Some("client".to_owned()),
            }
        );
    }

    /// `azure_identity` 0.21 accepted `azurecli`, and a deployment pinning it authenticates
    /// through `az login`. Falling through to detection instead lands on managed identity and
    /// sends the request to IMDS, which loses blob access entirely.
    #[test]
    fn test_azure_cli_kind_is_pinned() {
        let kind = select_pinned_credential_kind("azurecli", env_from(&[]));
        assert_eq!(kind, Some(TokenCredentialKind::AzureCli));
    }

    /// 0.21 matched after stripping spaces and lowercasing, so every spelling below named the
    /// same provider and has to keep doing so.
    #[test]
    fn test_kind_matching_ignores_case_and_spaces() {
        for spelling in [
            "Azure CLI",
            "AzureCLI",
            " azurecli ",
            "azure cli",
            "\tazurecli\n",
        ] {
            assert_eq!(
                select_pinned_credential_kind(spelling, env_from(&[])),
                Some(TokenCredentialKind::AzureCli),
                "`{spelling}` should name the Azure CLI credential"
            );
        }
        assert_eq!(
            select_pinned_credential_kind("Workload Identity", env_from(&[])),
            Some(TokenCredentialKind::WorkloadIdentity)
        );
    }

    /// Pinning managed identity still honours `AZURE_CLIENT_ID`, which names a user-assigned
    /// identity, and still trims it.
    #[test]
    fn test_pinned_managed_identity_keeps_the_user_assigned_id() {
        let kind =
            select_pinned_credential_kind("managedidentity", env_from(&[(AZURE_CLIENT_ID, " c ")]));
        assert_eq!(
            kind,
            Some(TokenCredentialKind::ManagedIdentity {
                user_assigned_client_id: Some("c".to_owned()),
            })
        );

        let blank =
            select_pinned_credential_kind("managedidentity", env_from(&[(AZURE_CLIENT_ID, "  ")]));
        assert_eq!(
            blank,
            Some(TokenCredentialKind::ManagedIdentity {
                user_assigned_client_id: None,
            })
        );
    }

    /// An unset or unrecognized value names no provider, and the caller detects from the
    /// environment instead.
    #[test]
    fn test_unset_and_unrecognized_kinds_name_no_provider() {
        for spelling in ["", "   ", "azurecl", "developertools"] {
            assert_eq!(
                select_pinned_credential_kind(spelling, env_from(&[])),
                None,
                "`{spelling}` should not name a provider"
            );
        }
    }

    #[test]
    fn test_join_container_appends_the_container() {
        let url = join_container("https://acct.blob.core.windows.net", "my-container").unwrap();
        assert_eq!(
            url.as_str(),
            "https://acct.blob.core.windows.net/my-container"
        );
    }

    #[test]
    fn test_join_container_tolerates_a_trailing_slash() {
        let url = join_container("https://acct.blob.core.windows.net/", "my-container").unwrap();
        assert_eq!(
            url.as_str(),
            "https://acct.blob.core.windows.net/my-container"
        );
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
