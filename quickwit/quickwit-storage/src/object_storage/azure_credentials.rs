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

use std::env;
use std::sync::Arc;

use azure_core::credentials::{Secret, TokenCredential};
use azure_core::http::policies::Policy;
use azure_core::http::{Transport, Url};
use azure_identity::{
    AzureCliCredential, ClientSecretCredential, ManagedIdentityCredential,
    ManagedIdentityCredentialOptions, UserAssignedId, WorkloadIdentityCredential,
    WorkloadIdentityCredentialOptions,
};
use azure_storage_blob::{BlobContainerClient, BlobContainerClientOptions};
use quickwit_config::AzureStorageConfig;
use tracing::info;

use crate::StorageResolverError;
use crate::object_storage::azure_shared_key::SharedKeyAuthorizationPolicy;

const AZURE_CLIENT_ID: &str = "AZURE_CLIENT_ID";
const AZURE_TENANT_ID: &str = "AZURE_TENANT_ID";
const AZURE_FEDERATED_TOKEN_FILE: &str = "AZURE_FEDERATED_TOKEN_FILE";
const AZURE_FEDERATED_TOKEN: &str = "AZURE_FEDERATED_TOKEN";
const AZURE_CLIENT_SECRET: &str = "AZURE_CLIENT_SECRET";
const AZURE_CREDENTIAL_KIND: &str = "AZURE_CREDENTIAL_KIND";

#[derive(Debug, Eq, PartialEq)]
enum TokenCredentialKind {
    WorkloadIdentity {
        tenant_id: String,
        client_id: String,
        token_file_path: String,
    },
    ClientSecret {
        tenant_id: String,
        client_id: String,
        secret: Secret,
    },
    /// `None` selects the system-assigned identity.
    ManagedIdentity {
        user_assigned_client_id: Option<String>,
    },
    AzureCli,
}

/// Keep in sync with the authentication section of `docs/configuration/storage-config.md`.
fn select_token_credential_kind(
    var_fn: impl Fn(&str) -> Option<String>,
) -> Result<TokenCredentialKind, String> {
    let var = |name: &str| {
        var_fn(name)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    };
    let reject_inline_token = || match var(AZURE_FEDERATED_TOKEN_FILE) {
        None if var(AZURE_FEDERATED_TOKEN).is_some() => Err(format!(
            "`{AZURE_FEDERATED_TOKEN}` is no longer supported, set `{AZURE_FEDERATED_TOKEN_FILE}` \
             instead"
        )),
        _ => Ok(()),
    };
    let workload_identity = || {
        Some(TokenCredentialKind::WorkloadIdentity {
            tenant_id: var(AZURE_TENANT_ID)?,
            client_id: var(AZURE_CLIENT_ID)?,
            token_file_path: var(AZURE_FEDERATED_TOKEN_FILE)?,
        })
    };
    let client_secret = || {
        Some(TokenCredentialKind::ClientSecret {
            tenant_id: var(AZURE_TENANT_ID)?,
            client_id: var(AZURE_CLIENT_ID)?,
            secret: Secret::new(var(AZURE_CLIENT_SECRET)?),
        })
    };
    let managed_identity = |user_assigned_client_id| TokenCredentialKind::ManagedIdentity {
        user_assigned_client_id,
    };

    let Some(credential_kind) = var(AZURE_CREDENTIAL_KIND) else {
        reject_inline_token()?;
        let kind = workload_identity()
            .or_else(client_secret)
            .unwrap_or_else(|| {
                info!(
                    "no workload identity or client secret configured, using managed identity: \
                     set `{AZURE_CREDENTIAL_KIND}=azurecli` to use `az login` instead"
                );
                managed_identity(var(AZURE_CLIENT_ID))
            });
        return Ok(kind);
    };
    let normalized_kind = credential_kind.replace(' ', "").to_lowercase();
    if matches!(normalized_kind.as_str(), "environment" | "workloadidentity") {
        reject_inline_token()?;
    }
    let kind_opt = match normalized_kind.as_str() {
        "environment" => workload_identity().or_else(client_secret),
        "workloadidentity" => workload_identity(),
        "clientsecret" => client_secret(),
        "managedidentity" => Some(managed_identity(var(AZURE_CLIENT_ID))),
        // 0.21 always used the system-assigned identity for these.
        "virtualmachine" | "appservice" => Some(managed_identity(None)),
        "azurecli" => Some(TokenCredentialKind::AzureCli),
        _ => {
            return Err(format!(
                "unknown `{AZURE_CREDENTIAL_KIND}` value `{credential_kind}`, expected one of \
                 `environment`, `workloadidentity`, `clientsecret`, `managedidentity`, \
                 `virtualmachine`, `appservice`, or `azurecli`"
            ));
        }
    };
    kind_opt.ok_or_else(|| {
        format!(
            "`{AZURE_CREDENTIAL_KIND}` is `{credential_kind}`, but the environment variables it \
             requires are not set"
        )
    })
}

#[derive(Clone)]
pub(crate) enum AzureCredential {
    SharedKey(String),
    Token(Arc<dyn TokenCredential>),
}

pub(crate) fn resolve_credential(
    azure_storage_config: &AzureStorageConfig,
) -> Result<AzureCredential, StorageResolverError> {
    if let Some(access_key) = azure_storage_config.resolve_access_key() {
        return Ok(AzureCredential::SharedKey(access_key));
    }
    let kind = select_token_credential_kind(|name| env::var(name).ok())
        .map_err(StorageResolverError::InvalidConfig)?;
    let token_credential = build_token_credential(kind).map_err(|error| {
        StorageResolverError::InvalidConfig(format!(
            "could not build an Azure token credential: {error}"
        ))
    })?;
    Ok(AzureCredential::Token(token_credential))
}

fn build_token_credential(
    kind: TokenCredentialKind,
) -> azure_core::Result<Arc<dyn TokenCredential>> {
    match kind {
        TokenCredentialKind::WorkloadIdentity {
            tenant_id,
            client_id,
            token_file_path,
        } => {
            info!("using azure workload identity credential");
            let options = WorkloadIdentityCredentialOptions {
                tenant_id: Some(tenant_id),
                client_id: Some(client_id),
                token_file_path: Some(token_file_path.into()),
                ..Default::default()
            };
            Ok(WorkloadIdentityCredential::new(Some(options))?)
        }
        TokenCredentialKind::ClientSecret {
            tenant_id,
            client_id,
            secret,
        } => {
            info!("using azure client secret credential");
            Ok(ClientSecretCredential::new(
                &tenant_id, client_id, secret, None,
            )?)
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

pub(crate) fn build_container_client(
    storage_account_name: &str,
    credential: AzureCredential,
    transport: Option<Transport>,
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
    client_options.client_options.transport = transport;
    let token_credential = match credential {
        AzureCredential::SharedKey(access_key) => {
            let policy =
                SharedKeyAuthorizationPolicy::new(storage_account_name.to_owned(), access_key)
                    .map_err(|error| {
                        StorageResolverError::InvalidConfig(format!(
                            "invalid Azure storage access key: {error}"
                        ))
                    })?;
            client_options
                .client_options
                .per_try_policies
                .push(Arc::new(policy) as Arc<dyn Policy>);
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

    fn select(pairs: &[(&str, &str)]) -> Result<TokenCredentialKind, String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect();
        select_token_credential_kind(move |name: &str| map.get(name).cloned())
    }

    fn managed_identity(user_assigned_client_id: Option<&str>) -> TokenCredentialKind {
        TokenCredentialKind::ManagedIdentity {
            user_assigned_client_id: user_assigned_client_id.map(str::to_owned),
        }
    }

    const SERVICE_PRINCIPAL: [(&str, &str); 3] = [
        (AZURE_TENANT_ID, "tenant"),
        (AZURE_CLIENT_ID, "client"),
        (AZURE_CLIENT_SECRET, "secret"),
    ];

    #[test]
    fn test_workload_identity_wins_over_client_secret() {
        let kind = select(&[
            (AZURE_TENANT_ID, " tenant\n"),
            (AZURE_CLIENT_ID, "\tclient "),
            (AZURE_CLIENT_SECRET, "secret"),
            (AZURE_FEDERATED_TOKEN_FILE, " /var/run/token "),
        ])
        .unwrap();
        assert_eq!(
            kind,
            TokenCredentialKind::WorkloadIdentity {
                tenant_id: "tenant".to_owned(),
                client_id: "client".to_owned(),
                token_file_path: "/var/run/token".to_owned(),
            }
        );
    }

    #[test]
    fn test_client_secret_wins_over_managed_identity() {
        let kind = select(&[
            (AZURE_TENANT_ID, "  tenant\n"),
            (AZURE_CLIENT_ID, "\tclient  "),
            (AZURE_CLIENT_SECRET, " secret "),
        ])
        .unwrap();
        assert_eq!(
            kind,
            TokenCredentialKind::ClientSecret {
                tenant_id: "tenant".to_owned(),
                client_id: "client".to_owned(),
                secret: Secret::new("secret"),
            }
        );
    }

    #[test]
    fn test_managed_identity_is_the_default() {
        assert_eq!(select(&[]).unwrap(), managed_identity(None));
        assert_eq!(
            select(&[(AZURE_CLIENT_ID, "client")]).unwrap(),
            managed_identity(Some("client"))
        );
    }

    #[test]
    fn test_pinned_kinds() {
        let select_pinned = |credential_kind: &str| {
            let mut env = SERVICE_PRINCIPAL.to_vec();
            env.push((AZURE_CREDENTIAL_KIND, credential_kind));
            select(&env).unwrap()
        };
        let client_secret = select(&SERVICE_PRINCIPAL).unwrap();
        assert_eq!(select_pinned("environment"), client_secret);
        assert_eq!(select_pinned("Client Secret"), client_secret);
        assert_eq!(
            select_pinned("managedidentity"),
            managed_identity(Some("client"))
        );
        assert_eq!(select_pinned("VirtualMachine"), managed_identity(None));
        assert_eq!(select_pinned("appservice"), managed_identity(None));
        assert_eq!(select_pinned(" Azure CLI "), TokenCredentialKind::AzureCli);
        assert_eq!(select_pinned("  "), client_secret);
    }

    #[test]
    fn test_unknown_kind_is_rejected() {
        for credential_kind in ["azurecl", "developertools", "workload-identity"] {
            let error = select(&[(AZURE_CREDENTIAL_KIND, credential_kind)]).unwrap_err();
            assert!(error.contains(credential_kind));
        }
    }
}
