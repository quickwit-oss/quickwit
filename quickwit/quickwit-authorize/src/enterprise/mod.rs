// The Quickwit Enterprise Edition (EE) license
// Copyright (c) 2024-present Quickwit Inc.
//
// With regard to the Quickwit Software:
//
// This software and associated documentation files (the "Software") may only be
// used in production, if you (and any entity that you represent) hold a valid
// Quickwit Enterprise license corresponding to your usage.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// For all third party components incorporated into the Quickwit Software, those
// components are licensed under the original license provided by the owner of the
// applicable component.

mod authorization_layer;
mod authorization_token_extraction_layer;

use std::future::Future;
use std::str::FromStr;
use std::sync::{Arc, OnceLock};

use anyhow::Context;
pub mod cli;
pub use authorization_layer::AuthorizationLayer;
pub use authorization_token_extraction_layer::AuthorizationTokenExtractionLayer;
use biscuit_auth::macros::authorizer;
use biscuit_auth::{Authorizer, Biscuit, RootKeyProvider};
use tokio::task::futures::TaskLocalFuture;
use tokio_inherit_task_local::TaskLocalInheritableTable;
use tracing::info;

use crate::AuthorizationError;

tokio_inherit_task_local::inheritable_task_local! {
    pub static AUTHORIZATION_TOKEN: AuthorizationToken;
}

static ROOT_KEY_PROVIDER: OnceLock<Arc<dyn RootKeyProvider + Sync + Send>> = OnceLock::new();
static NODE_TOKEN: OnceLock<AuthorizationToken> = OnceLock::new();

#[derive(Clone)]
pub struct AuthorizationToken(Arc<Biscuit>);

impl AuthorizationToken {
    pub fn into_biscuit(self) -> Arc<Biscuit> {
        self.0.clone()
    }

    pub fn print(&self) -> anyhow::Result<()> {
        let biscuit = &self.0;
        for i in 0..biscuit.block_count() {
            let block = biscuit.print_block_source(i)?;
            println!("--- Block #{} ---", i + 1);
            println!("{block}\n");
        }
        Ok(())
    }
}

impl From<Biscuit> for AuthorizationToken {
    fn from(biscuit: Biscuit) -> Self {
        AuthorizationToken(Arc::new(biscuit))
    }
}

impl std::fmt::Display for AuthorizationToken {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let token_base_64 = self.0.to_base64().map_err(|_err| std::fmt::Error)?;
        token_base_64.fmt(f)
    }
}

impl std::fmt::Debug for AuthorizationToken {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "AuthorizationToken({})", &self.0)
    }
}

pub fn set_node_token_base64(node_token_base64: &str) -> anyhow::Result<()> {
    info!("set node token hex: {node_token_base64}");
    let node_token =
        AuthorizationToken::from_str(node_token_base64).context("failed to set node token")?;
    if NODE_TOKEN.set(node_token).is_err() {
        tracing::error!("node token was already initialized");
    }
    Ok(())
}

pub fn set_root_public_key(root_key_base64: &str) -> anyhow::Result<()> {
    info!(root_key = root_key_base64, "setting root public key");
    let public_key = biscuit_auth::PublicKey::from_bytes_hex(root_key_base64)
        .context("failed to parse root public key")?;
    let key_provider: Arc<dyn RootKeyProvider + Sync + Send> = Arc::new(public_key);
    set_root_key_provider(key_provider);
    Ok(())
}

pub fn set_root_key_provider(key_provider: Arc<dyn RootKeyProvider + Sync + Send>) {
    if ROOT_KEY_PROVIDER.set(key_provider).is_err() {
        tracing::error!("root key provider was already initialized");
    }
}

fn get_root_key_provider() -> Arc<dyn RootKeyProvider> {
    ROOT_KEY_PROVIDER
        .get()
        .expect("root key provider should have been initialized beforehand")
        .clone()
}

impl FromStr for AuthorizationToken {
    type Err = AuthorizationError;

    fn from_str(token_base64: &str) -> Result<Self, AuthorizationError> {
        let root_key_provider = get_root_key_provider();
        let biscuit = Biscuit::from_base64(token_base64, root_key_provider)
            .map_err(|_| AuthorizationError::InvalidToken)?;
        Ok(AuthorizationToken::from(biscuit))
    }
}

const AUTHORIZATION_VALUE_PREFIX: &str = "Bearer ";

fn default_authorizer(
    request_family: RequestFamily,
    auth_token: &AuthorizationToken,
) -> Result<Authorizer, AuthorizationError> {
    let request_family_str = request_family.as_str();
    info!(request = request_family_str, "authorize");
    let mut authorizer: Authorizer = authorizer!(
        r#"
        request({request_family_str});

        right($request) <- role($role), role_right($role, $request);
        right($request) <- service($service), service_right($service, $request);

        service_right("control_plane", "index:read");
        service_right("control_plane", "index:write");
        service_right("control_plane", "index:admin");
        service_right("control_plane", "cluster");

        service_right("indexer", "index:write");
        service_right("indexer", "index:read");
        service_right("indexer", "cluster");

        service_right("searcher", "cluster");

        service_right("janitor", "index:read");
        service_right("janitor", "cluster");
        service_right("janitor", "index:write");

        // We generate the actual user role, by doing an union of the rights granted via roles.
        // right($request) <- role($role), role_right($role, $request);
        // right($operation, $resource) <- role($role), role_right($role, $operation, $resource);
        // right($operation) <- role("root"), operation($operation);
        // right($operation, $resource) <- role("root"), operation($operation), resource($resource);


        // Finally we check that we have access to index1 and index2.
        check all request($operation), right($operation);

        allow if true;
    "#
    );
    authorizer.set_time();
    auth_token.print().unwrap();
    println!("{}", authorizer.print_world());
    authorizer
        .add_token(&auth_token.0)
        .map_err(|_| AuthorizationError::PermissionDenied)?;
    Ok(authorizer)
}

#[derive(Default, Debug, Copy, Clone)]
pub enum RequestFamily {
    #[default]
    IndexRead,
    IndexWrite,
    IndexAdmin,
    Cluster,
}

impl RequestFamily {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::IndexRead => "index:read",
            Self::IndexWrite => "index:write",
            Self::IndexAdmin => "index:admin",
            Self::Cluster => "cluster",
        }
    }
}

pub trait Authorization {
    fn attenuate(
        &self,
        auth_token: AuthorizationToken,
    ) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }

    fn authorizer(
        &self,
        auth_token: &AuthorizationToken,
    ) -> Result<Authorizer, AuthorizationError> {
        default_authorizer(Self::request_family(), auth_token)
    }

    fn request_family() -> RequestFamily {
        RequestFamily::default()
    }
}

pub trait StreamAuthorization {
    fn attenuate(
        auth_token: AuthorizationToken,
    ) -> std::result::Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
    fn authorizer(
        auth_token: &AuthorizationToken,
    ) -> std::result::Result<Authorizer, AuthorizationError> {
        default_authorizer(Self::request_family(), &auth_token)
    }

    fn request_family() -> RequestFamily {
        RequestFamily::IndexRead
    }
}

// impl From<biscuit_auth::error::Token> for AuthorizationError {
//     fn from(token_error: biscuit_auth::error::Token) -> AuthorizationError {
//         error!(token_error=?token_error);
//         AuthorizationError::InvalidToken
//     }
// }

pub fn get_auth_token_from_str(
    authorization_header_value: &str,
) -> Result<AuthorizationToken, AuthorizationError> {
    let authorization_token_str: &str = authorization_header_value
        .strip_prefix(AUTHORIZATION_VALUE_PREFIX)
        .ok_or(AuthorizationError::InvalidToken)?;
    let biscuit: Biscuit = Biscuit::from_base64(authorization_token_str, get_root_key_provider())
        .map_err(|_| AuthorizationError::InvalidToken)?;
    Ok(AuthorizationToken::from(biscuit))
}

pub fn extract_auth_token(
    req_metadata: &tonic::metadata::MetadataMap,
) -> Result<AuthorizationToken, AuthorizationError> {
    let authorization_header_value: &str = req_metadata
        .get(http::header::AUTHORIZATION.as_str())
        .ok_or(AuthorizationError::AuthorizationTokenMissing)?
        .to_str()
        .map_err(|_| AuthorizationError::InvalidToken)?;
    get_auth_token_from_str(authorization_header_value)
}

pub fn set_auth_token(
    auth_token: &AuthorizationToken,
    req_metadata: &mut tonic::metadata::MetadataMap,
) {
    let authorization_header_value = format!("{AUTHORIZATION_VALUE_PREFIX}{auth_token}");
    req_metadata.insert(
        http::header::AUTHORIZATION.as_str(),
        authorization_header_value.parse().unwrap(),
    );
}

pub fn authorize<R: Authorization>(
    req: &R,
    auth_token: &AuthorizationToken,
) -> Result<(), AuthorizationError> {
    let mut authorizer = req.authorizer(auth_token)?;
    info!("authorizer");
    authorizer
        .authorize()
        .map_err(|_err| AuthorizationError::PermissionDenied)?;
    info!("authorize done");
    Ok(())
}

fn get_auth_token() -> Option<AuthorizationToken> {
    AUTHORIZATION_TOKEN
        .try_with(|auth_token| auth_token.clone())
        .ok()
        .or_else(|| NODE_TOKEN.get().cloned())
}

pub fn build_tonic_request_with_auth_token<R>(
    req: R,
) -> Result<tonic::Request<R>, AuthorizationError> {
    let Some(authorization_token) = get_auth_token() else {
        return Err(AuthorizationError::AuthorizationTokenMissing);
    };
    let mut tonic_request = tonic::Request::new(req);
    set_auth_token(&authorization_token, tonic_request.metadata_mut());
    Ok(tonic_request)
}

pub fn authorize_stream<R: StreamAuthorization>(
    auth_token: &AuthorizationToken,
) -> Result<(), AuthorizationError> {
    let mut authorizer = R::authorizer(auth_token)?;
    authorizer
        .add_token(&auth_token.0)
        .map_err(|_| AuthorizationError::PermissionDenied)?;
    authorizer
        .authorize()
        .map_err(|_| AuthorizationError::PermissionDenied)?;
    Ok(())
}

pub fn authorize_request<R: Authorization>(req: &R) -> Result<(), AuthorizationError> {
    info!("request authorization");
    let auth_token: AuthorizationToken = AUTHORIZATION_TOKEN
        .try_with(|auth_token| auth_token.clone())
        .ok()
        .or_else(|| NODE_TOKEN.get().cloned())
        .ok_or(AuthorizationError::AuthorizationTokenMissing)?;
    info!(token=%auth_token, "auth token");
    authorize(req, &auth_token)
}

pub fn execute_with_authorization<F, O>(
    token: AuthorizationToken,
    f: F,
) -> TaskLocalFuture<TaskLocalInheritableTable, F>
where
    F: Future<Output = O>,
{
    AUTHORIZATION_TOKEN.scope(token, f)
}

#[cfg(test)]
mod tests {
    use super::*;

    // #[test]
    // fn test_auth_token() {
    //     let mut req_metadata = tonic::metadata::MetadataMap::new();
    //     let token =
    //     let auth_token = "test_token".to_string();
    //     set_auth_token(&auth_token, &mut req_metadata);
    //     let auth_token_retrieved = get_auth_token(&req_metadata).unwrap();
    //     assert_eq!(auth_token_retrieved, auth_token);
    // }

    #[test]
    fn test_auth_token_missing() {
        let req_metadata = tonic::metadata::MetadataMap::new();
        let missing_error = extract_auth_token(&req_metadata).unwrap_err();
        assert!(matches!(
            missing_error,
            AuthorizationError::AuthorizationTokenMissing
        ));
    }

    #[test]
    fn test_auth_token_invalid() {
        let mut req_metadata = tonic::metadata::MetadataMap::new();
        req_metadata.insert(
            http::header::AUTHORIZATION.as_str(),
            "some_token".parse().unwrap(),
        );
        let missing_error = extract_auth_token(&req_metadata).unwrap_err();
        assert!(matches!(missing_error, AuthorizationError::InvalidToken));
    }
}
