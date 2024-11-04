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

use std::future::Future;
use std::str::FromStr;
use std::sync::{Arc, OnceLock};

use biscuit_auth::macros::authorizer;
use biscuit_auth::{Authorizer, Biscuit, RootKeyProvider};

use crate::AuthorizationError;

pub struct AuthorizationToken(Biscuit);

impl AuthorizationToken {
    pub fn into_biscuit(self) -> Biscuit {
        self.0
    }
}

impl From<Biscuit> for AuthorizationToken {
    fn from(biscuit: Biscuit) -> Self {
        AuthorizationToken(biscuit)
    }
}

impl std::fmt::Display for AuthorizationToken {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Debug for AuthorizationToken {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "AuthorizationToken({})", &self.0)
    }
}

static ROOT_KEY_PROVIDER: OnceLock<Arc<dyn RootKeyProvider + Sync + Send>> = OnceLock::new();

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
        let biscuit = Biscuit::from_base64(token_base64, root_key_provider)?;
        Ok(AuthorizationToken(biscuit))
    }
}

tokio::task_local! {
    pub static AUTHORIZATION_TOKEN: AuthorizationToken;
}

const AUTHORIZATION_VALUE_PREFIX: &str = "Bearer ";

fn default_operation_authorizer<T: ?Sized>(
    auth_token: &AuthorizationToken,
) -> Result<Authorizer, AuthorizationError> {
    let request_type = std::any::type_name::<T>();
    let operation: &str = request_type.strip_suffix("Request").unwrap();
    let mut authorizer: Authorizer = authorizer!(
        r#"
        operation({operation});

        // We generate the actual user role, by doing an union of the rights granted via roles.
        user_right($operation) <- role($role), right($role, $operation);
        user_right($operation, $resource) <- role($role), right($role, $operation, $resource);
        user_right($operation) <- role("root"), operation($operation);
        user_right($operation, $resource) <- role("root"), operation($operation), resource($resource);

        // Finally we check that we have access to index1 and index2.
        check all operation($operation), right($operation);

        allow if true;
    "#
    );
    authorizer.set_time();
    authorizer.add_token(&auth_token.0)?;
    Ok(authorizer)
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
        default_operation_authorizer::<Self>(auth_token)
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
        default_operation_authorizer::<Self>(&auth_token)
    }
}

impl From<biscuit_auth::error::Token> for AuthorizationError {
    fn from(_token_error: biscuit_auth::error::Token) -> AuthorizationError {
        AuthorizationError::InvalidToken
    }
}

pub fn get_auth_token(
    req_metadata: &tonic::metadata::MetadataMap,
) -> Result<AuthorizationToken, AuthorizationError> {
    let authorization_header_value: &str = req_metadata
        .get(http::header::AUTHORIZATION.as_str())
        .ok_or(AuthorizationError::AuthorizationTokenMissing)?
        .to_str()
        .map_err(|_| AuthorizationError::InvalidToken)?;
    let authorization_token_str: &str = authorization_header_value
        .strip_prefix(AUTHORIZATION_VALUE_PREFIX)
        .ok_or(AuthorizationError::InvalidToken)?;
    let biscuit: Biscuit = Biscuit::from_base64(authorization_token_str, get_root_key_provider())?;
    Ok(AuthorizationToken(biscuit))
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
    authorizer.add_token(&auth_token.0)?;
    authorizer.authorize()?;
    Ok(())
}

pub fn build_tonic_stream_request_with_auth_token<R>(
    req: R,
) -> Result<tonic::Request<R>, AuthorizationError> {
    AUTHORIZATION_TOKEN
        .try_with(|token| {
            let mut request = tonic::Request::new(req);
            set_auth_token(token, request.metadata_mut());
            Ok(request)
        })
        .unwrap_or(Err(AuthorizationError::AuthorizationTokenMissing))
}

pub fn build_tonic_request_with_auth_token<R: Authorization>(
    req: R,
) -> Result<tonic::Request<R>, AuthorizationError> {
    AUTHORIZATION_TOKEN
        .try_with(|token| {
            let mut request = tonic::Request::new(req);
            set_auth_token(token, request.metadata_mut());
            Ok(request)
        })
        .unwrap_or(Err(AuthorizationError::AuthorizationTokenMissing))
}

pub fn authorize_stream<R: StreamAuthorization>(
    auth_token: &AuthorizationToken,
) -> Result<(), AuthorizationError> {
    let mut authorizer = R::authorizer(auth_token)?;
    authorizer.add_token(&auth_token.0)?;
    authorizer.authorize()?;
    Ok(())
}

pub fn authorize_request<R: Authorization>(req: &R) -> Result<(), AuthorizationError> {
    AUTHORIZATION_TOKEN
        .try_with(|auth_token| authorize(req, auth_token))
        .unwrap_or(Err(AuthorizationError::AuthorizationTokenMissing))
}

pub fn execute_with_authorization<F, O>(
    token: AuthorizationToken,
    f: F,
) -> impl Future<Output = O>
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
        let missing_error = get_auth_token(&req_metadata).unwrap_err();
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
        let missing_error = get_auth_token(&req_metadata).unwrap_err();
        assert!(matches!(missing_error, AuthorizationError::InvalidToken));
    }
}
