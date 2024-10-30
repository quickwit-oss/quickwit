use quickwit_auth::Authorization;
use quickwit_auth::AuthorizationError;
use quickwit_auth::AuthorizationToken;

use crate::FetchRequest;
use crate::IngestRequest;
use crate::TailRequest;

impl Authorization for TailRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for IngestRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for FetchRequest {
    fn attenuate(&self, auth_token: AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}
