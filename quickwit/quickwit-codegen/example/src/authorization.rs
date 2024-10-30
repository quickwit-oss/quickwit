use quickwit_auth::Authorization;
use quickwit_auth::AuthorizationError;
use quickwit_auth::AuthorizationToken;
use quickwit_auth::StreamAuthorization;

use crate::GoodbyeRequest;
use crate::HelloRequest;
use crate::PingRequest;

impl Authorization for HelloRequest {
    fn attenuate(&self, auth_token: quickwit_auth::AuthorizationToken) -> Result<quickwit_auth::AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for GoodbyeRequest {
    fn attenuate(&self, auth_token: quickwit_auth::AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl StreamAuthorization for PingRequest {
    fn attenuate(auth_token: quickwit_auth::AuthorizationToken) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}
