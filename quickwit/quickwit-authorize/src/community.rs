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

use std::future::Future;

use crate::AuthorizationError;

pub type AuthorizationToken = ();

pub trait Authorization {
    fn attenuate(
        &self,
        _auth_token: AuthorizationToken,
    ) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(())
    }
}

impl<T> Authorization for T {}

pub trait StreamAuthorization {
    fn attenuate(
        _auth_token: AuthorizationToken,
    ) -> std::result::Result<AuthorizationToken, AuthorizationError> {
        Ok(())
    }
}

impl<T> StreamAuthorization for T {}

pub fn get_auth_token(
    _req_metadata: &tonic::metadata::MetadataMap,
) -> Result<AuthorizationToken, AuthorizationError> {
    Ok(())
}

pub fn set_auth_token(
    _auth_token: &AuthorizationToken,
    _req_metadata: &mut tonic::metadata::MetadataMap,
) {
}

pub fn authorize<R: Authorization>(
    _req: &R,
    _auth_token: &AuthorizationToken,
) -> Result<(), AuthorizationError> {
    Ok(())
}

pub fn build_tonic_stream_request_with_auth_token<R>(
    req: R,
) -> Result<tonic::Request<R>, AuthorizationError> {
    Ok(tonic::Request::new(req))
}

pub fn build_tonic_request_with_auth_token<R: Authorization>(
    req: R,
) -> Result<tonic::Request<R>, AuthorizationError> {
    Ok(tonic::Request::new(req))
}

pub fn authorize_stream<R: StreamAuthorization>(
    _auth_token: &AuthorizationToken,
) -> Result<(), AuthorizationError> {
    Ok(())
}

pub fn execute_with_authorization<F, O>(_: AuthorizationToken, f: F) -> impl Future<Output = O>
where F: Future<Output = O> {
    f
}

pub fn authorize_request<R: Authorization>(_req: &R) -> Result<(), AuthorizationError> {
    Ok(())
}
