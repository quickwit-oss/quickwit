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

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use biscuit_auth::{Biscuit, KeyPair, PrivateKey, PublicKey};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::AuthorizationError;

const AUTHORIZATION_VALUE_PREFIX: &str = "Bearer ";

/// A biscuit token verified against the root public key.
///
/// The authority block of the token holds the identity facts:
/// - `user("<name>")` and `role("<role>")` for user tokens;
/// - `service("<service>")` for node tokens.
#[derive(Clone)]
pub struct AuthorizationToken(Arc<Biscuit>);

impl AuthorizationToken {
    /// Parses and verifies the signature of a base64-encoded token.
    pub fn parse(
        token_base64: &str,
        root_public_key: PublicKey,
    ) -> Result<AuthorizationToken, AuthorizationError> {
        let biscuit = Biscuit::from_base64(token_base64, root_public_key)
            .map_err(|_| AuthorizationError::InvalidToken)?;
        Ok(AuthorizationToken(Arc::new(biscuit)))
    }

    /// Parses an `Authorization` header value of the form `Bearer <token>`.
    pub fn parse_header_value(
        authorization_header_value: &str,
        root_public_key: PublicKey,
    ) -> Result<AuthorizationToken, AuthorizationError> {
        let token_base64 = authorization_header_value
            .strip_prefix(AUTHORIZATION_VALUE_PREFIX)
            .ok_or(AuthorizationError::InvalidToken)?;
        Self::parse(token_base64.trim(), root_public_key)
    }

    pub(crate) fn biscuit(&self) -> &Biscuit {
        &self.0
    }

    pub(crate) fn to_header_value(&self) -> Result<String, AuthorizationError> {
        let token_base64 = self
            .0
            .to_base64()
            .map_err(|_| AuthorizationError::InvalidToken)?;
        Ok(format!("{AUTHORIZATION_VALUE_PREFIX}{token_base64}"))
    }
}

impl fmt::Display for AuthorizationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        let token_base64 = self.0.to_base64().map_err(|_| fmt::Error)?;
        token_base64.fmt(formatter)
    }
}

impl fmt::Debug for AuthorizationToken {
    // Never print the token itself: it is a bearer credential.
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "AuthorizationToken(<redacted>)")
    }
}

/// Describes the identity facts of a token to generate.
#[derive(Debug, Default)]
pub struct UserTokenSpec {
    /// Name of the user, `None` for node tokens.
    pub user_opt: Option<String>,
    pub roles: Vec<String>,
    /// Quickwit services (`searcher`, `indexer`, ...). Non-empty for node tokens.
    pub services: Vec<String>,
    /// Validity of the token. `None` means the token never expires.
    pub ttl_opt: Option<Duration>,
}

/// Returns `(private_key_hex, public_key_hex)` of a new random root key pair.
pub fn generate_root_key_pair() -> (String, String) {
    let key_pair = KeyPair::new();
    (
        key_pair.private().to_bytes_hex(),
        key_pair.public().to_bytes_hex(),
    )
}

/// Generates a token signed by the root private key.
pub fn generate_token(root_private_key_hex: &str, spec: &UserTokenSpec) -> anyhow::Result<String> {
    let private_key =
        PrivateKey::from_bytes_hex(root_private_key_hex).context("invalid root private key")?;
    let key_pair = KeyPair::from(&private_key);

    let mut datalog = String::new();
    if let Some(user) = &spec.user_opt {
        datalog.push_str(&format!("user(\"{}\");\n", validate_datalog_string(user)?));
    }
    for role in &spec.roles {
        datalog.push_str(&format!("role(\"{}\");\n", validate_datalog_string(role)?));
    }
    for service in &spec.services {
        datalog.push_str(&format!(
            "service(\"{}\");\n",
            validate_datalog_string(service)?
        ));
    }
    if let Some(ttl) = spec.ttl_opt {
        let expiration = (OffsetDateTime::now_utc() + ttl)
            .format(&Rfc3339)
            .context("failed to format token expiration")?;
        datalog.push_str(&format!("check if time($time), $time <= {expiration};\n"));
    }
    let mut biscuit_builder = Biscuit::builder();
    biscuit_builder
        .add_code(&datalog)
        .context("failed to build token authority block")?;
    let biscuit = biscuit_builder
        .build(&key_pair)
        .context("failed to sign token")?;
    let token_base64 = biscuit.to_base64().context("failed to serialize token")?;
    Ok(token_base64)
}

/// Names injected into datalog code must not be able to escape the string literal.
pub(crate) fn validate_datalog_string(value: &str) -> anyhow::Result<&str> {
    let is_valid = !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | ':' | '@'));
    if !is_valid {
        bail!("invalid name `{value}`: only ASCII alphanumeric characters and `_-.:@` are allowed");
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_and_parse_token() {
        let (private_key_hex, public_key_hex) = generate_root_key_pair();
        let public_key = PublicKey::from_bytes_hex(&public_key_hex).unwrap();
        let spec = UserTokenSpec {
            user_opt: Some("alice".to_string()),
            roles: vec!["reader".to_string()],
            ..Default::default()
        };
        let token_base64 = generate_token(&private_key_hex, &spec).unwrap();
        let token = AuthorizationToken::parse(&token_base64, public_key).unwrap();
        let header_value = token.to_header_value().unwrap();
        AuthorizationToken::parse_header_value(&header_value, public_key).unwrap();
    }

    #[test]
    fn test_parse_token_signed_by_other_key() {
        let (private_key_hex, _) = generate_root_key_pair();
        let (_, other_public_key_hex) = generate_root_key_pair();
        let other_public_key = PublicKey::from_bytes_hex(&other_public_key_hex).unwrap();
        let token_base64 = generate_token(&private_key_hex, &UserTokenSpec::default()).unwrap();
        let error = AuthorizationToken::parse(&token_base64, other_public_key).unwrap_err();
        assert_eq!(error, AuthorizationError::InvalidToken);
    }

    #[test]
    fn test_parse_header_value_without_bearer_prefix() {
        let (_, public_key_hex) = generate_root_key_pair();
        let public_key = PublicKey::from_bytes_hex(&public_key_hex).unwrap();
        let error = AuthorizationToken::parse_header_value("some_token", public_key).unwrap_err();
        assert_eq!(error, AuthorizationError::InvalidToken);
    }

    #[test]
    fn test_validate_datalog_string() {
        validate_datalog_string("index:read").unwrap();
        validate_datalog_string("alice@example.com").unwrap();
        validate_datalog_string("").unwrap_err();
        validate_datalog_string("a\"); role(\"admin").unwrap_err();
    }
}
