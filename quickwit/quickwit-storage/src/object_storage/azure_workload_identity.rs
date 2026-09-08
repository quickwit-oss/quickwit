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

//! Workload identity credential that re-reads the federated token file on every exchange.
//!
//! `azure_identity` 0.21's `WorkloadIdentityCredential` reads the file pointed to by
//! `AZURE_FEDERATED_TOKEN_FILE` once, when the credential is built, and reuses that client
//! assertion for the lifetime of the process. Kubernetes rotates the projected service account
//! token roughly hourly, while the access token obtained from Entra ID lives ~24h, so no token
//! exchange is even attempted for a day. By then the assertion on hand is long expired: Entra
//! rejects the exchange with `AADSTS700024`, the SDK classifies that as non-retryable, and the
//! indexing pipeline dies with no self-healing.
//!
//! This credential performs the same token exchange but reads the assertion from disk each time,
//! and refreshes before the access token expires rather than after.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use azure_core::auth::{AccessToken, TokenCredential};
use azure_core::error::{Error as AzureCoreError, ErrorKind, ResultExt};
use azure_core::{HttpClient, Url};
use azure_identity::{TokenCredentialOptions, federated_credentials_flow};
use time::OffsetDateTime;
use tracing::{debug, info, warn};

const AZURE_TENANT_ID_ENV_VAR: &str = "AZURE_TENANT_ID";
const AZURE_CLIENT_ID_ENV_VAR: &str = "AZURE_CLIENT_ID";
const AZURE_FEDERATED_TOKEN_FILE_ENV_VAR: &str = "AZURE_FEDERATED_TOKEN_FILE";
const AZURE_FEDERATED_TOKEN_ENV_VAR: &str = "AZURE_FEDERATED_TOKEN";
const AZURE_CREDENTIAL_KIND_ENV_VAR: &str = "AZURE_CREDENTIAL_KIND";

/// The only two `AZURE_CREDENTIAL_KIND` values that resolve to a workload identity credential
/// upstream, and therefore the only two this credential may stand in for.
const CREDENTIAL_KIND_ENVIRONMENT: &str = "environment";
const CREDENTIAL_KIND_WORKLOAD_IDENTITY: &str = "workloadidentity";

/// Refresh the access token this long before it actually expires, so that a failed refresh is
/// retried while the token in hand is still usable.
const EXPIRATION_MARGIN: Duration = Duration::from_secs(5 * 60);

/// Minimum spacing between token exchange attempts while a usable token is still cached.
///
/// `get_token` is called on every storage request and the Azure backend runs up to
/// `max_concurrent_uploads` requests at a time, so without this every in-flight request would
/// exchange at once when the token enters the refresh margin, and would keep re-exchanging on
/// every request for the whole margin if the token endpoint were failing. That turns a brief
/// outage into a retry storm against a service that throttles.
const REFRESH_RETRY_COOLDOWN: Duration = Duration::from_secs(30);

/// Sentinel for "no exchange has been attempted yet", so the first request never waits.
const NO_EXCHANGE_ATTEMPTED: i64 = i64::MIN;

/// Upper bound on a token lifetime taken from `expires_in`. Entra issues storage tokens with a
/// ~24h lifetime; this only exists so that a malformed response cannot overflow the expiry
/// computation, which would panic.
const MAX_TOKEN_LIFETIME: Duration = Duration::from_secs(48 * 3600);

/// An access token together with the scopes it was minted for.
///
/// A token is only valid for the scopes it was requested with, so the scopes are part of the
/// cache entry: a request for different scopes must trigger a new exchange rather than reuse
/// whatever happens to be cached.
#[derive(Debug)]
struct CachedToken {
    scopes: Vec<String>,
    access_token: AccessToken,
}

impl CachedToken {
    fn matches(&self, scopes: &[&str]) -> bool {
        self.scopes.len() == scopes.len()
            && self
                .scopes
                .iter()
                .zip(scopes)
                .all(|(cached_scope, scope)| cached_scope == scope)
    }
}

/// A [`TokenCredential`] for Azure AD Workload Identity that reads the federated token file on
/// every token exchange rather than caching its contents for the lifetime of the process.
///
/// The cached access token is stored in an [`ArcSwapOption`] rather than behind a lock: refreshes
/// are rare (roughly daily) and idempotent, so two callers racing to refresh is harmless and
/// preferable to holding a lock across the token exchange.
///
/// Only the most recently requested scopes are cached. Quickwit's Azure backend always requests
/// the same storage scope, so this single slot never thrashes in practice.
#[derive(Debug)]
pub(super) struct RefreshingWorkloadIdentityCredential {
    http_client: Arc<dyn HttpClient>,
    authority_host: Url,
    tenant_id: String,
    client_id: String,
    token_file_path: PathBuf,
    /// How long before actual expiry a cached token is treated as expiring.
    expiration_margin: Duration,
    /// Minimum spacing between exchange attempts while a usable token is still cached.
    refresh_retry_cooldown: Duration,
    /// Unix timestamp of the last claimed exchange attempt, or [`NO_EXCHANGE_ATTEMPTED`].
    last_exchange_attempt_unix_secs: AtomicI64,
    cached_token_opt: ArcSwapOption<CachedToken>,
}

impl RefreshingWorkloadIdentityCredential {
    fn new(
        http_client: Arc<dyn HttpClient>,
        authority_host: Url,
        tenant_id: String,
        client_id: String,
        token_file_path: PathBuf,
        expiration_margin: Duration,
        refresh_retry_cooldown: Duration,
    ) -> Self {
        Self {
            http_client,
            authority_host,
            tenant_id,
            client_id,
            token_file_path,
            expiration_margin,
            refresh_retry_cooldown,
            last_exchange_attempt_unix_secs: AtomicI64::new(NO_EXCHANGE_ATTEMPTED),
            cached_token_opt: ArcSwapOption::empty(),
        }
    }

    /// Claims the right to attempt a token exchange.
    ///
    /// Returns `false` when another caller has claimed one within the cooldown, in which case a
    /// caller holding a still-usable token should keep using it rather than piling on. Callers
    /// with nothing usable in hand must exchange regardless and do not consult this.
    fn try_claim_exchange(&self) -> bool {
        let now_unix_secs = OffsetDateTime::now_utc().unix_timestamp();
        let cooldown_secs = self.refresh_retry_cooldown.as_secs() as i64;
        loop {
            let last_attempt_unix_secs =
                self.last_exchange_attempt_unix_secs.load(Ordering::Relaxed);
            // A negative elapsed time means the wall clock stepped backwards, in which case the
            // cooldown is treated as elapsed rather than blocking refreshes until the clock
            // catches up.
            let elapsed_secs = now_unix_secs.saturating_sub(last_attempt_unix_secs);
            if last_attempt_unix_secs != NO_EXCHANGE_ATTEMPTED
                && (0..cooldown_secs).contains(&elapsed_secs)
            {
                return false;
            }
            // Only the caller that wins this swap performs the exchange; the rest fall back to
            // the token they already hold.
            if self
                .last_exchange_attempt_unix_secs
                .compare_exchange_weak(
                    last_attempt_unix_secs,
                    now_unix_secs,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Builds a credential from the standard workload identity environment variables.
    ///
    /// Returns `None` when the process is not configured for workload identity, in which case the
    /// caller should fall back to the default `azure_identity` credential chain.
    pub(super) fn from_env() -> Option<Self> {
        if !handles_env(
            non_empty_env_var(AZURE_CREDENTIAL_KIND_ENV_VAR).as_deref(),
            non_empty_env_var(AZURE_FEDERATED_TOKEN_ENV_VAR).as_deref(),
        ) {
            return None;
        }
        let tenant_id = non_empty_env_var(AZURE_TENANT_ID_ENV_VAR)?;
        let client_id = non_empty_env_var(AZURE_CLIENT_ID_ENV_VAR)?;
        let token_file_path = non_empty_env_var(AZURE_FEDERATED_TOKEN_FILE_ENV_VAR)?;
        let options = TokenCredentialOptions::default();
        let authority_host = options.authority_host().ok()?;

        info!(
            token_file_path = %token_file_path,
            "using azure workload identity credential with per-exchange token file reads"
        );
        Some(Self::new(
            options.http_client(),
            authority_host,
            tenant_id,
            client_id,
            PathBuf::from(token_file_path),
            EXPIRATION_MARGIN,
            REFRESH_RETRY_COOLDOWN,
        ))
    }

    /// Reads the client assertion from disk and exchanges it for an access token.
    async fn exchange_token(&self, scopes: &[&str]) -> azure_core::Result<AccessToken> {
        let assertion = read_token_file(&self.token_file_path)?;
        let login_response = federated_credentials_flow::perform(
            self.http_client.clone(),
            &self.client_id,
            assertion.trim(),
            scopes,
            &self.tenant_id,
            &self.authority_host,
        )
        .await
        .context(ErrorKind::Credential, "federated token exchange failed")?;

        // `OffsetDateTime + Duration` panics on overflow, so a malformed `expires_in` must not
        // reach it. Clamping rather than saturating also means a nonsensical lifetime results in
        // an earlier refresh instead of a token that is never refreshed at all.
        let expires_in = Duration::from_secs(login_response.expires_in).min(MAX_TOKEN_LIFETIME);
        let expires_on = OffsetDateTime::now_utc() + expires_in;
        debug!(%expires_on, "obtained azure access token from federated credentials flow");
        Ok(AccessToken::new(
            login_response.access_token().clone(),
            expires_on,
        ))
    }
}

#[async_trait]
impl TokenCredential for RefreshingWorkloadIdentityCredential {
    async fn get_token(&self, scopes: &[&str]) -> azure_core::Result<AccessToken> {
        let cached_token_opt = match self.cached_token_opt.load_full() {
            Some(cached_token) if cached_token.matches(scopes) => Some(cached_token),
            _ => None,
        };
        if let Some(cached_token) = &cached_token_opt
            && !is_expiring(&cached_token.access_token, self.expiration_margin)
        {
            return Ok(cached_token.access_token.clone());
        }
        // Nothing cached for these scopes, or the cached token is inside the refresh margin. A
        // token that has not actually expired yet remains a valid fallback.
        let usable_cached_token_opt = match cached_token_opt {
            Some(cached_token) if !is_expiring(&cached_token.access_token, Duration::ZERO) => {
                Some(cached_token)
            }
            _ => None,
        };
        if let Some(cached_token) = &usable_cached_token_opt
            && !self.try_claim_exchange()
        {
            return Ok(cached_token.access_token.clone());
        }
        let error = match self.exchange_token(scopes).await {
            Ok(access_token) => {
                self.cached_token_opt.store(Some(Arc::new(CachedToken {
                    scopes: scopes.iter().map(|scope| scope.to_string()).collect(),
                    access_token: access_token.clone(),
                })));
                return Ok(access_token);
            }
            Err(error) => error,
        };
        // The refresh margin exists so that a failed refresh can be retried while the token in
        // hand is still usable. Keep serving the cached token until it genuinely expires rather
        // than failing storage operations for a transient hiccup near expiry.
        if let Some(cached_token) = &usable_cached_token_opt {
            warn!(
                %error,
                "azure token refresh failed, reusing the cached token until it expires"
            );
            return Ok(cached_token.access_token.clone());
        }
        Err(error)
    }

    async fn clear_cache(&self) -> azure_core::Result<()> {
        self.cached_token_opt.store(None);
        Ok(())
    }
}

/// Whether this credential should stand in for the stock `azure_identity` chain, given the
/// relevant environment.
///
/// Two cases are deliberately left to the stock chain even though a projected token file is
/// present, because in both of them the stock chain would not have built a file-backed workload
/// identity credential either:
///
/// - an explicit `AZURE_CREDENTIAL_KIND` selecting some other provider. The workload identity
///   environment variables are injected automatically by the workload identity webhook, so they are
///   routinely present even when an operator has deliberately configured a different credential.
/// - an inline `AZURE_FEDERATED_TOKEN`, which upstream prefers over the file. Honouring the file
///   instead could authenticate as a different subject, and an inline assertion has nothing to
///   re-read anyway.
fn handles_env(
    credential_kind_opt: Option<&str>,
    inline_federated_token_opt: Option<&str>,
) -> bool {
    if let Some(credential_kind) = credential_kind_opt {
        // Mirrors the normalization `SpecificAzureCredential::create` applies.
        let credential_kind = credential_kind.replace(' ', "").to_lowercase();
        if credential_kind != CREDENTIAL_KIND_ENVIRONMENT
            && credential_kind != CREDENTIAL_KIND_WORKLOAD_IDENTITY
        {
            return false;
        }
    }
    inline_federated_token_opt.is_none()
}

fn is_expiring(access_token: &AccessToken, expiration_margin: Duration) -> bool {
    access_token.expires_on - expiration_margin <= OffsetDateTime::now_utc()
}

fn non_empty_env_var(key: &str) -> Option<String> {
    let value = std::env::var(key).ok()?;
    if value.trim().is_empty() {
        return None;
    }
    Some(value)
}

fn read_token_file(token_file_path: &Path) -> azure_core::Result<String> {
    std::fs::read_to_string(token_file_path).map_err(|error| {
        AzureCoreError::full(
            ErrorKind::Credential,
            error,
            format!(
                "failed to read azure federated token file `{}`",
                token_file_path.display()
            ),
        )
    })
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Mutex;

    use azure_core::auth::Secret;
    use azure_core::headers::Headers;
    use azure_core::{Request as AzureRequest, Response as AzureResponse, StatusCode};
    use azure_identity::WorkloadIdentityCredential;
    use bytes::Bytes;
    use tempfile::NamedTempFile;

    use super::*;

    /// Verbatim shape of what Entra returns when a federated client assertion has expired. This
    /// is the response that kills the indexer at T+24h: `azure_core` classifies it as
    /// non-retryable, so the pipeline dies rather than backing off and retrying.
    const AADSTS700024_BODY: &str = r#"{"error":"invalid_client","error_description":"AADSTS700024: Client assertion is not within its valid time range. Current time: 2026-08-13T00:00:00Z, assertion valid from 2026-08-11T23:00:00Z, expiry time of assertion 2026-08-12T00:00:00Z.","error_codes":[700024]}"#;

    /// A stand-in for the Entra token endpoint.
    ///
    /// It accepts exactly one client assertion — the one the projected service account token file
    /// currently holds — and rejects anything else with `AADSTS700024`, which is precisely how the
    /// real endpoint treats a stale assertion. Every assertion presented is recorded, so a test
    /// can assert on what the credential actually sent rather than only on the outcome.
    #[derive(Debug)]
    struct FakeEntra {
        accepted_assertion: Mutex<String>,
        presented_assertions: Mutex<Vec<String>>,
        expires_in_secs: u64,
    }

    impl FakeEntra {
        fn new(accepted_assertion: &str, expires_in_secs: u64) -> Arc<Self> {
            Arc::new(Self {
                accepted_assertion: Mutex::new(accepted_assertion.to_string()),
                presented_assertions: Mutex::new(Vec::new()),
                expires_in_secs,
            })
        }

        /// Simulates the previous assertion expiring: only `assertion` is accepted from now on.
        fn rotate_accepted_assertion(&self, assertion: &str) {
            *self.accepted_assertion.lock().unwrap() = assertion.to_string();
        }

        fn presented_assertions(&self) -> Vec<String> {
            self.presented_assertions.lock().unwrap().clone()
        }

        fn exchange_count(&self) -> usize {
            self.presented_assertions.lock().unwrap().len()
        }
    }

    fn azure_response(status: StatusCode, body: &str) -> AzureResponse {
        let bytes = Bytes::from(body.to_string());
        AzureResponse::new(
            status,
            Headers::new(),
            Box::pin(futures::stream::iter(std::iter::once(Ok(bytes)))),
        )
    }

    /// Extracts a field from an `application/x-www-form-urlencoded` body. Test assertions are
    /// restricted to characters that survive form encoding unchanged, so no decoding is needed.
    fn form_field(body: &str, key: &str) -> Option<String> {
        for pair in body.split('&') {
            let (pair_key, pair_value) = pair.split_once('=')?;
            if pair_key == key {
                return Some(pair_value.to_string());
            }
        }
        None
    }

    #[async_trait]
    impl HttpClient for FakeEntra {
        async fn execute_request(
            &self,
            request: &AzureRequest,
        ) -> azure_core::Result<AzureResponse> {
            let azure_core::Body::Bytes(body_bytes) = request.body() else {
                panic!("expected a bytes body on the token request");
            };
            let body = std::str::from_utf8(body_bytes).unwrap();
            let assertion = form_field(body, "client_assertion")
                .expect("token request must carry a client_assertion");
            self.presented_assertions
                .lock()
                .unwrap()
                .push(assertion.clone());

            if assertion != *self.accepted_assertion.lock().unwrap() {
                return Ok(azure_response(StatusCode::Unauthorized, AADSTS700024_BODY));
            }
            let body = format!(
                r#"{{"token_type":"Bearer","expires_in":{},"ext_expires_in":{},"access_token":"access-token-for-{}"}}"#,
                self.expires_in_secs, self.expires_in_secs, assertion
            );
            Ok(azure_response(StatusCode::Ok, &body))
        }
    }

    fn token_file_containing(assertion: &str) -> NamedTempFile {
        let mut token_file = NamedTempFile::new().unwrap();
        token_file.write_all(assertion.as_bytes()).unwrap();
        token_file.flush().unwrap();
        token_file
    }

    /// Simulates kubelet rotating the projected service account token in place.
    fn rotate_token_file(token_file: &NamedTempFile, assertion: &str) {
        std::fs::write(token_file.path(), assertion.as_bytes()).unwrap();
    }

    fn authority_host() -> Url {
        Url::parse("https://login.microsoftonline.com").unwrap()
    }

    const STORAGE_SCOPE: &str = "https://storage.azure.com/.default";

    fn refreshing_credential(
        fake_entra: Arc<FakeEntra>,
        token_file: &NamedTempFile,
        expiration_margin: Duration,
    ) -> RefreshingWorkloadIdentityCredential {
        // No cooldown by default, so tests exercise the exchange path on every call unless they
        // are specifically testing the cooldown.
        refreshing_credential_with_cooldown(
            fake_entra,
            token_file,
            expiration_margin,
            Duration::ZERO,
        )
    }

    fn refreshing_credential_with_cooldown(
        fake_entra: Arc<FakeEntra>,
        token_file: &NamedTempFile,
        expiration_margin: Duration,
        refresh_retry_cooldown: Duration,
    ) -> RefreshingWorkloadIdentityCredential {
        RefreshingWorkloadIdentityCredential::new(
            fake_entra,
            authority_host(),
            "test-tenant-id".to_string(),
            "test-client-id".to_string(),
            token_file.path().to_path_buf(),
            expiration_margin,
            refresh_retry_cooldown,
        )
    }

    fn access_token_expiring_in(duration: Duration) -> AccessToken {
        AccessToken::new(
            Secret::new("token".to_string()),
            OffsetDateTime::now_utc() + duration,
        )
    }

    #[test]
    fn test_is_expiring_within_margin() {
        assert!(is_expiring(
            &access_token_expiring_in(Duration::from_secs(60)),
            EXPIRATION_MARGIN
        ));
        assert!(is_expiring(
            &access_token_expiring_in(EXPIRATION_MARGIN),
            EXPIRATION_MARGIN
        ));
    }

    #[test]
    fn test_is_not_expiring_outside_margin() {
        assert!(!is_expiring(
            &access_token_expiring_in(EXPIRATION_MARGIN + Duration::from_secs(60)),
            EXPIRATION_MARGIN
        ));
        assert!(!is_expiring(
            &access_token_expiring_in(Duration::from_secs(24 * 3600)),
            EXPIRATION_MARGIN
        ));
    }

    #[test]
    fn test_read_token_file_picks_up_rotations() {
        let token_file = token_file_containing("first-assertion");
        assert_eq!(
            read_token_file(token_file.path()).unwrap(),
            "first-assertion"
        );
        rotate_token_file(&token_file, "second-assertion");
        assert_eq!(
            read_token_file(token_file.path()).unwrap(),
            "second-assertion"
        );
    }

    /// Sanity check that the fake endpoint has teeth: if it accepted anything, the two scenario
    /// tests below would pass even with a credential that never re-reads the file.
    #[tokio::test]
    async fn test_fake_entra_rejects_a_stale_assertion() {
        let fake_entra = FakeEntra::new("assertion-hour-24", 86_400);
        let token_file = token_file_containing("assertion-hour-0");
        let credential =
            refreshing_credential(fake_entra.clone(), &token_file, Duration::from_secs(5 * 60));

        let error = credential.get_token(&[STORAGE_SCOPE]).await.unwrap_err();
        assert!(matches!(error.kind(), ErrorKind::Credential));
        assert_eq!(fake_entra.presented_assertions(), vec!["assertion-hour-0"]);
    }

    /// Reproduces AE-16269 against the real upstream credential.
    ///
    /// `WorkloadIdentityCredential` is built while the token file holds the hour-0 assertion.
    /// Kubelet then rotates the file, and Entra stops honouring the old assertion — exactly the
    /// state of the world at T+24h. The credential still presents the assertion it captured at
    /// construction and is rejected with `AADSTS700024`, with no path to recovery short of a
    /// process restart.
    #[tokio::test]
    async fn test_upstream_credential_ignores_the_rotated_token_file() {
        let fake_entra = FakeEntra::new("assertion-hour-0", 86_400);
        let token_file = token_file_containing("assertion-hour-0");

        // `WorkloadIdentityCredential::create` reads the file exactly here, and never again.
        let assertion_at_startup = std::fs::read_to_string(token_file.path()).unwrap();
        let upstream_credential = WorkloadIdentityCredential::new(
            fake_entra.clone(),
            authority_host(),
            "test-tenant-id".to_string(),
            "test-client-id".to_string(),
            assertion_at_startup,
        );
        upstream_credential
            .get_token(&[STORAGE_SCOPE])
            .await
            .expect("the first exchange succeeds while the startup assertion is still valid");

        // 24h later: kubelet has rotated the file many times over and Entra has aged out the
        // assertion the credential is holding.
        rotate_token_file(&token_file, "assertion-hour-24");
        fake_entra.rotate_accepted_assertion("assertion-hour-24");
        upstream_credential.clear_cache().await.unwrap();

        let error = upstream_credential
            .get_token(&[STORAGE_SCOPE])
            .await
            .unwrap_err();
        assert!(matches!(error.kind(), ErrorKind::Credential));
        // Note that `AADSTS700024` does not survive into the error: `azure_core` keeps the status
        // and drops the response body, which is a large part of why this failure was hard to
        // diagnose from indexer logs in the first place.
        assert!(
            format!("{error:?}").contains("Unauthorized"),
            "expected the stale assertion to be rejected, got: {error:?}"
        );
        assert_eq!(
            fake_entra.presented_assertions(),
            vec!["assertion-hour-0", "assertion-hour-0"],
            "upstream re-presents the assertion it captured at startup, never re-reading the file"
        );
    }

    /// The same scenario, with the fix: the rotated assertion is picked up and the exchange
    /// succeeds, so the indexer survives past T+24h without a restart.
    #[tokio::test]
    async fn test_refreshing_credential_survives_token_rotation() {
        let fake_entra = FakeEntra::new("assertion-hour-0", 86_400);
        let token_file = token_file_containing("assertion-hour-0");
        // A margin wider than the token lifetime makes every cached token count as expiring, which
        // is how we reach the refresh path deterministically instead of waiting 24h.
        let credential = refreshing_credential(
            fake_entra.clone(),
            &token_file,
            Duration::from_secs(48 * 3600),
        );

        credential
            .get_token(&[STORAGE_SCOPE])
            .await
            .expect("the first exchange succeeds");

        rotate_token_file(&token_file, "assertion-hour-24");
        fake_entra.rotate_accepted_assertion("assertion-hour-24");

        let access_token = credential
            .get_token(&[STORAGE_SCOPE])
            .await
            .expect("the refresh picks up the rotated assertion and succeeds");
        assert_eq!(
            access_token.token.secret(),
            "access-token-for-assertion-hour-24"
        );
        assert_eq!(
            fake_entra.presented_assertions(),
            vec!["assertion-hour-0", "assertion-hour-24"],
            "the credential re-reads the token file on every exchange"
        );
    }

    /// The fix must not turn one exchange per day into one per request.
    #[tokio::test]
    async fn test_token_is_cached_until_it_approaches_expiry() {
        let fake_entra = FakeEntra::new("assertion-hour-0", 86_400);
        let token_file = token_file_containing("assertion-hour-0");
        let credential =
            refreshing_credential(fake_entra.clone(), &token_file, Duration::from_secs(5 * 60));

        for _ in 0..10 {
            credential.get_token(&[STORAGE_SCOPE]).await.unwrap();
        }
        assert_eq!(fake_entra.exchange_count(), 1);

        // A different scope must not reuse the cached token.
        credential
            .get_token(&["https://storage.azure.us/.default"])
            .await
            .unwrap();
        assert_eq!(fake_entra.exchange_count(), 2);
    }

    #[test]
    fn test_handles_env_defers_to_an_explicit_credential_kind() {
        // The workload identity variables are injected automatically by the webhook, so they are
        // present
        // even when an operator has explicitly selected a different provider.
        assert!(!handles_env(Some("azurecli"), None));
        assert!(!handles_env(Some("virtualmachine"), None));
        assert!(!handles_env(Some("appservice"), None));
        assert!(!handles_env(Some("clientsecret"), None));

        // These two do resolve to a workload identity credential upstream.
        assert!(handles_env(Some("environment"), None));
        assert!(handles_env(Some("workloadidentity"), None));
        // Upstream normalizes case and spaces before matching, so we must too.
        assert!(handles_env(Some(" Workload Identity "), None));
        assert!(!handles_env(Some(" Azure CLI "), None));

        assert!(handles_env(None, None));
    }

    #[test]
    fn test_handles_env_defers_to_an_inline_assertion() {
        // Upstream prefers `AZURE_FEDERATED_TOKEN` over the file, and an inline assertion has
        // nothing to re-read.
        assert!(!handles_env(None, Some("inline-assertion")));
        assert!(!handles_env(
            Some("workloadidentity"),
            Some("inline-assertion")
        ));
    }

    /// A transient failure near expiry must not fail storage operations while the cached token is
    /// still valid — that is the entire point of refreshing ahead of expiry.
    #[tokio::test]
    async fn test_refresh_failure_falls_back_to_the_still_valid_cached_token() {
        let fake_entra = FakeEntra::new("assertion-hour-0", 3600);
        let token_file = token_file_containing("assertion-hour-0");
        // Wider than the token lifetime, so the token is always inside the refresh margin while
        // remaining a long way from actually expiring.
        let credential = refreshing_credential(
            fake_entra.clone(),
            &token_file,
            Duration::from_secs(48 * 3600),
        );

        let first_token = credential.get_token(&[STORAGE_SCOPE]).await.unwrap();
        assert_eq!(fake_entra.exchange_count(), 1);

        // Entra starts rejecting: a hiccup, a revoked assertion, anything transient.
        fake_entra.rotate_accepted_assertion("some-other-assertion");

        let second_token = credential
            .get_token(&[STORAGE_SCOPE])
            .await
            .expect("the still-valid cached token is served rather than failing the request");
        assert_eq!(second_token.token.secret(), first_token.token.secret());
        assert_eq!(fake_entra.exchange_count(), 2, "the refresh was attempted");
    }

    /// Once the cached token has genuinely expired there is nothing to fall back on, so the error
    /// must surface rather than handing out an unusable token.
    #[tokio::test]
    async fn test_refresh_failure_surfaces_once_the_cached_token_expires() {
        // `expires_in: 0` means the token is already expired when it is cached.
        let fake_entra = FakeEntra::new("assertion-hour-0", 0);
        let token_file = token_file_containing("assertion-hour-0");
        let credential =
            refreshing_credential(fake_entra.clone(), &token_file, Duration::from_secs(5 * 60));

        credential.get_token(&[STORAGE_SCOPE]).await.unwrap();
        fake_entra.rotate_accepted_assertion("some-other-assertion");

        let error = credential.get_token(&[STORAGE_SCOPE]).await.unwrap_err();
        assert!(matches!(error.kind(), ErrorKind::Credential));
    }

    /// `get_token` runs on every storage request, so a failing token endpoint must not be retried
    /// once per request for the whole refresh margin.
    #[tokio::test]
    async fn test_failing_refresh_is_not_retried_on_every_request() {
        let fake_entra = FakeEntra::new("assertion-hour-0", 3600);
        let token_file = token_file_containing("assertion-hour-0");
        let credential = refreshing_credential_with_cooldown(
            fake_entra.clone(),
            &token_file,
            // Always inside the refresh margin, but a long way from actually expiring.
            Duration::from_secs(48 * 3600),
            Duration::from_secs(30),
        );

        credential.get_token(&[STORAGE_SCOPE]).await.unwrap();
        assert_eq!(fake_entra.exchange_count(), 1);

        // The token endpoint starts failing.
        fake_entra.rotate_accepted_assertion("some-other-assertion");
        for _ in 0..50 {
            credential
                .get_token(&[STORAGE_SCOPE])
                .await
                .expect("the cached token is still valid, so requests keep succeeding");
        }
        assert_eq!(
            fake_entra.exchange_count(),
            2,
            "only one retry should have been attempted within the cooldown"
        );
    }

    /// The same burst without a cooldown hammers the endpoint, which is what the cooldown exists
    /// to prevent.
    #[tokio::test]
    async fn test_without_a_cooldown_every_request_retries() {
        let fake_entra = FakeEntra::new("assertion-hour-0", 3600);
        let token_file = token_file_containing("assertion-hour-0");
        let credential = refreshing_credential_with_cooldown(
            fake_entra.clone(),
            &token_file,
            Duration::from_secs(48 * 3600),
            Duration::ZERO,
        );

        credential.get_token(&[STORAGE_SCOPE]).await.unwrap();
        fake_entra.rotate_accepted_assertion("some-other-assertion");
        for _ in 0..50 {
            credential.get_token(&[STORAGE_SCOPE]).await.unwrap();
        }
        assert_eq!(fake_entra.exchange_count(), 51);
    }

    /// A caller with nothing usable in hand must never be held back by the cooldown, otherwise a
    /// cold start behind a brief outage would be stuck serving errors it could have recovered
    /// from.
    #[tokio::test]
    async fn test_cooldown_never_blocks_a_caller_without_a_usable_token() {
        // `expires_in: 0` means every issued token is already expired, so there is never a usable
        // fallback and every call must be free to exchange.
        let fake_entra = FakeEntra::new("assertion-hour-0", 0);
        let token_file = token_file_containing("assertion-hour-0");
        let credential = refreshing_credential_with_cooldown(
            fake_entra.clone(),
            &token_file,
            Duration::from_secs(5 * 60),
            Duration::from_secs(3600),
        );

        for _ in 0..5 {
            credential.get_token(&[STORAGE_SCOPE]).await.unwrap();
        }
        assert_eq!(fake_entra.exchange_count(), 5);
    }

    /// Concurrent requests entering the refresh margin together must not all exchange at once.
    #[tokio::test]
    async fn test_concurrent_requests_do_not_stampede_the_token_endpoint() {
        let fake_entra = FakeEntra::new("assertion-hour-0", 3600);
        let token_file = token_file_containing("assertion-hour-0");
        let credential = Arc::new(refreshing_credential_with_cooldown(
            fake_entra.clone(),
            &token_file,
            Duration::from_secs(48 * 3600),
            Duration::from_secs(30),
        ));

        credential.get_token(&[STORAGE_SCOPE]).await.unwrap();
        assert_eq!(fake_entra.exchange_count(), 1);

        // Mirrors `max_concurrent_uploads` worth of in-flight storage requests.
        let mut handles = Vec::with_capacity(100);
        for _ in 0..100 {
            let credential = credential.clone();
            handles.push(tokio::spawn(async move {
                credential.get_token(&[STORAGE_SCOPE]).await.unwrap()
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }
        assert_eq!(
            fake_entra.exchange_count(),
            2,
            "a single caller should win the refresh, the rest reuse the cached token"
        );
    }

    /// A malformed `expires_in` must not overflow the expiry computation, which would panic and
    /// take the indexer down.
    #[tokio::test]
    async fn test_absurd_expires_in_is_clamped_rather_than_overflowing() {
        let fake_entra = FakeEntra::new("assertion-hour-0", u64::MAX);
        let token_file = token_file_containing("assertion-hour-0");
        let credential =
            refreshing_credential(fake_entra.clone(), &token_file, Duration::from_secs(5 * 60));

        let access_token = credential.get_token(&[STORAGE_SCOPE]).await.unwrap();
        assert!(
            access_token.expires_on <= OffsetDateTime::now_utc() + MAX_TOKEN_LIFETIME,
            "the token lifetime should be clamped to a sane bound"
        );
    }

    /// A failed exchange must leave the credential able to recover on the next call, rather than
    /// wedging the way the upstream one does.
    #[tokio::test]
    async fn test_failed_exchange_recovers_on_next_call() {
        let fake_entra = FakeEntra::new("assertion-hour-24", 86_400);
        let token_file = token_file_containing("assertion-hour-0");
        let credential =
            refreshing_credential(fake_entra.clone(), &token_file, Duration::from_secs(5 * 60));

        credential
            .get_token(&[STORAGE_SCOPE])
            .await
            .expect_err("the stale assertion is rejected");

        // Kubelet rotates the file; no restart, no intervention.
        rotate_token_file(&token_file, "assertion-hour-24");
        credential
            .get_token(&[STORAGE_SCOPE])
            .await
            .expect("the credential self-heals once the file is rotated");
    }

    #[test]
    fn test_cached_token_matches_scopes() {
        let cached_token = CachedToken {
            scopes: vec!["https://storage.azure.com/.default".to_string()],
            access_token: access_token_expiring_in(Duration::from_secs(3600)),
        };
        assert!(cached_token.matches(&["https://storage.azure.com/.default"]));
        // A token minted for the public cloud must not be reused for a sovereign cloud scope.
        assert!(!cached_token.matches(&["https://storage.azure.us/.default"]));
        assert!(!cached_token.matches(&[]));
        assert!(!cached_token.matches(&[
            "https://storage.azure.com/.default",
            "https://storage.azure.us/.default",
        ]));
    }

    #[test]
    fn test_read_token_file_missing() {
        let error = read_token_file(Path::new("/does/not/exist")).unwrap_err();
        assert!(matches!(error.kind(), ErrorKind::Credential));
    }
}
