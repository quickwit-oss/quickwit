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

use azure_core::auth::{AccessToken, TokenCredential};

/// Wraps a [`TokenCredential`] and requests tokens for a fixed OAuth scope.
///
/// The legacy `azure_storage` 0.21 SDK hardcodes the public-cloud storage scope when using token
/// credentials. Sovereign clouds require a different audience, so this wrapper ignores the scope
/// requested by the SDK and uses the configured national-cloud scope instead.
#[derive(Clone)]
pub struct ScopedTokenCredential {
    inner: Arc<dyn TokenCredential>,
    scope: &'static str,
}

impl ScopedTokenCredential {
    pub fn new(inner: Arc<dyn TokenCredential>, scope: &'static str) -> Self {
        Self { inner, scope }
    }
}

impl fmt::Debug for ScopedTokenCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScopedTokenCredential")
            .field("inner", &self.inner)
            .field("scope", &self.scope)
            .finish()
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl TokenCredential for ScopedTokenCredential {
    async fn get_token(&self, _scopes: &[&str]) -> azure_core::Result<AccessToken> {
        self.inner.get_token(&[self.scope]).await
    }

    async fn clear_cache(&self) -> azure_core::Result<()> {
        self.inner.clear_cache().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use azure_core::auth::{AccessToken, Secret, TokenCredential};
    use time::OffsetDateTime;

    use super::ScopedTokenCredential;

    #[derive(Debug)]
    struct MockTokenCredential {
        requested_scopes: Arc<std::sync::Mutex<Vec<String>>>,
    }

    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    impl TokenCredential for MockTokenCredential {
        async fn get_token(&self, scopes: &[&str]) -> azure_core::Result<AccessToken> {
            self.requested_scopes
                .lock()
                .expect("lock poisoned")
                .extend(scopes.iter().map(|scope| (*scope).to_string()));
            Ok(AccessToken::new(
                Secret::new("mock-token"),
                OffsetDateTime::now_utc(),
            ))
        }

        async fn clear_cache(&self) -> azure_core::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_scoped_token_credential_uses_configured_scope() {
        let requested_scopes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let inner = Arc::new(MockTokenCredential {
            requested_scopes: requested_scopes.clone(),
        }) as Arc<dyn TokenCredential>;
        let credential = ScopedTokenCredential::new(inner, "https://storage.azure.us/.default");

        let _token = credential
            .get_token(&["https://storage.azure.com/.default"])
            .await
            .expect("token request should succeed");

        let requested_scopes = requested_scopes.lock().expect("lock poisoned");
        assert_eq!(
            requested_scopes.as_slice(),
            &["https://storage.azure.us/.default".to_string()]
        );
    }
}
