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

//! Shared key (storage account key) request signing, which the 1.0 Azure SDK dropped
//! (<https://github.com/Azure/azure-sdk-for-rust/issues/2975>).
//!
//! See <https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key>.

use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;

use azure_core::credentials::Secret;
use azure_core::hmac::hmac_sha256;
use azure_core::http::headers::{HeaderName, Headers};
use azure_core::http::policies::{Policy, PolicyResult};
use azure_core::http::{Context, Method, Request, Url};
use azure_core::time::{OffsetDateTime, to_rfc7231};

const X_MS_DATE: &str = "x-ms-date";
const CONTENT_LENGTH: &str = "content-length";

/// Must be installed as a per-try policy: the service rejects an `x-ms-date` more than 15
/// minutes old, so each retry needs a fresh signature.
pub(crate) struct SharedKeyAuthorizationPolicy {
    account: String,
    key: Secret,
}

impl fmt::Debug for SharedKeyAuthorizationPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SharedKeyAuthorizationPolicy")
            .field("account", &self.account)
            .finish_non_exhaustive()
    }
}

impl SharedKeyAuthorizationPolicy {
    pub(crate) fn new(account: String, key: String) -> azure_core::Result<Self> {
        azure_core::base64::decode(&key)?;
        Ok(Self {
            account,
            key: Secret::new(key),
        })
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl Policy for SharedKeyAuthorizationPolicy {
    async fn send(
        &self,
        ctx: &Context,
        request: &mut Request,
        next: &[Arc<dyn Policy>],
    ) -> PolicyResult {
        request.insert_header(X_MS_DATE, to_rfc7231(&OffsetDateTime::now_utc()));

        // Some operations (`commit_block_list`) leave `Content-Length` to the transport, which
        // would then differ from the signed value.
        if header_or_empty(request.headers(), CONTENT_LENGTH).is_empty()
            && let Some(body_len) = request.body().len()
            && body_len > 0
        {
            request.insert_header(CONTENT_LENGTH, body_len.to_string());
        }

        let string_to_sign = string_to_sign(
            &self.account,
            &request.method(),
            request.url(),
            request.headers(),
        );
        let signature = hmac_sha256(&string_to_sign, &self.key)?;
        request.insert_header(
            "authorization",
            format!("SharedKey {}:{}", self.account, signature),
        );

        next[0].send(ctx, request, &next[1..]).await
    }
}

fn header_or_empty(headers: &Headers, header_name: &'static str) -> String {
    headers
        .get_optional_str(&HeaderName::from_static(header_name))
        .unwrap_or_default()
        .to_owned()
}

fn string_to_sign(account: &str, method: &Method, url: &Url, headers: &Headers) -> String {
    // The spec signs a zero `Content-Length` as an empty slot.
    let content_length = match header_or_empty(headers, CONTENT_LENGTH) {
        length if length == "0" => String::new(),
        length => length,
    };
    format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}{}",
        method.as_ref(),
        header_or_empty(headers, "content-encoding"),
        header_or_empty(headers, "content-language"),
        content_length,
        header_or_empty(headers, "content-md5"),
        header_or_empty(headers, "content-type"),
        // `Date` stays empty because `x-ms-date` is signed in the canonicalized headers.
        "",
        header_or_empty(headers, "if-modified-since"),
        header_or_empty(headers, "if-match"),
        header_or_empty(headers, "if-none-match"),
        header_or_empty(headers, "if-unmodified-since"),
        header_or_empty(headers, "range"),
        canonicalized_headers(headers),
        canonicalized_resource(account, url),
    )
}

fn canonicalized_headers(headers: &Headers) -> String {
    let mut ms_headers: Vec<(String, String)> = Vec::new();
    for (header_name, header_value) in headers.iter() {
        let name = header_name.as_str().to_lowercase();
        if name.starts_with("x-ms-") {
            let value = header_value.as_str().split_whitespace().collect::<Vec<_>>();
            ms_headers.push((name, value.join(" ")));
        }
    }
    ms_headers.sort_unstable();

    let mut rendered = String::new();
    for (name, value) in ms_headers {
        rendered.push_str(&name);
        rendered.push(':');
        rendered.push_str(&value);
        rendered.push('\n');
    }
    rendered
}

/// The path is signed percent-encoded, exactly as it appears in the URL, while query
/// parameters are decoded. Both are required by the spec: decoding the path fails with
/// `AuthorizationFailure`.
fn canonicalized_resource(account: &str, url: &Url) -> String {
    let mut resource = String::with_capacity(url.as_str().len());
    resource.push('/');
    resource.push_str(account);
    for segment in url.path_segments().into_iter().flatten() {
        resource.push('/');
        resource.push_str(segment);
    }

    let mut query_pairs: Vec<(Cow<'_, str>, Cow<'_, str>)> = url.query_pairs().collect();
    if query_pairs.is_empty() {
        return resource;
    }
    query_pairs.sort_by(|(left_name, left_value), (right_name, right_value)| {
        let left = left_name.to_lowercase();
        let right = right_name.to_lowercase();
        left.cmp(&right).then_with(|| left_value.cmp(right_value))
    });

    let mut current_name: Option<String> = None;
    for (name, value) in query_pairs {
        let name = name.to_lowercase();
        match current_name.as_deref() {
            Some(previous) if previous == name => {
                resource.push(',');
            }
            _ => {
                resource.push('\n');
                resource.push_str(&name);
                resource.push(':');
                current_name = Some(name);
            }
        }
        resource.push_str(&value);
    }
    resource
}

#[cfg(test)]
mod tests {
    use azure_core::http::headers::{HeaderName, Headers};
    use azure_core::http::{Method, Url};

    use super::*;

    const EMULATOR_ACCOUNT: &str = "devstoreaccount1";
    const EMULATOR_ACCOUNT_KEY: &str =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    #[test]
    fn test_signature_for_a_known_request() {
        let url = Url::parse("https://devstoreaccount1.blob.core.windows.net/c/blob").unwrap();
        let mut headers = Headers::new();
        headers.insert(
            HeaderName::from(X_MS_DATE.to_owned()),
            "Fri, 14 Aug 2026 12:00:00 GMT".to_owned(),
        );
        let string_to_sign = string_to_sign(EMULATOR_ACCOUNT, &Method::Get, &url, &headers);
        let signature = hmac_sha256(&string_to_sign, &Secret::new(EMULATOR_ACCOUNT_KEY)).unwrap();
        assert_eq!(signature, "iAWErbW5o26bhfJFESVV2MGDjcQI2uIFRQ6XmuVCjwM=");
    }
}
