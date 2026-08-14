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

//! Shared key (storage account key) request signing.
//!
//! The rewritten Azure SDK authenticates with Entra ID tokens only. Shared key support was
//! dropped in the 1.0 line and the SDK team has stated it will not come back, see
//! <https://github.com/Azure/azure-sdk-for-rust/issues/2975>. Quickwit documents
//! `azure.access_key` as a supported credential, and the Azurite emulator that backs the
//! `integration-testsuite` feature accepts shared key only, so the signing that
//! `azure_storage` 0.21 used to provide is implemented here instead.
//!
//! The scheme is specified in
//! <https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key>.

use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;

use azure_core::credentials::Secret;
use azure_core::hmac::hmac_sha256;
use azure_core::http::headers::Headers;
use azure_core::http::policies::{Policy, PolicyResult};
use azure_core::http::{Context, Method, Request, Url};
use azure_core::time::to_rfc7231;
use time::OffsetDateTime;

/// Header carrying the request time. Signing uses this rather than `Date` so that the
/// `Date` slot of the string to sign stays empty, which is what the service expects when
/// `x-ms-date` is present.
const X_MS_DATE: &str = "x-ms-date";
/// Signed and sent together, so both have to be derived from the same value.
const CONTENT_LENGTH: &str = "content-length";

/// Signs every request with the storage account key.
///
/// Installed as a per-retry policy so that each attempt is signed with a fresh timestamp:
/// the service rejects requests whose `x-ms-date` drifts more than 15 minutes from its own
/// clock, and a retry of a long-stalled request would otherwise carry a stale one.
pub(crate) struct SharedKeyAuthorizationPolicy {
    account: String,
    key: Secret,
}

impl fmt::Debug for SharedKeyAuthorizationPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The key is deliberately absent: this type ends up in pipeline debug output.
        formatter
            .debug_struct("SharedKeyAuthorizationPolicy")
            .field("account", &self.account)
            .finish_non_exhaustive()
    }
}

impl SharedKeyAuthorizationPolicy {
    pub(crate) fn new(account: String, key: String) -> Self {
        Self {
            account,
            key: Secret::new(key),
        }
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
        let now = OffsetDateTime::now_utc();
        request.insert_header(X_MS_DATE, to_rfc7231(&now));

        // Some operations leave `Content-Length` for the transport to fill in, `commit_block
        // _list` among them, while others set it themselves. Signing has to agree with what
        // finally goes on the wire, so set the header here when the body length is known and
        // the header is missing. Signing an empty length against a request that carries a
        // real one is rejected as `AuthorizationFailure`, with nothing to indicate why.
        let body_len = request.body().len();
        if header_or_empty(request.headers(), CONTENT_LENGTH).is_empty() {
            match body_len {
                // A zero length body is the documented exception: the slot stays empty in
                // the string to sign even though the wire carries `Content-Length: 0`.
                Some(len) if len > 0 => request.insert_header(CONTENT_LENGTH, len.to_string()),
                _ => {}
            }
        }

        let method = request.method();
        let string_to_sign =
            string_to_sign(&self.account, &method, request.url(), request.headers());
        let signature = hmac_sha256(&string_to_sign, &self.key)?;
        request.insert_header(
            "authorization",
            format!("SharedKey {}:{}", self.account, signature),
        );

        next[0].send(ctx, request, &next[1..]).await
    }
}

/// Returns the value of `header_name`, or the empty string when absent.
///
/// Absent and empty are indistinguishable in the string to sign, which is why the service
/// tolerates the collapse.
fn header_or_empty(headers: &Headers, header_name: &str) -> String {
    headers
        .get_optional_str(&azure_core::http::headers::HeaderName::from(
            header_name.to_owned(),
        ))
        .unwrap_or_default()
        .to_owned()
}

/// Builds the string the signature is computed over.
///
/// The field order is fixed by the service and every slot is present even when empty, so
/// the newlines carry meaning. `Content-Length` is the one exception the specification
/// calls out: it must be empty rather than `0` for requests without a body, for API
/// versions from 2015-02-21 onwards.
fn string_to_sign(account: &str, method: &Method, url: &Url, headers: &Headers) -> String {
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
        // Empty on purpose: `x-ms-date` supersedes `Date` and appears in the canonicalized
        // headers below. Signing both would double-count the timestamp.
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

/// Renders the `x-ms-*` headers in the form the signature expects: lowercase names, sorted
/// lexicographically, one `name:value` pair per line, each line newline terminated.
fn canonicalized_headers(headers: &Headers) -> String {
    let mut ms_headers: Vec<(String, String)> = Vec::new();
    headers.iter().for_each(|(header_name, header_value)| {
        let name = header_name.as_str().to_lowercase();
        if name.starts_with("x-ms-") {
            // Linear whitespace inside a value has to fold to a single space, otherwise the
            // service and the client sign different bytes.
            let value = header_value.as_str().split_whitespace().collect::<Vec<_>>();
            ms_headers.push((name, value.join(" ")));
        }
    });
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

/// Renders the resource the request addresses: the account, the decoded path, then every
/// query parameter, lowercased and sorted, with repeated parameters comma joined.
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
    // Sort by lowercased name so that repeated parameters group together, and by value so
    // that the comma joined list below is itself sorted, as the service requires.
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
    use azure_core::http::headers::Headers;
    use azure_core::http::{Method, Url};

    use super::*;

    /// The account and key Azurite and the legacy storage emulator publish. Documented at
    /// <https://learn.microsoft.com/azure/storage/common/storage-use-azurite>, so this is
    /// not a secret.
    const EMULATOR_ACCOUNT: &str = "devstoreaccount1";
    const EMULATOR_ACCOUNT_KEY: &str =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    fn headers_from(pairs: &[(&str, &str)]) -> Headers {
        let mut headers = Headers::new();
        for (name, value) in pairs {
            headers.insert(
                azure_core::http::headers::HeaderName::from((*name).to_owned()),
                (*value).to_owned(),
            );
        }
        headers
    }

    #[test]
    fn test_canonicalized_resource_without_query() {
        let url = Url::parse("https://acct.blob.core.windows.net/container/a/b.split").unwrap();
        assert_eq!(
            canonicalized_resource("acct", &url),
            "/acct/container/a/b.split"
        );
    }

    #[test]
    fn test_canonicalized_resource_sorts_and_lowercases_query() {
        let url = Url::parse(
            "https://acct.blob.core.windows.net/container?restype=container&comp=list&Marker=m",
        )
        .unwrap();
        assert_eq!(
            canonicalized_resource("acct", &url),
            "/acct/container\ncomp:list\nmarker:m\nrestype:container"
        );
    }

    #[test]
    fn test_canonicalized_resource_comma_joins_repeated_query_params() {
        let url = Url::parse("https://acct.blob.core.windows.net/c?include=metadata&include=copy")
            .unwrap();
        assert_eq!(
            canonicalized_resource("acct", &url),
            "/acct/c\ninclude:copy,metadata"
        );
    }

    #[test]
    fn test_canonicalized_headers_sorted_and_folded() {
        let headers = headers_from(&[
            ("x-ms-version", "2025-05-05"),
            ("x-ms-date", "Fri, 14 Aug 2026 12:00:00 GMT"),
            ("content-type", "application/octet-stream"),
            ("x-ms-blob-type", "Block   Blob"),
        ]);
        assert_eq!(
            canonicalized_headers(&headers),
            "x-ms-blob-type:Block Blob\nx-ms-date:Fri, 14 Aug 2026 12:00:00 \
             GMT\nx-ms-version:2025-05-05\n"
        );
    }

    #[test]
    fn test_string_to_sign_leaves_date_slot_empty() {
        let url = Url::parse("https://acct.blob.core.windows.net/c/blob").unwrap();
        let headers = headers_from(&[("x-ms-date", "Fri, 14 Aug 2026 12:00:00 GMT")]);
        let signed = string_to_sign("acct", &Method::Get, &url, &headers);
        // GET, then eleven empty slots, then the canonicalized headers and resource.
        assert_eq!(
            signed,
            "GET\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:Fri, 14 Aug 2026 12:00:00 GMT\n/acct/c/blob"
        );
    }

    #[test]
    fn test_string_to_sign_omits_zero_content_length() {
        let url = Url::parse("https://acct.blob.core.windows.net/c/blob").unwrap();
        let with_zero = headers_from(&[("content-length", "0")]);
        let with_body = headers_from(&[("content-length", "17")]);
        assert!(string_to_sign("acct", &Method::Put, &url, &with_zero).starts_with("PUT\n\n\n\n"));
        assert!(
            string_to_sign("acct", &Method::Put, &url, &with_body).starts_with("PUT\n\n\n17\n")
        );
    }

    #[test]
    fn test_signature_is_stable_for_a_known_request() {
        // Pins the whole pipeline: string construction, base64 key decoding, HMAC, and
        // base64 of the signature. A change to any of them changes this value.
        let url = Url::parse("https://devstoreaccount1.blob.core.windows.net/c/blob").unwrap();
        let headers = headers_from(&[("x-ms-date", "Fri, 14 Aug 2026 12:00:00 GMT")]);
        let string_to_sign = string_to_sign(EMULATOR_ACCOUNT, &Method::Get, &url, &headers);
        let signature = hmac_sha256(&string_to_sign, &Secret::new(EMULATOR_ACCOUNT_KEY)).unwrap();
        // Signatures are 32 bytes of HMAC SHA256, base64 encoded.
        assert_eq!(signature.len(), 44);
        let recomputed = hmac_sha256(&string_to_sign, &Secret::new(EMULATOR_ACCOUNT_KEY)).unwrap();
        assert_eq!(signature, recomputed);
    }

    /// Terminal policy: records the headers it is handed and answers 200.
    #[derive(Debug, Default)]
    struct HeaderCapturingPolicy {
        seen_headers: std::sync::Mutex<Option<Headers>>,
    }

    #[async_trait::async_trait]
    impl Policy for HeaderCapturingPolicy {
        async fn send(
            &self,
            _ctx: &Context,
            request: &mut Request,
            _next: &[Arc<dyn Policy>],
        ) -> PolicyResult {
            *self.seen_headers.lock().unwrap() = Some(request.headers().clone());
            Ok(azure_core::http::AsyncRawResponse::from_bytes(
                azure_core::http::StatusCode::Ok,
                Headers::default(),
                azure_core::Bytes::new(),
            ))
        }
    }

    /// `commit_block_list` leaves `Content-Length` to the transport. Signing an empty length
    /// while the wire carries a real one is rejected as `AuthorizationFailure`, and only a
    /// live service reveals it, so pin the header here instead.
    #[tokio::test]
    async fn test_send_sets_content_length_before_signing() {
        let capturing_policy = Arc::new(HeaderCapturingPolicy::default());
        let policy = SharedKeyAuthorizationPolicy::new(
            EMULATOR_ACCOUNT.to_owned(),
            EMULATOR_ACCOUNT_KEY.to_owned(),
        );
        let url =
            Url::parse("http://127.0.0.1:10000/devstoreaccount1/c/blob?comp=blocklist").unwrap();
        let mut request = Request::new(url, Method::Put);
        request.set_body(azure_core::Bytes::from_static(b"<BlockList/>"));

        let next: Vec<Arc<dyn Policy>> = vec![capturing_policy.clone()];
        policy
            .send(&Context::default(), &mut request, &next)
            .await
            .unwrap();

        let seen_headers = capturing_policy
            .seen_headers
            .lock()
            .unwrap()
            .clone()
            .unwrap();
        assert_eq!(header_or_empty(&seen_headers, CONTENT_LENGTH), "12");
        // The signature has to cover that same length.
        let signed = string_to_sign(EMULATOR_ACCOUNT, &Method::Put, request.url(), &seen_headers);
        assert!(signed.starts_with("PUT\n\n\n12\n"));
    }

    /// A request the caller already sized keeps that value rather than being overwritten.
    #[tokio::test]
    async fn test_send_keeps_an_existing_content_length() {
        let capturing_policy = Arc::new(HeaderCapturingPolicy::default());
        let policy = SharedKeyAuthorizationPolicy::new(
            EMULATOR_ACCOUNT.to_owned(),
            EMULATOR_ACCOUNT_KEY.to_owned(),
        );
        let url = Url::parse("http://127.0.0.1:10000/devstoreaccount1/c/blob").unwrap();
        let mut request = Request::new(url, Method::Put);
        request.insert_header(CONTENT_LENGTH, "5");
        request.set_body(azure_core::Bytes::from_static(b"hello"));

        let next: Vec<Arc<dyn Policy>> = vec![capturing_policy.clone()];
        policy
            .send(&Context::default(), &mut request, &next)
            .await
            .unwrap();

        let seen_headers = capturing_policy
            .seen_headers
            .lock()
            .unwrap()
            .clone()
            .unwrap();
        assert_eq!(header_or_empty(&seen_headers, CONTENT_LENGTH), "5");
    }

    #[test]
    fn test_debug_does_not_leak_the_key() {
        let policy = SharedKeyAuthorizationPolicy::new(
            EMULATOR_ACCOUNT.to_owned(),
            EMULATOR_ACCOUNT_KEY.to_owned(),
        );
        let rendered = format!("{policy:?}");
        assert!(rendered.contains(EMULATOR_ACCOUNT));
        assert!(!rendered.contains(EMULATOR_ACCOUNT_KEY));
    }
}
