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

use quickwit_telemetry::ApiKind;
use warp::http::Method;

pub(crate) fn classify_rest_request(method: &Method, path: &str) -> Option<ApiKind> {
    let api_path = path.strip_prefix("/api/v1/")?;
    let api_path = api_path.strip_suffix('/').unwrap_or(api_path);
    let (first_segment, rest) = api_path.split_once('/').unwrap_or((api_path, ""));
    if first_segment == "_elastic" {
        return Some(classify_elasticsearch_request(rest));
    }
    // Management routes such as `indexes/{index_id}/...` and `templates/{template_id}` put a
    // user-chosen ID after the first segment, so only the exact shapes of the counted routes match.
    match (method, rest) {
        (&Method::GET | &Method::POST, "search") => Some(ApiKind::NativeSearch),
        (&Method::POST, "ingest") => Some(ApiKind::NativeIngest),
        (&Method::POST, "v1/logs" | "v1/traces") if first_segment == "otlp" => Some(ApiKind::Otlp),
        (&Method::POST, "otlp/v1/logs" | "otlp/v1/traces") => Some(ApiKind::Otlp),
        (&Method::GET, _) if rest.starts_with("jaeger/api/") => Some(ApiKind::Jaeger),
        _ => None,
    }
}

fn classify_elasticsearch_request(elasticsearch_path: &str) -> ApiKind {
    let mut api_kind = ApiKind::ElasticsearchOther;
    for segment in elasticsearch_path.split('/') {
        match segment {
            "_bulk" => return ApiKind::ElasticsearchIngest,
            "_search" | "_msearch" | "_count" => api_kind = ApiKind::ElasticsearchSearch,
            _ => {}
        }
    }
    api_kind
}

pub(crate) fn count_otlp_grpc_request(
    request: tonic::Request<()>,
) -> Result<tonic::Request<()>, tonic::Status> {
    quickwit_telemetry::record_api_request(ApiKind::Otlp);
    Ok(request)
}

pub(crate) fn count_jaeger_grpc_request(
    request: tonic::Request<()>,
) -> Result<tonic::Request<()>, tonic::Status> {
    quickwit_telemetry::record_api_request(ApiKind::Jaeger);
    Ok(request)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_rest_request() {
        let cases = [
            (
                Method::POST,
                "/api/v1/_elastic/logs/_bulk",
                Some(ApiKind::ElasticsearchIngest),
            ),
            (
                Method::POST,
                "/api/v1/_elastic/_msearch",
                Some(ApiKind::ElasticsearchSearch),
            ),
            (
                Method::GET,
                "/api/v1/_elastic/_field_caps",
                Some(ApiKind::ElasticsearchOther),
            ),
            (
                Method::GET,
                "/api/v1/logs/search",
                Some(ApiKind::NativeSearch),
            ),
            (
                Method::POST,
                "/api/v1/logs/ingest",
                Some(ApiKind::NativeIngest),
            ),
            (Method::POST, "/api/v1/otlp/v1/logs", Some(ApiKind::Otlp)),
            (
                Method::POST,
                "/api/v1/logs/otlp/v1/traces",
                Some(ApiKind::Otlp),
            ),
            (
                Method::GET,
                "/api/v1/otlp/search",
                Some(ApiKind::NativeSearch),
            ),
            (
                Method::GET,
                "/api/v1/traces/jaeger/api/services",
                Some(ApiKind::Jaeger),
            ),
            (Method::GET, "/api/v1/indexes", None),
            (Method::GET, "/api/v1/indexes/otlp/splits", None),
            (Method::DELETE, "/api/v1/indexes/jaeger", None),
            (Method::PUT, "/api/v1/templates/ingest", None),
            (Method::GET, "/api/v1/indexes/search/describe", None),
            (Method::GET, "/ui/search", None),
        ];
        for (method, path, expected_api_kind) in cases {
            assert_eq!(
                classify_rest_request(&method, path),
                expected_api_kind,
                "{method} {path}"
            );
        }
    }
}
