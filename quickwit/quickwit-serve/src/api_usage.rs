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

pub(crate) fn classify_rest_request(path: &str) -> Option<ApiKind> {
    let api_path = path.strip_prefix("/api/v1/")?;
    let segments: Vec<&str> = api_path.split('/').collect();
    if segments[0] == "_elastic" {
        return Some(classify_elasticsearch_request(&segments[1..]));
    }
    // `/api/v1/otlp/v1/{logs,traces}`, before the index routes since an index may be named `otlp`.
    if segments[0] == "otlp" && segments.get(1) == Some(&"v1") {
        return Some(ApiKind::Otlp);
    }
    match segments.get(1) {
        Some(&"search") => Some(ApiKind::NativeSearch),
        Some(&"ingest") => Some(ApiKind::NativeIngest),
        Some(&"otlp") => Some(ApiKind::Otlp),
        Some(&"jaeger") => Some(ApiKind::Jaeger),
        _ => None,
    }
}

fn classify_elasticsearch_request(segments: &[&str]) -> ApiKind {
    if segments.contains(&"_bulk") {
        return ApiKind::ElasticsearchIngest;
    }
    let is_search = segments
        .iter()
        .any(|segment| matches!(*segment, "_search" | "_msearch" | "_count"));
    if is_search {
        return ApiKind::ElasticsearchSearch;
    }
    ApiKind::ElasticsearchOther
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
                "/api/v1/_elastic/logs/_bulk",
                Some(ApiKind::ElasticsearchIngest),
            ),
            (
                "/api/v1/_elastic/_msearch",
                Some(ApiKind::ElasticsearchSearch),
            ),
            (
                "/api/v1/_elastic/_field_caps",
                Some(ApiKind::ElasticsearchOther),
            ),
            ("/api/v1/logs/search", Some(ApiKind::NativeSearch)),
            ("/api/v1/logs/ingest", Some(ApiKind::NativeIngest)),
            ("/api/v1/otlp/v1/logs", Some(ApiKind::Otlp)),
            ("/api/v1/otlp/search", Some(ApiKind::NativeSearch)),
            ("/api/v1/traces/jaeger/api/services", Some(ApiKind::Jaeger)),
            ("/api/v1/indexes", None),
            ("/ui/search", None),
        ];
        for (path, expected_api_kind) in cases {
            assert_eq!(classify_rest_request(path), expected_api_kind, "{path}");
        }
    }
}
