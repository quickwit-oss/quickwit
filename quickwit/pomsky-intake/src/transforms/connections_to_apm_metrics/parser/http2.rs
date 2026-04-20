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

//! Extracts per-endpoint HTTP/2 and gRPC `ProtoStat` records from the
//! agent's opaque `Http2Aggregations` byte payload.
//!
//! Port of `parser/http.go::parseHTTP2Aggregations` and
//! `parser/grpc.go::grpcPattern`.

use std::sync::OnceLock;

use prost::Message;
use regex::Regex;

use super::super::types::{Operation, ProtoStat};
use super::http::{is_method_unknown, method_string, optional_bytes, optional_f64};
use crate::protos::process::{Http2Aggregations, HttpMethod};

/// Path regex the Go sidecar uses to classify gRPC traffic from the HTTP/2
/// aggregation stream (`parser/grpc.go::grpcPattern`).
const GRPC_REGEX: &str = r"^/([^./]+(\.[^./]+)*?)\.([^./]+(\.[^./]+)*?)/([^./]+?)$";

pub(in crate::transforms::connections_to_apm_metrics) fn parse_http2_aggregations(
    data: &[u8],
) -> Vec<ProtoStat> {
    let agg = match Http2Aggregations::decode(data) {
        Ok(agg) => agg,
        Err(_) => return Vec::new(),
    };
    let re = grpc_pattern();
    let mut out: Vec<ProtoStat> = Vec::new();
    for ep in agg.endpoint_aggregations {
        if is_method_unknown(ep.method) {
            continue;
        }
        if ep.path.is_empty() || !ep.path.starts_with('/') {
            continue;
        }

        let is_grpc = ep.method == HttpMethod::Post as i32 && re.is_match(&ep.path);
        let (operation, resource) = if is_grpc {
            (Operation::Grpc, ep.path.clone())
        } else {
            (
                Operation::Http2,
                format!("{} {}", method_string(ep.method), ep.path),
            )
        };

        for (status_code, data) in &ep.stats_by_status_code {
            if data.count == 0 {
                continue;
            }
            let errors = if *status_code >= 400 { data.count } else { 0 };
            out.push(ProtoStat {
                operation,
                resource: resource.clone(),
                status: *status_code,
                hits: data.count,
                errors,
                latencies: optional_bytes(&data.latencies),
                first_latency_sample: optional_f64(data.first_latency_sample),
            });
        }
    }
    out
}

fn grpc_pattern() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| Regex::new(GRPC_REGEX).expect("static regex"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protos::process::http_stats::Data;
    use crate::protos::process::{Http2Aggregations, HttpStats};

    fn ep(method: HttpMethod, path: &str, stats_by_code: &[(i32, u32)]) -> HttpStats {
        let mut stats_by_status_code = std::collections::HashMap::new();
        for (code, count) in stats_by_code {
            stats_by_status_code.insert(
                *code,
                Data {
                    count: *count,
                    latencies: Vec::new(),
                    first_latency_sample: 0.0,
                },
            );
        }
        HttpStats {
            path: path.into(),
            method: method as i32,
            full_path: true,
            stats_by_response_status: Vec::new(),
            stats_by_status_code,
        }
    }

    #[test]
    fn grpc_path_matches_regex() {
        let agg = Http2Aggregations {
            endpoint_aggregations: vec![ep(
                HttpMethod::Post,
                "/example.v1.Service/Method",
                &[(200, 4)],
            )],
        };
        let stats = parse_http2_aggregations(&agg.encode_to_vec());
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].operation, Operation::Grpc);
        assert_eq!(stats[0].resource, "/example.v1.Service/Method");
    }

    #[test]
    fn http2_path_unchanged() {
        let agg = Http2Aggregations {
            endpoint_aggregations: vec![ep(HttpMethod::Get, "/healthz", &[(200, 2)])],
        };
        let stats = parse_http2_aggregations(&agg.encode_to_vec());
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].operation, Operation::Http2);
        assert_eq!(stats[0].resource, "GET /healthz");
    }

    #[test]
    fn mixed_grpc_and_http2_endpoints() {
        let agg = Http2Aggregations {
            endpoint_aggregations: vec![
                ep(HttpMethod::Post, "/pkg.Service/Call", &[(200, 1)]),
                ep(HttpMethod::Get, "/metrics", &[(200, 1)]),
            ],
        };
        let stats = parse_http2_aggregations(&agg.encode_to_vec());
        let ops: Vec<_> = stats.iter().map(|s| s.operation).collect();
        assert!(ops.contains(&Operation::Grpc));
        assert!(ops.contains(&Operation::Http2));
    }

    #[test]
    fn empty_or_relative_path_skipped() {
        let agg = Http2Aggregations {
            endpoint_aggregations: vec![
                ep(HttpMethod::Get, "", &[(200, 1)]),
                ep(HttpMethod::Get, "relative/path", &[(200, 1)]),
            ],
        };
        assert!(parse_http2_aggregations(&agg.encode_to_vec()).is_empty());
    }
}
