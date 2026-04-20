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

//! Extracts per-endpoint HTTP/1.x `ProtoStat` records from the agent's
//! opaque `HttpAggregations` byte payload.
//!
//! Port of `parser/http.go::parseHTTPAggregations`.

use bytes::Bytes;
use prost::Message;

use super::super::types::{Operation, ProtoStat};
use crate::protos::process::{HttpAggregations, HttpMethod};

pub(in crate::transforms::connections_to_apm_metrics) fn parse_http_aggregations(
    data: &[u8],
) -> Vec<ProtoStat> {
    let agg = match HttpAggregations::decode(data) {
        Ok(agg) => agg,
        Err(_) => return Vec::new(),
    };
    let mut out: Vec<ProtoStat> = Vec::new();
    for ep in agg.endpoint_aggregations {
        if is_method_unknown(ep.method) {
            continue;
        }
        let method = method_string(ep.method);
        let resource = format!("{method} {}", ep.path);

        for (status_code, data) in &ep.stats_by_status_code {
            if data.count == 0 {
                continue;
            }
            let errors = if *status_code >= 400 { data.count } else { 0 };
            out.push(ProtoStat {
                operation: Operation::Http,
                resource: resource.clone(),
                status: *status_code,
                hits: data.count,
                errors,
                latencies: optional_bytes(&data.latencies),
                first_latency_sample: optional_f64(data.first_latency_sample),
            });
        }

        for data in &ep.stats_by_response_status {
            if data.count == 0 {
                continue;
            }
            out.push(ProtoStat {
                operation: Operation::Http,
                resource: resource.clone(),
                status: 0,
                hits: data.count,
                errors: 0,
                latencies: optional_bytes(&data.latencies),
                first_latency_sample: optional_f64(data.first_latency_sample),
            });
        }
    }
    out
}

pub(super) fn is_method_unknown(m: i32) -> bool {
    m == HttpMethod::Unknown as i32
}

pub(super) fn method_string(m: i32) -> &'static str {
    match HttpMethod::try_from(m) {
        Ok(HttpMethod::Get) => "GET",
        Ok(HttpMethod::Post) => "POST",
        Ok(HttpMethod::Put) => "PUT",
        Ok(HttpMethod::Delete) => "DELETE",
        Ok(HttpMethod::Head) => "HEAD",
        Ok(HttpMethod::Options) => "OPTIONS",
        Ok(HttpMethod::Patch) => "PATCH",
        Ok(HttpMethod::Trace) => "TRACE",
        _ => "UNKNOWN",
    }
}

pub(super) fn optional_bytes(v: &[u8]) -> Option<Bytes> {
    if v.is_empty() {
        None
    } else {
        Some(Bytes::copy_from_slice(v))
    }
}

pub(super) fn optional_f64(v: f64) -> Option<f64> {
    if v > 0.0 && v.is_finite() {
        Some(v)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protos::process::http_stats::Data;
    use crate::protos::process::{HttpAggregations, HttpStats};

    fn encode(agg: &HttpAggregations) -> Vec<u8> {
        agg.encode_to_vec()
    }

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
    fn unknown_method_skipped() {
        let agg = HttpAggregations {
            endpoint_aggregations: vec![ep(HttpMethod::Unknown, "/x", &[(200, 1)])],
        };
        assert!(parse_http_aggregations(&encode(&agg)).is_empty());
    }

    #[test]
    fn modern_stats_by_status_code() {
        let agg = HttpAggregations {
            endpoint_aggregations: vec![ep(HttpMethod::Get, "/hi", &[(200, 3)])],
        };
        let stats = parse_http_aggregations(&encode(&agg));
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].resource, "GET /hi");
        assert_eq!(stats[0].hits, 3);
        assert_eq!(stats[0].status, 200);
        assert_eq!(stats[0].errors, 0);
    }

    #[test]
    fn status_ge_400_sets_errors_equal_hits() {
        let agg = HttpAggregations {
            endpoint_aggregations: vec![ep(HttpMethod::Post, "/err", &[(500, 5)])],
        };
        let stats = parse_http_aggregations(&encode(&agg));
        assert_eq!(stats[0].hits, 5);
        assert_eq!(stats[0].errors, 5);
        assert_eq!(stats[0].resource, "POST /err");
    }

    #[test]
    fn zero_count_entries_skipped() {
        let agg = HttpAggregations {
            endpoint_aggregations: vec![ep(HttpMethod::Get, "/x", &[(200, 0), (404, 1)])],
        };
        let stats = parse_http_aggregations(&encode(&agg));
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].status, 404);
    }

    #[test]
    fn legacy_stats_by_response_status_gives_status_zero() {
        let agg = HttpAggregations {
            endpoint_aggregations: vec![HttpStats {
                path: "/legacy".into(),
                method: HttpMethod::Get as i32,
                full_path: true,
                stats_by_response_status: vec![Data {
                    count: 2,
                    latencies: Vec::new(),
                    first_latency_sample: 0.0,
                }],
                stats_by_status_code: std::collections::HashMap::new(),
            }],
        };
        let stats = parse_http_aggregations(&encode(&agg));
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].status, 0);
        assert_eq!(stats[0].hits, 2);
    }

    #[test]
    fn malformed_bytes_return_empty() {
        assert!(parse_http_aggregations(b"\xff\xff\xff").is_empty());
    }
}
