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

//! Extracts per-topic Kafka `ProtoStat` records from the agent's
//! `DataStreamsAggregations` payload.
//!
//! Port of `parser/kafka.go::parseKafkaAggregations`.

use prost::Message;
use tracing::warn;

use super::super::types::{Operation, ProtoStat};
use super::http::{optional_bytes, optional_f64};
use crate::protos::process::DataStreamsAggregations;

// Kafka API keys ("request_type" in the agent proto). Named constants so the
// parser's `match` is self-documenting rather than magic 0/1.
const KAFKA_API_PRODUCE: u32 = 0;
const KAFKA_API_CONSUME: u32 = 1;

/// Maps Kafka request-type (API key) values to operation strings. Matches
/// the Go sidecar's `kafkaAPIKeyToOperation` map.
fn operation_for(request_type: u32) -> Option<&'static str> {
    match request_type {
        KAFKA_API_PRODUCE => Some("produce"),
        KAFKA_API_CONSUME => Some("consume"),
        _ => None,
    }
}

const KAFKA_SUCCESS_ERROR_CODE: i32 = 0;

pub(in crate::transforms::connections_to_apm_metrics) fn parse_kafka_aggregations(
    data: &[u8],
) -> Vec<ProtoStat> {
    let agg = match DataStreamsAggregations::decode(data) {
        Ok(agg) => agg,
        Err(err) => {
            warn!(%err, bytes = data.len(), "kafka aggregation decode failed, dropping");
            return Vec::new();
        }
    };
    let mut out: Vec<ProtoStat> = Vec::new();
    for ka in agg.kafka_aggregations {
        // Go's proto getter returns 0 (produce) on a nil header. Match that
        // so agents that omit the header don't silently lose their Kafka
        // USM data.
        let request_type = ka
            .header
            .map(|h| h.request_type)
            .unwrap_or(KAFKA_API_PRODUCE);
        let Some(op_str) = operation_for(request_type) else {
            continue;
        };
        let resource = format!("{op_str}/{}", ka.topic);

        if !ka.stats_by_error_code.is_empty() {
            for (error_code, ks) in &ka.stats_by_error_code {
                if ks.count == 0 {
                    continue;
                }
                let errors = if *error_code != KAFKA_SUCCESS_ERROR_CODE {
                    ks.count
                } else {
                    0
                };
                out.push(ProtoStat {
                    operation: Operation::Kafka,
                    resource: resource.clone(),
                    status: *error_code,
                    hits: ks.count,
                    errors,
                    latencies: optional_bytes(&ks.latencies),
                    first_latency_sample: optional_f64(ks.first_latency_sample),
                });
            }
        } else {
            // Deprecated legacy path for older agents that track hits
            // without error codes.
            #[allow(deprecated)]
            let count = ka.count;
            if count > 0 {
                out.push(ProtoStat {
                    operation: Operation::Kafka,
                    resource,
                    status: 0,
                    hits: count,
                    errors: 0,
                    latencies: None,
                    first_latency_sample: None,
                });
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protos::process::{KafkaAggregation, KafkaRequestHeader, KafkaStats};

    fn kafka_agg(request_type: u32, topic: &str, by_code: &[(i32, u32)]) -> KafkaAggregation {
        let mut stats_by_error_code = std::collections::HashMap::new();
        for (code, count) in by_code {
            stats_by_error_code.insert(
                *code,
                KafkaStats {
                    count: *count,
                    latencies: Vec::new(),
                    first_latency_sample: 0.0,
                },
            );
        }
        #[allow(deprecated)]
        KafkaAggregation {
            header: Some(KafkaRequestHeader {
                request_type,
                request_version: 0,
            }),
            topic: topic.into(),
            stats_by_error_code,
            count: 0,
        }
    }

    #[test]
    fn produce_consume_operations() {
        let agg = DataStreamsAggregations {
            kafka_aggregations: vec![
                kafka_agg(0, "orders", &[(0, 4)]),
                kafka_agg(1, "events", &[(0, 2)]),
            ],
        };
        let stats = parse_kafka_aggregations(&agg.encode_to_vec());
        let resources: Vec<&str> = stats.iter().map(|s| s.resource.as_str()).collect();
        assert!(resources.contains(&"produce/orders"));
        assert!(resources.contains(&"consume/events"));
    }

    #[test]
    fn nonzero_error_code_sets_errors() {
        let agg = DataStreamsAggregations {
            kafka_aggregations: vec![kafka_agg(0, "topic", &[(3, 7)])],
        };
        let stats = parse_kafka_aggregations(&agg.encode_to_vec());
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].errors, 7);
        assert_eq!(stats[0].status, 3);
    }

    #[test]
    fn unknown_request_type_skipped() {
        let agg = DataStreamsAggregations {
            kafka_aggregations: vec![kafka_agg(99, "topic", &[(0, 1)])],
        };
        assert!(parse_kafka_aggregations(&agg.encode_to_vec()).is_empty());
    }

    #[test]
    fn missing_header_defaults_to_produce() {
        // Go's generated proto getter returns 0 (produce) on nil header.
        // Our parser must match — otherwise older or buggy agents that
        // omit the header lose their Kafka USM data entirely.
        #[allow(deprecated)]
        let ka = KafkaAggregation {
            header: None,
            topic: "orders".into(),
            stats_by_error_code: std::collections::HashMap::from([(
                0,
                KafkaStats {
                    count: 3,
                    latencies: Vec::new(),
                    first_latency_sample: 0.0,
                },
            )]),
            count: 0,
        };
        let agg = DataStreamsAggregations {
            kafka_aggregations: vec![ka],
        };
        let stats = parse_kafka_aggregations(&agg.encode_to_vec());
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].resource, "produce/orders");
    }

    #[test]
    fn malformed_bytes_return_empty() {
        assert!(parse_kafka_aggregations(b"\xff\xff\xff").is_empty());
    }

    #[test]
    fn legacy_count_fallback() {
        #[allow(deprecated)]
        let ka = KafkaAggregation {
            header: Some(KafkaRequestHeader {
                request_type: 0,
                request_version: 0,
            }),
            topic: "t".into(),
            stats_by_error_code: std::collections::HashMap::new(),
            count: 5,
        };
        let agg = DataStreamsAggregations {
            kafka_aggregations: vec![ka],
        };
        let stats = parse_kafka_aggregations(&agg.encode_to_vec());
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].hits, 5);
        assert_eq!(stats[0].status, 0);
    }
}
