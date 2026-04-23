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

//! Extracts `UsmStat` records from each connection's opaque aggregation
//! byte fields by dispatching to the per-protocol parser submodules.

pub(super) mod db;
pub(super) mod http;
pub(super) mod http2;
pub(super) mod kafka;

use super::resolver;
use super::types::{Direction, UsmStat};
use crate::protos::process::CollectorConnections;

/// Orchestrates resolver + protocol parsers. Returns the full list of
/// `UsmStat` records the aggregator will consume.
pub(super) fn extract_usm_stats(cc: &mut CollectorConnections) -> Vec<UsmStat> {
    resolver::fixup_directions(cc);

    let mut out: Vec<UsmStat> = Vec::new();
    for conn in &cc.connections {
        let direction = Direction::from_agent(conn.direction);
        let service = resolver::resolve_service(cc, conn);
        let env = resolver::resolve_env(cc, conn);

        if !conn.http_aggregations.is_empty() {
            for ps in http::parse_http_aggregations(&conn.http_aggregations) {
                out.push(UsmStat::from_proto_stat(
                    ps,
                    service.clone(),
                    env.clone(),
                    direction,
                ));
            }
        }
        if !conn.http2_aggregations.is_empty() {
            for ps in http2::parse_http2_aggregations(&conn.http2_aggregations) {
                out.push(UsmStat::from_proto_stat(
                    ps,
                    service.clone(),
                    env.clone(),
                    direction,
                ));
            }
        }
        if !conn.data_streams_aggregations.is_empty() {
            for ps in kafka::parse_kafka_aggregations(&conn.data_streams_aggregations) {
                out.push(UsmStat::from_proto_stat(
                    ps,
                    service.clone(),
                    env.clone(),
                    direction,
                ));
            }
        }
        if !conn.database_aggregations.is_empty() {
            for ps in db::parse_database_aggregations(&conn.database_aggregations) {
                out.push(UsmStat::from_proto_stat(
                    ps,
                    service.clone(),
                    env.clone(),
                    direction,
                ));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use prost::Message;

    use super::super::types::{Direction, Operation};
    use super::*;
    use crate::protos::process::{
        Addr, CollectorConnections, Connection, ConnectionDirection, ConnectionType,
    };

    fn v1_buffer(tags: &[&str]) -> Vec<u8> {
        let mut buf = vec![1_u8];
        buf.extend_from_slice(&(tags.len() as u16).to_le_bytes());
        for t in tags {
            buf.extend_from_slice(&(t.len() as u16).to_le_bytes());
            buf.extend_from_slice(t.as_bytes());
        }
        buf
    }

    /// Builds a minimal HttpAggregations payload with one `GET /hello` 200
    /// endpoint and 3 hits.
    fn http_bytes() -> Vec<u8> {
        use crate::protos::process::http_stats::Data;
        use crate::protos::process::{HttpAggregations, HttpMethod, HttpStats};
        let mut stats_by_status_code: std::collections::HashMap<i32, Data> =
            std::collections::HashMap::new();
        stats_by_status_code.insert(
            200,
            Data {
                count: 3,
                latencies: Vec::new(),
                first_latency_sample: 0.0,
            },
        );
        let agg = HttpAggregations {
            endpoint_aggregations: vec![HttpStats {
                method: HttpMethod::Get as i32,
                path: "/hello".into(),
                full_path: true,
                stats_by_response_status: Vec::new(),
                stats_by_status_code,
            }],
        };
        agg.encode_to_vec()
    }

    #[test]
    fn orchestrator_single_http_connection() {
        let mut cc = CollectorConnections {
            host_name: "host-1".into(),
            encoded_tags: v1_buffer(&["service:web", "env:prod"]),
            host_tags_index: 1,
            ..Default::default()
        };
        cc.connections.push(Connection {
            r#type: ConnectionType::Tcp as i32,
            direction: ConnectionDirection::Incoming as i32,
            laddr: Some(Addr {
                port: 8080,
                ..Default::default()
            }),
            http_aggregations: Bytes::from(http_bytes()).to_vec(),
            ..Default::default()
        });

        let stats = extract_usm_stats(&mut cc);
        assert_eq!(stats.len(), 1);
        let stat = &stats[0];
        assert_eq!(stat.service, "web");
        assert_eq!(stat.env.as_deref(), Some("prod"));
        assert_eq!(stat.direction, Direction::Server);
        assert_eq!(stat.operation, Operation::Http);
        assert_eq!(stat.resource, "GET /hello");
        assert_eq!(stat.status, 200);
        assert_eq!(stat.hits, 3);
        assert_eq!(stat.errors, 0);
    }
}
