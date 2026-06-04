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

use std::collections::HashMap;

use pomsky_intake::protos::process::http_stats::Data;
use pomsky_intake::protos::process::{
    Addr, CollectorConnections, Connection, ConnectionDirection, ConnectionFamily, ConnectionType,
    HttpAggregations, HttpMethod, HttpStats,
};
use pomsky_intake::protos::sketch::index_mapping::Interpolation;
use pomsky_intake::protos::sketch::{DdSketch, IndexMapping, Store};
use pomsky_intake::sources::connections::{CONNECTIONS_PROTO_FIELD, CONNECTIONS_TIMESTAMP_FIELD};
use pomsky_intake::transforms::connections_to_apm_metrics::ConnectionsToApmMetricsConfig;
use prost::Message;
use vector::config::{TransformConfig, TransformContext};
use vector::event::{Event, LogEvent, MetricValue};
use vector::transforms::{OutputBuffer, Transform};
use vector_lib::event::metric::MetricSketch;

fn v1_tag_buffer(tags: &[&str]) -> Vec<u8> {
    let mut buf = vec![1_u8];
    buf.extend_from_slice(&(tags.len() as u16).to_le_bytes());
    for tag in tags {
        buf.extend_from_slice(&(tag.len() as u16).to_le_bytes());
        buf.extend_from_slice(tag.as_bytes());
    }
    buf
}

fn encode_sketches_go_bins(raw_bins: &[(i32, f64)]) -> Vec<u8> {
    let ra = 1.0 / 128.0_f64;
    let mut bin_counts = HashMap::new();
    for (key, count) in raw_bins {
        bin_counts.insert(*key, *count);
    }
    DdSketch {
        mapping: Some(IndexMapping {
            gamma: (1.0 + ra) / (1.0 - ra),
            index_offset: 0.0,
            interpolation: Interpolation::None as i32,
        }),
        positive_values: Some(Store {
            bin_counts,
            contiguous_bin_counts: Vec::new(),
            contiguous_bin_index_offset: 0,
        }),
        negative_values: None,
        zero_count: 0.0,
    }
    .encode_to_vec()
}

fn synthetic_connections_log() -> LogEvent {
    let raw_bins = [(947, 5.0), (1019, 3.0), (1066, 2.0)];
    let count = raw_bins.iter().map(|(_, count)| *count as u32).sum();
    let mut stats_by_status_code = HashMap::new();
    stats_by_status_code.insert(
        200,
        Data {
            count,
            latencies: encode_sketches_go_bins(&raw_bins),
            first_latency_sample: 0.0,
        },
    );
    let cc = CollectorConnections {
        host_name: "host-1".into(),
        encoded_connections_tags: v1_tag_buffer(&["service:mobile-store", "env:prod"]),
        connections: vec![Connection {
            laddr: Some(Addr {
                ip: "10.0.0.1".into(),
                port: 8080,
                container_id: "abcdef123456".into(),
                ..Default::default()
            }),
            raddr: Some(Addr {
                ip: "10.0.0.2".into(),
                port: 443,
                ..Default::default()
            }),
            family: ConnectionFamily::V4 as i32,
            r#type: ConnectionType::Tcp as i32,
            direction: ConnectionDirection::Incoming as i32,
            tags_idx: 1,
            local_container_tags_index: -1,
            http_aggregations: HttpAggregations {
                endpoint_aggregations: vec![HttpStats {
                    path: "/checkout".into(),
                    method: HttpMethod::Get as i32,
                    full_path: true,
                    stats_by_response_status: Vec::new(),
                    stats_by_status_code,
                }],
            }
            .encode_to_vec(),
            ..Default::default()
        }],
        ..Default::default()
    };

    let mut cc_bytes = Vec::new();
    cc.encode(&mut cc_bytes).unwrap();
    let mut log = LogEvent::default();
    log.insert(CONNECTIONS_PROTO_FIELD, bytes::Bytes::from(cc_bytes));
    log.insert(CONNECTIONS_TIMESTAMP_FIELD, 1_780_491_600_i64);
    log
}

#[tokio::test]
async fn apm_sketch_intake_remaps_bins_before_percentiles() {
    let Transform::Function(mut transform) = ConnectionsToApmMetricsConfig
        .build(&TransformContext::default())
        .await
        .unwrap()
    else {
        panic!("connections_to_apm_metrics should build a function transform");
    };
    let mut output = OutputBuffer::default();
    transform.transform(&mut output, Event::Log(synthetic_connections_log()));

    let metric = output
        .drain()
        .find_map(|event| match event {
            Event::Metric(metric) if metric.name() == "universal.http.server" => Some(metric),
            _ => None,
        })
        .expect("universal.http.server sketch emitted");
    let tags = metric.tags().unwrap();
    assert_eq!(tags.get("service"), Some("mobile-store"));
    assert_eq!(tags.get("env"), Some("prod"));
    assert_eq!(tags.get("resource_name"), Some("GET /checkout"));

    let MetricValue::Sketch {
        sketch: MetricSketch::AgentDDSketch(sketch),
    } = metric.value()
    else {
        panic!("expected AgentDDSketch metric");
    };
    let (keys, counts_u16) = sketch.bin_map().into_parts();
    let counts: Vec<u64> = counts_u16.iter().map(|count| u64::from(*count)).collect();
    assert_eq!(keys, vec![2293, 2365, 2413]);
    assert_eq!(counts, vec![5, 3, 2]);
    assert_eq!(sketch.count(), 10);

    let p50 = sketch.quantile(0.50).unwrap();
    let p75 = sketch.quantile(0.75).unwrap();
    let p95 = sketch.quantile(0.95).unwrap();
    assert!(p75 > p50, "p75={p75}, p50={p50}");
    assert!(p95 > p75, "p95={p95}, p75={p75}");
}
