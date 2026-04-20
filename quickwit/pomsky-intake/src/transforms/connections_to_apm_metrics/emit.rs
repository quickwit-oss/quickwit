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

//! Constructs `Metric` events and pushes them to the `OutputBuffer`.
//!
//! Two metric families are emitted per per-payload bucket:
//!
//! * `universal.<proto>.<dir>.hits` (counter) and `universal.<proto>.<dir>` (sketch) at the
//!   fine-grained tag set — matching the Go sidecar's `buildBucketTags` output shape.
//! * `trace.services_by_operation` (sketch), `.hits`, `.top_level_hits`, `.errors`, `.duration` —
//!   at the coarser tag set TSS produces.

use chrono::{DateTime, Utc};
use vector::event::{Event, Metric, MetricKind, MetricTags, MetricValue};
use vector::transforms::OutputBuffer;
use vector_lib::event::metric::MetricSketch;

use super::aggregator::{Bucket, Buckets};
use super::types::{BucketKey, ServiceIndexKey};

pub(super) fn emit_all(
    host: &str,
    ts: Option<DateTime<Utc>>,
    buckets: &Buckets,
    output: &mut OutputBuffer,
) {
    for (key, bucket) in &buckets.fine_grained {
        emit_universal_hits(output, host, ts, key, bucket);
        emit_universal_sketch(output, host, ts, key, bucket);
    }
    for (key, bucket) in &buckets.service_index {
        emit_service_index(output, host, ts, key, bucket);
    }
}

fn emit_universal_hits(
    output: &mut OutputBuffer,
    host: &str,
    ts: Option<DateTime<Utc>>,
    key: &BucketKey,
    bucket: &Bucket,
) {
    let name = format!("{}.hits", key.operation);
    let tags = build_universal_tags(host, key);
    let metric = Metric::new(
        name,
        MetricKind::Incremental,
        MetricValue::Counter {
            value: bucket.hits as f64,
        },
    )
    .with_tags(Some(tags))
    .with_timestamp(ts);
    output.push(Event::Metric(metric));
}

fn emit_universal_sketch(
    output: &mut OutputBuffer,
    host: &str,
    ts: Option<DateTime<Utc>>,
    key: &BucketKey,
    bucket: &Bucket,
) {
    let Some(sketch) = bucket.sketch.clone() else {
        return;
    };
    let tags = build_universal_tags(host, key);
    let metric = Metric::new(
        key.operation.clone(),
        MetricKind::Incremental,
        MetricValue::Sketch {
            sketch: MetricSketch::AgentDDSketch(sketch),
        },
    )
    .with_tags(Some(tags))
    .with_timestamp(ts);
    output.push(Event::Metric(metric));
}

fn emit_service_index(
    output: &mut OutputBuffer,
    host: &str,
    ts: Option<DateTime<Utc>>,
    key: &ServiceIndexKey,
    bucket: &Bucket,
) {
    let base_tags = build_service_index_tags(host, key);

    if let Some(sketch) = bucket.sketch.clone() {
        let sum = sketch.sum().unwrap_or(0.0);
        let metric = Metric::new(
            "trace.services_by_operation",
            MetricKind::Incremental,
            MetricValue::Sketch {
                sketch: MetricSketch::AgentDDSketch(sketch),
            },
        )
        .with_tags(Some(base_tags.clone()))
        .with_timestamp(ts);
        output.push(Event::Metric(metric));

        if sum > 0.0 {
            let metric = Metric::new(
                "trace.services_by_operation.duration",
                MetricKind::Incremental,
                MetricValue::Counter { value: sum },
            )
            .with_tags(Some(base_tags.clone()))
            .with_timestamp(ts);
            output.push(Event::Metric(metric));
        }
    }

    let hits_metric = Metric::new(
        "trace.services_by_operation.hits",
        MetricKind::Incremental,
        MetricValue::Counter {
            value: bucket.hits as f64,
        },
    )
    .with_tags(Some(base_tags.clone()))
    .with_timestamp(ts);
    output.push(Event::Metric(hits_metric));

    let top_level_hits_metric = Metric::new(
        "trace.services_by_operation.top_level_hits",
        MetricKind::Incremental,
        MetricValue::Counter {
            value: bucket.hits as f64,
        },
    )
    .with_tags(Some(base_tags.clone()))
    .with_timestamp(ts);
    output.push(Event::Metric(top_level_hits_metric));

    if bucket.errors > 0 {
        let errors_metric = Metric::new(
            "trace.services_by_operation.errors",
            MetricKind::Incremental,
            MetricValue::Counter {
                value: bucket.errors as f64,
            },
        )
        .with_tags(Some(base_tags))
        .with_timestamp(ts);
        output.push(Event::Metric(errors_metric));
    }
}

fn build_universal_tags(host: &str, key: &BucketKey) -> MetricTags {
    let mut tags = MetricTags::default();
    if !host.is_empty() {
        tags.replace("host".into(), host.to_string());
    }
    tags.replace("service".into(), key.service.clone());
    if let Some(env) = key.env.as_ref() {
        tags.replace("env".into(), env.clone());
    }
    if !key.resource.is_empty() {
        tags.replace("resource_name".into(), key.resource.clone());
    }
    if let Some(sc) = key.status_class {
        tags.replace("http.status_class".into(), sc.as_tag_value().to_string());
    }
    tags.replace(
        "error".into(),
        if key.is_error { "true" } else { "false" }.to_string(),
    );
    tags.replace("instr_src".into(), "usm".to_string());
    tags
}

fn build_service_index_tags(host: &str, key: &ServiceIndexKey) -> MetricTags {
    let mut tags = MetricTags::default();
    if !host.is_empty() {
        tags.replace("host".into(), host.to_string());
    }
    tags.replace("service".into(), key.service.clone());
    tags.replace("operation_name".into(), key.operation.clone());
    tags.replace("base_service".into(), key.service.clone());
    tags.replace("instr_src".into(), "usm".to_string());
    if let Some(env) = key.env.as_ref() {
        tags.replace("env".into(), env.clone());
    }
    tags
}

#[cfg(test)]
mod tests {
    use super::super::aggregator::{Bucket, Buckets};
    use super::super::types::{BucketKey, ServiceIndexKey, StatusClass};
    use super::*;

    fn sample_buckets() -> Buckets {
        let mut buckets = Buckets::default();
        let key = BucketKey {
            service: "web".into(),
            env: Some("prod".into()),
            operation: "universal.http.server".into(),
            resource: "GET /x".into(),
            status_class: Some(StatusClass::TwoXx),
            is_error: false,
        };
        buckets.fine_grained.insert(
            key,
            Bucket {
                hits: 5,
                errors: 0,
                sketch: None,
            },
        );
        let idx = ServiceIndexKey {
            service: "web".into(),
            env: Some("prod".into()),
            operation: "universal.http.server".into(),
        };
        buckets.service_index.insert(
            idx,
            Bucket {
                hits: 5,
                errors: 2,
                sketch: None,
            },
        );
        buckets
    }

    #[test]
    fn universal_hits_counter_shape() {
        let buckets = sample_buckets();
        let mut output = OutputBuffer::default();
        emit_all("host-1", None, &buckets, &mut output);

        let events: Vec<_> = output.drain().collect();
        let counter = events
            .iter()
            .find_map(|e| match e {
                Event::Metric(m) if m.name() == "universal.http.server.hits" => Some(m),
                _ => None,
            })
            .expect("hits counter emitted");
        let tags = counter.tags().expect("tags");
        assert_eq!(tags.get("service"), Some("web"));
        assert_eq!(tags.get("env"), Some("prod"));
        assert_eq!(tags.get("resource_name"), Some("GET /x"));
        assert_eq!(tags.get("http.status_class"), Some("2xx"));
        assert_eq!(tags.get("error"), Some("false"));
        assert_eq!(tags.get("instr_src"), Some("usm"));
        assert_eq!(tags.get("host"), Some("host-1"));
        match counter.value() {
            MetricValue::Counter { value } => assert_eq!(*value, 5.0),
            _ => panic!("expected counter"),
        }
    }

    #[test]
    fn service_index_no_sketch_skips_sketch_and_duration() {
        let buckets = sample_buckets();
        let mut output = OutputBuffer::default();
        emit_all("host-1", None, &buckets, &mut output);

        let events: Vec<_> = output.drain().collect();
        let names: Vec<&str> = events
            .iter()
            .filter_map(|e| match e {
                Event::Metric(m) => Some(m.name()),
                _ => None,
            })
            .collect();
        assert!(!names.contains(&"trace.services_by_operation"));
        assert!(!names.contains(&"trace.services_by_operation.duration"));
        assert!(names.contains(&"trace.services_by_operation.hits"));
        assert!(names.contains(&"trace.services_by_operation.top_level_hits"));
        assert!(names.contains(&"trace.services_by_operation.errors"));
    }
}
