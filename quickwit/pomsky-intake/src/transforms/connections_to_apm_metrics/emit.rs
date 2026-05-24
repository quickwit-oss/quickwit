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
    apply_connection_tags(&mut tags, &key.tags);
    if !key.resource.is_empty() {
        tags.replace("resource_name".into(), key.resource.clone());
        // `resource` is the murmur3-hashed resource-name ID the SaaS UI uses
        // for resource-page URL routing. Hash function matches dd-go's
        // `trace/model/resource.go::HashResourceMurmur3` exactly (`Sum64` of
        // the Go `twmb/murmur3` package = low 64 bits of x64_128).
        tags.replace("resource".into(), hash_resource(&key.resource));
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
    apply_connection_tags(&mut tags, &key.tags);
    tags
}

/// Mirrors NSX's `USMInfo`-to-tag projection: copy each populated optional
/// onto the metric tag set and expand the `iisTags` map into individual
/// `http.iis.*` tags. Empty optionals and empty IIS maps are skipped so the
/// emitter never produces `version:` or `http.iis.app_pool:` for workloads
/// that don't set those tags.
fn apply_connection_tags(tags: &mut MetricTags, ct: &super::types::ConnectionTags) {
    if let Some(env) = ct.env.as_ref() {
        tags.replace("env".into(), env.clone());
    }
    if let Some(version) = ct.version.as_ref() {
        tags.replace("version".into(), version.clone());
    }
    if let Some(tls_library) = ct.tls_library.as_ref() {
        tags.replace("tls.library".into(), tls_library.clone());
    }
    for (key, value) in &ct.iis_tags {
        tags.replace(key.clone(), value.clone());
    }
}

/// Resource-name → resource-ID hash. Mirrors the SaaS-side
/// `dd-go/trace/model/resource.go::HashResourceMurmur3`:
///
/// ```go
/// func HashResourceMurmur3(r string) string {
///     return strconv.FormatUint(murmur3.Sum64([]byte(r)), 16)
/// }
/// ```
///
/// `twmb/murmur3.Sum64` returns the low 64 bits of the 128-bit x64 hash
/// with seed=0. We replicate that exactly so resource IDs emitted by BYOC
/// match the SaaS UI's expected format (lowercase hex, no leading zeros).
fn hash_resource(resource: &str) -> String {
    let mut cursor = std::io::Cursor::new(resource.as_bytes());
    let h128 = murmur3::murmur3_x64_128(&mut cursor, 0)
        .expect("murmur3 over an in-memory cursor cannot fail");
    let h64 = (h128 & 0xFFFF_FFFF_FFFF_FFFF) as u64;
    format!("{h64:x}")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::super::aggregator::{Bucket, Buckets};
    use super::super::types::{BucketKey, ConnectionTags, ServiceIndexKey, StatusClass};
    use super::*;

    fn sample_tags() -> ConnectionTags {
        ConnectionTags {
            env: Some("prod".into()),
            version: Some("v1.2.3".into()),
            ..Default::default()
        }
    }

    fn sample_buckets() -> Buckets {
        let mut buckets = Buckets::default();
        let key = BucketKey {
            service: "web".into(),
            tags: sample_tags(),
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
            tags: sample_tags(),
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
        assert_eq!(tags.get("version"), Some("v1.2.3"));
        assert_eq!(tags.get("resource_name"), Some("GET /x"));
        // Murmur3-hashed resource ID — see hash_resource for the parity test.
        assert_eq!(tags.get("resource"), Some(hash_resource("GET /x").as_str()));
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
    fn service_index_carries_version_tag() {
        let buckets = sample_buckets();
        let mut output = OutputBuffer::default();
        emit_all("host-1", None, &buckets, &mut output);
        let events: Vec<_> = output.drain().collect();
        let hits = events
            .iter()
            .find_map(|e| match e {
                Event::Metric(m) if m.name() == "trace.services_by_operation.hits" => Some(m),
                _ => None,
            })
            .expect("service-index hits emitted");
        let tags = hits.tags().expect("tags");
        assert_eq!(tags.get("service"), Some("web"));
        assert_eq!(tags.get("env"), Some("prod"));
        assert_eq!(tags.get("version"), Some("v1.2.3"));
        assert_eq!(tags.get("operation_name"), Some("universal.http.server"));
        assert_eq!(tags.get("base_service"), Some("web"));
    }

    #[test]
    fn hash_resource_matches_dd_go_reference() {
        // Reference values produced by Go's
        // `trace/model/resource.go::HashResourceMurmur3` (= `twmb/murmur3.Sum64`)
        // for these exact inputs. If this assertion ever fails, the BYOC
        // resource-page URL routing in the SaaS UI silently breaks because the
        // emitted `resource` tag won't match the resource catalog's hash. So
        // we pin the algorithm against known good outputs rather than just
        // exercising the function.
        assert_eq!(hash_resource(""), "0");
        assert_eq!(hash_resource("GET /health"), "29b4fb26dc503948");
        assert_eq!(hash_resource("get_/api/v1/users"), "588c5864905328a8");
        assert_eq!(hash_resource("hello world"), "533f6046eb7f610e");
        assert_eq!(hash_resource("POST /api/v1/orders"), "5595737db9746ba2");
    }

    #[test]
    fn no_optional_tags_when_unset() {
        // All optional ConnectionTags fields default-empty → emitter omits
        // every one (no `version:`, no `tls.library:`, no `http.iis.*`). This
        // is the common case for Linux/K8s workloads.
        let mut buckets = Buckets::default();
        buckets.fine_grained.insert(
            BucketKey {
                service: "web".into(),
                tags: ConnectionTags {
                    env: Some("prod".into()),
                    ..Default::default()
                },
                operation: "universal.http.server".into(),
                resource: "GET /x".into(),
                status_class: Some(StatusClass::TwoXx),
                is_error: false,
            },
            Bucket {
                hits: 1,
                errors: 0,
                sketch: None,
            },
        );
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
        assert!(tags.get("version").is_none());
        assert!(tags.get("tls.library").is_none());
        assert!(tags.get("http.iis.app_pool").is_none());
        assert!(tags.get("http.iis.site").is_none());
        assert!(tags.get("http.iis.sitename").is_none());
        assert!(tags.get("http.iis.subsite").is_none());
    }

    #[test]
    fn emits_tls_library_and_iis_tags_when_present() {
        let mut iis = BTreeMap::new();
        iis.insert(
            "http.iis.app_pool".to_string(),
            "DefaultAppPool".to_string(),
        );
        iis.insert("http.iis.site".to_string(), "Default Web Site".to_string());
        let key = BucketKey {
            service: "iis-app".into(),
            tags: ConnectionTags {
                env: Some("prod".into()),
                version: Some("1.0".into()),
                tls_library: Some("openssl".into()),
                iis_tags: iis,
            },
            operation: "universal.http.server".into(),
            resource: "GET /".into(),
            status_class: Some(StatusClass::TwoXx),
            is_error: false,
        };
        let mut buckets = Buckets::default();
        buckets.fine_grained.insert(
            key,
            Bucket {
                hits: 3,
                errors: 0,
                sketch: None,
            },
        );
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
        assert_eq!(tags.get("tls.library"), Some("openssl"));
        assert_eq!(tags.get("http.iis.app_pool"), Some("DefaultAppPool"));
        assert_eq!(tags.get("http.iis.site"), Some("Default Web Site"));
        // Keys that aren't in the IIS map shouldn't be emitted.
        assert!(tags.get("http.iis.sitename").is_none());
        assert!(tags.get("http.iis.subsite").is_none());
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
