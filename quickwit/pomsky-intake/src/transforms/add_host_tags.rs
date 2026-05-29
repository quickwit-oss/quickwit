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

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use vector::config::{
    DataType, GenerateConfig, Input, OutputId, TransformConfig, TransformContext, TransformOutput,
};
use vector::event::{Event, LogEvent, Metric, TraceEvent};
use vector::schema::Definition;
use vector::transforms::{FunctionTransform, OutputBuffer, Transform};
use vector_lib::config::clone_input_definitions;

use crate::host_tags::HostTagsStore;
use crate::host_tags_poller::UnknownHostsCollector;
use crate::unix_timestamp::UnixTimestamp;

/// Adds host tags from the shared [`HostTagsStore`] to each event.
///
/// For each event, the transform extracts the hostname (from the
/// appropriate field per signal type), looks it up in the store, and
/// merges the associated tags into the event. Existing tags are never
/// overwritten — this transform only fills in missing keys.
///
/// When a hostname is not found in the store, it is reported to the
/// [`UnknownHostsCollector`] so the background poller can resolve it.
/// When a hostname is found but its entry has expired, the tags are still
/// applied (serving stale data is better than no data), and the host is
/// also reported to the collector so fresh tags are fetched on the next
/// poll cycle.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AddHostTagsConfig;

impl vector_lib::configurable::NamedComponent for AddHostTagsConfig {
    fn get_component_name(&self) -> &'static str {
        "add_host_tags"
    }
}

impl GenerateConfig for AddHostTagsConfig {
    fn generate_config() -> toml::Value {
        toml::Value::Table(Default::default())
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "add_host_tags")]
impl TransformConfig for AddHostTagsConfig {
    async fn build(&self, _context: &TransformContext) -> vector::Result<Transform> {
        let store = HostTagsStore::global();
        let collector = UnknownHostsCollector::global();
        Ok(Transform::function(AddHostTags { store, collector }))
    }

    fn input(&self) -> Input {
        Input::all()
    }

    fn outputs(
        &self,
        _: &TransformContext,
        input_definitions: &[(OutputId, Definition)],
    ) -> Vec<TransformOutput> {
        vec![TransformOutput::new(
            DataType::all_bits(),
            clone_input_definitions(input_definitions),
        )]
    }

    fn enable_concurrency(&self) -> bool {
        true
    }
}

#[derive(Clone)]
struct AddHostTags {
    store: Arc<HostTagsStore>,
    collector: UnknownHostsCollector,
}

impl FunctionTransform for AddHostTags {
    fn transform(&mut self, output: &mut OutputBuffer, mut event: Event) {
        match &mut event {
            Event::Log(log) => add_log_tags(&self.store, &self.collector, log),
            Event::Metric(metric) => add_metric_tags(&self.store, &self.collector, metric),
            Event::Trace(trace) => add_trace_tags(&self.store, &self.collector, trace),
        }
        output.push(event);
    }
}

/// Logs: hostname lives in the `hostname` field (DD agent convention).
/// Host tags are added under the `tags` object.
fn add_log_tags(store: &HostTagsStore, collector: &UnknownHostsCollector, log: &mut LogEvent) {
    let hostname_opt = log.get("hostname").and_then(|hostname| hostname.as_str());

    let Some(hostname) = hostname_opt else {
        return;
    };
    let Some(tags) = store.lookup(&hostname) else {
        collector.record(hostname.to_string());
        return;
    };
    if tags.is_expired(UnixTimestamp::now()) {
        collector.record(hostname.to_string());
    }
    for (key, value) in tags.iter() {
        let path = format!("tags.{key}");

        if log.get(path.as_str()).is_none() {
            log.insert(path.as_str(), value);
        }
    }
}

/// Metrics: hostname lives in the `host` tag.
/// Host tags are added as additional metric tags.
fn add_metric_tags(store: &HostTagsStore, collector: &UnknownHostsCollector, metric: &mut Metric) {
    let Some(hostname) = metric.tag_value("host") else {
        return;
    };
    let Some(tags) = store.lookup(&hostname) else {
        collector.record(hostname);
        return;
    };
    for (key, value) in tags.iter() {
        if metric.tag_value(key).is_none() {
            metric.replace_tag(key.to_string(), value.to_string());
        }
    }
    if tags.is_expired(UnixTimestamp::now()) {
        collector.record(hostname);
    }
}

/// Traces (post-preprocess): hostname lives in the top-level `host` field,
/// promoted there by `span_to_schema`'s `promote_host_and_env` step before
/// `meta` is folded into `custom`. Host tags and `host_id` are inserted as
/// top-level fields so they are available to downstream sinks.
fn add_trace_tags(
    store: &HostTagsStore,
    collector: &UnknownHostsCollector,
    trace: &mut TraceEvent,
) {
    let hostname_opt = trace.get("host").and_then(|hostname| hostname.as_str());
    let Some(hostname) = hostname_opt else {
        return;
    };
    let Some(tags) = store.lookup(&hostname) else {
        collector.record(hostname.to_string());
        return;
    };
    if tags.is_expired(UnixTimestamp::now()) {
        collector.record(hostname.to_string());
    }
    // After span_to_schema, meta has been folded into custom. Insert host
    // tags directly into custom.{key} so they land in the catch-all JSON
    // field that downstream consumers (trace-query) read.
    for (key, value) in tags.iter() {
        let path = format!("custom.{key}");

        if trace.get(path.as_str()).is_none() {
            trace.insert(path.as_str(), value);
        }
    }
    // Populate the host_id fast field if the metadata service returned one
    // and the span doesn't already carry a non-zero value.
    if let Some(host_id) = tags.host_id
        && trace
            .get("host_id")
            .and_then(|v| v.as_integer())
            .map(|v| v == 0)
            .unwrap_or(true)
    {
        trace.insert("host_id", host_id);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use vector::event::{
        Event, LogEvent, Metric, MetricKind, MetricTags, MetricValue, TraceEvent, Value,
    };

    use super::*;
    use crate::host_tags::HostTagsEntry;
    use crate::unix_timestamp::UnixTimestamp;

    fn make_store(ttl: Duration) -> Arc<HostTagsStore> {
        let store = Arc::new(HostTagsStore::default());
        let expires_at = UnixTimestamp::now() + ttl;
        let mut entries = HashMap::new();
        entries.insert(
            "web-01".to_string(),
            HostTagsEntry {
                tags: vec![
                    ("env".to_string(), "prod".to_string()),
                    ("region".to_string(), "us-east-1".to_string()),
                ]
                .into(),
                host_id: None,
                expires_at,
            },
        );
        entries.insert(
            "db-01".to_string(),
            HostTagsEntry {
                tags: vec![("env".to_string(), "staging".to_string())].into(),
                host_id: None,
                expires_at,
            },
        );
        store.store(entries);
        store
    }

    fn make_transform(store: Arc<HostTagsStore>) -> AddHostTags {
        AddHostTags {
            store,
            collector: UnknownHostsCollector::default(),
        }
    }

    fn run_transform(store: Arc<HostTagsStore>, event: Event) -> Vec<Event> {
        let mut transform = make_transform(store);
        let mut output = OutputBuffer::with_capacity(1);
        transform.transform(&mut output, event);
        output.into_events().collect()
    }

    // --- Metric tests ---

    fn make_metric(name: &str, tags: &[(&str, &str)]) -> Event {
        let mut metric_tags = MetricTags::default();
        for (key, value) in tags {
            metric_tags.insert(key.to_string(), value.to_string());
        }
        Event::Metric(
            Metric::new(
                name,
                MetricKind::Absolute,
                MetricValue::Gauge { value: 1.0 },
            )
            .with_tags(Some(metric_tags)),
        )
    }

    #[test]
    fn test_metric_host_tags_added() {
        let store = make_store(Duration::from_secs(3600));
        let event = make_metric("cpu.usage", &[("host", "web-01"), ("service", "api")]);
        let events = run_transform(store, event);
        let m = events[0].as_metric();
        assert_eq!(m.tag_value("env").as_deref(), Some("prod"));
        assert_eq!(m.tag_value("region").as_deref(), Some("us-east-1"));
        // Original tags preserved.
        assert_eq!(m.tag_value("host").as_deref(), Some("web-01"));
        assert_eq!(m.tag_value("service").as_deref(), Some("api"));
    }

    #[test]
    fn test_metric_existing_tag_not_overwritten() {
        let store = make_store(Duration::from_secs(3600));
        let event = make_metric("cpu.usage", &[("host", "web-01"), ("env", "canary")]);
        let events = run_transform(store, event);
        let m = events[0].as_metric();
        assert_eq!(m.tag_value("env").as_deref(), Some("canary"));
        assert_eq!(m.tag_value("region").as_deref(), Some("us-east-1"));
    }

    #[test]
    fn test_metric_unknown_host_reported_to_collector() {
        let store = make_store(Duration::from_secs(3600));
        let collector = UnknownHostsCollector::default();
        let mut transform = AddHostTags {
            store,
            collector: collector.clone(),
        };
        let event = make_metric("cpu.usage", &[("host", "unknown-42")]);
        let mut output = OutputBuffer::with_capacity(1);
        transform.transform(&mut output, event);

        // Event passes through without added tags.
        let events: Vec<_> = output.into_events().collect();
        assert!(events[0].as_metric().tag_value("env").is_none());

        // The unknown host was reported.
        let drained = collector.drain(10);
        assert_eq!(drained, vec!["unknown-42".to_string()]);
    }

    #[test]
    fn test_metric_expired_host_tags_applied_and_requeued() {
        let store = make_store(Duration::ZERO);
        let collector = UnknownHostsCollector::default();
        let mut transform = AddHostTags {
            store,
            collector: collector.clone(),
        };
        let event = make_metric("cpu.usage", &[("host", "web-01")]);
        let mut output = OutputBuffer::with_capacity(1);
        transform.transform(&mut output, event);

        let events: Vec<_> = output.into_events().collect();
        let m = events[0].as_metric();
        // Stale tags are still applied.
        assert_eq!(m.tag_value("env").as_deref(), Some("prod"));
        assert_eq!(m.tag_value("region").as_deref(), Some("us-east-1"));
        // Host is also re-queued for refresh.
        let drained = collector.drain(10);
        assert_eq!(drained, vec!["web-01".to_string()]);
    }

    // --- Log tests ---

    fn make_log(hostname: Option<&str>) -> Event {
        let mut log = LogEvent::default();
        log.insert("message", "something happened");
        if let Some(host) = hostname {
            log.insert("hostname", host);
        }
        Event::Log(log)
    }

    #[test]
    fn test_log_host_tags_added() {
        let store = make_store(Duration::from_secs(3600));
        let event = make_log(Some("web-01"));
        let events = run_transform(store, event);
        let log = events[0].as_log();
        assert_eq!(log.get("tags.env"), Some(&Value::from("prod")));
        assert_eq!(log.get("tags.region"), Some(&Value::from("us-east-1")));
    }

    #[test]
    fn test_log_existing_tag_not_overwritten() {
        let store = make_store(Duration::from_secs(3600));
        let mut log = LogEvent::default();
        log.insert("hostname", "web-01");
        log.insert("tags.env", "canary");
        let events = run_transform(store, Event::Log(log));
        let log = events[0].as_log();
        assert_eq!(log.get("tags.env"), Some(&Value::from("canary")));
        assert_eq!(log.get("tags.region"), Some(&Value::from("us-east-1")));
    }

    #[test]
    fn test_log_unknown_host_reported_to_collector() {
        let store = make_store(Duration::from_secs(3600));
        let collector = UnknownHostsCollector::default();
        let mut transform = AddHostTags {
            store,
            collector: collector.clone(),
        };
        let event = make_log(Some("unknown-42"));
        let mut output = OutputBuffer::with_capacity(1);
        transform.transform(&mut output, event);

        let drained = collector.drain(10);
        assert_eq!(drained, vec!["unknown-42".to_string()]);
    }

    #[test]
    fn test_log_expired_host_tags_applied_and_requeued() {
        let store = make_store(Duration::ZERO);
        let collector = UnknownHostsCollector::default();
        let mut transform = AddHostTags {
            store,
            collector: collector.clone(),
        };
        let event = make_log(Some("web-01"));
        let mut output = OutputBuffer::with_capacity(1);
        transform.transform(&mut output, event);

        let events: Vec<_> = output.into_events().collect();
        let log = events[0].as_log();
        // Stale tags are still applied.
        assert_eq!(log.get("tags.env"), Some(&Value::from("prod")));
        // Host is also re-queued for refresh.
        let drained = collector.drain(10);
        assert_eq!(drained, vec!["web-01".to_string()]);
    }

    // --- Trace tests ---

    fn make_trace(hostname: Option<&str>) -> Event {
        let mut trace = TraceEvent::default();
        trace.insert("service", "my-svc");
        trace.insert("name", "http.request");
        if let Some(host) = hostname {
            trace.insert("host", host);
        }
        Event::Trace(trace)
    }

    #[test]
    fn test_trace_host_tags_added() {
        let store = make_store(Duration::from_secs(3600));
        let event = make_trace(Some("web-01"));
        let events = run_transform(store, event);
        let trace = events[0].as_trace();
        assert_eq!(trace.get("custom.env"), Some(&Value::from("prod")));
        assert_eq!(trace.get("custom.region"), Some(&Value::from("us-east-1")));
    }

    #[test]
    fn test_trace_existing_meta_not_overwritten() {
        let store = make_store(Duration::from_secs(3600));
        let mut trace = TraceEvent::default();
        trace.insert("host", "web-01");
        trace.insert("custom.env", "canary");
        let events = run_transform(store, Event::Trace(trace));
        let trace = events[0].as_trace();
        assert_eq!(trace.get("custom.env"), Some(&Value::from("canary")));
        assert_eq!(trace.get("custom.region"), Some(&Value::from("us-east-1")));
    }

    #[test]
    fn test_trace_unknown_host_reported_to_collector() {
        let store = make_store(Duration::from_secs(3600));
        let collector = UnknownHostsCollector::default();
        let mut transform = AddHostTags {
            store,
            collector: collector.clone(),
        };
        let event = make_trace(Some("unknown-42"));
        let mut output = OutputBuffer::with_capacity(1);
        transform.transform(&mut output, event);

        let drained = collector.drain(10);
        assert_eq!(drained, vec!["unknown-42".to_string()]);
    }

    #[test]
    fn test_trace_expired_host_tags_applied_and_requeued() {
        let store = make_store(Duration::ZERO);
        let collector = UnknownHostsCollector::default();
        let mut transform = AddHostTags {
            store,
            collector: collector.clone(),
        };
        let event = make_trace(Some("web-01"));
        let mut output = OutputBuffer::with_capacity(1);
        transform.transform(&mut output, event);

        let events: Vec<_> = output.into_events().collect();
        let trace = events[0].as_trace();
        // Stale tags are still applied.
        assert_eq!(trace.get("custom.env"), Some(&Value::from("prod")));
        // Host is also re-queued for refresh.
        let drained = collector.drain(10);
        assert_eq!(drained, vec!["web-01".to_string()]);
    }
}
