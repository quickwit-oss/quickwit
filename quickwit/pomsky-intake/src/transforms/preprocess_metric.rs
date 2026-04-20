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

use serde::{Deserialize, Serialize};
use vector::config::{
    DataType, GenerateConfig, Input, OutputId, TransformConfig, TransformContext, TransformOutput,
};
use vector::event::{Event, Metric};
use vector::schema::Definition;
use vector::transforms::{FunctionTransform, OutputBuffer, Transform};
use vector_lib::config::clone_input_definitions;

// Well-known tag names the Arrow schema has dedicated columns for.
pub(crate) const TAG_SERVICE: &str = "service";
pub(crate) const TAG_ENV: &str = "env";
pub(crate) const TAG_DATACENTER: &str = "datacenter";
pub(crate) const TAG_REGION: &str = "region";
pub(crate) const TAG_HOST: &str = "host";

/// Preprocesses metric events before indexing. Dispatches to a
/// source-specific handler based on the event's `source_type` metadata.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PreprocessMetricConfig;

impl vector_lib::configurable::NamedComponent for PreprocessMetricConfig {
    fn get_component_name(&self) -> &'static str {
        "preprocess_metric"
    }
}

impl GenerateConfig for PreprocessMetricConfig {
    fn generate_config() -> toml::Value {
        toml::Value::Table(Default::default())
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "preprocess_metric")]
impl TransformConfig for PreprocessMetricConfig {
    async fn build(&self, _context: &TransformContext) -> vector::Result<Transform> {
        Ok(Transform::function(PreprocessMetric))
    }

    fn input(&self) -> Input {
        Input::metric()
    }

    fn outputs(
        &self,
        _: &TransformContext,
        input_definitions: &[(OutputId, Definition)],
    ) -> Vec<TransformOutput> {
        vec![TransformOutput::new(
            DataType::Metric,
            clone_input_definitions(input_definitions),
        )]
    }

    fn enable_concurrency(&self) -> bool {
        true
    }
}

#[derive(Clone)]
struct PreprocessMetric;

impl FunctionTransform for PreprocessMetric {
    fn transform(&mut self, output: &mut OutputBuffer, mut event: Event) {
        if let Event::Metric(ref mut metric) = event {
            let source_type = metric.metadata().source_type().unwrap_or("unknown");
            match source_type {
                "datadog_agent" => preprocess_datadog_metric(metric),
                "opentelemetry" => preprocess_otlp_metric(metric),
                _ => {}
            }
        }
        output.push(event);
    }
}

/// Datadog agent metrics already use standard tag names (`service`, `env`,
/// `host`, `region`, `datacenter`). No renaming needed.
fn preprocess_datadog_metric(_metric: &mut Metric) {
    // DD tags are already in the expected format.
}

/// OTel metrics store resource attributes with `resource.` prefix and use
/// different naming conventions. Normalize to the standard tag names so the
/// Arrow sink can extract them uniformly.
fn preprocess_otlp_metric(metric: &mut Metric) {
    // OTel → standard tag mappings.
    let mappings: &[(&str, &str)] = &[
        ("resource.service.name", TAG_SERVICE),
        ("resource.deployment.environment", TAG_ENV),
        ("resource.host.name", TAG_HOST),
        ("resource.cloud.region", TAG_REGION),
        ("resource.cloud.availability_zone", TAG_DATACENTER),
    ];
    for &(otel_key, standard_key) in mappings {
        if let Some(value) = metric.remove_tag(otel_key) {
            // Only set if the standard tag isn't already present.
            if metric.tag_value(standard_key).is_none() {
                metric.replace_tag(standard_key.to_string(), value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use vector::event::{Event, Metric, MetricKind, MetricTags, MetricValue};

    use super::*;

    fn run_transform(event: Event) -> Vec<Event> {
        let mut transform = PreprocessMetric;
        let mut output = OutputBuffer::with_capacity(1);
        transform.transform(&mut output, event);
        output.into_events().collect()
    }

    fn make_dd_metric(name: &str, tags: &[(&str, &str)]) -> Event {
        let mut metric_tags = MetricTags::default();
        for (k, v) in tags {
            metric_tags.insert(k.to_string(), v.to_string());
        }
        let mut metric = Metric::new(
            name,
            MetricKind::Absolute,
            MetricValue::Gauge { value: 1.0 },
        )
        .with_tags(Some(metric_tags));
        metric
            .metadata_mut()
            .set_source_type("datadog_agent".to_string());
        Event::Metric(metric)
    }

    fn make_otel_metric(name: &str, tags: &[(&str, &str)]) -> Event {
        let mut metric_tags = MetricTags::default();
        for (k, v) in tags {
            metric_tags.insert(k.to_string(), v.to_string());
        }
        let mut metric = Metric::new(
            name,
            MetricKind::Absolute,
            MetricValue::Gauge { value: 1.0 },
        )
        .with_tags(Some(metric_tags));
        metric
            .metadata_mut()
            .set_source_type("opentelemetry".to_string());
        Event::Metric(metric)
    }

    #[test]
    fn test_datadog_tags_preserved_as_is() {
        let event = make_dd_metric(
            "cpu",
            &[("service", "web"), ("env", "prod"), ("host", "h1")],
        );
        let events = run_transform(event);
        let m = events[0].as_metric();
        assert_eq!(m.tag_value("service").as_deref(), Some("web"));
        assert_eq!(m.tag_value("env").as_deref(), Some("prod"));
        assert_eq!(m.tag_value("host").as_deref(), Some("h1"));
    }

    #[test]
    fn test_otel_resource_tags_normalized() {
        let event = make_otel_metric(
            "http.requests",
            &[
                ("resource.service.name", "api-gw"),
                ("resource.deployment.environment", "staging"),
                ("resource.host.name", "otel-host-1"),
                ("resource.cloud.region", "us-east-1"),
                ("resource.cloud.availability_zone", "us-east-1a"),
                ("custom.attr", "keep-me"),
            ],
        );
        let events = run_transform(event);
        let m = events[0].as_metric();
        assert_eq!(m.tag_value("service").as_deref(), Some("api-gw"));
        assert_eq!(m.tag_value("env").as_deref(), Some("staging"));
        assert_eq!(m.tag_value("host").as_deref(), Some("otel-host-1"));
        assert_eq!(m.tag_value("region").as_deref(), Some("us-east-1"));
        assert_eq!(m.tag_value("datacenter").as_deref(), Some("us-east-1a"));
        // Original OTel keys removed.
        assert!(m.tag_value("resource.service.name").is_none());
        // Non-mapped tags preserved.
        assert_eq!(m.tag_value("custom.attr").as_deref(), Some("keep-me"));
    }

    #[test]
    fn test_otel_does_not_overwrite_existing_standard_tag() {
        let event = make_otel_metric(
            "http.requests",
            &[
                ("service", "already-set"),
                ("resource.service.name", "otel-service"),
            ],
        );
        let events = run_transform(event);
        let m = events[0].as_metric();
        assert_eq!(m.tag_value("service").as_deref(), Some("already-set"));
    }
}
