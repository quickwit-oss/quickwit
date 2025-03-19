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

use regex::Regex;

#[derive(Debug, PartialEq, Clone)]
pub struct PrometheusMetric {
    pub name: String,
    pub labels: HashMap<String, String>,
    pub metric_value: f64,
}

/// Parse Prometheus metrics serialized with prometheus::TextEncoder
///
/// Unfortunately, the prometheus crate does not provide a way to parse metrics
pub fn parse_prometheus_metrics(input: &str) -> Vec<PrometheusMetric> {
    let mut metrics = Vec::new();
    let re = Regex::new(r"(?P<name>[^{]+)(?:\{(?P<labels>[^\}]*)\})? (?P<value>.+)").unwrap();

    for line in input.lines() {
        if line.starts_with('#') {
            continue;
        }

        if let Some(caps) = re.captures(line) {
            let name = caps.name("name").unwrap().as_str().to_string();
            let metric_value: f64 = caps
                .name("value")
                .unwrap()
                .as_str()
                .parse()
                .expect("Failed to parse value");

            let labels = caps.name("labels").map_or(HashMap::new(), |m| {
                m.as_str()
                    .split(',')
                    .map(|label| {
                        let mut parts = label.splitn(2, '=');
                        let key = parts.next().unwrap().to_string();
                        let value = parts.next().unwrap().trim_matches('"').to_string();
                        (key, value)
                    })
                    .collect()
            });

            metrics.push(PrometheusMetric {
                name,
                labels,
                metric_value,
            });
        }
    }

    metrics
}

/// Filter metrics by name and a subset of the available labels
///
/// Specify an empty Vec of labels to return all metrics with the specified name
pub fn filter_metrics(
    metrics: &[PrometheusMetric],
    name: &str,
    labels: Vec<(&'static str, &'static str)>,
) -> Vec<PrometheusMetric> {
    metrics
        .iter()
        .filter(|metric| metric.name == name)
        .filter(|metric| {
            labels
                .iter()
                .all(|(key, value)| metric.labels.get(*key).map(String::as_str) == Some(*value))
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_INPUT: &str = r#"
quickwit_search_leaf_search_single_split_warmup_num_bytes_sum 0
# HELP quickwit_storage_object_storage_request_duration_seconds Duration of object storage requests in seconds.
# TYPE quickwit_storage_object_storage_request_duration_seconds histogram
quickwit_storage_object_storage_request_duration_seconds_bucket{action="delete_objects",le="30"} 0
quickwit_storage_object_storage_request_duration_seconds_bucket{action="delete_objects",le="+Inf"} 0
quickwit_storage_object_storage_request_duration_seconds_sum{action="delete_objects"} 0
quickwit_search_root_search_request_duration_seconds_sum{kind="server",status="success"} 0.004093958
quickwit_storage_object_storage_requests_total{action="delete_object"} 0
quickwit_storage_object_storage_requests_total{action="delete_objects"} 0
"#;

    #[test]
    fn test_parse_prometheus_metrics() {
        let metrics = parse_prometheus_metrics(TEST_INPUT);
        assert_eq!(metrics.len(), 7);
        assert_eq!(
            metrics[0],
            PrometheusMetric {
                name: "quickwit_search_leaf_search_single_split_warmup_num_bytes_sum".to_string(),
                labels: HashMap::new(),
                metric_value: 0.0,
            }
        );
        assert_eq!(
            metrics[1],
            PrometheusMetric {
                name: "quickwit_storage_object_storage_request_duration_seconds_bucket".to_string(),
                labels: [
                    ("action".to_string(), "delete_objects".to_string()),
                    ("le".to_string(), "30".to_string())
                ]
                .iter()
                .cloned()
                .collect(),
                metric_value: 0.0,
            }
        );
        assert_eq!(
            metrics[2],
            PrometheusMetric {
                name: "quickwit_storage_object_storage_request_duration_seconds_bucket".to_string(),
                labels: [
                    ("action".to_string(), "delete_objects".to_string()),
                    ("le".to_string(), "+Inf".to_string())
                ]
                .iter()
                .cloned()
                .collect(),
                metric_value: 0.0,
            }
        );
        assert_eq!(
            metrics[3],
            PrometheusMetric {
                name: "quickwit_storage_object_storage_request_duration_seconds_sum".to_string(),
                labels: [("action".to_string(), "delete_objects".to_string())]
                    .iter()
                    .cloned()
                    .collect(),
                metric_value: 0.0,
            }
        );
        assert_eq!(
            metrics[4],
            PrometheusMetric {
                name: "quickwit_search_root_search_request_duration_seconds_sum".to_string(),
                labels: [
                    ("kind".to_string(), "server".to_string()),
                    ("status".to_string(), "success".to_string())
                ]
                .iter()
                .cloned()
                .collect(),
                metric_value: 0.004093958,
            }
        );
        assert_eq!(
            metrics[5],
            PrometheusMetric {
                name: "quickwit_storage_object_storage_requests_total".to_string(),
                labels: [("action".to_string(), "delete_object".to_string())]
                    .iter()
                    .cloned()
                    .collect(),
                metric_value: 0.0,
            }
        );
        assert_eq!(
            metrics[6],
            PrometheusMetric {
                name: "quickwit_storage_object_storage_requests_total".to_string(),
                labels: [("action".to_string(), "delete_objects".to_string())]
                    .iter()
                    .cloned()
                    .collect(),
                metric_value: 0.0,
            }
        );
    }

    #[test]
    fn test_filter_prometheus_metrics() {
        let metrics = parse_prometheus_metrics(TEST_INPUT);
        {
            let filtered_metric = filter_metrics(
                &metrics,
                "quickwit_storage_object_storage_request_duration_seconds_bucket",
                vec![],
            );
            assert_eq!(filtered_metric.len(), 2);
        }
        {
            let filtered_metric = filter_metrics(
                &metrics,
                "quickwit_search_root_search_request_duration_seconds_sum",
                vec![("status", "success")],
            );
            assert_eq!(filtered_metric.len(), 1);
        }
        {
            let filtered_metric =
                filter_metrics(&metrics, "quickwit_doest_not_exist_metric", vec![]);
            assert_eq!(filtered_metric.len(), 0);
        }
        {
            let filtered_metric = filter_metrics(
                &metrics,
                "quickwit_storage_object_storage_requests_total",
                vec![("does_not_exist_label", "value")],
            );
            assert_eq!(filtered_metric.len(), 0);
        }
        {
            let filtered_metric = filter_metrics(
                &metrics,
                "quickwit_storage_object_storage_requests_total",
                vec![("action", "does_not_exist_value")],
            );
            assert_eq!(filtered_metric.len(), 0);
        }
    }
}
