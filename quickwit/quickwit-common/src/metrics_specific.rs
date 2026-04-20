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

/// Returns whether the given index ID corresponds to a metrics index.
///
/// Metrics indexes use the Parquet/DataFusion pipeline instead of the Tantivy pipeline.
/// An index is considered a metrics index if it starts with "otel-metrics" or "metrics-".
pub fn is_metrics_index(index_id: &str) -> bool {
    index_id.starts_with("otel-metrics") || index_id.starts_with("metrics-")
}

/// Returns whether the given index ID corresponds to a sketches index.
///
/// Sketches indexes use the Parquet/DataFusion pipeline with sketch-specific
/// processors and writers.
pub fn is_sketches_index(index_id: &str) -> bool {
    index_id.starts_with("sketches-")
}

/// Returns whether the given index ID uses the Parquet/DataFusion pipeline.
pub fn is_parquet_pipeline_index(index_id: &str) -> bool {
    is_metrics_index(index_id) || is_sketches_index(index_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_metrics_index() {
        // OpenTelemetry metrics indexes
        assert!(is_metrics_index("otel-metrics-v0_7"));
        assert!(is_metrics_index("otel-metrics"));
        assert!(is_metrics_index("otel-metrics-custom"));

        // Generic metrics indexes
        assert!(is_metrics_index("metrics-default"));
        assert!(is_metrics_index("metrics-"));
        assert!(is_metrics_index("metrics-my-app"));

        // Non-metrics indexes
        assert!(!is_metrics_index("otel-logs-v0_7"));
        assert!(!is_metrics_index("otel-traces-v0_7"));
        assert!(!is_metrics_index("my-index"));
        assert!(!is_metrics_index("logs-default"));
        assert!(!is_metrics_index("metrics")); // No hyphen after "metrics"
        assert!(!is_metrics_index("my-metrics-index")); // Not prefixed
    }

    #[test]
    fn test_is_sketches_index() {
        assert!(is_sketches_index("sketches-default"));
        assert!(!is_sketches_index("otel-metrics"));
        assert!(!is_sketches_index("my-index"));
    }

    #[test]
    fn test_is_parquet_pipeline_index() {
        assert!(is_parquet_pipeline_index("otel-metrics"));
        assert!(is_parquet_pipeline_index("sketches-default"));
        assert!(!is_parquet_pipeline_index("otel-logs-v0_7"));
        assert!(!is_parquet_pipeline_index("my-index"));
    }
}
