/// Returns whether the given index ID corresponds to a metrics index.
///
/// Metrics indexes use the Parquet/DataFusion pipeline instead of the Tantivy pipeline.
/// An index is considered a metrics index if it starts with "otel-metrics" or "metrics-".
pub fn is_metrics_index(index_id: &str) -> bool {
    index_id.starts_with("otel-metrics") || index_id.starts_with("metrics-")
}

#[cfg(test)]
mod tests {
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
}
