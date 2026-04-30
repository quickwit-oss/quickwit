# Metrics-RS Macro Migration Plan

## Summary
Replace Quickwit’s current `prometheus` crate-based metrics with the metricspp API directly. The new implementation lives inside `quickwit-common` as `quickwit_common::metrics`, not as a new Cargo crate, and call sites are migrated to `counter!`, `gauge!`, and `histogram!`.

Metric names stay compatible as `quickwit_{subsystem}_{name}` so existing dashboards and `/metrics` users keep working.

## Design Authority
The proposed design in `~/go/src/github.com/DataDog/experimental/users/luca.cominardi/metricspp` is the reference architecture for this migration. Quickwit code should be updated to follow that design, including its API shape, macro behavior, metadata model, observable metric behavior, histogram bucket handling, and exporter setup.

When existing Quickwit metrics code conflicts with the metricspp design, prefer adapting the existing Quickwit code to the metricspp design. Do not reshape the metricspp approach around Quickwit’s current `prometheus` crate patterns, constructors, or historical implementation details unless there is a concrete Quickwit constraint that makes the proposed design impossible to apply.

## Key Changes
- Port the metricspp design from `~/go/src/github.com/DataDog/experimental/users/luca.cominardi/metricspp` into `quickwit-common/src/metrics/`: typed `Counter`, `Gauge`, `Histogram`, `GaugeGuard`, observable shadows, inventory metadata, histogram bucket registry, and `describe_metrics()`.
- Add dependencies needed by metricspp: `metrics-util`, `metrics-exporter-prometheus`, `metrics-exporter-otel`, `inventory`, `const_format`, and `atomic_float`.
- Replace existing `new_counter`, `new_gauge`, `new_histogram`, and `*Vec` declarations across Quickwit with static `LazyLock` metrics declared through metricspp macros.
- Replace metric operations with the new API:
  - counters: `.increment(n)` or `.absolute(n)`
  - gauges: `.increment(n)`, `.decrement(n)`, `.set(n)`
  - histograms: `.record(value)`
  - labeled metrics: `counter!(parent: BASE, "label" => value)`, etc.
- Use `observable: true` only where production code or tests currently read values through `.get()`.

## Exporters
- Install one global metrics-rs recorder during CLI startup.
- Always include a Prometheus recorder and retain Quickwit’s existing `/metrics` route by rendering the stored `PrometheusHandle`.
- Configure Prometheus and OTLP histogram buckets from `metricspp::histogram_buckets()` before metrics are first used.
- Add OTLP metrics export behind `QW_ENABLE_OPENTELEMETRY_OTLP_EXPORTER=true`, using the existing OTLP protocol env behavior:
  - `OTEL_EXPORTER_OTLP_METRICS_PROTOCOL`
  - fallback to `OTEL_EXPORTER_OTLP_PROTOCOL`
  - support `grpc`, `http/protobuf`, and `http/json`
- Fan out to Prometheus, OTLP, and the existing DogStatsD/invariant recorder path where applicable.

## Migration Work
- For every migrated module, start from the metricspp design and ask how the existing Quickwit code should change to match it, not how metricspp should be adjusted to preserve the old Quickwit implementation.
- Convert metric modules in `quickwit-serve`, `quickwit-search`, `quickwit-indexing`, `quickwit-ingest`, `quickwit-storage`, `quickwit-opentelemetry`, `quickwit-jaeger`, `quickwit-cluster`, `quickwit-actors`, and `quickwit-common`.
- Remove direct `prometheus` usage from Quickwit-owned code, including tower/circuit-breaker helpers.
- Replace `register_info("build_info", ...)` with a metricspp-declared counter/info metric set to `1` with build labels.
- Update docs/comments mentioning Prometheus crate semantics to describe metrics-rs plus Prometheus rendering.

## Test Plan
- Port metricspp tests for counters, gauges, histograms, parent labels, observable metrics, guards, and histogram bucket inventory.
- Add Quickwit integration tests for `/metrics` output: metric names, labels, descriptions, and histogram buckets.
- Add OTLP metrics tests with an in-memory OpenTelemetry exporter.
- Update tests that assert metric values to use `observable: true` declarations or `metrics_util::debugging::DebuggingRecorder`.
- Run:
  - `cargo test -p quickwit-common metrics`
  - `cargo test -p quickwit-serve metrics_api`
  - `cargo test -p quickwit-cli logger`
  - `cargo clippy --workspace --tests --all-features`
  - `make fmt`

## Assumptions
- `quickeit-metrics` means the Quickwit metrics module under `quickwit_common::metrics`.
- A broad call-site migration is acceptable; preserving old constructor/type APIs is explicitly out of scope.
- Existing Prometheus metric names must remain stable.
- The existing Quickwit `metrics` Cargo feature for metrics ingestion is unrelated and unchanged.
