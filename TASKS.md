# Metrics-RS Macro Migration Tasks

This task list breaks `PLAN.md` into reviewable sections. Each section should be implemented and reviewed independently unless its prerequisites say otherwise.

## Section 0: Ground Rules And Compatibility Targets

Status: Done.

Goal: make the migration safe to review by preserving externally visible behavior.

Tasks:
- [x] Preserve metric names as `quickwit_{subsystem}_{name}`.
- [x] Preserve empty-subsystem names as `quickwit_{name}`, without a double underscore.
- [x] Preserve existing label names and label values.
- [x] Preserve `/metrics` as the Prometheus text endpoint.
- [x] Do not preserve the old `new_counter`, `new_gauge`, `new_histogram`, or `*Vec` APIs as compatibility shims unless a later section proves a temporary shim is needed for incremental compilation.
- [x] Treat Quickwit metric ingestion features and metrics index logic as unrelated to this migration.

Review checklist:
- [x] No compatibility target is contradicted by later implementation sections.
- [x] Any intentional metric name, label, or bucket change is explicitly called out in the PR.

## Section 1: Add Workspace Dependencies

Status: Done.

Goal: add the metrics-rs exporter and metricspp support dependencies without changing behavior.

Tasks:
- [x] Add workspace dependencies in `quickwit/Cargo.toml`:
  - [x] `metrics-util`
  - [x] `metrics-exporter-prometheus`
  - [x] `metrics-exporter-otel`
  - [x] `inventory`
  - [x] `const_format`
  - [x] `atomic_float`
- [x] Add the needed dependencies to `quickwit/quickwit-common/Cargo.toml`.
- [x] Add exporter dependencies to the crate that installs recorders, expected to be `quickwit-cli`.
- [x] Keep `prometheus` temporarily until all direct usages are migrated.
- [x] Run a dependency update/build check to refresh `quickwit/Cargo.lock`.

Validation:
- [x] `cargo check -p quickwit-common`
- [x] `cargo check -p quickwit-cli`

Review checklist:
- [x] Dependency versions match the existing workspace style.
- [x] New dependencies are only added to crates that use them.

## Section 2: Port Metricspp Core Into quickwit-common

Goal: implement the new `quickwit_common::metrics` API while keeping the scope local to `quickwit-common`.

Tasks:
- Move the current `quickwit/quickwit-common/src/metrics.rs` implementation into a module layout such as `quickwit/quickwit-common/src/metrics/`.
- Port metricspp core types:
  - `Counter`
  - `Gauge`
  - `Histogram`
  - `GaugeGuard`
  - observable `CounterShadow` and `GaugeShadow`
  - `MetricInfo`
  - `HistogramConfig`
- Port macros:
  - `counter!`
  - `gauge!`
  - `histogram!`
  - hidden helper macros for key names, metadata, and label counts.
- Keep the public module path as `quickwit_common::metrics`.
- Implement `SYSTEM = "quickwit"`.
- Implement `describe_metrics()`.
- Implement `metrics_info()` and `histogram_buckets()` for inventory introspection.
- Ensure the macros work for:
  - base metrics with no labels
  - base metrics with static labels
  - parent metrics with dynamic label values
  - nested parent extension
  - observable counters and gauges
- Add or keep bucket helper functions if call sites still rely on `linear_buckets` and `exponential_buckets`.

Quickwit-specific adjustments:
- Extend `GaugeGuard` or add an equivalent guard so existing in-flight byte/count use cases can add and subtract variable deltas over the guard lifetime.
- Decide how to handle `OwnedGaugeGuard` use sites. Prefer adapting the new guard to cover the same behavior instead of keeping a Prometheus-specific type.
- Provide a histogram timer helper if needed by existing `start_timer()` call sites, or plan those call sites for manual `Instant` plus `record()`.

Validation:
- `cargo test -p quickwit-common metrics`

Review checklist:
- The port is inside `quickwit-common`; no new Cargo crate is introduced.
- The API does not expose Prometheus crate types.
- `observable: true` is opt-in and only affects counters/gauges.

## Section 3: Port Metricspp Unit Tests

Goal: validate the new metrics primitives before migrating call sites.

Tasks:
- Port metricspp tests into `quickwit-common`.
- Cover:
  - counter increment and absolute values
  - gauge set/increment/decrement
  - histogram record
  - static labels
  - parent labels
  - dynamic parent labels
  - nested parent extension
  - observable counter and gauge `get()`
  - non-observable sentinel values
  - `GaugeGuard`
  - histogram bucket inventory
  - `describe_metrics()`
- Use `metrics_util::debugging::DebuggingRecorder` for value assertions where possible.

Validation:
- `cargo test -p quickwit-common metrics`

Review checklist:
- Tests assert key names include the `quickwit` prefix.
- Tests cover empty subsystem behavior.
- Tests do not depend on the Prometheus crate registry.

## Section 4: Install Global Metrics Recorder And Prometheus Handle

Goal: install one global metrics-rs recorder during CLI startup and keep `/metrics` working.

Tasks:
- Replace Prometheus crate registration/gathering with a metrics-rs recorder setup.
- Install a Prometheus recorder in `quickwit-cli` startup.
- Store a `metrics_exporter_prometheus::PrometheusHandle` somewhere accessible to `quickwit_common::metrics::metrics_text_payload()` or directly to `quickwit-serve`'s metrics handler.
- Configure Prometheus histogram buckets from `quickwit_common::metrics::histogram_buckets()` before metrics are first used.
- Call `quickwit_common::metrics::describe_metrics()` after installing the recorder.
- Keep the existing `/metrics` route in `quickwit-serve`.
- Replace `quickwit_common::metrics::metrics_text_payload()` internals with Prometheus handle rendering.

Ordering risk:
- Metrics declared through `LazyLock` register on first access. The global recorder must be installed before production metrics are first accessed. Check current startup order around runtime metrics, build info metrics, jemalloc metrics, and CLI setup.

Validation:
- `cargo test -p quickwit-serve metrics_api`
- A manual or integration scrape returns Prometheus text.

Review checklist:
- There is exactly one global recorder installation path in production startup.
- `/metrics` does not call `prometheus::gather()`.
- Histogram buckets are configured before metrics registration.

## Section 5: Preserve DogStatsD And Invariant Metrics

Goal: keep existing DogStatsD and invariant behavior while using the new recorder path.

Tasks:
- Replace direct `metrics_exporter_dogstatsd::DogStatsDBuilder::install()` if it conflicts with the single global recorder.
- Fan out metrics to:
  - Prometheus
  - DogStatsD
  - optional OTLP metrics
  - the existing invariant recorder path
- Keep existing DogStatsD global labels and prefix behavior.
- Update invariant metrics from raw `metrics::counter!` calls only if needed to fit the fanout recorder.

Validation:
- `cargo test -p quickwit-cli logger`
- Existing invariant tests, if any, still pass.

Review checklist:
- DogStatsD is not silently disabled.
- Existing global labels are preserved.
- Recorder fanout does not double-record any metric.

## Section 6: Add Optional OTLP Metrics Export

Goal: add OTLP metrics export behind the existing telemetry flag.

Tasks:
- Enable OTLP metrics only when `QW_ENABLE_OPENTELEMETRY_OTLP_EXPORTER=true`.
- Read `OTEL_EXPORTER_OTLP_METRICS_PROTOCOL`.
- Fall back to `OTEL_EXPORTER_OTLP_PROTOCOL`.
- Support:
  - `grpc`
  - `http/protobuf`
  - `http/json`
- Reuse the existing protocol parsing style from tracing/log export where possible.
- Configure OTLP histogram buckets from `quickwit_common::metrics::histogram_buckets()`.
- Add in-memory exporter tests if the dependency stack supports it cleanly.

Validation:
- `cargo test -p quickwit-cli logger`
- OTLP metrics test with in-memory or local test exporter.

Review checklist:
- OTLP traces/logs behavior remains unchanged.
- Metrics protocol env vars do not affect trace/log protocol selection.
- Unsupported protocol errors are clear.

## Section 7: Replace Build Info Metric

Goal: replace `register_info("build_info", ...)` with the new metrics API.

Tasks:
- Remove `register_info` usage from `quickwit-cli`.
- Declare a build info counter metric with build labels.
- Set the metric to `1` using the new counter API.
- Preserve the Prometheus output shape as closely as possible:
  - name: `quickwit_build_info`
  - labels: build date, commit hash, version, optional tags, target
  - value: `1`
- Account for dynamic label values from `BuildInfo`.

Validation:
- `/metrics` output contains `quickwit_build_info`.
- `cargo test -p quickwit-cli logger`

Review checklist:
- Build labels match the previous labels.
- The metric is registered after the global recorder is installed.

## Section 8: Migrate quickwit-common Internal Metrics

Goal: convert `quickwit-common` call sites and helpers to the new API.

Tasks:
- Convert `quickwit-common/src/metrics.rs` consumers to the new module layout.
- Convert `MEMORY_METRICS` and in-flight gauges.
- Convert `quickwit-common/src/tower/metrics.rs`.
- Convert `quickwit-common/src/tower/circuit_breaker.rs`.
- Convert `quickwit-common/src/thread_pool.rs`.
- Convert `quickwit-common/src/stream_utils.rs`.
- Remove direct Prometheus crate imports from `quickwit-common`.

Special cases:
- Tests in tower metrics currently read `.get()` from counters. Mark those test metrics `observable: true` or use `DebuggingRecorder`.
- In-flight data guards rely on variable add/sub behavior.

Validation:
- `cargo test -p quickwit-common metrics`
- `cargo test -p quickwit-common tower`
- `cargo check -p quickwit-common --all-features`

Review checklist:
- `quickwit-common` no longer exposes Prometheus types.
- Guard behavior is preserved for byte accounting.

## Section 9: Migrate Server And Search Metrics

Goal: migrate the REST/gRPC/search-facing metric modules and their call sites.

Tasks:
- Convert `quickwit-serve/src/metrics.rs`.
- Convert HTTP request metrics in `quickwit-serve/src/rest.rs`.
- Convert `quickwit-serve` circuit breaker metric call sites.
- Convert `quickwit-search/src/metrics.rs`.
- Convert `quickwit-search/src/metrics_trackers.rs`.
- Convert search permit and scroll context gauge guards.
- Convert histogram timers in search code.

Special cases:
- `SplitSearchOutcomeCounters` has local unregistered counters. Decide whether to replace with observable local counters, `DebuggingRecorder` in tests, or a small non-exported local helper type.

Validation:
- `cargo test -p quickwit-serve metrics_api`
- `cargo test -p quickwit-search metrics`
- `cargo check -p quickwit-serve --all-features`
- `cargo check -p quickwit-search --all-features`

Review checklist:
- HTTP metric names and labels match previous output.
- Search display/debug code that reads counters remains correct.

## Section 10: Migrate Indexing, Ingest, And Storage Metrics

Goal: migrate the high-volume ingestion, indexing, and storage metric modules.

Tasks:
- Convert `quickwit-indexing/src/metrics.rs`.
- Convert indexing actors that use counters, gauges, and gauge guards.
- Convert `quickwit-ingest/src/metrics.rs`.
- Convert `quickwit-ingest/src/ingest_v2/metrics.rs`.
- Convert ingest router, ingester, and replication call sites.
- Convert `quickwit-storage/src/metrics.rs`.
- Convert object storage request counters and histograms.
- Convert cache metrics.
- Convert histogram timers in object storage code.

Validation:
- `cargo check -p quickwit-indexing --all-features`
- `cargo check -p quickwit-ingest --all-features`
- `cargo check -p quickwit-storage --all-features`
- Run focused unit tests in changed modules.

Review checklist:
- High-cardinality dynamic labels use parent extension consistently.
- Per-index metric behavior through `index_label()` is preserved.
- In-flight byte gauges balance on drop as before.

## Section 11: Migrate Remaining Service Crates

Goal: remove Prometheus-specific metric types from the remaining Quickwit-owned crates.

Tasks:
- Convert `quickwit-jaeger/src/metrics.rs` and Jaeger call sites.
- Convert `quickwit-cluster/src/metrics.rs` and cluster call sites.
- Convert `quickwit-actors` mailbox/backpressure metrics.
- Convert `quickwit-opentelemetry` OTLP ingest metrics.
- Convert `quickwit-lambda-client` metrics.
- Convert `quickwit-parquet-engine` metrics.
- Convert `quickwit-metastore` Postgres metrics.
- Convert `quickwit-control-plane` metrics.
- Convert `quickwit-janitor` metrics if present.

Validation:
- `cargo check -p quickwit-jaeger --all-features`
- `cargo check -p quickwit-cluster --all-features`
- `cargo check -p quickwit-actors --all-features`
- `cargo check -p quickwit-opentelemetry --all-features`
- `cargo check -p quickwit-lambda-client --all-features`
- `cargo check -p quickwit-parquet-engine --all-features`
- `cargo check -p quickwit-metastore --all-features`
- `cargo check -p quickwit-control-plane --all-features`

Review checklist:
- No remaining Quickwit-owned metric modules import Prometheus crate metric types.
- Counter/gauge/histogram operation names are migrated consistently.

## Section 12: Remove Direct Prometheus Usage From Quickwit-Owned Code

Goal: finish the backend migration and remove obsolete dependencies.

Tasks:
- Search for direct Prometheus imports:
  - `prometheus::`
  - `IntCounter`
  - `IntGauge`
  - `HistogramVec`
  - `IntCounterVec`
  - `IntGaugeVec`
  - `new_counter`
  - `new_gauge`
  - `new_histogram`
  - `register_info`
- Remove old constructor functions and vector wrapper types.
- Remove `prometheus` from `quickwit-common` dependencies if no longer needed.
- Keep any third-party or generated Prometheus references only if they are not Quickwit-owned migration targets.
- Update docs and comments that describe Prometheus crate semantics.

Validation:
- `rg "prometheus::|IntCounter|IntGauge|HistogramVec|IntCounterVec|IntGaugeVec|new_counter|new_gauge|new_histogram|register_info" quickwit -g '*.rs'`
- `cargo check --workspace --all-features`

Review checklist:
- No compatibility shim remains by accident.
- Dependency cleanup does not remove unrelated Prometheus functionality.

## Section 13: Add /metrics Integration Coverage

Goal: prove the Prometheus rendering contract survived the migration.

Tasks:
- Add integration tests for `/metrics` output.
- Assert:
  - metric names
  - labels
  - descriptions/help text
  - histogram bucket boundaries
  - build info metric
- Include at least one counter, gauge, and histogram.
- Include at least one labeled child metric.

Validation:
- `cargo test -p quickwit-serve metrics_api`
- Relevant integration test target if tests live outside `quickwit-serve`.

Review checklist:
- Tests validate Prometheus text output, not internal implementation details.
- Tests avoid depending on global metric state in a flaky way.

## Section 14: Final Workspace Verification

Goal: verify the whole migration after all sections land.

Tasks:
- Run focused tests:
  - `cargo test -p quickwit-common metrics`
  - `cargo test -p quickwit-serve metrics_api`
  - `cargo test -p quickwit-cli logger`
- Run broader checks:
  - `cargo clippy --workspace --tests --all-features`
  - `make fmt`
- If runtime confidence is needed, start a local Quickwit node and scrape `/metrics`.
- Document any tests that are skipped due to environment requirements.

Review checklist:
- Formatting uses the repository entrypoint `make fmt`.
- Clippy and tests are reported with exact failures if they cannot fully pass.
- The final PR description lists any expected metric-output differences.
