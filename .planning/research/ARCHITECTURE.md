# Architecture: Stateful Vector Transform for Metric Metadata

**Domain:** Stateful in-process Vector transform with background tasks, shared state, and graceful shutdown
**Researched:** 2026-04-16
**Confidence:** HIGH (based on direct source analysis of vector-core transform traits, aggregate.rs, aws_ec2_metadata.rs, expiration_map.rs, and the existing pomsky-intake transform patterns)

---

## System Overview

```
pomsky-intake process (single binary)
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Vector Topology (built by intake_runner.rs)                                    │
│                                                                                 │
│  Sources                  Transforms                       Sinks                │
│  ┌──────────────┐         ┌──────────────────┐                                  │
│  │datadog_agent │─metrics─▶ preprocess_metric │──────────────────────────────┐  │
│  │   .metrics   │         └──────────────────┘                              │  │
│  └──────────────┘                  │                                        │  │
│  ┌──────────────┐                  │ (same events, pass-through)            │  │
│  │     otlp     │─metrics─────────▶│                                        │  │
│  │   .metrics   │         ┌────────▼──────────────────────┐                │  │
│  └──────────────┘         │ metric_metadata (NEW)         │                │  │
│                           │                               │                │  │
│                           │  ┌─────────────────────────┐ │                │  │
│                           │  │ KnownMetrics HashMap     │ │                │  │
│                           │  │  name → expiry_instant   │ │                │  │
│                           │  └─────────────────────────┘ │                │  │
│                           │  ┌─────────────────────────┐ │                │  │
│                           │  │ PendingList Vec          │ │                │  │
│                           │  │  (new metrics to POST)   │ │                │  │
│                           │  └─────────────────────────┘ │                │  │
│                           │              │                │                │  │
│                           │   timer tick │  stream end    │                │  │
│                           │     flush    │  flush+persist │                │  │
│                           └─────────────┼────────────────┘                │  │
│                                         │                                  │  │
│                           side-channel  │                                  │  │
│                           ┌─────────────▼──────────────┐                  │  │
│                           │ HTTP POST                   │                  │  │
│                           │ byoc-ingest-metadata-svc    │                  │  │
│                           └────────────────────────────┘                  │  │
│                                                                            │  │
│                           ┌────────────────────────────┐                  │  │
│                           │ File I/O                    │                  │  │
│                           │ known_metrics.csv (persist) │                  │  │
│                           └────────────────────────────┘                  │  │
│                                                                            ▼  │
│                                                           ┌────────────────┐  │
│                                                           │ arrow_ipc_     │  │
│                                                           │ metrics sink   │  │
│                                                           └────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Vector Transform Trait Selection

Vector provides three transform variants. The right choice for this milestone is `TaskTransform`.

### Why Not FunctionTransform

`FunctionTransform` is stateless and clonable — Vector may run multiple instances concurrently when `enable_concurrency() -> true`. A metric metadata tracker requires shared state: a single `HashMap` of known metrics that all processing must see. Cloning it per-event would give every event its own isolated state.

`PreprocessMetricConfig` uses `FunctionTransform` because it is truly stateless (tag renaming with no cross-event memory). Metric metadata tracking is the opposite.

### Why TaskTransform

`TaskTransform` owns the stream directly. It receives a `Pin<Box<dyn Stream<Item = EventArray> + Send>>` and returns a transformed stream. This is fundamentally `async fn(Stream) -> Stream` expressed as a trait. The implementation struct holds state directly as owned fields — no `Arc`, no `Mutex`, no synchronization required because only one task ever touches the state.

The key property: Vector calls `TaskTransform::transform()` exactly once per component instance. The returned stream is driven by a single tokio task. This means the state is effectively single-threaded from the perspective of the transform's event loop, despite running inside an async runtime.

```
TransformConfig::build() -> Transform::event_task(MetricMetadata { state... })
                                  │
                                  └─ Vector topology builder calls:
                                       MetricMetadata::transform(input_stream)
                                       └─ returns output stream (single ownership)
                                          └─ driven by single tokio task in builder.rs
```

### The `aggregate.rs` Pattern (Canonical Reference)

`aggregate.rs` is the closest existing example to what we need: a stateful metric transform with timer-based flushing. Its `TaskTransform` implementation is the template to follow:

```rust
impl TaskTransform<Event> for Aggregate {
    fn transform(
        mut self: Box<Self>,
        mut input_rx: Pin<Box<dyn Stream<Item = Event> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Event> + Send>> {
        let mut flush_stream = tokio::time::interval(self.interval);

        Box::pin(stream! {
            let mut output = Vec::new();
            let mut done = false;
            while !done {
                tokio::select! {
                    _ = flush_stream.tick() => {
                        self.flush_into(&mut output);
                    },
                    maybe_event = input_rx.next() => {
                        match maybe_event {
                            None => {
                                self.flush_into(&mut output);
                                done = true;
                            }
                            Some(event) => self.record(event),
                        }
                    }
                };
                for event in output.drain(..) {
                    yield event;
                }
            }
        })
    }
}
```

The `MetricMetadata` transform follows this exact structure with two intervals (flush-to-HTTP and persist-to-file) instead of one, and the flush produces no output events (side-channel HTTP only), while all input events pass through unchanged.

---

## Component Boundaries

### MetricMetadataConfig (TransformConfig)

**Responsibility:** Configuration parsing and transform construction. Instantiated once at topology build time. Reads config from YAML (org_id, flush_interval, persist_interval, persist_path, metadata_svc_url, size_threshold).

**Boundary:** Calls `TransformConfig::build()` which performs startup I/O (load CSV, construct reqwest client) and returns `Transform::event_task(MetricMetadata { ... })`. After `build()` returns, `MetricMetadataConfig` is not referenced again.

### MetricMetadata (TaskTransform state)

**Responsibility:** All runtime logic. Lives entirely inside the stream returned by `transform()`. Fields:

- `known_metrics: HashMap<String, Instant>` — tracks seen metric names with expiry deadline
- `pending: Vec<MetricEntry>` — new metrics awaiting flush to HTTP
- `http_client: reqwest::Client` — for POST to byoc-ingest-metadata-svc
- `metadata_svc_url: String` — target URL
- `api_key: String` — from env var DD_API_KEY
- `org_id: String` — from config
- `persist_path: PathBuf` — CSV file for known_metrics persistence
- `flush_interval: Duration` — how often to POST pending list (default 15s)
- `persist_interval: Duration` — how often to persist known_metrics CSV (default 30s)
- `size_threshold: usize` — flush pending list when it reaches this size (default 200)

**Boundary:** All state is private to the `stream!` block. No `Arc`, no shared references. The stream is the only way to interact with this state.

### Timer Channels (within the stream)

There are no background tasks. Both the HTTP flush timer and the persistence timer live as `tokio::time::Interval` values inside the `stream!` block, selected alongside `input_rx.next()`. This is the same mechanism `aggregate.rs` uses for its flush timer.

Using `tokio::spawn` for side effects (as `aws_ec2_metadata.rs` does for its refresh loop) is an option but is not necessary here because the HTTP POST and file write are not concurrent with event processing — they happen during the `select!` arm that wins. This is simpler and avoids the pitfalls in GAP-002 (no `JoinHandle::abort()`, no cross-task state sharing).

---

## Data Flow

### Per-Event Processing

```
Input metric event arrives via input_rx.next()
        │
        ▼
Extract metric name, kind, and interval_ms tag
        │
        ├─ Is name in known_metrics AND not expired?
        │      YES → pass event through unchanged (no side effects)
        │      NO  → record in known_metrics with new expiry
        │             add MetricEntry to pending list
        │             if pending.len() >= size_threshold: trigger HTTP flush
        │
        ▼
Yield event downstream (always — all events pass through)
```

### Timer-Triggered HTTP Flush (flush_interval tick)

```
flush_interval.tick() fires
        │
        ▼
Is pending list non-empty?
        │
        ├─ YES → HTTP POST to byoc-ingest-metadata-svc
        │          Body: JSON array of MetricEntry
        │          Headers: DD-API-KEY, Content-Type: application/json
        │          Response 2xx → clear pending list
        │          Response error → drop pending list (drop-on-failure policy)
        │
        └─ NO  → no-op
        │
No events yielded (HTTP flush is side-channel only)
```

### Timer-Triggered Persistence (persist_interval tick)

```
persist_interval.tick() fires
        │
        ▼
Write known_metrics HashMap to CSV file at persist_path
        format: name,expiry_unix_secs
        Write atomically (write to tempfile, rename over target)
        Error → log warning, continue (persistence failure is non-fatal)
        │
No events yielded
```

### Stream End (graceful shutdown)

```
input_rx.next() returns None (upstream sources shut down)
        │
        ▼
Final HTTP flush of pending list (best-effort)
        │
        ▼
Final CSV persist of known_metrics
        │
        ▼
set done = true → stream! block exits → output stream closes
        │
        ▼
Vector topology builder sees closed stream → marks transform as finished
```

This is how Vector achieves graceful shutdown for task transforms: it closes sources first, which propagates stream termination downstream. The `None` arm in `select!` is the shutdown hook. No `CancellationToken`, no `JoinHandle::abort()` needed.

---

## Multiple Timer Selection

The transform needs two independent timer intervals. `aggregate.rs` uses one. `map_with_expiration` in vector-stream supports exactly one expiration interval. For two timers, use the `aggregate.rs` pattern directly with two intervals in `select!`:

```rust
Box::pin(stream! {
    let mut flush_timer = tokio::time::interval(state.flush_interval);
    let mut persist_timer = tokio::time::interval(state.persist_interval);
    let mut done = false;
    while !done {
        tokio::select! {
            _ = flush_timer.tick() => {
                state.flush_pending().await;
            }
            _ = persist_timer.tick() => {
                state.persist_known_metrics().await;
            }
            maybe_event = input_rx.next() => {
                match maybe_event {
                    None => {
                        state.flush_pending().await;
                        state.persist_known_metrics().await;
                        done = true;
                    }
                    Some(event) => {
                        state.process_event(event, &mut output);
                        if state.pending.len() >= state.size_threshold {
                            state.flush_pending().await;
                        }
                    }
                }
            }
        }
        for event in output.drain(..) {
            yield event;
        }
    }
})
```

The `tokio::select!` macro handles arbitrary numbers of futures. Priority between the two timers is non-deterministic when both fire simultaneously, which is acceptable — the exact ordering of HTTP flush and CSV persist does not matter.

---

## HTTP Client Architecture

Do not spawn a background task for HTTP. The POST is blocking-by-network but non-blocking-by-code (reqwest is async). The `.await` inside the `select!` arm is fine because it is the winning arm of a single `select!` call — no other events can arrive while the POST is in flight, which is the intended behavior (backpressure through the transform during flush).

Use `reqwest::Client` (not `reqwest::blocking::Client`). Construct it once in `TransformConfig::build()` and move it into the `MetricMetadata` struct. `reqwest::Client` is cheaply clonable (`Arc` internally) and reuses connection pools.

The `reqwest` crate is already a dependency in `pomsky-intake/Cargo.toml`.

---

## Config Wiring

The transform config references `org_id` from YAML and `DD_API_KEY` from an environment variable. The topology YAML in `intake_runner.rs` is a Rust format string — config values can be interpolated at template-build time. The `MetricMetadataConfig` struct reads env vars in `build()`, not at deserialization time (following the pattern of `aws_ec2_metadata.rs` which reads proxy config from environment in `build()`).

Adding the new transform to the topology requires two changes in `intake_runner.rs`:

1. Add a new transform block in `build_vector_config()`:

```yaml
transforms:
  metric_metadata:
    type: metric_metadata
    inputs:
      - preprocess_metrics
    org_id: "{org_id}"
    metadata_svc_url: "{metadata_svc_url}"
    persist_path: "{data_dir}/known_metrics.csv"
```

2. Update `metrics_out` sink inputs from `preprocess_metrics` to `metric_metadata`.

The `IntakeConfig` struct in `config.rs` needs new fields for `org_id`, `metadata_svc_url`, and optionally the interval/threshold overrides.

---

## File Layout

```
pomsky-intake/src/
├── transforms/
│   ├── mod.rs                        # Add: pub mod metric_metadata;
│   ├── preprocess_metric.rs          # Existing — unchanged
│   └── metric_metadata/
│       ├── mod.rs                    # MetricMetadataConfig, TransformConfig impl
│       ├── transform.rs              # MetricMetadata struct, TaskTransform impl
│       ├── state.rs                  # KnownMetrics, PendingList, TTL logic
│       ├── http.rs                   # HTTP flush logic (POST + response handling)
│       ├── persist.rs                # CSV read/write for known_metrics
│       └── types.rs                  # MetricEntry, MetricKind mapping
├── config.rs                         # Add: org_id, metadata_svc_url fields
└── intake_runner.rs                  # Update: topology YAML template
```

Each file stays well under 500 lines. Splitting by responsibility (state, HTTP, persist, types) makes each unit independently testable without the full Vector topology.

---

## Suggested Build Order

The build order is driven by testability: each layer is independently testable before the next layer is built on top.

### Step 1: Types and State Logic

**Build:** `types.rs`, `state.rs`

`MetricEntry` (name, kind, interval), metric kind mapping (Counter/Rate/Gauge/DDSketch), `KnownMetrics` with TTL expiry (randomized 12-36h range), `PendingList` with size-threshold check.

This is pure Rust with no async, no I/O. Unit-testable in isolation. The TTL randomization logic and expiry checking belong here.

### Step 2: Persistence

**Build:** `persist.rs`

CSV read (startup load) and write (periodic persist). Atomic write via `tempfile` rename. No async needed — file I/O is small enough to run synchronously in the async context (CSV of metric names, not large files).

Unit-testable with `tempfile::TempDir`.

### Step 3: HTTP Client

**Build:** `http.rs`

Construct the POST body (JSON array of `MetricEntry`), set headers (DD-API-KEY, Content-Type), send, handle response. Drop-on-failure policy. Integration-testable with `wiremock` or `httpmock`.

### Step 4: Transform Core (TaskTransform)

**Build:** `transform.rs`, `mod.rs`

Wire the `stream!` loop using `aggregate.rs` as the structural template. Two intervals, event passthrough, size-threshold flush. Call `http.rs` and `persist.rs` from within the stream.

Unit-testable via `Transform::into_task().transform_events()` (same pattern as throttle tests and reduce tests) with a synthetic event stream and mock HTTP server.

### Step 5: Config and Topology Wiring

**Build:** `MetricMetadataConfig::build()`, `intake_runner.rs` topology update, `config.rs` extensions.

Integration-testable: start the full Vector topology with `run_intake()`, send metrics, verify HTTP POST via a test server, verify CSV written to disk.

---

## Patterns to Follow

### Pattern 1: stream! + select! for Stateful Timed Transforms

**What:** The `stream!` macro from `async_stream` combined with `tokio::select!` is how Vector's internal stateful transforms implement timer-triggered flushing. State is held in mutable local variables inside the stream closure.

**When:** Any transform that needs periodic side-effects (HTTP calls, file writes) alongside event processing.

**Reference:** `aggregate.rs` lines 384-417.

### Pattern 2: Build-Time I/O in TransformConfig::build()

**What:** Startup I/O (load CSV, read env vars, construct HTTP client) happens in the async `build()` method of `TransformConfig`, not in the stream itself. The stream receives a fully-initialized struct.

**When:** Any transform with startup dependencies.

**Reference:** `aws_ec2_metadata.rs` lines 206-248 (HTTP client construction, initial metadata fetch).

### Pattern 3: Event Passthrough with Side-Channel Effects

**What:** All events yield downstream unchanged. The transform's value is the side effects it produces (HTTP POSTs) based on what it observes. The event stream carries metrics to the Arrow sink; the metadata channel carries names to byoc-ingest-metadata-svc.

**When:** Monitoring/tracking transforms that must not modify the primary data path.

**Trade-offs:** Clean separation of concerns. The downstream sink (arrow_ipc_metrics) sees an unmodified stream. The only risk is that a slow HTTP flush delays event processing — acceptable given the drop-on-failure policy and the rarity of flush operations relative to event throughput.

### Pattern 4: Drop-on-Failure for Pending State

**What:** If the HTTP POST fails (network error, 5xx, timeout), the pending list is cleared rather than retried. Metrics will be re-detected as "new" on their next arrival after their TTL expires.

**When:** When retry complexity is not justified and the SaaS side is tolerant of re-detection.

**Trade-offs:** Simple code path, no retry queue, no memory growth on sustained failure. Cost: a gap in metadata submissions during SaaS downtime. Accepted per PROJECT.md Key Decisions.

---

## Anti-Patterns to Avoid

### Anti-Pattern 1: FunctionTransform with Arc<Mutex<State>>

**What:** Implement `FunctionTransform` (instead of `TaskTransform`) and share state behind `Arc<Mutex<...>>`.

**Why bad:** `tokio::sync::Mutex` is forbidden per GAP-002 and CLAUDE.md. `std::sync::Mutex` held across `.await` points (HTTP calls) is deadlock-prone. `FunctionTransform` is designed for stateless transforms; forcing stateful behavior requires concurrency primitives that create correctness risks.

**Instead:** Use `TaskTransform`. State is owned by the stream, no synchronization needed.

### Anti-Pattern 2: tokio::spawn for Background Timer Tasks

**What:** Spawn a separate tokio task in `build()` that runs the flush timer loop, sharing state with the transform via `Arc<Mutex<...>>`.

**Why bad:** This is the `aws_ec2_metadata.rs` pattern for the metadata refresh, but that transform's background task only reads from the network and writes to an `ArcSwap` — it does not share mutable state with the event-processing path. For metric metadata, the timer needs to mutate `pending` and `known_metrics` — the same data the event path mutates. Sharing via `Arc<Mutex>` is forbidden.

**Instead:** Keep both timers inside the `stream!` loop as `tokio::time::Interval` values selected alongside `input_rx.next()`. Single ownership, no synchronization.

### Anti-Pattern 3: Blocking File I/O in the Async Stream

**What:** Call `std::fs::write()` synchronously inside the `stream!` loop for CSV persistence.

**Why bad:** Blocks the tokio runtime thread. The CSV will be small (metric names only, not metric data), so in practice it may not be measurable — but it violates the runtime contract. CLAUDE.md mandates `run_cpu_intensive` for CPU-intensive work and the same principle applies to blocking I/O.

**Instead:** Use `tokio::fs::write()` for the async variant, or if the CSV is small enough (< 1MB), use `tokio::task::spawn_blocking`. The persist operation happens at most every 30 seconds; spawn_blocking overhead is negligible.

### Anti-Pattern 4: Parsing DD_API_KEY in the Stream

**What:** Read `std::env::var("DD_API_KEY")` inside the `stream!` loop on every event.

**Why bad:** `env::var` acquires a lock. It also means the transform silently fails if the env var is absent — failure appears as HTTP 401s rather than a startup error.

**Instead:** Read and validate `DD_API_KEY` in `TransformConfig::build()`. Return an error if absent. Store the value in `MetricMetadata` fields.

---

## Integration Points

### External Service

| Service | Integration Pattern | Notes |
|---------|---------------------|-------|
| byoc-ingest-metadata-svc | `reqwest::Client` HTTP POST | URL from config, API key from env. Drop-on-failure. No retry. |

### Internal Boundaries

| Boundary | Communication | Notes |
|----------|---------------|-------|
| `preprocess_metrics` → `metric_metadata` | Vector event stream (EventArray) | Same event type (Metric), same format, no schema change |
| `metric_metadata` → `arrow_ipc_metrics` | Vector event stream (EventArray) | Events pass through unmodified |
| `metric_metadata` → filesystem | Direct file I/O in stream | known_metrics.csv, atomic write via rename |
| `intake_runner.rs` → `MetricMetadataConfig` | YAML topology string interpolation | org_id, metadata_svc_url, persist_path injected at topology build time |

### Topology Change in intake_runner.rs

Current topology path for metrics:
```
datadog_agent.metrics → preprocess_metrics → metrics_out
otlp.metrics         → preprocess_metrics → metrics_out
```

New topology path:
```
datadog_agent.metrics → preprocess_metrics → metric_metadata → metrics_out
otlp.metrics         → preprocess_metrics → metric_metadata → metrics_out
```

The transform is inserted after `preprocess_metrics` (tag normalization must happen first so metric names and tags are in canonical form before metadata tracking).

---

## Sources

All findings are based on direct source analysis:

- `vector-core/src/transform/mod.rs` — `Transform` enum, `FunctionTransform`, `TaskTransform`, `SyncTransform` trait definitions and concurrency semantics
- `vector/src/transforms/aggregate.rs` — canonical `TaskTransform` with timer-based flushing (structural template)
- `vector/src/transforms/aws_ec2_metadata.rs` — `tokio::spawn` background task pattern and `TransformConfig::build()` I/O pattern
- `vector/src/transforms/reduce/transform.rs` — `map_with_expiration` usage; `flush_all_into` at stream end
- `vector-stream/src/expiration_map.rs` — `map_with_expiration` implementation showing `tokio::select!` + `flush_fn` on stream end
- `vector/src/topology/builder.rs` — how `build_task_transform` drives the returned stream in a single tokio task; shutdown via stream closure
- `pomsky-intake/src/transforms/preprocess_metric.rs` — existing `FunctionTransform` pattern for comparison
- `pomsky-intake/src/intake_runner.rs` — topology construction via format string; `ExtraContext` and Application lifecycle
- `pomsky-intake/src/transforms/mod.rs` — transform registration via `typetag`
- `pomsky-intake/Cargo.toml` — existing dependencies (`reqwest`, `tokio`, `serde`, `typetag`)
- `docs/internals/adr/gaps/002-cancellation-safety.md` (GAP-002) — prohibition on `tokio::sync::Mutex` and `JoinHandle::abort()`

---

*Architecture research for: stateful Vector transform (metric metadata tracking)*
*Researched: 2026-04-16*
