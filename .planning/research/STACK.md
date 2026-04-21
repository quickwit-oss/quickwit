# Technology Stack: Metric Metadata Transform (pomsky-intake)

**Project:** Pomsky Intake -- v0.1 Metric Metadata Transform
**Researched:** 2026-04-16
**Overall confidence:** HIGH (all claims verified against local source in the workspace)

## Existing Stack (Validated -- DO NOT change)

These are already present in `pomsky-intake/Cargo.toml` and must not be modified or replaced.

| Technology | Version | Purpose |
|------------|---------|---------|
| `vector` | workspace (git rev fbb1e4b) | Pipeline runtime, `TransformConfig` trait, event types |
| `vector-lib` | workspace (git rev fbb1e4b) | `FunctionTransform`, `TaskTransform`, `OutputBuffer`, `NamedComponent` |
| `tokio` | 1.48 | Async runtime, `tokio::time::interval` for flush timers |
| `reqwest` | 0.12 | HTTP client (already used in `arrow_ipc_metrics` sink for posting Arrow IPC) |
| `serde` / `serde_yaml` | 1.0 / 0.9 | Config deserialization via `typetag` |
| `typetag` | 0.2 | Transform registration -- links transform into Vector at compile time |
| `async-trait` | 0.1 | Required for `TransformConfig::build` |
| `tracing` | 0.1 | Structured logging |
| `anyhow` | 1 | Error propagation in `build()` |
| `rand` | 0.9 | Random number generation (already in workspace) |
| `rand_distr` | 0.5 | `Uniform` distribution for randomized TTL (already in workspace) |
| `futures` | 0.3 | `StreamExt` for `TaskTransform` stream loop |

## Stack Additions Required

### 1. `async-stream` crate -- Stream construction for TaskTransform

| Technology | Version | Purpose | Confidence |
|------------|---------|---------|------------|
| `async-stream` | 0.3.6 | `stream! {}` macro for writing the TaskTransform event loop | HIGH |

**Why needed:** The metric metadata transform must be a `TaskTransform`, not a `FunctionTransform`. `FunctionTransform` requires `Clone + Sync` (Vector's invariant) and is called synchronously per-event -- it cannot hold mutable state, spawn timers, or make async HTTP calls. `TaskTransform` wraps an owned stream and can run a full async loop with `tokio::select!`.

The `aggregate` and `throttle` transforms in Vector use `async-stream` for exactly this pattern. It is already a transitive dependency via the `vector` crate (confirmed in `Cargo.lock`: `async-stream 0.3.6`). Adding it directly to `pomsky-intake/Cargo.toml` makes the dependency explicit and avoids relying on transitive resolution.

**Why NOT `FunctionTransform`:** The existing transforms (`preprocess_metric`, `preprocess_log`, etc.) are all stateless and implement `FunctionTransform`. A stateful transform with background flush timers cannot fit this model without violating CLAUDE.md's prohibition on `tokio::sync::Mutex` (which would be needed to share state across concurrent `FunctionTransform` invocations).

```toml
# pomsky-intake/Cargo.toml
async-stream = { workspace = true }
```

The workspace already pins this at 0.3.6 via `vector`'s dependency tree. Add it to `[workspace.dependencies]` in the root `Cargo.toml` if not already listed (check first -- it is not currently listed explicitly but is resolvable via lockfile).

### 2. `csv` crate -- Persistence file format

| Technology | Version | Purpose | Confidence |
|------------|---------|---------|------------|
| `csv` | 1.4.0 | Reading and writing known-metrics persistence file | HIGH |

**Why `csv`:** The PROJECT.md explicitly specifies CSV format for the persistence file ("Matches Go service, human-readable, simple to parse"). CSV with headers is sufficient for the data structure: `name,metric_type,expires_at`. The `csv` crate handles quoting, escaping, and header management correctly without manual string manipulation.

**Why NOT manual string formatting:** Writing CSV by hand (format strings + split on commas) is brittle for metric names that may contain commas. The `csv` crate handles this correctly. `serde_csv` serialization via `#[derive(Serialize, Deserialize)]` makes the format explicit and type-safe.

The `csv` crate is already a transitive dependency at version 1.4.0 (confirmed in `Cargo.lock` -- pulled in by `vector`). It is not currently in `[workspace.dependencies]`, so it must be added there before use.

```toml
# Root Cargo.toml [workspace.dependencies]
csv = "1"

# pomsky-intake/Cargo.toml
csv = { workspace = true }
```

### No Other External Dependencies Required

All other requirements are met by existing workspace dependencies:

| Requirement | Library | Already Present |
|-------------|---------|----------------|
| In-memory HashMap | `std::collections::HashMap` | stdlib |
| TTL expiry timestamps | `std::time::Instant` or `tokio::time::Instant` | stdlib / tokio |
| Randomized TTL (12-36h) | `rand` + `rand_distr::Uniform` | workspace |
| Periodic file flush timer | `tokio::time::interval` | tokio workspace |
| HTTP POST to metadata service | `reqwest::Client` | workspace |
| JSON body serialization | `serde_json` (via `reqwest` `json` feature) | workspace |
| Environment variable for API key | `std::env::var` | stdlib |
| Error propagation | `anyhow` | workspace |
| Structured logging | `tracing` | workspace |

## Transform Architecture: TaskTransform with Owned Event Loop

**This is the critical structural decision.** The new transform must implement `TaskTransform<Event>` (via `Transform::event_task`), following the `aggregate` transform pattern exactly.

### Why TaskTransform and Not FunctionTransform

`FunctionTransform` is:
- Called synchronously per event
- Required to implement `Clone` (Vector clones it for concurrent execution via `enable_concurrency`)
- Prohibited from holding non-`Clone` state (timers, HTTP clients, file handles)
- Prohibited from awaiting async operations during `transform()`

The metric metadata transform needs:
- Mutable `HashMap` state shared across events (no Clone needed)
- Two independent timers (`tokio::time::interval`) for file flush and HTTP flush
- An async `reqwest::Client` for HTTP POST on timer expiry
- Startup file loading before first event

All of these are incompatible with `FunctionTransform`. Use `TaskTransform<Event>` with `Transform::event_task`.

### Pattern (from Vector's `aggregate` transform, verified in local source)

```rust
// TransformConfig::build returns:
Ok(Transform::event_task(MetricMetadata::new(self).await?))

// The struct owns all state:
struct MetricMetadata {
    known_metrics: HashMap<String, Instant>,  // name -> expiry
    pending: Vec<MetricRecord>,
    client: reqwest::Client,
    // config fields...
}

impl TaskTransform<Event> for MetricMetadata {
    fn transform(
        mut self: Box<Self>,
        mut input_rx: Pin<Box<dyn Stream<Item = Event> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Event> + Send>> {
        let mut persist_tick = tokio::time::interval(self.persist_interval);
        let mut flush_tick = tokio::time::interval(self.flush_interval);

        Box::pin(stream! {
            loop {
                tokio::select! {
                    maybe_event = input_rx.next() => {
                        match maybe_event {
                            None => break,
                            Some(event) => {
                                // Process event, update known_metrics, maybe add to pending
                                yield event;  // pass through unchanged
                            }
                        }
                    }
                    _ = persist_tick.tick() => {
                        // Write known_metrics to CSV file
                    }
                    _ = flush_tick.tick() => {
                        // POST pending list to byoc-ingest-metadata-svc
                        // Drop pending list on failure
                    }
                }
                // Also flush if pending.len() >= size_threshold
            }
            // Stream ended: do final persist
        })
    }
}
```

**Key constraint from CLAUDE.md GAP-002:** Do NOT recreate futures inside the `select!` loop. Use `&mut persist_tick` / `&mut flush_tick` (interval futures are reusable via `tick()`). The intervals created before the loop are reused on each iteration via `.tick()`, which is the correct pattern.

**Key constraint from CLAUDE.md GAP-002:** Do NOT use `tokio::sync::Mutex` to share state. The `TaskTransform` owns all state; there is no sharing. This is the correct architecture.

## TTL Implementation: Manual Expiry with `Instant`

Use `std::collections::HashMap<String, std::time::Instant>` where the value is the expiry timestamp (`Instant::now() + duration`). On each event, check `Instant::now() > expiry` to detect known-metric entries that need refresh.

**Why NOT `ttl_cache::TtlCache`:** The `ttl_cache` crate (already in workspace, used by `quickwit-search`) uses a `BTreeMap` internally with automatic eviction. It does not expose "is this entry expired?" without removing it, which makes the "re-detect metric as new after expiry" semantics harder to express. Manual tracking with `HashMap<String, Instant>` is simpler and more explicit for this use case.

**Why NOT `mini-moka`:** `mini-moka` (also in workspace) is a concurrent cache. Concurrency is irrelevant here -- the transform owns its state and runs on a single async task. The overhead of `mini-moka`'s concurrent internals is unnecessary.

**TTL randomization:** On each cache miss (new metric detected), assign a TTL via:
```rust
use rand::Rng;
let ttl_secs: u64 = rng.random_range(12 * 3600..=36 * 3600);
let expiry = std::time::Instant::now() + std::time::Duration::from_secs(ttl_secs);
```

`rand::Rng::random_range` is the correct API in `rand` 0.9 (the `gen_range` method was renamed in 0.9). Confirmed in workspace `Cargo.toml`: `rand = "0.9"`.

## HTTP Client: Reuse `reqwest::Client` Pattern from Arrow IPC Sink

The existing `arrow_ipc_metrics` sink already creates a `reqwest::Client::new()` in `SinkConfig::build()` and holds it in the sink struct. Follow the identical pattern:

1. Create `reqwest::Client::new()` in `TransformConfig::build()` (async, so construction is safe there)
2. Store in the `MetricMetadata` struct
3. Post JSON body with `client.post(url).header("DD-API-KEY", api_key).json(&payload).send().await`

The `reqwest` workspace dependency already has the `json` and `rustls-tls` features enabled. No feature changes needed.

**Error handling:** On HTTP failure, drop the pending list and log a warning. Do not retry (PROJECT.md: "Drop pending list on flush failure -- metrics will be re-detected on next arrival"). Log with `tracing::warn!` at the flush site.

## CSV Persistence: `serde`-driven Row Types

Define a row struct and derive `Serialize`/`Deserialize`:
```rust
#[derive(Debug, Serialize, Deserialize)]
struct KnownMetricRow {
    name: String,
    metric_type: String,
    expires_at_secs: u64,  // Unix seconds for cross-restart portability
}
```

Use `csv::Writer` / `csv::Reader` with `has_headers(true)`. Write atomically using `tempfile` (already in workspace) to avoid partial writes corrupting the file on crash.

## Config Structure

Follow the `PreprocessMetricConfig` pattern: a config struct with `#[serde(deny_unknown_fields)]`, `NamedComponent`, `GenerateConfig`, and `TransformConfig` impls. Config fields:

```rust
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MetricMetadataConfig {
    /// URL of byoc-ingest-metadata-svc (e.g. "https://example.datadoghq.com")
    pub metadata_svc_url: String,
    /// org_id passed in the request body (not a secret)
    pub org_id: String,
    /// Path to the CSV file for known-metrics persistence
    pub persist_path: String,
    /// Interval between file persistence writes (default: 30s)
    #[serde(default = "default_persist_interval_secs")]
    pub persist_interval_secs: u64,
    /// Interval between HTTP flushes to metadata service (default: 15s)
    #[serde(default = "default_flush_interval_secs")]
    pub flush_interval_secs: u64,
    /// Size threshold that triggers an early HTTP flush (default: 200)
    #[serde(default = "default_flush_size_threshold")]
    pub flush_size_threshold: usize,
}
```

The DD-API-KEY is read from `std::env::var("DD_API_KEY")` at build time (in `TransformConfig::build`), not stored in config (it is a secret).

## Dependency Changes Required

### In `pomsky-intake/Cargo.toml`

```toml
[dependencies]
# ... existing deps unchanged ...
async-stream = { workspace = true }
csv = { workspace = true }
```

### In root `quickwit/Cargo.toml` `[workspace.dependencies]`

```toml
async-stream = "0.3"
csv = "1"
```

Both are already in `Cargo.lock` (transitive deps of `vector`). Adding them to `[workspace.dependencies]` makes them explicit and version-coordinated.

### No feature flag changes required

`reqwest` already has `json` and `rustls-tls` features. `tokio` already has `full`. No new features needed for any existing dependency.

## What NOT to Add

### Do NOT add `tokio::sync::Mutex` for state sharing

Forbidden by CLAUDE.md (GAP-002: causes data corruption on cancel). The `TaskTransform` model is the correct alternative -- it owns all state in a single async task without shared mutable state.

### Do NOT add `JoinHandle::abort()` or spawned background tasks

CLAUDE.md prohibits `JoinHandle::abort()`. Do not spawn background tokio tasks for the flush timers. Instead, drive timers inside the `TaskTransform` event loop using `tokio::select!`. This integrates cleanly with Vector's shutdown (stream end = drain state + exit loop).

### Do NOT add a retry library (e.g., `reqwest-retry`)

PROJECT.md explicitly specifies "Drop pending list on flush failure". Retry adds complexity and changes the specified failure semantics. If requirements change, `reqwest-middleware` + `reqwest-retry` are already in the workspace, but do not add them now.

### Do NOT add `mini-moka` or `ttl_cache` for the known-metrics map

Both are overkill. Manual `HashMap<String, Instant>` is simpler, requires no additional dependencies, and expresses the TTL semantics correctly. Both are already in the workspace for other uses; do not pull them into pomsky-intake.

### Do NOT implement FunctionTransform with Arc<Mutex<...>> state

This pattern circumvents the prohibition on `tokio::sync::Mutex` by using `std::sync::Mutex`, but holding a `std::sync::Mutex` across await points causes deadlocks. The `TaskTransform` pattern is the correct solution.

### Do NOT use `serde_yaml` for the persistence file

The persistence file format is CSV (specified in PROJECT.md). Using YAML or JSON would break compatibility with the existing Go service's known_metrics.csv file.

## Alternatives Considered

| Category | Recommended | Alternative | Why Not |
|----------|-------------|-------------|---------|
| Transform type | `TaskTransform<Event>` | `FunctionTransform` | FunctionTransform requires Clone, cannot hold async state or timers |
| Transform type | `TaskTransform<Event>` | `SyncTransform` with background thread | Thread-based approach violates tokio async model; no clean shutdown |
| TTL map | `HashMap<String, Instant>` | `mini-moka` | Concurrent cache; unnecessary complexity for single-task state |
| TTL map | `HashMap<String, Instant>` | `ttl_cache::TtlCache` | BTreeMap-backed; eviction semantics don't match "re-detect on expiry" |
| Stream construction | `async-stream` (`stream! {}`) | `futures::stream::unfold` | `unfold` cannot express multi-branch `select!` loops cleanly |
| Persistence | `csv` crate | Manual string formatting | Fails for metric names with commas; format must match Go service exactly |
| Persistence | `csv` crate + `tempfile` atomic write | Direct file write | Partial writes on crash corrupt the known-metrics file |
| HTTP | `reqwest::Client` | `ureq` | `reqwest` is already in the crate; `ureq` is sync-only (version 3 is async but not the workspace version) |
| Error handling on flush | Drop pending list | Retry with backoff | Contradicts PROJECT.md specification; adds state complexity |

## Sources

- Verified against local source: `/Users/alan.gates/.cargo/git/checkouts/vector-7010c25277c07669/fbb1e4b/lib/vector-core/src/transform/mod.rs` -- `FunctionTransform` (requires `Clone + Sync`), `TaskTransform` trait signatures, `Transform::event_task`
- Verified against local source: `/Users/alan.gates/.cargo/git/checkouts/vector-7010c25277c07669/fbb1e4b/src/transforms/aggregate.rs` -- `TaskTransform<Event>` + `async-stream` + `tokio::select!` pattern for stateful transforms with flush timers
- Verified against local source: `/Users/alan.gates/.cargo/git/checkouts/vector-7010c25277c07669/fbb1e4b/src/transforms/throttle/transform.rs` -- second example of `TaskTransform<Event>` pattern
- Verified against local source: `pomsky-intake/src/sinks/arrow_ipc_metrics.rs` -- `reqwest::Client::new()` in `build()`, HTTP POST pattern
- Verified against local source: `pomsky-intake/src/transforms/preprocess_metric.rs` -- `FunctionTransform` pattern (shows what NOT to use for stateful transforms)
- Verified: `Cargo.lock` -- `async-stream 0.3.6`, `csv 1.4.0` already in lockfile as transitive deps
- Verified: `Cargo.toml` workspace deps -- `rand = "0.9"`, `rand_distr = "0.5"`, `reqwest 0.12`, `tokio 1.48`, `tempfile 3`, `anyhow 1`, `tracing 0.1`, `futures 0.3`, `serde 1`, `typetag 0.2`, `async-trait 0.1`
- Verified: CLAUDE.md GAP-002 -- `tokio::sync::Mutex` forbidden, `JoinHandle::abort()` forbidden, recreating futures in `select!` forbidden
