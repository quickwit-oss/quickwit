# Pitfalls Research

**Domain:** Stateful Vector custom transform with background I/O and HTTP egress
**Researched:** 2026-04-16
**Confidence:** HIGH — findings grounded in the actual Vector source at rev `fbb1e4b`, the existing pomsky-intake code, and CLAUDE.md invariants

---

## Critical Pitfalls

### Pitfall 1: Using `FunctionTransform` for a Stateful Transform

**What goes wrong:**
The existing transforms (`preprocess_metric`, `preprocess_log`, etc.) implement `FunctionTransform`, which requires `Clone + Send + Sync`. Vector may run multiple clones concurrently when `enable_concurrency() -> bool` returns `true` (see `preprocess_metric.rs` line 71). A HashMap with TTL and a pending flush list is not safely clonable — each clone would have a divergent, independent view of state. Metrics would be re-reported as "new" on every concurrent invocation.

**Why it happens:**
`FunctionTransform` is the happy path for simple stateless transforms. Developers copy the existing pattern without recognizing that statefulness requires `TaskTransform`.

**How to avoid:**
Use `Transform::event_task(impl TaskTransform<Event>)` or `Transform::task(impl TaskTransform<EventArray>)`. The `TaskTransform` trait receives the entire `Stream<Item = Event>` and owns it, so it is the single sequential owner of all state. Vector's topology builder routes all events through it single-file. Do NOT set `enable_concurrency() -> true` on a stateful transform's config — that flag only applies to `FunctionTransform` and `SyncTransform`.

**Warning signs:**
- Transform struct derives `Clone`
- `TransformConfig::enable_concurrency` returns `true`
- State diverges in integration tests under load (metrics rediscovered every flush cycle)

**Phase to address:** Phase 1 (initial skeleton) — pick the correct trait before writing any state management logic.

---

### Pitfall 2: Spawning a Detached Background Task for Timers

**What goes wrong:**
Spawning a `tokio::spawn` background task to drive the persist-to-file timer or the HTTP flush timer, then communicating with the main stream loop via `tokio::sync::Mutex`-protected shared state. This creates two independent problems:

1. `tokio::sync::Mutex` is **forbidden** in this codebase (CLAUDE.md, GAP-002). If the future holding the lock is cancelled while awaiting, the mutex is poisoned or the critical section is abandoned mid-update — the HashMap or pending list ends up in an inconsistent state.
2. The background task is detached from Vector's shutdown signal. Vector calls `topology.stop()` by closing the input stream. The `TaskTransform::transform` future resolves, but the orphaned background `JoinHandle` is either aborted mid-flush (data loss) or left running against a defunct runtime (panic).

**Why it happens:**
The two-timer pattern (persist every 30s, flush HTTP every 15s) looks naturally like two concurrent tasks. Developers reach for `tokio::spawn` + `Mutex` because it mirrors how they would write this in Go or a typical Tokio tutorial.

**How to avoid:**
Drive both timers inside the single `async_stream::stream!` loop of the `TaskTransform`, using `tokio::select!`. The `map_with_expiration` helper in `vector-stream` (`lib/vector-stream/src/expiration_map.rs`) shows the canonical pattern: one `tokio::time::interval` for the expiration tick, with `input.next()` and the timer arm in the same `select!`. For two independent intervals (15s HTTP flush, 30s file persist), add a second `tokio::time::interval` as a third arm of the same `select!`. All state lives on the `TaskTransform` struct — no separate task, no mutex.

When the input stream ends (Vector shutdown), the `None` branch of `input.next()` fires. This is the correct graceful shutdown hook. The stream body must perform the final file-persist write and the final HTTP POST before the generator returns.

**Warning signs:**
- `tokio::spawn(async move { ... })` anywhere in transform code
- `tokio::sync::Mutex<HashMap<...>>`
- `JoinHandle` stored on the transform struct
- `Arc<Mutex<...>>` passed between a task and the stream loop

**Phase to address:** Phase 1 (architecture decision) — the event-loop structure must be settled before any timer or I/O code is written.

---

### Pitfall 3: Performing Blocking I/O Inside the Async Stream Loop

**What goes wrong:**
Reading or writing the persistence CSV file with synchronous `std::fs` calls inside the `async_stream::stream!` body. This blocks the Tokio worker thread servicing the transform, which stalls the entire metric pipeline for the duration of the disk write. Under backpressure, upstream sources time out or buffer overflow.

**Why it happens:**
File I/O looks trivial. The persist path fires only every 30 seconds. Developers write `std::fs::write(...)` inline because `tokio::fs` requires `.await` which feels heavyweight for a small file.

**How to avoid:**
Use `tokio::fs` for the persistence write, or `quickwit_common::run_cpu_intensive` if the CSV serialisation is non-trivial. CLAUDE.md explicitly documents `run_cpu_intensive` as the correct hook for CPU-bound work inside Tokio tasks. Keep the async path unblocked. The file is written at most every 30s so the overhead of `spawn_blocking` is negligible.

**Warning signs:**
- `std::fs::write` or `std::fs::File::open` inside an `async fn` or `async_stream::stream!`
- No `.await` between timer tick and file operation completing
- Pipeline latency spikes visible in Vector's internal metrics every 30s

**Phase to address:** Phase 1 (I/O strategy) — established before writing the persistence layer.

---

### Pitfall 4: Losing the Pending Flush List on Graceful Shutdown

**What goes wrong:**
The pending HTTP flush list (metrics waiting to be POSTed to `byoc-ingest-metadata-svc`) is discarded when Vector's topology shuts down because the flush-on-stream-end logic is absent or unreachable. Metrics collected in the seconds before shutdown are silently dropped without being submitted.

**Why it happens:**
The stream-end code path is only exercised during shutdown. Unit tests typically drive a finite stream and check output events — they never exercise the "input closed, flush remaining state" branch. The `flush_fn` callback in `map_with_expiration` is the only canonical hook; if it only flushes known-metrics to file but not the pending HTTP list, the data is lost.

The PROJECT.md explicitly says "drop pending list on flush failure" — that is fine for HTTP errors during normal operation. But at shutdown, a best-effort POST should still be attempted.

**How to avoid:**
The stream-end branch of the `select!` loop must:
1. Attempt one final HTTP POST of any non-empty pending list (using `tokio::time::timeout` to avoid blocking shutdown indefinitely).
2. Persist the current known-metrics HashMap to file.
Both must happen before the stream generator returns. Write a dedicated integration test that sends a batch of events, drops the input channel (`drop(tx)`), waits for `topology.stop()`, then asserts the HTTP mock received the expected POST.

**Warning signs:**
- No stream-end handler that calls the HTTP client
- Tests that only check the pass-through output events, never the HTTP mock
- Integration tests that skip `topology.stop()` after `drop(tx)`

**Phase to address:** Phase 2 (flush logic) — verify with a test that explicitly exercises shutdown.

---

### Pitfall 5: Using `tokio::sync::Mutex` on the HashMap

**What goes wrong:**
`tokio::sync::Mutex` is explicitly forbidden by CLAUDE.md (GAP-002). If a future holding the lock is cancelled at an await point — for example during a `tokio::select!` when the other arm resolves first — the critical section is abandoned with the HashMap in a partially-updated state. For a HashMap of known metric names with TTL values, a partial update means a corrupt expiry time, a missing entry, or a leftover entry that should have been removed. This is a silent data corruption, not a panic.

**Why it happens:**
Developers who want shared access between the stream loop and a timer callback reach for `Mutex` because it is the obvious Rust synchronisation primitive. The difference between `std::sync::Mutex` (safe, non-async) and `tokio::sync::Mutex` (unsafe under cancel) is subtle.

**How to avoid:**
No mutex of any kind is needed if the architecture follows Pitfall 2's guidance: the HashMap lives exclusively on the `TaskTransform` struct, accessed only from the single `async_stream::stream!` coroutine. All mutations happen synchronously inside one arm of the `select!` — no concurrent access is possible, so no lock is needed.

If a mutex is genuinely required, use `std::sync::Mutex` with exclusively synchronous critical sections (no `.await` inside the lock guard's scope).

**Warning signs:**
- `tokio::sync::Mutex` imported or used anywhere in the transform
- `Arc<tokio::sync::Mutex<HashMap<...>>>` passed through `Clone` bounds

**Phase to address:** Phase 1 — architectural choice that eliminates the need for the mutex.

---

### Pitfall 6: Recreating the HTTP Client on Every Flush

**What goes wrong:**
Constructing a new `reqwest::Client` on every HTTP flush call (every 15s, or on every size-threshold trigger). `reqwest::Client` maintains a connection pool. Reconstructing it abandons existing connections, prevents HTTP keep-alive reuse to `byoc-ingest-metadata-svc`, and adds TLS handshake overhead to every flush. Under high metric novelty rates, this degrades flush latency.

**Why it happens:**
The flush function receives a `&self` reference or a plain closure without the client in scope, so developers construct it inline as the simplest approach.

**How to avoid:**
Construct `reqwest::Client::new()` once in `TransformConfig::build` and store it on the transform struct. The existing sink pattern in `arrow_ipc_metrics.rs` (line 169) shows the correct approach: client is built at construction, stored in the sink, reused on every `flush()` call.

**Warning signs:**
- `reqwest::Client::new()` inside a `flush` method or a closure that is called on a timer
- No `reqwest::Client` field on the transform struct

**Phase to address:** Phase 1 (construction) — a one-line fix at struct creation time.

---

### Pitfall 7: Writing the Persistence File Non-Atomically

**What goes wrong:**
Writing the known-metrics CSV to the target path directly with `File::create(path)` followed by `write_all(...)`. If the process is killed between truncation and the final `flush()`, the file is left empty or partially written. On the next startup, the transform loads zero known metrics — all metrics are treated as new and a massive flush is sent to `byoc-ingest-metadata-svc` on the first interval.

**Why it happens:**
Direct file writes are the obvious pattern. The failure window (truncate → write → flush) is milliseconds, so it is rarely hit in development.

**How to avoid:**
Write to a `.tmp` sibling file first, then `std::fs::rename` to the target path. On POSIX systems, rename is atomic within the same filesystem. The `tempfile` crate (already in `pomsky-intake`'s dependencies) provides `NamedTempFile::persist` which does exactly this. On startup, if the main file is absent but a `.tmp` exists, attempt to recover it.

**Warning signs:**
- `File::create(final_path)` followed by `write_all` without a rename step
- No test that kills the process between writes and verifies startup recovery

**Phase to address:** Phase 2 (persistence layer) — design atomicity upfront before writing the first persistence integration test.

---

### Pitfall 8: Storing the API Key in the Serialised `TransformConfig`

**What goes wrong:**
Embedding `DD_API_KEY` as a field in the YAML-serialised `TransformConfig`. This means the API key is written to the Vector config file, which is a temporary file generated in `run_intake` from `IntakeConfig`. The key ends up in the process's `/tmp` — potentially world-readable, logged by Vector on config load, or leaked in crash dumps.

**Why it happens:**
Other Vector transform configs store all parameters in TOML/YAML. It is the natural path of least resistance when building a new `TransformConfig`.

**How to avoid:**
Per PROJECT.md's decision: `DD_API_KEY` must come from the environment variable (never from config). Read it in `TransformConfig::build` via `std::env::var("DD_API_KEY")` and return an error if absent. Store the resolved key on the built transform struct (not the config struct). `org_id` is not a secret and can live in the config file.

**Warning signs:**
- `api_key: String` field on the `TransformConfig` struct
- `api_key` visible in Vector's `--config-yaml` dump
- `tracing::info!` that logs the full config struct

**Phase to address:** Phase 1 (config design) — before any serialisation code is written.

---

## Technical Debt Patterns

| Shortcut | Immediate Benefit | Long-term Cost | When Acceptable |
|----------|-------------------|----------------|-----------------|
| Hardcode flush thresholds as constants instead of config | Simpler struct, no validation | Cannot tune without recompile; ops cannot adapt to traffic spikes | Never — thresholds are explicitly configurable per PROJECT.md |
| Skip atomic file write (direct `File::create`) | 5 lines instead of 10 | Corrupt persistence file on crash; cold-start surge to SaaS API | Never in production path |
| `std::sync::Mutex` to coordinate timer callbacks | Avoids restructuring stream loop | Deadlock if a lock guard leaks into an async context | Only if critical section is guaranteed sync and short; prefer the `select!` pattern |
| One `reqwest::Client` per flush | No lifecycle management needed | TLS handshake on every flush, connection pool never warms up | Never |
| Skip the flush-on-shutdown HTTP POST | Simpler end-of-stream handling | Metrics in the last flush window are silently dropped | Only if the requirement is formally removed from PROJECT.md |

---

## Integration Gotchas

| Integration | Common Mistake | Correct Approach |
|-------------|----------------|------------------|
| `byoc-ingest-metadata-svc` HTTP POST | Retry on failure with exponential backoff, holding the pending list | Per PROJECT.md: drop the pending list on failure; metrics will be re-detected. No retry, no queue growth |
| `byoc-ingest-metadata-svc` auth | Pass `DD_API_KEY` in query string or body | Always use `DD-API-Key` header; the key must not appear in access logs or URLs |
| Persistence CSV on startup | Skip loading if file is absent — silently cold-start | On absent file: log at `info` level, start with empty HashMap. On parse error: log `warn` and start empty; do not panic |
| Vector topology shutdown | Assume `TaskTransform::transform` future lives until process exit | Vector closes the input stream to signal shutdown; the `None` branch of `input.next()` is the only graceful shutdown hook available to a transform |
| `enable_concurrency()` flag | Inherit `true` from a copied `FunctionTransform` config | Must return `false` for the stateful transform; `true` on a `TaskTransform` config is a no-op in Vector but signals intent incorrectly |

---

## Performance Traps

| Trap | Symptoms | Prevention | When It Breaks |
|------|----------|------------|----------------|
| HashMap unbounded growth — no TTL enforcement on insert | Memory grows proportionally to unique metric count | Enforce TTL on every event insert; also scan for expired entries on the flush-timer arm | When unique metric count exceeds ~100k over a 12-36h TTL window |
| Scanning the full HashMap for expired entries on every event | CPU spike proportional to HashMap size at high event throughput | Separate TTL from the per-event hot path; only scan on the interval tick (every 30s is fine) | At ~10k known metrics with high event throughput |
| Blocking file write on flush timer tick | Pipeline stalls every 30s; upstream source buffers fill | Use `tokio::fs::write` or `spawn_blocking` for the sync write | First production deployment under any load |
| Large pending list flushed synchronously before yielding events | Downstream sink starves during a large HTTP POST | The HTTP POST is `.await`-ed but must not block event pass-through — pass events downstream immediately, issue POST as a side effect | First time pending list hits size threshold 200 under load |

---

## Security Mistakes

| Mistake | Risk | Prevention |
|---------|------|------------|
| Log the `DD_API_KEY` value (even at `debug` level) | Key leaked to log aggregator | Never format the key into any log message; read with `std::env::var` and treat as opaque bytes immediately |
| Store `DD_API_KEY` in the serialised `TransformConfig` | Key written to tmpfile on disk, logged by Vector at startup | Read from env in `build()`, not from the config struct |
| Use `http://` to `byoc-ingest-metadata-svc` | API key transmitted in plaintext | Require `https://` in the URL; validate at config build time |
| Parse the persistence CSV without bounding input size | Malformed or unexpectedly large file causes OOM on startup | Cap the number of rows read; return an error (not a panic) if the limit is exceeded |

---

## "Looks Done But Isn't" Checklist

- [ ] **Graceful shutdown flush:** Integration test drops the tx channel, calls `topology.stop()`, asserts the HTTP mock received the final POST — not just that events passed through.
- [ ] **Persistence round-trip:** Test writes a known-metrics CSV, starts a fresh transform instance, sends a metric from the CSV, verifies it is NOT reported as new.
- [ ] **TTL expiry:** Test advances time past the 36h TTL maximum, verifies the metric IS reported as new again.
- [ ] **Atomic file write:** Test verifies a `.tmp` file written before a simulated crash is recovered on the next startup.
- [ ] **DD_API_KEY absent:** `TransformConfig::build` returns a clear `Err` when the env var is missing — not a panic, not a silent empty string.
- [ ] **Pass-through semantics:** Every input event appears in the output stream regardless of whether it is new, known, or causes a flush — the transform is side-effect only, not a filter.
- [ ] **`enable_concurrency()` not `true`:** Confirm the stateful transform's config does not return `true` for this flag.

---

## Recovery Strategies

| Pitfall | Recovery Cost | Recovery Steps |
|---------|---------------|----------------|
| `FunctionTransform` chosen instead of `TaskTransform` | HIGH | Restructure the entire stream loop; state management must be rewritten; tests likely invalid |
| Detached background task with `tokio::sync::Mutex` | HIGH | Remove background task; move timer arms into `select!` loop; remove all `Mutex` usage; retest shutdown |
| Non-atomic file write causing corrupt persistence | LOW | Delete the corrupt file; process restarts with empty state; metrics re-detected on next arrival |
| Pending list lost on shutdown | LOW | Metrics will reappear on next arrival and be flushed in the next cycle; acceptable per PROJECT.md design |
| HTTP client recreated per flush | MEDIUM | Add client as struct field; rebuild the binary |
| API key in config | HIGH | Rotate the key immediately; patch config design; audit any log sinks for leaked values |

---

## Pitfall-to-Phase Mapping

| Pitfall | Prevention Phase | Verification |
|---------|------------------|--------------|
| `FunctionTransform` for stateful transform | Phase 1: architecture skeleton | Code review confirms `TaskTransform` impl; no `enable_concurrency: true` |
| Detached background task + `tokio::sync::Mutex` | Phase 1: architecture skeleton | No `tokio::spawn` in transform code; no `tokio::sync::Mutex` anywhere |
| Blocking I/O in async stream | Phase 1: I/O strategy decision | Code review; integration test shows no 30s latency spikes |
| Pending list lost on shutdown | Phase 2: flush logic | Shutdown integration test with HTTP mock assertion |
| `tokio::sync::Mutex` on HashMap | Phase 1: architecture skeleton | CLAUDE.md review; grep for `tokio::sync::Mutex` in CI |
| HTTP client recreated per flush | Phase 1: struct construction | Code review confirms single `reqwest::Client` field |
| Non-atomic file write | Phase 2: persistence layer | Test simulating truncation-before-flush verifies recovery |
| API key in config struct | Phase 1: config design | `DD_API_KEY` field absent from `TransformConfig`; present only on built transform |

---

## Sources

- Vector source at `fbb1e4b` — `lib/vector-core/src/transform/mod.rs` (Transform, FunctionTransform, TaskTransform, enable_concurrency semantics), `lib/vector-stream/src/expiration_map.rs` (map_with_expiration canonical pattern), `src/transforms/reduce/transform.rs` (stateful TaskTransform reference implementation), `src/transforms/throttle/transform.rs` (async_stream! + select! pattern)
- `pomsky-intake/src/sinks/arrow_ipc_metrics.rs` — canonical pattern for single `reqwest::Client` construction and stateful sink loop (lines 169, 196–227)
- `pomsky-intake/src/transforms/preprocess_metric.rs` — canonical `FunctionTransform` pattern (correct for stateless; must NOT be the model for stateful transforms)
- `pomsky-intake/src/intake_runner.rs` — Vector topology lifecycle: `started.main().await` → `finished.shutdown().await`; transform input stream is closed at this boundary
- CLAUDE.md GAP-002 — `tokio::sync::Mutex` and `JoinHandle::abort()` forbidden; actor model / message passing required for async coordination
- CLAUDE.md reliability rules — `debug_assert` vs `Result`, no `unwrap()` in library code
- `.planning/workstreams/alans-workstream/PROJECT.md` — explicit decisions: drop-on-failure, env-var for API key, CSV persistence format, 15s HTTP flush, 30s file persist, 12-36h TTL

---
*Pitfalls research for: stateful Vector custom transform (metric metadata tracker)*
*Researched: 2026-04-16*
