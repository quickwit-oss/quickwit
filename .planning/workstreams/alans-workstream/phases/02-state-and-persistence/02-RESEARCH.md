# Phase 2: State and Persistence - Research

**Researched:** 2026-04-20
**Domain:** In-memory state management, CSV file persistence, TTL randomization in Rust
**Confidence:** HIGH

## Summary

Phase 2 adds in-memory state (a known-metrics HashMap with per-entry TTL) and atomic CSV persistence to the existing `MetricMetadataTransform` skeleton from Phase 1. The transform currently sits at 437 lines in a single file, so the 500-line limit from CLAUDE.md requires splitting into focused modules before adding ~200+ lines of state and persistence logic.

The technical domain is straightforward Rust standard library + two workspace dependencies (`rand` 0.9, `tempfile` 3.27). No external services or complex frameworks are needed. The key design decisions are already locked (eager pruning only, HashMap for pending dedup, CSV with header, tempfile-then-rename for atomicity). The primary implementation risk is the tempfile same-filesystem constraint and correctly wiring the `NamedTempFile::new_in` call to create the temp file in the same directory as the final persist path.

**Primary recommendation:** Split `metric_metadata.rs` into a `metric_metadata/` module directory with `mod.rs` (transform + config), `known_metrics.rs` (KnownMetrics struct + TTL logic), and `csv_persistence.rs` (load/save). Add `rand = { workspace = true }` to `pomsky-intake/Cargo.toml`. Use short TTL values in tests rather than an injectable clock.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** Add `persist_file_path: String` to `MetricMetadataConfig` with a serde default (e.g. `/tmp/metric_metadata_known.csv`). Operator controls location via YAML config.
- **D-02:** CSV columns: `metric_name,expiry_ts` (Unix timestamp). Minimal format -- type info is re-derived from live events, not persisted.
- **D-03:** CSV includes a header row (`metric_name,expiry_ts`) as the first line. Parser skips header on load. Aids human debugging.
- **D-04:** On startup load (PERSIST-03): missing file treated as empty known set; malformed rows skipped with a warning log; header row skipped.
- **D-05:** File writes use tempfile-then-rename pattern (PERSIST-02) for atomicity.
- **D-06:** Pending list is a `HashMap<String, MetricTypeInfo>` keyed by metric name. Natural dedup via HashMap semantics (XFRM-04).
- **D-07:** Last-seen-wins on dedup conflict. If the same metric name arrives twice with different type info in one flush cycle, `HashMap::insert` overwrites with the latest observation.
- **D-08:** Eager pruning only -- expired entries are swept during the persist tick, before writing CSV. No lazy eviction on lookup.
- **D-09:** Between persist ticks, expired entries are still treated as "known" on lookup. A metric whose TTL expired will not be re-added to pending until the next persist tick sweeps it from the map. This simplifies the lookup path and the maximum re-submission delay is bounded by `persist_interval_secs` (default 30s).
- **D-10:** Persist tick only writes known-metrics CSV and prunes expired entries. Pending list is managed by the flush cycle (Phase 3). Clean separation of concerns.

### Claude's Discretion
- Module structure: whether to split state management into `known_metrics.rs` / `csv_persistence.rs` or keep consolidated. Current file is 437 lines; Claude should split if adding state pushes past 500.
- RNG choice for TTL randomization (standard `rand` crate with thread-local RNG expected)
- Exact error types for CSV parse failures
- Test helper design for time-based TTL tests (e.g., injectable clock vs. short TTL values)

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| STATE-01 | In-memory HashMap tracks known metric names with per-entry expiry timestamp | KnownMetrics struct with `HashMap<String, u64>` (name -> expiry Unix timestamp) |
| STATE-02 | TTL is randomized uniformly in range [12h, 36h] per metric entry | `rand::rng().random_range(min_secs..=max_secs)` using workspace `rand` 0.9 |
| STATE-03 | Expired entries are pruned during periodic persistence or lookup | `prune()` method called during persist tick; per D-08/D-09, eager pruning only (no lazy eviction on lookup) |
| PERSIST-01 | Known metrics written to CSV file every configurable interval (default 30s) | `save_to_csv()` writes header + entries; interval driven by Phase 4's select! loop (Phase 2 provides the function, not the timer) |
| PERSIST-02 | File writes use atomic tempfile-then-rename pattern | `NamedTempFile::new_in(parent_dir)` + write + `persist(target_path)` |
| PERSIST-03 | On startup, known metrics loaded from CSV; missing file treated as empty; malformed rows skipped with warning | `load_from_csv()` with `std::io::ErrorKind::NotFound` check and per-line parsing with `tracing::warn!` |
| XFRM-02 | Each metric is checked against the known set; unknowns added to pending list | `KnownMetrics::contains()` check in transform closure; unknown metrics inserted into pending HashMap |
| XFRM-04 | Pending list deduplicates by metric name within a flush cycle | `HashMap<String, MetricTypeInfo>` -- HashMap::insert naturally deduplicates (D-06, D-07) |
</phase_requirements>

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Known-metrics state | In-process Rust (transform) | -- | HashMap lives in the transform struct; no external service needed |
| TTL randomization | In-process Rust (transform) | -- | Pure computation using `rand` crate |
| CSV persistence | Local filesystem | -- | Atomic file write to operator-configured path |
| Pending list accumulation | In-process Rust (transform) | -- | HashMap in transform struct; Phase 3 flushes via HTTP |
| Metric known/unknown classification | In-process Rust (transform) | -- | Lookup against in-memory HashMap on each event |

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| rand | 0.9.4 (workspace: "0.9") | TTL randomization with uniform distribution | Already in workspace; 13 other crates use it. API: `rand::rng().random_range(min..=max)` [VERIFIED: workspace Cargo.toml + codebase grep] |
| tempfile | 3.27.0 (workspace: "3") | Atomic file writes via NamedTempFile::persist | Already in workspace AND pomsky-intake Cargo.toml [VERIFIED: Cargo.toml] |
| tracing | (workspace) | Warning logs for malformed CSV rows | Already in pomsky-intake dependencies [VERIFIED: Cargo.toml] |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| std::collections::HashMap | stdlib | Known-metrics map and pending list | Core data structures per D-01, D-06 |
| std::time::SystemTime | stdlib | Current Unix timestamp for TTL expiry computation | When inserting entries and checking expiry |
| std::io::BufRead | stdlib | Line-by-line CSV parsing | On startup load (PERSIST-03) |
| std::io::Write | stdlib | CSV file writing | During persist tick (PERSIST-01) |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Manual CSV parsing | `csv` crate | Two-column format is trivial; csv crate adds unnecessary dependency weight for `metric_name,expiry_ts` |
| `SystemTime::now()` | `chrono` crate | SystemTime suffices for Unix timestamps; chrono already in dev-dependencies but not needed at runtime |
| `HashMap<String, u64>` | `BTreeMap` for sorted iteration | HashMap is faster for lookups (O(1) vs O(log n)); no ordering requirement exists |

**Installation (add to pomsky-intake/Cargo.toml):**
```toml
rand = { workspace = true }
```

**Version verification:**
- `rand`: workspace declares "0.9", resolves to 0.9.4 [VERIFIED: cargo metadata]
- `tempfile`: workspace declares "3", resolves to 3.27.0; already in pomsky-intake Cargo.toml [VERIFIED: cargo metadata + Cargo.toml]

## Architecture Patterns

### System Architecture Diagram

```
                    metric event (from preprocess_metric)
                              |
                              v
                   +---------------------+
                   | MetricMetadataTransform |
                   |  .transform() stream   |
                   +---------------------+
                              |
                    map_metric_type(metric)
                              |
                              v
                   +---------------------+
                   | known_metrics.contains(name)?  |
                   +---------------------+
                     /              \
                  YES                NO
                   |                  |
                (skip)        pending.insert(name, type_info)
                   |                  |        [D-06 HashMap dedup]
                   |                  |        [D-07 last-seen-wins]
                   v                  v
              emit event unchanged
                              |
                              v
                        downstream sink

    === Persist Tick (called from Phase 4 select! loop) ===

    known_metrics.prune_expired()  -->  remove entries where now > expiry_ts
                  |
                  v
    save_to_csv(persist_file_path)
       |
       +-- NamedTempFile::new_in(parent_dir)
       +-- write "metric_name,expiry_ts\n" header
       +-- write each entry as "name,ts\n"
       +-- temp_file.persist(persist_file_path)  [atomic rename]

    === Startup Load ===

    load_from_csv(persist_file_path)
       |
       +-- open file (missing -> empty HashMap)
       +-- skip header line
       +-- for each line: parse name,ts; skip malformed with warn!
       +-- return HashMap<String, u64>
```

### Recommended Project Structure

```
pomsky-intake/src/transforms/
    metric_metadata/
        mod.rs               # MetricMetadataConfig, MetricMetadataTransform, TaskTransform impl
        known_metrics.rs     # KnownMetrics struct (HashMap + TTL + contains/insert/prune)
        csv_persistence.rs   # load_from_csv(), save_to_csv() free functions
        types.rs             # MetadataMetricType, MetricTypeInfo, map_metric_type()
    mod.rs                   # updated: pub mod metric_metadata (unchanged re-export)
    preprocess_metric.rs
    preprocess_log.rs
    preprocess_trace.rs
    explode_trace_spans.rs
```

**Rationale for split:** Current `metric_metadata.rs` is 437 lines. Phase 2 adds ~200+ lines (KnownMetrics struct, CSV read/write, pending list logic, and their tests). This would push past the 500-line project limit. Splitting into a module directory with focused files keeps each under 250 lines. [VERIFIED: `wc -l` on current file]

**Module re-export compatibility:** `transforms/mod.rs` currently has `pub mod metric_metadata;`. A directory `metric_metadata/` with `mod.rs` is transparent to Rust's module system -- no change needed to the re-export. [VERIFIED: Rust module resolution rules]

### Pattern 1: KnownMetrics Struct

**What:** Encapsulates the in-memory known-metrics HashMap with TTL logic
**When to use:** All known-metric operations (insert, contains, prune, load, save)

```rust
// Source: designed per D-01, D-08, D-09, STATE-01, STATE-02
use std::collections::HashMap;

pub struct KnownMetrics {
    /// Maps metric name -> expiry Unix timestamp (seconds).
    entries: HashMap<String, u64>,
    ttl_min_secs: u64,
    ttl_max_secs: u64,
}

impl KnownMetrics {
    pub fn new(ttl_min_hours: u64, ttl_max_hours: u64) -> Self {
        Self {
            entries: HashMap::new(),
            ttl_min_secs: ttl_min_hours * 3600,
            ttl_max_secs: ttl_max_hours * 3600,
        }
    }

    /// Checks whether a metric name is in the known set.
    /// Per D-09: expired entries are still treated as "known" between ticks.
    pub fn contains(&self, name: &str) -> bool {
        self.entries.contains_key(name)
    }

    /// Inserts a metric with a fresh randomized TTL.
    pub fn insert(&mut self, name: String) {
        let now_secs = now_unix_secs();
        let ttl = rand::rng().random_range(self.ttl_min_secs..=self.ttl_max_secs);
        self.entries.insert(name, now_secs + ttl);
    }

    /// Removes all entries whose expiry timestamp is in the past.
    /// Called during persist tick per D-08.
    pub fn prune_expired(&mut self) {
        let now_secs = now_unix_secs();
        self.entries.retain(|_name, expiry| *expiry > now_secs);
    }

    /// Returns an iterator over (name, expiry_ts) for CSV serialization.
    pub fn iter(&self) -> impl Iterator<Item = (&str, u64)> {
        self.entries.iter().map(|(k, v)| (k.as_str(), *v))
    }

    /// Number of known metrics (for logging/testing).
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

fn now_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
        .as_secs()
}
```

### Pattern 2: CSV Persistence (Load)

**What:** Reads known metrics from CSV on startup
**When to use:** During `TransformConfig::build()`

```rust
// Source: designed per D-03, D-04, PERSIST-03
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::Path;
use tracing::warn;

const CSV_HEADER: &str = "metric_name,expiry_ts";

pub fn load_from_csv(path: &Path) -> anyhow::Result<HashMap<String, u64>> {
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            // D-04: missing file = empty known set
            return Ok(HashMap::new());
        }
        Err(err) => return Err(err.into()),
    };

    let reader = BufReader::new(file);
    let mut entries = HashMap::new();

    for (line_number, line_result) in reader.lines().enumerate() {
        let line = match line_result {
            Ok(l) => l,
            Err(err) => {
                warn!(line_number, error=%err, "skipping unreadable CSV line");
                continue;
            }
        };

        // D-03: skip header row
        if line_number == 0 && line.trim() == CSV_HEADER {
            continue;
        }

        let Some((name, ts_str)) = line.split_once(',') else {
            warn!(line_number, line=%line, "skipping malformed CSV line: no comma");
            continue;
        };

        let expiry_ts: u64 = match ts_str.trim().parse() {
            Ok(ts) => ts,
            Err(err) => {
                warn!(
                    line_number,
                    value=%ts_str.trim(),
                    error=%err,
                    "skipping malformed CSV line: invalid timestamp"
                );
                continue;
            }
        };

        entries.insert(name.trim().to_string(), expiry_ts);
    }

    Ok(entries)
}
```

### Pattern 3: CSV Persistence (Save with Atomic Write)

**What:** Atomically writes known metrics to CSV
**When to use:** During persist tick (called from Phase 4's select! loop)

```rust
// Source: designed per D-02, D-03, D-05, PERSIST-01, PERSIST-02
use std::io::Write;
use std::path::Path;
use tempfile::NamedTempFile;

pub fn save_to_csv(
    path: &Path,
    entries: impl Iterator<Item = (&str, u64)>,
) -> anyhow::Result<()> {
    // D-05: create temp file in same directory for atomic rename
    let parent = path.parent().unwrap_or(Path::new("."));
    let mut temp_file = NamedTempFile::new_in(parent)?;

    // D-03: header row
    writeln!(temp_file, "{}", CSV_HEADER)?;

    // D-02: metric_name,expiry_ts
    for (name, expiry_ts) in entries {
        writeln!(temp_file, "{},{}", name, expiry_ts)?;
    }

    temp_file.flush()?;
    // D-05: atomic rename
    temp_file.persist(path)?;
    Ok(())
}
```

### Pattern 4: Transform Integration (Phase 2 changes to transform closure)

**What:** Wire KnownMetrics and pending list into the transform stream
**When to use:** In `TaskTransform::transform()` -- replaces Phase 1's no-op closure

```rust
// Source: designed per XFRM-02, XFRM-04, D-06, D-07
// NOTE: Phase 2 changes the simple stream.map to check known set
// The actual select! loop with timers is Phase 4; Phase 2 just
// provides the data structures and per-event logic.

impl TaskTransform<Event> for MetricMetadataTransform {
    fn transform(
        self: Box<Self>,
        task: Pin<Box<dyn Stream<Item = Event> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Event> + Send>> {
        let mut known_metrics = self.known_metrics;
        let mut pending = self.pending;

        Box::pin(task.map(move |event| {
            if let Event::Metric(ref metric) = event {
                let name = metric.name().to_string();
                if !known_metrics.contains(&name) {
                    let type_info = map_metric_type(metric);
                    // D-06: HashMap dedup; D-07: last-seen-wins
                    pending.insert(name, type_info);
                }
            }
            event
        }))
    }
}
```

### Anti-Patterns to Avoid

- **Lazy eviction on lookup:** D-08/D-09 explicitly mandate eager-only pruning. Do NOT check expiry in `contains()`. An expired entry is still "known" until the persist tick sweeps it.
- **Using `Path::exists()` to check for CSV file:** Disallowed by `clippy.toml`. Use `std::fs::File::open()` and match on `ErrorKind::NotFound` instead.
- **Creating NamedTempFile in system temp dir:** MUST use `NamedTempFile::new_in(parent_dir)` where `parent_dir` is the parent of `persist_file_path`. Cross-filesystem rename will fail silently with EXDEV. [CITED: docs.rs/tempfile/3.27.0]
- **Using `unwrap()` in library code:** Per CLAUDE.md, use `?` operator or proper error types. The only acceptable `expect()` is on `SystemTime::now().duration_since(UNIX_EPOCH)` which panics only if the system clock is before 1970.
- **Holding the HashMap across await points:** Not applicable in Phase 2 (no async in the per-event path), but important to note for Phase 4 integration. The `stream.map()` closure is synchronous. [VERIFIED: Phase 1 code]
- **Shadowing variable names:** Per CODE_STYLE.md, avoid reusing the same variable name within a function.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Atomic file writes | Manual write + rename | `tempfile::NamedTempFile::new_in()` + `.persist()` | Handles cleanup on error, platform-specific rename semantics, same-dir creation |
| Random number generation | Custom PRNG or time-based seeding | `rand::rng().random_range(min..=max)` | Thread-local, properly seeded, uniform distribution guaranteed |
| CSV parsing | Custom state machine | Simple `line.split_once(',')` | Format is trivial (2 columns, no quoting needed); a full CSV parser is overkill |

**Key insight:** The CSV format is intentionally minimal (two columns, no quoted fields, no commas in metric names). A full CSV parsing library would add unnecessary dependency weight. String splitting is the correct choice here.

## Common Pitfalls

### Pitfall 1: Cross-Filesystem Tempfile

**What goes wrong:** `NamedTempFile::persist()` fails with an OS error when the temp file and target path are on different filesystems.
**Why it happens:** `persist()` uses `rename()` under the hood, which cannot cross filesystem boundaries.
**How to avoid:** Always use `NamedTempFile::new_in(path.parent())` to create the temp file in the same directory as the target.
**Warning signs:** `persist()` returns `Err` with `EXDEV` or "Invalid cross-device link" on Linux/macOS.

### Pitfall 2: Metric Names Containing Commas

**What goes wrong:** CSV parsing breaks if a metric name contains a comma.
**Why it happens:** Using `split_once(',')` would split on the first comma in the name.
**How to avoid:** Datadog metric names follow the pattern `[a-zA-Z][a-zA-Z0-9_.]*` -- commas are not valid. Document this assumption with a `debug_assert!` in the save path and a warning in the load path.
**Warning signs:** Malformed CSV entries after round-trip.

### Pitfall 3: System Clock Regression

**What goes wrong:** If the system clock jumps backward, entries appear to have very long remaining TTL.
**Why it happens:** Expiry timestamps are absolute Unix timestamps; a clock jump makes `now < expiry` even for entries that should have expired.
**How to avoid:** This is an accepted limitation per the design (simple eager pruning). No mitigation needed in v1. The maximum impact is that some metrics are not re-submitted until the clock catches up. Document with a code comment.
**Warning signs:** Known-metrics set grows without bound over a restart.

### Pitfall 4: File Permission Errors on Persist Path

**What goes wrong:** `NamedTempFile::new_in()` or `persist()` fails because the directory doesn't exist or lacks write permissions.
**Why it happens:** Operator misconfigures `persist_file_path` in YAML.
**How to avoid:** Validate the parent directory exists and is writable during `TransformConfig::build()`. Return a descriptive error at startup rather than failing silently at the first persist tick.
**Warning signs:** Transform starts but persist tick logs errors every 30 seconds.

### Pitfall 5: Header Row Treated as Data Entry

**What goes wrong:** The header row `metric_name,expiry_ts` is parsed as a metric entry, causing a parse error on the timestamp.
**Why it happens:** Naive line-by-line parsing without skipping line 0.
**How to avoid:** Per D-03, explicitly check if `line_number == 0 && line.trim() == CSV_HEADER` and skip. Also handle the edge case where the header is missing (old/hand-edited file).

### Pitfall 6: Serde Default for persist_file_path

**What goes wrong:** `#[serde(deny_unknown_fields)]` on the config struct means adding a new field breaks backward compatibility if no default is provided.
**Why it happens:** The field is new in Phase 2.
**How to avoid:** Use `#[serde(default = "default_persist_file_path")]` with a sensible default like `/tmp/metric_metadata_known.csv`. [VERIFIED: existing config pattern in metric_metadata.rs uses this exact pattern for all optional fields]

## Code Examples

### TTL Randomization with rand 0.9

```rust
// Source: existing codebase pattern (quickwit-metastore/src/metastore/file_backed/mod.rs:1918-1920)
// Adapted for TTL generation per STATE-02
use rand::Rng;

let ttl_secs = rand::rng().random_range(self.ttl_min_secs..=self.ttl_max_secs);
let expiry_ts = now_unix_secs() + ttl_secs;
```

### Atomic File Write with tempfile

```rust
// Source: tempfile 3.27 API (docs.rs/tempfile/3.27.0)
use std::io::Write;
use tempfile::NamedTempFile;

let parent_dir = persist_path.parent().unwrap_or(std::path::Path::new("."));
let mut tmp = NamedTempFile::new_in(parent_dir)?;
writeln!(tmp, "metric_name,expiry_ts")?;
for (name, ts) in entries {
    writeln!(tmp, "{},{}", name, ts)?;
}
tmp.flush()?;
tmp.persist(persist_path)?;
```

### Structured Warning Log (per CODE_STYLE.md)

```rust
// Source: CODE_STYLE.md log format rules
// Log messages: lowercase, no trailing punctuation, structured fields
use tracing::warn;

warn!(
    line_number,
    value=%ts_str.trim(),
    "skipping malformed CSV line: invalid timestamp"
);
```

### Testing TTL Bounds (1000 entries)

```rust
// Source: designed per STATE-02 success criterion
#[test]
fn test_ttl_randomization_bounds() {
    let min_hours = 12u64;
    let max_hours = 36u64;
    let min_secs = min_hours * 3600;
    let max_secs = max_hours * 3600;
    let mut known = KnownMetrics::new(min_hours, max_hours);

    let before = now_unix_secs();
    for i in 0..1000 {
        known.insert(format!("metric_{}", i));
    }
    let after = now_unix_secs();

    for (_name, expiry) in known.iter() {
        let ttl = expiry - before; // conservative: use earliest possible now
        assert!(
            ttl >= min_secs && ttl <= max_secs + (after - before),
            "TTL {} out of range [{}, {}]",
            ttl,
            min_secs,
            max_secs
        );
    }
}
```

### Testing CSV Round-Trip

```rust
// Source: designed per PERSIST-01/03 success criterion
#[test]
fn test_csv_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("known.csv");

    let mut original = HashMap::new();
    original.insert("cpu.user".to_string(), 1_700_000_000u64);
    original.insert("mem.free".to_string(), 1_700_100_000u64);

    save_to_csv(&path, original.iter().map(|(k, v)| (k.as_str(), *v))).unwrap();
    let loaded = load_from_csv(&path).unwrap();

    assert_eq!(original, loaded);
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `rand::thread_rng()` + `gen_range()` | `rand::rng()` + `random_range()` | rand 0.9 (2024) | API rename; `thread_rng()` deprecated in 0.9 |
| `tempfile::NamedTempFile::persist(path)` returns `io::Result<File>` | Same signature in 3.27 | Stable since tempfile 3.x | No change |
| `unsafe { std::env::set_var() }` | Still required in Rust 1.93 for test env manipulation | Rust 1.66+ | Must use `unsafe` block in tests that set/remove env vars |

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Datadog metric names never contain commas | Pitfall 2 | CSV parsing would break; would need quoting or escaping |
| A2 | Default persist path `/tmp/metric_metadata_known.csv` is acceptable | Architecture Patterns | Operators in containerized environments may need to override; `/tmp` may be tmpfs (lost on reboot) |
| A3 | `SystemTime::now()` is monotonic enough for TTL purposes | Pitfall 3 | Clock jumps could extend TTL unexpectedly; accepted limitation per design |

## Open Questions (RESOLVED)

1. **Parent directory validation at build() time** (RESOLVED)
   - **Resolution:** Validate at `build()` time using `std::fs::metadata(parent)`. If the parent directory does not exist or is not accessible, return a descriptive error. This matches the DD_API_KEY fail-fast pattern from Phase 1 and aligns with CLAUDE.md Known Pitfalls ("Silently swallows unexpected state"). Cannot use `Path::exists()` per `clippy.toml`. Implemented in Plan 02-02, Task 2, Step 3.

2. **Module split: 3 files or 4?** (RESOLVED)
   - **Resolution:** Split into 4 files (mod.rs, known_metrics.rs, csv_persistence.rs, types.rs). The types extraction keeps mod.rs under 300 lines and gives types.rs a focused responsibility. This was chosen because adding persist_file_path, KnownMetrics fields, CSV loading in build(), and pending list logic to mod.rs would push it well past comfortable size even with the KnownMetrics and CSV code extracted. Implemented in Plan 02-01, Task 1.

## Project Constraints (from CLAUDE.md)

**Enforced by clippy.toml -- MUST avoid:**
- `Path::exists()` -- use fallible alternatives (try_exists or open + match ErrorKind)
- `Option::is_some_and`, `is_none_or`, `xor`, `map_or`, `map_or_else` -- use explicit match/if-let

**Coding standards:**
- No `unwrap()` in library code -- use `?` operator
- Files under 500 lines -- split by responsibility
- `debug_assert!` for invariants (TTL range bounds, metric name format)
- Log messages: lowercase start, no trailing punctuation, prefer structured fields
- No shadowing of variable names within a function
- Early return style preferred over nested else chains
- Avoid deep nesting
- License headers on all `.rs` files (Apache 2.0)

**Testing:**
- Tests through production path when applicable (Phase 2 tests state logic, not network)
- Property-based tests welcome for TTL randomization bounds
- `cargo clippy --workspace --all-features --tests` must pass
- `cargo +nightly fmt --all` must pass
- `cargo machete` must pass (no unused deps)

**Security:**
- `api_key` field must not appear in Debug output (T-01-02, already handled in Phase 1)
- No new security surface in Phase 2 (file I/O is local, no network)

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | cargo nextest (Rust, workspace-level) |
| Config file | `quickwit/.config/nextest.toml` (if exists) or default |
| Quick run command | `cargo nextest run -p pomsky-intake --all-features` |
| Full suite command | `cargo nextest run -p pomsky-intake --all-features` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| STATE-01 | HashMap tracks known metric names with expiry | unit | `cargo nextest run -p pomsky-intake test_known_metrics_insert_and_contains` | Wave 0 |
| STATE-02 | TTL randomized in [12h, 36h] range | unit | `cargo nextest run -p pomsky-intake test_ttl_randomization_bounds` | Wave 0 |
| STATE-03 | Expired entries pruned during persist tick | unit | `cargo nextest run -p pomsky-intake test_prune_expired_entries` | Wave 0 |
| PERSIST-01 | Known metrics written to CSV | unit | `cargo nextest run -p pomsky-intake test_save_to_csv` | Wave 0 |
| PERSIST-02 | Atomic tempfile-then-rename writes | unit | `cargo nextest run -p pomsky-intake test_atomic_write` | Wave 0 |
| PERSIST-03 | Load from CSV; missing=empty; malformed=skip+warn | unit | `cargo nextest run -p pomsky-intake test_load_from_csv` | Wave 0 |
| XFRM-02 | Unknown metrics added to pending list | unit | `cargo nextest run -p pomsky-intake test_unknown_metric_added_to_pending` | Wave 0 |
| XFRM-04 | Pending list deduplicates by name | unit | `cargo nextest run -p pomsky-intake test_pending_dedup` | Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo nextest run -p pomsky-intake --all-features`
- **Per wave merge:** `cargo clippy --workspace --all-features --tests && cargo nextest run -p pomsky-intake --all-features`
- **Phase gate:** Full clippy + nextest + fmt + machete before verification

### Wave 0 Gaps
- [ ] `known_metrics.rs` -- KnownMetrics unit tests (STATE-01, STATE-02, STATE-03)
- [ ] `csv_persistence.rs` -- CSV load/save unit tests (PERSIST-01, PERSIST-02, PERSIST-03)
- [ ] Tests in `mod.rs` for transform integration (XFRM-02, XFRM-04)

## Security Domain

Phase 2 introduces local file I/O only. No network, no authentication, no user input beyond operator-controlled YAML config.

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | no | -- |
| V3 Session Management | no | -- |
| V4 Access Control | no | File permissions managed by OS |
| V5 Input Validation | yes (CSV parsing) | Manual validation with warn! for malformed rows; metric names from trusted upstream pipeline |
| V6 Cryptography | no | -- |

### Known Threat Patterns

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| CSV injection (metric name containing formulas) | Tampering | Metric names come from trusted Datadog Agent pipeline, not user input; accepted risk |
| Symlink attack on persist file path | Tampering | `NamedTempFile::persist()` uses `rename()` which replaces the target atomically; symlink would be followed -- operator controls the path |
| Disk exhaustion from unbounded CSV growth | Denial of Service | TTL expiry + prune ensures bounded size; growth proportional to unique metric count |

## Sources

### Primary (HIGH confidence)
- Workspace `Cargo.toml`: rand "0.9" (resolves to 0.9.4), tempfile "3" (resolves to 3.27.0) [VERIFIED: cargo metadata]
- `pomsky-intake/Cargo.toml`: tempfile already present; rand needs to be added [VERIFIED: file read]
- `pomsky-intake/src/transforms/metric_metadata.rs`: 437 lines, Phase 1 skeleton code [VERIFIED: file read + wc -l]
- `quickwit-metastore/src/metastore/file_backed/mod.rs:1918`: rand 0.9 API usage pattern (`rand::rng().random_range()`) [VERIFIED: codebase grep]
- `clippy.toml`: Disallowed methods list [VERIFIED: file read]
- `CODE_STYLE.md`: Log format rules, naming conventions [VERIFIED: file read]

### Secondary (MEDIUM confidence)
- `docs.rs/tempfile/3.27.0/tempfile/struct.NamedTempFile.html`: persist() semantics, new_in() API [CITED: docs.rs/tempfile/3.27.0]
- `docs.rs/rand/0.9.0/rand/trait.Rng.html`: random_range() API [CITED: docs.rs/rand/0.9.0]

### Tertiary (LOW confidence)
- None

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all dependencies already in workspace, APIs verified against codebase usage
- Architecture: HIGH -- all design decisions locked in CONTEXT.md, patterns straightforward
- Pitfalls: HIGH -- identified from concrete API constraints (tempfile cross-FS, clippy.toml rules) and codebase conventions

**Research date:** 2026-04-20
**Valid until:** 2026-05-20 (stable domain, no fast-moving dependencies)
