# Quickhouse-Pomsky Development Guide

## Before Writing Any Code (Plan Mode)

**MUST** follow this sequence before implementation:

1. **Define the plan**: What are you doing and why? What invariants must hold?
2. **Check ADR/roadmap**: `docs/internals/adr/README.md` → find relevant supplement
3. **Read the spec**: If touching state machines or protocols, read `docs/internals/specs/tla/*.tla`
4. **Write tests first**: DST tests define correctness, write them before code
5. **Only then**: Start implementation

## Core Policies

- Execute given tasks fully, avoid TODOs or stubs.
- If TODOs or stubs are absolutely necessary, ensure user is made aware and they are recorded in any resulting plans, phases, or specs.
- Produce code and make decisions that are consistent across metrics, traces, and logs. Metrics is the current priority, then traces, then logs — but decisions should generalize to all three.
- Tests should be holistic: do not work around broken implementations by manipulating tests.

## Known Pitfalls (Update When Claude Misbehaves)

**Add rules here when Claude makes mistakes. This is a living document.**

| Mistake | Correct Behavior | Bug Reference |
|---------|------------------|---------------|
| Adds mock/fallback implementations | Use real dependencies, no error masking | User preference |
| Claims feature works without integration test | Run through the actual REST/gRPC stack | CLAUDE.md policy |
| Uses workarounds to avoid proper setup | **NEVER** workaround — follow the rigorous path (clone deps, fix env, run real tests) | User policy |
| Bypasses production path in tests | **MUST** test through HTTP/gRPC, not internal APIs | CLAUDE.md policy |
| Uses `Path::exists()` | Disallowed by `clippy.toml` — use fallible alternatives | clippy.toml |
| Uses `Option::is_some_and`, `is_none_or`, `xor`, `map_or`, `map_or_else` | Disallowed by `clippy.toml` — use explicit match/if-let instead | clippy.toml |
| Ignores clippy warnings | Run `cargo clippy --workspace --all-features --tests`. Fix warnings or add targeted `#[allow()]` with justification | Code quality |
| Uses `debug_assert` for user-facing validation | Use `Result` errors — debug_assert is silent in release | Code quality |
| Uses `unwrap()` in library code | Use `?` operator or proper error types | Quickwit style |
| File over 500 lines | Split into focused modules by responsibility | Code quality |
| Unnecessary `.clone()` in non-concurrent code | Return `&self` references or `Arc<T>` — cloning is OK in actor/async code for simplicity | Code quality |
| Raw String for new domain types | Prefer existing type aliases (`IndexId`, `SplitId`, `SourceId` from `quickwit-proto`) | Quickwit style |
| Shadowing variable names within a function | Avoid reusing the same variable name (see CODE_STYLE.md) | Quickwit style |
| Uses chained iterators with complex error handling | Use procedural for-loops when chaining hurts readability | Quickwit style |
| Uses `tokio::sync::Mutex` | **FORBIDDEN** — causes data corruption on cancel. Use actor model with message passing | GAP-002 |
| Uses `JoinHandle::abort()` | **FORBIDDEN** — arbitrary cancellation violates invariants. Use `CancellationToken` | GAP-002 |
| Recreates futures in `select!` loops | Use `&mut fut` to resume, not recreate — dropping loses data | GAP-002 |
| Holds locks across await points | Invariant violations on cancel. Use message passing or synchronous critical sections | GAP-002 |

## What is Quickhouse-Pomsky?

**Fork of [Quickwit](https://github.com/quickwit-oss/quickwit)** — a cloud-native search engine for observability. This is the DataDog fork, adding:

- **Metrics engine** (`quickwit-metrics-engine`): DataFusion/Parquet-based analytics pipeline (current priority)
- **Remote API** (`quickwit-remote-api`): gRPC/REST interface for remote operations
- **Document transforms** (`quickwit-doc-transforms`): Preprocessing pipeline
- **CloudPrem UI**: Datadog-specific frontend
- **Tantivy + Parquet hybrid**: Full-text search via Tantivy, columnar analytics via Parquet

**Signal priority**: Metrics first, then traces, then logs. Architectural decisions must generalize across all three.

## Three Engineering Pillars

Every code change **MUST** respect all three:

| Pillar | Location | Purpose |
|--------|----------|---------|
| **Code Quality** | [CODE_STYLE.md](CODE_STYLE.md) + this doc | Coding standards & reliability |
| **Formal Specs** | `docs/internals/specs/tla/`, `stateright_*.rs` | Protocol correctness |
| **DST** | DST crate (when created) | Fault tolerance |

**Priority**: Safety > Performance > Developer Experience

## Reliability Rules

```rust
// 1. Use debug_assert! to document invariants
// (Quickwit CODE_STYLE.md endorses this — helps reviewers proofread)
debug_assert!(offset >= HEADER_SIZE, "offset must include header");
debug_assert!(splits.is_sorted_by_key(|s| s.time_range.end));

// 2. Validate inputs at API boundaries (Result, not debug_assert)
if duration.as_nanos() == 0 {
    return Err(Error::InvalidParameter("duration must be positive"));
}

// 3. Define explicit limits as constants
const MAX_SEGMENT_SIZE: usize = 256 * 1024 * 1024;
if size > MAX_SEGMENT_SIZE {
    return Err(Error::LimitExceeded(...));
}

// 4. No unwrap() in library code — propagate errors
let timestamp = DateTime::from_timestamp(secs, nsecs)
    .ok_or_else(|| anyhow!("invalid timestamp: {}", nanos))?;
```

## The Verification Pyramid

All verification layers share the same invariants:

```
         TLA+ Specs (docs/internals/specs/tla/*.tla)
                    │ mirrors
         Shared Invariants (invariants/)  ← SINGLE SOURCE
                    │ used by
    ┌───────────────┼───────────────┐
    ▼               ▼               ▼
Stateright      DST Tests      Production Metrics
(exhaustive)    (simulation)   (Datadog)
```

## Testing Through Production Path (CRITICAL)

**MUST NOT** claim a feature works unless tested through the actual network stack.

```bash
# 1. Start quickwit
cargo run -p quickwit-cli -- run --config ../config/quickwit.yaml

# 2. Ingest via OTLP
# (send logs/traces to localhost:4317)

# 3. Query via REST API
curl http://localhost:7280/api/v1/<index>/search -d '{"query": "*"}'
```

**Bypasses to AVOID**: Testing indexing pipeline without the HTTP/gRPC server, testing search without the REST API layer.

## Coding Style

Follow [CODE_STYLE.md](CODE_STYLE.md) — the primary style reference. Key points:

- **Readability over cleverness**: Optimize for "proofreadability"
- **Naming**: Long descriptive names preferred; standard Rust snake_case
- **Explanatory variables**: Introduce intermediary variables to convey semantics
- **No shadowing**: Do not reuse variable names within a function
- **Early returns**: Prefer early return over nested `else` chains
- **Invariants as `debug_assert`**: Use assertions to help reviewers proofread
- **Hidden contracts**: Avoid them; use types or `Result`/`Option` to enforce constraints
- **Generics/macros sparingly**: Only where necessary; they hurt readability and compile time
- **Async code**: Must not block for more than 500 microseconds; use `tokio::spawn_blocking` if unsure
- **No silent error ignoring** (`let _ =` without justification)

### Error and Log Messages

- Concise, lowercase (except proper names), no trailing punctuation
- Use `tracing` structured logging over string interpolation:
  ```rust
  // GOOD
  warn!(remaining = remaining_attempts, "rpc retry failed");
  // BAD
  warn!("rpc retry failed ({remaining_attempts} attempts remaining)");
  ```

### Enforced Clippy Rules (`quickwit/clippy.toml`)

These methods are **disallowed** and will fail CI:
- `std::path::Path::exists` — not sound (no `Result`)
- `Option::is_some_and`, `is_none_or`, `xor`, `map_or`, `map_or_else` — hurt readability

## Repository Layout

```
quickhouse-pomsky/
├── quickwit/                    # Main Rust workspace (all crates live here)
│   ├── Cargo.toml               # Workspace root
│   ├── Makefile                 # Inner build targets (fmt, fix, test-all, build)
│   ├── clippy.toml              # Disallowed methods (enforced)
│   ├── rustfmt.toml             # Nightly formatter config
│   ├── rust-toolchain.toml      # Pinned to Rust 1.91
│   └── rest-api-tests/          # Python-based REST API integration tests
├── docs/
│   └── internals/               # All architecture docs
│       ├── adr/                 # Architecture Decision Records
│       │   ├── README.md        # ADR index
│       │   ├── gaps/            # Design limitations from incidents
│       │   └── deviations/      # Intentional divergences from ADR intent
│       └── specs/
│           └── tla/             # TLA+ specs for protocols and state machines
├── config/                      # Runtime YAML configs (quickwit.yaml, etc.)
├── scripts/                     # DD-specific operational scripts
├── Makefile                     # Outer orchestration (docker, k8s, delegates to quickwit/)
├── docker-compose.yml           # Local services (localstack, postgres, kafka, jaeger, etc.)
└── k8s/                         # Kubernetes local dev (kind cluster)
```

## Crate Map

```
# Core services
quickwit-cli/                # Main binary entry point — start here for E2E
quickwit-serve/              # HTTP/gRPC server, REST API handlers
quickwit-cluster/            # Cluster membership (chitchat protocol)
quickwit-control-plane/      # Scheduling, shard management
quickwit-config/             # Configuration types and parsing

# Data path
quickwit-ingest/             # Ingestion pipeline, WAL, sharding
quickwit-indexing/           # Indexing actors, merge/compaction
quickwit-search/             # Search execution, distributed search
quickwit-query/              # Query parsing and AST
quickwit-doc-mapper/         # Schema, field mappings, doc-to-term
quickwit-doc-transforms/     # [DD] Log/trace preprocessing

# Storage & metadata
quickwit-metastore/          # Split metadata, index metadata
quickwit-storage/            # Object storage abstraction (S3, Azure, GCS, local)
quickwit-directories/        # Tantivy directory implementations

# Protocols & APIs
quickwit-proto/              # Protobuf definitions, generated gRPC code
quickwit-opentelemetry/      # OTLP ingest (logs, traces)
quickwit-jaeger/             # Jaeger-compatible trace API
quickwit-rest-client/        # HTTP client for Quickwit API
quickwit-remote-api/         # [DD] Remote gRPC/REST interface

# Metrics (DD additions)
quickwit-metrics-engine/     # DataFusion/Parquet metrics pipeline

# Infrastructure
quickwit-actors/             # Actor framework (mailbox, supervisor)
quickwit-common/             # Shared utilities
quickwit-datetime/           # Date/time parsing and formatting
quickwit-macros/             # Proc macros
quickwit-codegen/            # Code generation utilities
quickwit-aws/                # AWS SDK helpers

# Housekeeping
quickwit-janitor/            # GC, retention, delete tasks
quickwit-index-management/   # Index CRUD operations

# Testing
quickwit-integration-tests/  # Rust integration tests
rest-api-tests/              # Python REST API tests (Elasticsearch compat)
```

## Architecture Evolution

Quickhouse-Pomsky tracks architectural change through three lenses. See `docs/internals/adr/EVOLUTION.md` for the full process.

```
                    Architecture Evolution
                            │
       ┌────────────────────┼────────────────────┐
       ▼                    ▼                    ▼
 Characteristics          Gaps              Deviations
  (Proactive)          (Reactive)          (Pragmatic)
 "What we need"      "What we learned"   "What we accepted"
```

| Lens | Location | When to Use |
|------|----------|-------------|
| **Characteristics** | `docs/internals/adr/` | Track cloud-native requirements |
| **Gaps** | `docs/internals/adr/gaps/` | Design limitation from incident/production |
| **Deviations** | `docs/internals/adr/deviations/` | Intentional divergence from ADR intent |

**Before implementing, check for**:
- Open gaps (design limitations to be aware of)
- Deviations (intentional divergence from ADRs)
- Characteristic status (what's implemented vs planned)

## Common Commands

All Rust commands run from the `quickwit/` subdirectory.

```bash
# Build
cd quickwit && cargo build

# Run all tests (requires Docker services)
# From repo root:
make docker-compose-up
make test-all
# Or from quickwit/:
cargo nextest run --all-features --retries 5

# Run tests for a specific crate
cargo nextest run -p quickwit-indexing --all-features

# Run failpoint tests
cargo nextest run --test failpoints --features fail/failpoints

# Clippy (must pass before commit)
cargo clippy --workspace --all-features --tests

# Format (requires nightly)
cargo +nightly fmt

# Auto-fix clippy + format
make fix    # from quickwit/

# Check license headers
bash scripts/check_license_headers.sh

# Check log format
bash scripts/check_log_format.sh

# Spellcheck (from repo root)
make typos  # or: typos

# REST API integration tests (Python)
cd quickwit/rest-api-tests
pipenv shell && pipenv install
./run_tests.py --engine quickwit

# Metrics E2E tests (requires Docker infra)
# From quickwit/:
make docker-metrics-up
make test-metrics-e2e

# Docker build
make docker-build  # from repo root

# Local k8s (kind)
make k8s-up        # start cluster
make k8s-status    # check status
make k8s-logs      # follow logs
make k8s-down      # tear down
```

## Testing Strategy

### Unit Tests
- Run fast, avoid IO when possible
- Testing private functions is encouraged
- Property-based tests (`proptest`) are welcome — narrow the search space
- Not always deterministic — proptests are fine

### Integration Tests
- `quickwit-integration-tests/`: Rust integration tests exercising the full stack
- `rest-api-tests/`: Python YAML-driven tests for Elasticsearch API compatibility
- Metrics E2E: `make test-metrics-e2e` against Docker Compose (Minio + Postgres)

### DST (Deterministic Simulation Testing)
- DST tests define correctness for stateful components
- Write DST tests before implementation for new state machines
- Shared invariants are the single source of truth across all verification layers

### Required for CI
- `cargo nextest run --all-features --retries 5` (with Docker services running)
- Failpoint tests: `cargo nextest run --test failpoints --features fail/failpoints`
- `RUST_MIN_STACK=67108864` is set for test runs (64MB stack)

## Docker Services for Testing

```bash
# Start all services (localstack, postgres, kafka, jaeger, etc.)
make docker-compose-up

# Start specific services
make docker-compose-up DOCKER_SERVICES='jaeger,localstack'

# Tear down
make docker-compose-down

# Metrics-specific infra (Minio + Postgres)
cd quickwit && make docker-metrics-up
```

Environment variables set during test-all:
- `AWS_ACCESS_KEY_ID=ignored`, `AWS_SECRET_ACCESS_KEY=ignored`
- `QW_S3_ENDPOINT=http://localhost:4566` (localstack)
- `QW_S3_FORCE_PATH_STYLE_ACCESS=1`
- `QW_TEST_DATABASE_URL=postgres://quickwit-dev:quickwit-dev@localhost:5432/quickwit-metastore-dev`

## Key Entry Points

| Port | Protocol | Purpose |
|------|----------|---------|
| 7280 | HTTP | Quickwit REST API |
| 7281 | gRPC | Quickwit gRPC services |
| 4317 | gRPC | OTLP ingest |

## Checklist Before Committing

**MUST** (required for merge):
- [ ] `cargo clippy --workspace --all-features --tests` passes with no warnings
- [ ] `cargo +nightly fmt -- --check` passes
- [ ] `debug_assert!` for non-obvious invariants
- [ ] No `unwrap()` in library code
- [ ] No silent error ignoring (`let _ =`)
- [ ] New files under 500 lines (split by responsibility if larger)
- [ ] No unnecessary `.clone()` (OK in actor/async code for clarity)
- [ ] Tests through production path (HTTP/gRPC)
- [ ] License headers present (run `bash quickwit/scripts/check_license_headers.sh`)
- [ ] Log format correct (run `bash quickwit/scripts/check_log_format.sh`)
- [ ] `typos` passes (spellcheck)
- [ ] Tests pass: `cargo nextest run --all-features`

**SHOULD** (expected unless justified):
- [ ] Functions under 70 lines
- [ ] Explanatory variables for complex expressions
- [ ] Documentation explains "why"
- [ ] ADR/roadmap updated if applicable
- [ ] DST test for new state transitions
- [ ] Integration test for new API endpoints

## Detailed Documentation

| Topic | Location |
|-------|----------|
| Code style (Quickwit) | [CODE_STYLE.md](CODE_STYLE.md) |
| Rust style patterns | [docs/internals/RUST_STYLE.md](docs/internals/RUST_STYLE.md) |
| Verification & DST | [docs/internals/VERIFICATION.md](docs/internals/VERIFICATION.md) |
| Verification philosophy | [docs/internals/VERIFICATION_STACK.md](docs/internals/VERIFICATION_STACK.md) |
| Simulation workflow | [docs/internals/SIMULATION_FIRST_WORKFLOW.md](docs/internals/SIMULATION_FIRST_WORKFLOW.md) |
| Benchmarking | [docs/internals/BENCHMARKING.md](docs/internals/BENCHMARKING.md) |
| Contributing guide | [CONTRIBUTING.md](CONTRIBUTING.md) |
| ADR index | [docs/internals/adr/README.md](docs/internals/adr/README.md) |
| Architecture evolution | [docs/internals/adr/EVOLUTION.md](docs/internals/adr/EVOLUTION.md) |
| Compaction architecture | [docs/internals/compaction-architecture.md](docs/internals/compaction-architecture.md) |
| Tantivy + Parquet design | [docs/internals/tantivy-parquet-architecture.md](docs/internals/tantivy-parquet-architecture.md) |
| Locality compaction | [docs/internals/locality-compaction/](docs/internals/locality-compaction/) |
| Runtime config | [config/quickwit.yaml](config/quickwit.yaml) |

## References

- [Quickwit upstream](https://github.com/quickwit-oss/quickwit)
- [Tantivy search engine](https://github.com/quickwit-oss/tantivy)
- [Apache DataFusion](https://datafusion.apache.org/)
- [PomChi dependency](https://github.com/DataDog/PomChi) (private)
