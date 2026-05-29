# Quickwit Development Guide

## About Quickwit

[Quickwit](https://github.com/quickwit-oss/quickwit) is a cloud-native search engine for observability data (logs, traces, metrics). Key components:

- **Tantivy + Parquet hybrid**: Full-text search via Tantivy, columnar analytics via Parquet
- **Parquet metrics pipeline** (`quickwit-parquet-engine`): DataFusion/Parquet-based analytics (under active development)
- **Three observability signals**: Metrics, traces, and logs — architectural decisions must generalize across all three

See `quickwit/CLAUDE.md` for architecture overview, crate descriptions, and build commands.

## Core Policies

- Execute given tasks fully, avoid TODOs or stubs.
- If TODOs or stubs are absolutely necessary, ensure user is made aware and they are recorded in any resulting plans, phases, or specs.
- Produce code and make decisions that are consistent across metrics, traces, and logs. Metrics is the current priority, then traces, then logs — but decisions should generalize to all three.
- Tests should be holistic: do not work around broken implementations by manipulating tests.
- Follow [CODE_STYLE.md](CODE_STYLE.md) for all coding conventions.

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
| Silently swallows unexpected state | If a condition "shouldn't happen," return an error or assert — don't silently return Ok. Skipping optional/missing data is fine; pretending a bug didn't occur is not | Code quality |
| Replies to PR review comments as standalone inline comments | Use `gh api repos/.../pulls/{pr}/comments/{codex_comment_id}/replies` (or `in_reply_to_id` in the POST body) so the reply is **threaded under** the original review comment. Standalone inline comments at the same line are NOT replies; they show as separate threads. Verify with the GitHub API that `in_reply_to_id` is set on your reply. | Review hygiene |

## Engineering Priority

**Safety > Performance > Developer Experience**

| Pillar | Location | Purpose |
|--------|----------|---------|
| **Code Quality** | [CODE_STYLE.md](CODE_STYLE.md) + this doc | Coding standards & reliability |

> For formal specs (TLA+, Stateright) and DST pillars, see the verification docs below. These describe the target workflow — implementation is in progress.

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

## Testing Through Production Path

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

## Repository Layout

```
quickwit/                            # Repository root
├── quickwit/                        # Main Rust workspace (all crates live here)
│   ├── Cargo.toml                   # Workspace root
│   ├── CLAUDE.md                    # Build commands, architecture overview, crate guide
│   ├── Makefile                     # Build targets (fmt, fix, test-all, build)
│   ├── clippy.toml                  # Disallowed methods (enforced)
│   ├── rustfmt.toml                 # Nightly formatter config
│   ├── rust-toolchain.toml          # Pinned Rust toolchain
│   ├── scripts/                     # License header checks, log format checks
│   └── rest-api-tests/              # Python-based REST API integration tests
├── docs/
│   └── internals/                   # Architecture docs
│       ├── adr/                     # Architecture Decision Records
│       │   ├── README.md            # ADR index
│       │   ├── gaps/                # Design limitations from incidents
│       │   └── deviations/          # Intentional divergences from ADR intent
│       └── specs/
│           └── tla/                 # TLA+ specs for protocols and state machines
├── config/                          # Runtime YAML configs (quickwit.yaml, etc.)
├── Makefile                         # Outer orchestration (delegates to quickwit/)
└── docker-compose.yml               # Local services (localstack, postgres, kafka, jaeger, etc.)
```

## Architecture Evolution

Quickwit tracks architectural change through three lenses. See `docs/internals/adr/EVOLUTION.md` for the full process.

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
cargo +nightly fmt --all

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
- [ ] `cargo +nightly fmt --all -- --check` passes (run `cargo +nightly fmt --all` to fix; applies to **all** changed `.rs` files including tests — CI checks every file, not just lib code)
- [ ] `debug_assert!` for non-obvious invariants
- [ ] No `unwrap()` in library code
- [ ] No silent error ignoring (`let _ =`)
- [ ] New files under 500 lines (split by responsibility if larger)
- [ ] No unnecessary `.clone()` (OK in actor/async code for clarity)
- [ ] Tests through production path (HTTP/gRPC)
- [ ] License headers present (run `bash quickwit/scripts/check_license_headers.sh` — every `.rs`, `.proto`, and `.py` file needs the Apache 2.0 header)
- [ ] Log format correct (run `bash quickwit/scripts/check_log_format.sh`)
- [ ] `typos` passes (spellcheck)
- [ ] `cargo machete` passes (no unused dependencies in Cargo.toml)
- [ ] `cargo doc --no-deps` passes (each PR must compile independently, not just the final stack)
- [ ] Tests pass: `cargo nextest run --all-features`

**SHOULD** (expected unless justified):
- [ ] Functions under 70 lines
- [ ] Explanatory variables for complex expressions
- [ ] Documentation explains "why"
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

- [Quickwit](https://github.com/quickwit-oss/quickwit)
- [Tantivy search engine](https://github.com/quickwit-oss/tantivy)
- [Apache DataFusion](https://datafusion.apache.org/)
