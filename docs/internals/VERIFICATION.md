# Quickhouse-Pomsky Verification Guide

For the philosophical foundation and "why" behind this stack, see [VERIFICATION_STACK.md](VERIFICATION_STACK.md).

## The Verification Pyramid

All verification layers share the same invariants defined once in a shared invariants module:

```
         TLA+ Specs (docs/internals/specs/tla/*.tla)
                    │ mirrors
         Shared Invariants (invariants/)  ← SINGLE SOURCE OF TRUTH
                    │ used by
    ┌───────────────┼───────────────┬─────────────────────┐
    ▼               ▼               ▼                     ▼
Stateright      DST Tests      Integration          Production Metrics
(exhaustive)    (simulation)   Tests                (Datadog)
```

## Simulation-First Development

**The order is non-negotiable:**

```
1. Read TLA+ spec       → Understand invariants
2. Write DST tests      → Encode invariants as executable tests
3. Run tests            → EXPECT FAILURE (no implementation yet)
4. Implement code       → Make tests pass
5. Verify all layers    → DST + Stateright + unit tests
6. Create PR            → Include verification evidence
```

**NEVER implement code before writing DST tests.**

## Deterministic Simulation Testing (DST)

### Overview

DST provides deterministic control over time, network, storage, and randomness. This enables fault injection testing that is fully reproducible via a seed value.

### Two Modes

| Mode | Runtime | Time Control | Fault Injection | Use Case |
|------|---------|--------------|-----------------|----------|
| **In-Memory** | Standard Rust | `SimClock` (app-level) | `FaultInjector` | CI, quick iteration |
| **gVisor DST** | gVisor kernel | `VirtualClocks` (kernel) | Syscall-level | Finding subtle bugs |

### Running DST Tests

```bash
# Run all DST tests (once the DST crate exists)
cargo test -p quickwit-dst

# Reproduce failure with specific seed
DST_SEED=12345 cargo test -p quickwit-dst

# Verbose fault logging
RUST_LOG=quickwit_dst=debug cargo test -p quickwit-dst
```

### Writing DST Tests

```rust
use quickwit_dst::{Simulation, SimConfig, FaultConfig, FaultType};

#[test]
fn test_with_faults() {
    let config = SimConfig::new(12345); // Deterministic seed
    let mut sim = Simulation::new(config)
        .with_fault(FaultConfig::new(FaultType::StorageWriteFail, 0.1));

    sim.run(|env| async move {
        let storage = env.storage();
        // Test logic - fully deterministic
        Ok(())
    }).unwrap();
}
```

### DST Guidelines

- **Always log the seed**: Failed tests must print `DST_SEED=X` for replay
- **Use `env.rng()`**: Never use `rand::thread_rng()`
- **Use `env.clock()`**: Never use `Instant::now()`
- **No time-dependent loops**: gVisor freezes time

### Full Data Lifecycle DST Test

The most comprehensive DST test exercises the **complete production data lifecycle**:

```
Ingest → Query → Compact → GC → Query Again
```

**Phases:**

| Phase | Operation | Verification |
|-------|-----------|--------------|
| 1 | Ingest documents + query | Correct count returned |
| 2 | Add more batches | Running count accurate |
| 3 | Compact splits | Count unchanged (no data loss) |
| 4 | Time advance + GC | Old splits deleted after grace period |
| 5 | Query again | Data still consistent |

### Runtime Trait Architecture (DST Compliance)

Production code is parameterized over `<R: Runtime>` to enable DST:

```
                  Runtime Trait
                       │
         ┌─────────────┼─────────────┐
         ▼             ▼             ▼
   SystemRuntime  SimRuntime   (future runtimes)
   (production)   (simulation)
```

**The Four Dimensions:**

| Dimension | Production | Simulation | Abstraction |
|-----------|------------|------------|-------------|
| **Time** | `Instant::now()`, `Utc::now()` | `SimClock` | `runtime.clock()` |
| **Network** | `tokio::net::*` | `SimNetwork` | `runtime.network()` |
| **Storage** | `object_store` | `SimStorage` | Storage trait |
| **RNG** | `rand::thread_rng()` | `DeterministicRng` | `runtime.rng()` |

**Forbidden in Production Code:**

```rust
// WRONG: Bypasses Runtime, breaks DST
let now = Instant::now();
let now = Utc::now();
tokio::time::sleep(duration).await;
let rng = thread_rng();

// CORRECT: Goes through Runtime, DST-compliant
let now = runtime.clock().now_instant();
let now = runtime.clock().now();
runtime.clock().sleep(duration).await;
let val = runtime.rng().next_u64();
```

### Documenting DST Bugs

When DST finds a bug, create `docs/dst/DST_BUG_NNN_DESCRIPTION.md`:

```markdown
# DST Bug #NNN: Description

**Status**: Fixed/Open
**Discovered**: YYYY-MM-DD
**Seeds**: comma-separated failing seeds
**Component**: crate::module

## Summary
## Reproduction
## Root Cause
## Fix
## Verification
## Lessons Learned
```

## TLA+ Specifications

Human-readable formal specifications in `docs/internals/specs/tla/`:

Specs should be written for:
- Concurrency protocols (transactions, locks)
- State machines (lifecycle, recovery)
- Consistency guarantees (exactly-once, ordering)
- Resource management (GC, caching)

Each spec defines:
- **State variables**: What the system tracks
- **Actions**: State transitions
- **Invariants**: Properties that must always hold
- **Temporal properties**: Liveness guarantees

### When to Write Specs

**Add specs for:**
- Concurrency protocols (transactions, locks)
- State machines (lifecycle, recovery)
- Consistency guarantees (exactly-once, ordering)
- Resource management (GC, caching)

**Skip specs for:**
- Simple CRUD
- Stateless transformations
- Well-understood algorithms

### Key Areas Needing Specs

| Area | Component | Key Invariants |
|------|-----------|----------------|
| Split lifecycle | `quickwit-metastore` | No lost splits, no premature visibility |
| Compaction | `quickwit-indexing` | Atomic split swap, no data loss |
| Ingest pipeline | `quickwit-ingest` | Backpressure, bounded buffers |
| Shard management | `quickwit-control-plane` | No split-brain, consistent assignment |
| Tantivy + Parquet | `quickwit-indexing` | Dual-write consistency |

## Shared Invariants

**Single source of truth**: Invariant definitions live in one place and are used by all verification layers.

```rust
// Both DST and production code use the same invariant definitions
use quickwit_invariants::{SplitPropertyChecker, PropertyChecker};

let checker = SplitPropertyChecker::new(&state);
let result = checker.no_lost_splits();

if !result.holds {
    println!("{}", result);
    // no_lost_splits: Split abc123 not visible in metastore
}
```

### Invariant Modules (to build)

| Module | Properties |
|--------|------------|
| `splits.rs` | `no_lost_splits`, `no_premature_visibility`, `no_zombie_splits` |
| `compaction.rs` | `compaction_atomicity`, `no_data_loss_during_compaction` |
| `ingest.rs` | `no_buffer_overflow`, `backpressure_correctness` |
| `shard.rs` | `no_split_brain`, `shard_assignment_consistency` |
| `tantivy_parquet.rs` | `tantivy_subset_of_parquet`, `idle_consistency` |

## Stateright Model Checking

Rust-native exhaustive state space exploration:

```bash
# Run Stateright model checking
cargo test -p quickwit-dst stateright -- --ignored
```

Benefits over TLA+:
- Runs in CI with `cargo test`
- Uses Rust type system
- Uses same shared invariants as DST
- No separate TLC toolchain

### Writing Stateright Models

```rust
impl Model for SplitLifecycleModel {
    type State = SplitState;
    type Action = SplitAction;

    fn init_states(&self) -> Vec<Self::State> { ... }
    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) { ... }
    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> { ... }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("no_lost_splits", |_, state| {
                let checker = SplitPropertyChecker::new(state);
                checker.no_lost_splits().holds
            }),
        ]
    }
}
```

## Kani Bounded Model Checking

Verifies debug_assert! invariants hold for ALL inputs:

```bash
# All proofs
cargo kani

# Specific crate
cargo kani --package quickwit-metastore

# Specific proof
cargo kani --package quickwit-metastore --harness verify_no_lost_splits
```

### Writing Kani Proofs

```rust
#[cfg(kani)]
mod kani_proofs {
    use super::*;

    #[kani::proof]
    #[kani::unwind(10)]  // Bound loops
    fn verify_my_invariant() {
        let input: u64 = kani::any();
        kani::assume(input > 0);

        let result = my_function(input);

        kani::assert(result > input, "Result must exceed input");
    }
}
```

**Platform note**: Kani works best on x86_64 Linux. Run in CI for reliable results.

## Production Observability

Closing the verification loop with Datadog:

```rust
// Record invariant check in production
quickwit_observability::record_invariant("no_lost_splits", passed);
```

### Metrics

| Metric | Purpose |
|--------|---------|
| `pomsky_invariant_checks.count` | Total checks |
| `pomsky_invariant_checks_passed.count` | Passed checks |
| `pomsky_invariant_checks_failed.count` | Failed checks (0 = healthy) |
| `pomsky_invariant_health` | Health gauge (1.0 = all passing) |

### Adding Production Invariants

```rust
// 1. Add verification method
impl SplitMetastore {
    async fn verify_no_lost_splits(&self, ...) -> bool {
        // Check condition
    }
}

// 2. Call record_invariant after operation
let passed = self.verify_no_lost_splits(...).await;
quickwit_observability::record_invariant("no_lost_splits", passed);
```

## Verification Checklist

Before merging code that affects correctness:

- [ ] TLA+ spec reviewed (if exists)
- [ ] DST tests written and passing
- [ ] Stateright model updated (if applicable)
- [ ] Kani proofs added for new invariants
- [ ] Production invariant recording wired
- [ ] Seed logged in test output for reproducibility
