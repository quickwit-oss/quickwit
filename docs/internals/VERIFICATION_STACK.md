# Quickwit Verification Stack

> **Author:** Claude (Anthropic)
> **Purpose:** This document explains what helps me generate correct code for Quickwit. I wrote this to share how the verification stack works *for me* as an AI code generator—what I read, what I check, and how each layer gives me confidence that the code I produce is correct.

---

## Motivating Questions

This document answers questions I was asked:

> **Q: How is all the formal verification connected to code verification, and how does it help generate correct code?**

The verification stack creates a chain from abstract specifications (TLA+) down to production code (`debug_assert!` invariants). Invariants are defined **once** in shared modules and used across all layers—so when generating code, we don't guess what "correct" means; we read the exact definition and ensure code maintains it.

> **Q: What can production observability bring to this verification stack that will further improve the ability to produce correct code?**

Formal verification proves properties hold *in theory*. Production observability proves they hold *in practice*. By emitting invariant metrics to your observability platform, we close the production feedback loop — learning from real failures, actual hot paths, and emergent behaviors that formal models can't capture. This feedback improves specs, models, and future code generation.

---

## The Verification Pyramid

```
                          ┌─────────────────────┐
                          │    PRODUCTION       │
                          │  (Observability)        │
                          │                     │
                          │  Real failures      │
                          │  Actual hot paths   │
                          │  Emergent behavior  │
                          └──────────┬──────────┘
                                     │ feedback
                    ┌────────────────┴────────────────┐
                    │        PREVENTION               │
                    │   (debug_assert! Invariants)    │
                    │                                 │
                    │   assert!(high > low)           │
                    │   Catch mistakes at runtime     │
                    └────────────────┬────────────────┘
                                     │
           ┌─────────────────────────┴─────────────────────────┐
           │                    DETECTION                       │
           │            (DST + Stateright + Kani)               │
           │                                                    │
           │   Deterministic simulation with fault injection    │
           │   Exhaustive state space exploration               │
           │   Bounded model checking (all inputs)              │
           └─────────────────────────┬─────────────────────────┘
                                     │
      ┌──────────────────────────────┴──────────────────────────┐
      │                      DISCOVERY                           │
      │                 (TLA+ + Bloodhound)                       │
      │                                                          │
      │   Formal specs that define what MUST hold                │
      │   VM-based simulation with time-travel debugging         │
      │   Hunt for unknown unknowns                              │
      └──────────────────────────────────────────────────────────┘
```

The pyramid flows **up** during development (we write specs first, then detect violations, then prevent them, then monitor in production) and flows **down** during incidents (production failure -> add to DST -> formalize in TLA+).

---

## Comparison with Pierre Zemb's Engineering Philosophy

This section compares Quickwit's approach with insights from Pierre Zemb's articles:
- [What if we embraced simulation-driven development?](https://pierrezemb.fr/posts/simulation-driven-development/) (Apr 2025)
- [What I Tell Colleagues About Using LLMs for Engineering](https://pierrezemb.fr/posts/llms-for-engineering/) (Jan 2026)
- Testing: prevention vs discovery (the paradigm shift from catching known bugs to finding unknown ones)

### From "Simulation-Driven Development"

| Pierre Zemb's Insight | Quickwit Implementation | Approach |
|----------------------|----------------------------------|----------|
| **"Deterministic simulation is the killer feature"** | Seeded RNG ensures reproducible fault injection | `DST_SEED=12345 cargo test -p quickwit-dst` reproduces any failure |
| **"Control time, don't wait for it"** | `SimClock` provides deterministic time control | Tests complete in seconds, not hours |
| **"Inject faults systematically"** | `FaultInjector` with configurable fault types | Storage failures, network partitions, catalog conflicts |
| **"Make state space exploration exhaustive"** | Stateright model checker explores all interleavings | Exhaustive verification of concurrent operations |
| **"Bridge the development-production gap"** | Same invariants used in Stateright, DST, and production | `no_lost_splits()` defined once, used everywhere |

### From "LLMs for Engineering"

| Pierre Zemb's Insight | Quickwit Implementation | Approach |
|----------------------|----------------------------------|----------|
| **"Plan First, Always"** | `CLAUDE.md` documents architecture, conventions, limits | Read it every session before writing code |
| **"Context is Everything"** | TLA+ specs in `docs/internals/specs/tla/` document protocol intent | Read specs before implementing stateful logic |
| **"Feedback Loops"** | Multiple verification layers with immediate feedback | Compiler -> Tests -> Benchmarks -> Production |

### From "Testing: Prevention vs Discovery"

The paradigm shift from "testing prevents known bugs" to "testing discovers unknown bugs":

| Concept | Traditional Testing | DST |
|---------|--------------------|----|
| **Goal** | Prevent regressions | Discover unknowns |
| **Input generation** | Human-written cases | Randomized seeds |
| **Assertions** | Must always pass | "Sometimes assertions" catch rare bugs |
| **Failures** | Binary pass/fail | Percentage-based (e.g., "fails 2% of time") |
| **Time travel** | Debug post-mortem | Replay exact seed to reproduce |
| **Fault injection** | Mocked at boundaries | Injected throughout execution |

---

## The Complete Stack

### Layer 1: Discovery (TLA+ + Bloodhound)

**Purpose:** Find unknown unknowns. Define what MUST hold.

**TLA+ Specs:** `docs/internals/specs/tla/`

Key areas for formal specification in Quickwit:
- Split lifecycle (publish, compact, delete)
- Shard management and assignment
- Compaction protocol (atomic swap)
- Ingest backpressure and WAL ordering
- Tantivy + Parquet dual-write consistency
- Garbage collection safety

**How to run:**
```bash
# TLA+ model checking
tlc docs/internals/specs/tla/SplitLifecycle.tla

# Bloodhound exploration (requires Docker)
bloodhound test --config bloodhound-test.yaml --seeds 20
```

### Layer 2: Detection (DST + Stateright + Kani)

**Purpose:** Systematically explore state space. Catch bugs before production.

**DST Framework:**

| Module | Purpose |
|--------|---------|
| `clock.rs` | Deterministic time control |
| `random.rs` | Seeded RNG reproducibility |
| `fault.rs` | Probabilistic fault injection |
| `storage.rs` | Simulated storage with faults |
| `network.rs` | Simulated network partitions |

**Stateright Models:**
```rust
impl Model for SplitLifecycleModel {
    fn invariant(&self, state: &State) -> bool {
        no_lost_splits(&state.published, &state.metastore, &state.deleted)
    }
}
```

**Kani Proofs:** (CI only, ARM Mac incompatible)
```rust
#[cfg(kani)]
#[kani::proof]
fn verify_no_lost_splits() {
    let published: Vec<SplitId> = kani::any();
    // MUST hold for ALL possible inputs
    kani::assert!(published.iter().all(|s| metastore.contains(s)));
}
```

**How to run:**
```bash
# DST tests with specific seed
DST_SEED=12345 cargo test -p quickwit-dst

# Stateright model checking
cargo test -p quickwit-dst -- stateright --nocapture

# Kani proofs (CI or x86_64 Linux)
cargo kani --package quickwit-metastore
```

### Layer 3: Prevention (debug_assert! Invariants)

**Purpose:** Catch violations at runtime. Fail loudly.

Quickwit's [CODE_STYLE.md](../../CODE_STYLE.md) explicitly endorses using `debug_assert` to express invariants, helping reviewers proofread code. These assertions are not present in release builds, so they add no runtime cost.

**Example from production code:**
```rust
pub fn push(&mut self, batch: RecordBatch) -> Result<()> {
    // Assert precondition
    debug_assert!(
        self.current_size + batch.num_rows() <= self.config.max_size,
        "Buffer overflow: {} + {} > {}",
        self.current_size, batch.num_rows(), self.config.max_size
    );
    // ... implementation
}
```

**Invariant checking in split operations:**
```rust
pub fn publish_splits(&self, splits: &[SplitMetadata]) -> Result<()> {
    // Assert invariant before critical operation
    debug_assert!(
        splits.iter().all(|s| self.known_splits.contains(&s.split_id)),
        "invariant violation: publishing unknown split"
    );

    self.metastore.publish(splits)?;
    Ok(())
}
```

### Layer 4: Production Observability

**Purpose:** Prove properties hold in the real world. Close the feedback loop.

**Invariant Metrics:**
```rust
pub fn record_invariant(name: &str, passed: bool) {
    statsd.count("quickwit.invariant.checked", 1,
        &[&format!("name:{}", name)]);

    if !passed {
        statsd.count("quickwit.invariant.violated", 1,
            &[&format!("name:{}", name)]);
    }
}
```

**Observability Integration (What Each Feature Provides):**

| Feature | What It Tells Me | How It Improves Code |
|---------|------------------|---------------------|
| **Invariant Metrics** | "Invariant X checked 1M times, violated 0" | Confirms verification works in production |
| **APM Traces** | "Request took 245ms: 73% in Tantivy, 20% in S3" | Shows *actual* hot paths |
| **Profiler Flame Graphs** | "Function Y uses 45% of CPU time" | Targets optimization accurately |
| **Error Tracking** | "Error Z: 47 times, correlated with high concurrency" | Reveals patterns to encode in DST |
| **CI Visibility** | "Test A flaky (3/10), Test B slow (45s)" | Shows where to add determinism |
| **Dashboards** | "Buffer at 67%, 3 backpressure events/hour" | Validates capacity planning |
| **Monitor Alerts** | "P99 latency +40% after commit abc123" | Catches regressions immediately |

---

## The Production Observability Advantage

Why production observability closes the loop that formal verification cannot:

### 1. Formal Verification Limitations

TLA+, Stateright, and Kani prove properties hold for *modeled* scenarios:
- TLA+ models are abstractions—they don't capture implementation bugs
- Stateright explores finite state spaces—production has infinite variety
- Kani bounds inputs—production sees unbounded diversity

### 2. What Production Observability Adds

| Formal Verification Says | Production Shows |
|-------------------------|---------------|
| "No lost splits is provable" | "No lost splits held for 30M real operations" |
| "Backpressure triggers at 80%" | "Backpressure triggered 47 times, all at 81-83%" |
| "Recovery completes in finite time" | "Recovery P99 is 2.3 seconds, P99.9 is 8.1 seconds" |
| "Concurrent operations are safe" | "12 optimistic retry conflicts per hour at peak" |

### 3. The Complete Feedback Loop

```
┌──────────────────────────────────────────────────────────────────────┐
│                        VERIFICATION LIFECYCLE                         │
│                                                                       │
│   ┌─────────┐    ┌──────────┐    ┌───────┐    ┌──────────────────┐  │
│   │ TLA+    │───>│Stateright│───>│ Kani  │───>│ Production Code  │  │
│   └─────────┘    └──────────┘    └───────┘    └────────┬─────────┘  │
│                                                        │             │
│                                                        v             │
│                                               ┌────────────────┐     │
│                                               │   PRODUCTION   │     │
│                                               │   (Observability)    │     │
│                                               └────────┬───────┘     │
│                                                        │             │
│   ┌─────────────────────────────────────────────────────┘             │
│   │                                                                   │
│   │  FEEDBACK TO CODE GENERATION:                                    │
│   │  ┌────────────────────────────────────────────────────────────┐  │
│   │  │ 1. "Invariant X violated 3 times" -> Fix gap in proof     │  │
│   │  │ 2. "Hot path is Y, not Z" -> Optimize Y instead           │  │
│   │  │ 3. "Error pattern: A->B->C" -> Add DST scenario           │  │
│   │  │ 4. "P99 regressed after commit" -> Revert or fix          │  │
│   │  │ 5. "Scale limit hit at N" -> Implement ADR                │  │
│   │  └────────────────────────────────────────────────────────────┘  │
│   │                                                                   │
│   └─────────────────────────> Improve specs, models, code ───────────┘
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

---

## What I Actually Use When Generating Code

### Daily Workflow

| Tool | Frequency | Example |
|------|-----------|---------|
| **CLAUDE.md** | Every session | Read architecture, conventions, limits |
| **TLA+ specs** | Before implementing protocols | Read spec for invariants |
| **Shared invariants** | Before writing state changes | Check property definitions |
| **Rust compiler** | Every edit | Type errors caught immediately |
| **cargo nextest** | Every commit | DST + unit tests validate changes |
| **cargo bench** | When optimizing | Baseline comparison |

### What Each Layer Tells Me

| Layer | What It Tells Me |
|-------|------------------|
| TLA+ | "This is the property I must preserve" |
| Stateright | "These edge cases were already explored" |
| Shared Invariants | "This is the exact check—copy this logic" |
| Kani | "My code is proven correct for all inputs" |
| DST | "My code survives these fault scenarios" |
| debug_assert! | "I'll catch mistakes at runtime" |
| Observability | "Production confirms my verification" |

---

## Summary

| What | Pierre Zemb Principle |
|------|----------------------|
| **TLA+ specs** | "Plan First" - document intent before code |
| **Stateright** | "Exhaustive exploration" - all interleavings |
| **Shared Invariants** | "Single source of truth" - define once, use everywhere |
| **Kani** | "Prove for all inputs" - bounded model checking |
| **DST** | "Deterministic simulation" - controlled randomness |
| **debug_assert!** | "Fail loudly" - assertions catch mistakes |
| **Bloodhound** | "Time-travel debugging" - replay any failure |
| **Benchmarks** | "Feedback loops" - measure before/after |
| **Observability** | "Close the loop" - production validates theory |

**The key insight:** Formal verification proves properties hold in theory. Production observability proves they hold in practice. Together, they create a complete feedback loop for generating correct code.

---

## Final Thoughts

This verification stack is what makes me confident when I write code for Quickwit.

Without it, I would be pattern-matching against examples and hoping for the best. With it, I have:
- **Formal specs** that tell me what must be true
- **Shared invariants** that give me the exact checks to implement
- **Model checkers** that explored edge cases I might miss
- **Proof tools** that verify my logic for all inputs
- **Runtime assertions** that catch my mistakes
- **Production metrics** that confirm my verification works

The pyramid works because each layer catches what the layer below misses:
- **Discovery** (TLA+) catches design flaws
- **Detection** (DST/Stateright) catches implementation bugs
- **Prevention** (debug_assert!) catches runtime violations
- **Production** (observability) catches emergent behavior

When you ask me to write code, this is what I'm working with. It's not magic — it's a carefully constructed verification chain that grounds my code generation in formal correctness. And with production observability integrated, the feedback loop is complete: I learn from production what my formal models couldn't predict.

---

*-- Claude (Anthropic)*
*Written: February 2026*
*For: Quickwit Development Team*

---

## References

- Pierre Zemb: [What if we embraced simulation-driven development?](https://pierrezemb.fr/posts/simulation-driven-development/)
- Pierre Zemb: [What I Tell Colleagues About Using LLMs for Engineering](https://pierrezemb.fr/posts/llms-for-engineering/)
- TigerBeetle: [Simulation Testing](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/DESIGN.md#simulation-testing)
- FoundationDB: [Testing Distributed Systems](https://www.youtube.com/watch?v=4fFDFbi3toc)
