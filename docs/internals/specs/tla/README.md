# TLA+ Specifications for Quickhouse-Pomsky

This directory contains formal TLA+ specifications for critical Quickhouse-Pomsky protocols.

Specifications are written before implementation (see [Simulation-First Workflow](../../SIMULATION_FIRST_WORKFLOW.md)). Each spec defines the invariants that DST tests and production code must preserve.

## Signal Priority

Metrics first, then traces, then logs. Specs should be written to generalize across all three signals where possible, but initial specs will focus on the metrics pipeline.

## Setup (One-Time)

Install the TLA+ Toolbox (includes TLC model checker):

```bash
# Option 1: TLA+ Toolbox app (tested, recommended)
# Download from https://github.com/tlaplus/tlaplus/releases
# The jar is at: /Applications/TLA+ Toolbox.app/Contents/Eclipse/tla2tools.jar

# Option 2: Homebrew
brew install tlaplus

# Option 3: Download jar directly
mkdir -p ~/.local/lib
curl -L -o ~/.local/lib/tla2tools.jar \
  "https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar"
```

Requires Java (e.g., `brew install openjdk` if not already installed).

## Running Model Checker

All commands run from the repository root. The TLA+ Toolbox jar path is:

```bash
TLA_JAR="/Applications/TLA+ Toolbox.app/Contents/Eclipse/tla2tools.jar"
```

### Quick Verification (small configs, ~1 second each)

```bash
# ParquetDataModel (ADR-001): 8 states
java -XX:+UseParallelGC -jar "$TLA_JAR" \
  -config docs/internals/specs/tla/ParquetDataModel_small.cfg \
  docs/internals/specs/tla/ParquetDataModel.tla

# SortSchema (ADR-002): ~49K states
java -XX:+UseParallelGC -jar "$TLA_JAR" \
  -config docs/internals/specs/tla/SortSchema_small.cfg \
  docs/internals/specs/tla/SortSchema.tla

# TimeWindowedCompaction (ADR-003): ~938 states
java -XX:+UseParallelGC -jar "$TLA_JAR" \
  -config docs/internals/specs/tla/TimeWindowedCompaction_small.cfg \
  docs/internals/specs/tla/TimeWindowedCompaction.tla
```

### Full Verification (full configs, minutes to hours)

```bash
# ParquetDataModel: millions of states
java -XX:+UseParallelGC -Xmx4g -jar "$TLA_JAR" -workers 4 \
  -config docs/internals/specs/tla/ParquetDataModel.cfg \
  docs/internals/specs/tla/ParquetDataModel.tla

# SortSchema: millions of states
java -XX:+UseParallelGC -Xmx4g -jar "$TLA_JAR" -workers 4 \
  -config docs/internals/specs/tla/SortSchema.cfg \
  docs/internals/specs/tla/SortSchema.tla

# TimeWindowedCompaction: very large state space — use with caution
java -XX:+UseParallelGC -Xmx4g -jar "$TLA_JAR" -workers 4 \
  -config docs/internals/specs/tla/TimeWindowedCompaction.cfg \
  docs/internals/specs/tla/TimeWindowedCompaction.tla
```

### Verified Results (2026-02-20)

All small configs pass with no invariant violations:

| Spec | Config | States | Time | Result |
|------|--------|--------|------|--------|
| ParquetDataModel | small | 8 | <1s | Pass |
| SortSchema | small | 49,490 | 1s | Pass |
| TimeWindowedCompaction | small | 938 | <1s | Pass |

Mutation testing (9 mutations, 9 caught) confirms invariants detect:
- LWW dedup, synthetic point injection, TSID corruption (ADR-001)
- Unsorted writes, schema mutation of existing splits, inconsistent schema copies (ADR-002)
- Dropped rows, skipped sorting, cross-window merges (ADR-003)

## Available Specifications

| Spec | Config | Purpose |
|------|--------|---------|
| [ParquetDataModel](ParquetDataModel.tla) | [Full](ParquetDataModel.cfg), [Small](ParquetDataModel_small.cfg) | ADR-001 data model invariants: point-per-row, no LWW, no interpolation, deterministic timeseries_id, TSID persistence through compaction |
| [SortSchema](SortSchema.tla) | [Full](SortSchema.cfg), [Small](SortSchema_small.cfg) | ADR-002 sort schema invariants: rows sorted per split schema, null ordering, missing columns as null, schema immutability, three-copy consistency |
| [TimeWindowedCompaction](TimeWindowedCompaction.tla) | [Full](TimeWindowedCompaction.cfg), [Small](TimeWindowedCompaction_small.cfg) | ADR-003 time-windowed sorted compaction invariants: one window per split, duration divides hour, no cross-window merge, scope compatibility, row set/content preservation, sort order, column union |

### Candidate Areas for Specification

These areas are likely to need formal specs as the system evolves:

| Area | Component | Key Invariants |
|------|-----------|----------------|
| Split lifecycle | `quickwit-metastore` | No lost splits, no premature visibility, atomic publish |
| Compaction protocol | `quickwit-indexing` | Atomic split swap, no data loss during merge |
| Ingest backpressure | `quickwit-ingest` | Bounded buffers, no overflow, WAL ordering |
| Shard management | `quickwit-control-plane` | No split-brain, consistent shard assignment |
| Tantivy + Parquet consistency | `quickwit-indexing` | Dual-write atomicity, index-to-data consistency |
| Metrics ingestion | `quickwit-metrics-engine` | No lost data points, correct aggregation |
| Garbage collection | `quickwit-janitor` | Safe deletion, no deletion of live data |

## Creating New Specs

1. Create `NewProtocol.tla` with the specification
2. Create `NewProtocol.cfg` with constants and properties to check
3. Optionally create `NewProtocol_small.cfg` for quick iteration
4. Add entry to the table in this README
5. Run model checker to verify
6. Link from the corresponding ADR

### Spec File Template

```tla
---- MODULE NewProtocol ----
\* Formal specification for [protocol description]
\*
\* Models:
\*   - [what this spec covers]
\*
\* Key Invariants:
\*   - [invariant 1]
\*   - [invariant 2]
\*
\* Signal Applicability:
\*   - Metrics: [how this applies to metrics]
\*   - Traces:  [how this applies to traces, or "same as metrics"]
\*   - Logs:    [how this applies to logs, or "same as metrics"]

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    \* @type: Set(NODE);
    Nodes,
    \* @type: Int;
    MaxItems

VARIABLES
    \* @type: NODE -> STATE;
    state

vars == <<state>>

\* ---- Type Invariant ----

TypeInvariant ==
    TRUE \* Define type constraints

\* ---- Safety Properties ----

Safety ==
    TRUE \* Define safety invariants

\* ---- Actions ----

Init ==
    state = [n \in Nodes |-> "idle"]

Next ==
    \E n \in Nodes:
        \/ \* Action 1
           TRUE
        \/ \* Action 2
           TRUE

\* ---- Specification ----

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

\* ---- Liveness ----

Liveness ==
    TRUE \* Define liveness properties

====
```

### Config File Template

```tla
\* TLC Configuration for NewProtocol.tla

CONSTANTS
    Nodes = {n1, n2}
    MaxItems = 3

\* Disable deadlock detection — bounded models naturally terminate
CHECK_DEADLOCK FALSE

SPECIFICATION Spec

INVARIANTS
    TypeInvariant
    Safety

PROPERTIES
    Liveness
```

## Mapping Specs to Code

Each spec should document how TLA+ concepts map to Rust implementation:

```
| TLA+ Concept          | Rust Implementation                    |
|------------------------|----------------------------------------|
| `Nodes`               | Cluster nodes in `quickwit-cluster`    |
| `state`               | Component state struct                 |
| `SafetyProperty`      | `debug_assert!` + shared invariant     |
| `CommitAction`         | `metastore.publish_splits()`           |
```

### Keeping Spec and Code in Sync

When modifying a protocol:

1. Update TLA+ spec first
2. Run TLC to verify safety preserved
3. Update Rust implementation
4. Run DST tests to verify implementation matches spec

## Cleanup

TLC generates trace files and state directories on errors. Clean them up with:

```bash
rm -rf docs/internals/specs/tla/*_TTrace_*.tla docs/internals/specs/tla/*.bin docs/internals/specs/tla/states
```

## References

- [TLA+ Home](https://lamport.azurewebsites.net/tla/tla.html)
- [TLC Model Checker](https://github.com/tlaplus/tlaplus)
- [Learn TLA+](https://learntla.com/)
- [Verification Guide](../../VERIFICATION.md)
- [Simulation-First Workflow](../../SIMULATION_FIRST_WORKFLOW.md)
