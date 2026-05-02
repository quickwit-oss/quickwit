# TLA+ Specifications

This directory contains formal TLA+ specifications for Quickwit protocols.

## Setup (One-Time)

Install TLA+ Toolbox via Homebrew cask:

```bash
brew install --cask tla+-toolbox
```

This installs the TLA+ Toolbox app to `/Applications/TLA+ Toolbox.app`,
which bundles `tla2tools.jar` at:

```
/Applications/TLA+ Toolbox.app/Contents/Eclipse/tla2tools.jar
```

No separate `tlc` CLI is installed — run TLC via `java -jar`.

## Running Model Checker

All commands run from the **repository root** (`quickwit-oss/quickwit/`):

```bash
# Quick check (small config, ~seconds):
java -XX:+UseParallelGC -Xmx4g \
  -jar "/Applications/TLA+ Toolbox.app/Contents/Eclipse/tla2tools.jar" \
  -workers 4 \
  -config docs/internals/specs/tla/SomeSpec_small.cfg \
  docs/internals/specs/tla/SomeSpec.tla

# Full check (larger state space, ~seconds to minutes):
java -XX:+UseParallelGC -Xmx4g \
  -jar "/Applications/TLA+ Toolbox.app/Contents/Eclipse/tla2tools.jar" \
  -workers 4 \
  -config docs/internals/specs/tla/SomeSpec.cfg \
  docs/internals/specs/tla/SomeSpec.tla
```

### Run All Specs

```bash
cd /path/to/quickwit-oss/quickwit

for cfg in docs/internals/specs/tla/*_small.cfg; do
  tla="${cfg%_small.cfg}.tla"
  echo "=== $(basename "$tla") (small) ==="
  java -XX:+UseParallelGC -Xmx4g \
    -jar "/Applications/TLA+ Toolbox.app/Contents/Eclipse/tla2tools.jar" \
    -workers 4 -config "$cfg" "$tla"
  echo
done
```

### Quick vs Full Verification

Every spec has a `_small.cfg` (fast iteration, hundreds of states) and a
`.cfg` (thorough, thousands+ of states). Always run small first.

## Available Specifications

| Spec | Small States | Full States | Invariants |
|------|-------------|-------------|------------|
| ParquetDataModel | 8 | millions | DM-1..DM-5 |
| SortSchema | 49,490 | millions | SS-1..SS-5 |
| TimeWindowedCompaction | 938 | thousands | TW-1..3, CS-1..3, MC-1..4 |
| MergePipelineShutdown | 22 | 1,806 | NoSplitLoss, NoDuplicateMerge, FinalizeWithinBound, ShutdownOnlyWhenDrained + liveness |

## Creating New Specs

1. Create `NewProtocol.tla` with the specification
2. Create `NewProtocol_small.cfg` (minimal constants for fast iteration)
3. Create `NewProtocol.cfg` (larger constants for thorough checking)
4. Update the table above
5. Run both configs through TLC to verify

### Config File Template

```tla
\* TLC Configuration for NewProtocol.tla

CONSTANTS
    Nodes = {n1, n2}
    MaxItems = 3

CHECK_DEADLOCK FALSE

SPECIFICATION Spec

INVARIANTS
    TypeInvariant
    SafetyProperty

PROPERTIES
    LivenessProperty
```

## Cleanup

TLC generates trace files and state directories on errors:

```bash
rm -rf docs/internals/specs/tla/*_TTrace_*.tla docs/internals/specs/tla/*.bin docs/internals/specs/tla/states
```

## References

- [TLA+ Home](https://lamport.azurewebsites.net/tla/tla.html)
- [TLC Model Checker](https://github.com/tlaplus/tlaplus)
- [Learn TLA+](https://learntla.com/)
