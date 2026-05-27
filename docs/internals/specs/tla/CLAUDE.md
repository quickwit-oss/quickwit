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

# Iterate every .cfg next to a .tla and run TLC on each pair. Some specs
# ship multiple configs (e.g. one for fast iteration, one for thorough
# coverage, one focused on a specific behavior); this loop handles all.
for cfg in docs/internals/specs/tla/*.cfg; do
  base="$(basename "${cfg%.cfg}")"
  # Try <base>.tla as-is; if absent, strip a trailing _suffix
  # (e.g. MergePipelineShutdown_chains.cfg -> MergePipelineShutdown.tla).
  if [ -f "docs/internals/specs/tla/${base}.tla" ]; then
    tla="docs/internals/specs/tla/${base}.tla"
  else
    tla="docs/internals/specs/tla/${base%_*}.tla"
  fi
  echo "=== $(basename "$tla") with $(basename "$cfg") ==="
  java -XX:+UseParallelGC -Xmx4g \
    -jar "/Applications/TLA+ Toolbox.app/Contents/Eclipse/tla2tools.jar" \
    -workers 4 -config "$cfg" "$tla"
  echo
done
```

### Multiple Configs per Spec

A spec may have several `.cfg` files exploring different bounds:
- `<Spec>.cfg` — primary configuration, balanced for routine verification
- `<Spec>_<focus>.cfg` — alternate configurations targeting specific
  behaviors (e.g. `_chains` for deep merge chains, `_small` for fastest
  iteration). Each `.cfg` runs against the *same* `.tla` source.

## Available Specifications

| Spec | Small States | Full States | Invariants |
|------|-------------|-------------|------------|
| ParquetDataModel | 8 | millions | DM-1..DM-5 |
| SortSchema | 49,490 | millions | SS-1..SS-5 |
| TimeWindowedCompaction | 938 | thousands | TW-1..3, CS-1..3, MC-1..4 |
| MergePipelineShutdown (primary, multi-lifetime) | — | 15,732 (~1s) | RowsConserved, NoSplitLoss, NoDuplicateMerge, NoOrphanInPlanner, NoOrphanWhenConnected, LeakIsObjectStoreOnly, MP1\_LevelHomogeneity, BoundedWriteAmp, FinalizeWithinBound, ShutdownOnlyWhenDrained + RestartReSeedsAllImmature (action) + ShutdownEventuallyCompletes, NoPersistentOrphan (liveness) |
| MergePipelineShutdown\_chains (deep merge chain) | — | 217,854 (~12s) | same invariant set; MaxIngests=4, MaxRestarts=1 |

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
