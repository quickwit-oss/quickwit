# TLA+ Specifications

This directory contains formal TLA+ specifications for Quickhouse-Pomsky protocols.

## Setup (One-Time)

Install TLA+ tools to a standard location (NOT in this directory):

```bash
# Option 1: Homebrew (recommended on macOS)
brew install tlaplus

# Option 2: Download to ~/.local/lib
mkdir -p ~/.local/lib
curl -L -o ~/.local/lib/tla2tools.jar \
  "https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar"

# Option 3: VS Code extension (for interactive use)
# Install: alygin.vscode-tlaplus
```

## Running Model Checker

From the repository root:

```bash
# If installed via Homebrew:
tlc -config docs/internals/specs/tla/ExampleProtocol.cfg docs/internals/specs/tla/ExampleProtocol.tla

# If using downloaded jar:
java -XX:+UseParallelGC -Xmx4g -jar ~/.local/lib/tla2tools.jar \
  -workers 4 \
  -config docs/internals/specs/tla/ExampleProtocol.cfg \
  docs/internals/specs/tla/ExampleProtocol.tla
```

### Quick vs Full Verification

Some specs have `_small.cfg` variants for faster iteration:

```bash
# Quick (~1s, hundreds of states)
tlc -config docs/internals/specs/tla/ExampleProtocol_small.cfg docs/internals/specs/tla/ExampleProtocol.tla

# Full (~minutes, millions of states)
tlc -workers 4 -config docs/internals/specs/tla/ExampleProtocol.cfg docs/internals/specs/tla/ExampleProtocol.tla
```

## Available Specifications

| Spec | Config | Purpose |
|------|--------|---------|

*No specifications yet. Specs will be created as protocols are designed.*

## Creating New Specs

1. Create `NewProtocol.tla` with the specification
2. Create `NewProtocol.cfg` with constants and properties to check
3. Add entry to `README.md` mapping table
4. Run model checker to verify

### Config File Template

```tla
\* TLC Configuration for NewProtocol.tla

CONSTANTS
    Nodes = {n1, n2}
    MaxItems = 3

SPECIFICATION Spec

INVARIANTS
    TypeInvariant
    SafetyProperty

PROPERTIES
    LivenessProperty
```

## Cleanup

TLC generates trace files and state directories on errors. Clean them up with:

```bash
rm -rf docs/internals/specs/tla/*_TTrace_*.tla docs/internals/specs/tla/*.bin docs/internals/specs/tla/states
```

## References

- [TLA+ Home](https://lamport.azurewebsites.net/tla/tla.html)
- [TLC Model Checker](https://github.com/tlaplus/tlaplus)
- [Learn TLA+](https://learntla.com/)
