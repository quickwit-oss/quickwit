# Vendored protobuf definitions

This directory holds `.proto` files copied from Datadog-owned repos that
publish Go/Java bindings but do not publish Rust crates. `build.rs` in the
crate root compiles them via `prost-build` into `src/codegen/` (checked into
git; see `src/protos.rs` for the module wiring).

## Contents

```
proto/
├── ddsketch/
│   └── ddsketch.proto       # DDSketch / IndexMapping / Store (embedded in agent bytes fields)
└── process/
    ├── agent.proto          # HttpAggregations, DatabaseAggregations, DataStreamsAggregations, …
    ├── connections.proto    # CollectorConnections, Connection, Addr, and related types
    └── header.proto         # MessageHeader envelope wrapping each payload over the wire
```

## Provenance

| File | Upstream | Vendored from |
|------|----------|---------------|
| `process/agent.proto` | `proto/process/agent.proto` | github.com/DataDog/agent-payload @ v5.0.184 |
| `process/connections.proto` | `proto/process/connections.proto` | github.com/DataDog/agent-payload @ v5.0.184 |
| `process/header.proto` | `process/protobuf/conn/header.proto` | github.com/DataDog/dd-go @ b1f9f4c (private monorepo) |
| `ddsketch/ddsketch.proto` | `ddsketch/pb/ddsketch.proto` | github.com/DataDog/sketches-go @ 0a92170 (via vector @ fbb1e4b `proto/vector/ddsketch_full.proto`) |

The `agent-payload` version tracks what `dd-go` currently uses (see
`dd-go/go.mod`).

## Updating

To sync with a newer upstream version, copy the relevant `.proto` into the
matching subdirectory, bump the Provenance table, and rebuild:

```bash
# Example: refresh agent-payload protos.
AGENT_PAYLOAD=~/go/src/github.com/DataDog/agent-payload
DEST=~/dd/pomsky/quickwit/pomsky-intake/proto

cp $AGENT_PAYLOAD/proto/process/agent.proto $DEST/process/
cp $AGENT_PAYLOAD/proto/process/connections.proto $DEST/process/

cargo build -p pomsky-intake   # regenerates src/codegen/*.rs in place
```

Commit the updated `.proto` alongside the regenerated `src/codegen/` output
so reviewers can see the generated diff.
