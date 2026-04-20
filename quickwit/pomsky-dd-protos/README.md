# pomsky-dd-protos

Vendored Datadog-external protobuf definitions and generated Rust types.

## Why this crate exists

Several Datadog-defined upstream `.proto` files have generated Go and Java
bindings, but no published Rust bindings. The
[`agent-payload`](https://github.com/DataDog/agent-payload) README is typical:

> Other consumers may copy the `.proto` files into their repository and
> generate their own bindings.

This crate is Pomsky's single home for those vendored `.proto` files plus the
Rust types generated from them via `prost-build`. Grouping them here (rather
than one crate per upstream source) keeps the plumbing in one place; each
submodule below maps cleanly to its upstream. Sources include both
public-Go-module repos (`agent-payload`, `sketches-go` via vector) and
sibling internal repos (`dd-go`); the vendoring pattern is the same regardless.

## Contents

```
proto/
  ├── process/
  │   ├── agent.proto          # HTTPAggregations, DatabaseAggregations, DataStreamsAggregations, etc. (agent-payload)
  │   ├── connections.proto    # CollectorConnections, Connection, Addr, and related types (agent-payload)
  │   └── header.proto         # MessageHeader envelope wrapping each payload (dd-go)
  └── ddsketch/
      └── ddsketch.proto       # DDSketch, IndexMapping, Store (embedded inside agent bytes fields) (sketches-go)
```

Add new files in their own subdirectory, one per upstream source, and update
the submodule list in `src/lib.rs` and the provenance table below.

## Provenance

| File | Upstream path | Vendored from |
|------|---------------|---------------|
| `proto/process/agent.proto` | `proto/process/agent.proto` | github.com/DataDog/agent-payload @ v5.0.184 |
| `proto/process/connections.proto` | `proto/process/connections.proto` | github.com/DataDog/agent-payload @ v5.0.184 |
| `proto/process/header.proto` | `process/protobuf/conn/header.proto` | github.com/DataDog/dd-go @ b1f9f4c (private monorepo) |
| `proto/ddsketch/ddsketch.proto` | `ddsketch/pb/ddsketch.proto` | github.com/DataDog/sketches-go @ 0a92170 (via vector @ fbb1e4b proto/vector/ddsketch_full.proto) |

The agent-payload version tracks what `dd-go` currently uses (see `dd-go/go.mod`).

## Updating

To sync with a newer upstream version, copy the relevant `.proto` file into
the matching subdirectory, bump the Provenance table, and rebuild:

```bash
# Example: refresh agent-payload protos.
AGENT_PAYLOAD=~/go/src/github.com/DataDog/agent-payload
DEST=~/dd/pomsky/quickwit/pomsky-dd-protos/proto

cp $AGENT_PAYLOAD/proto/process/agent.proto $DEST/process/
cp $AGENT_PAYLOAD/proto/process/connections.proto $DEST/process/

cargo build -p pomsky-dd-protos
cargo test -p pomsky-dd-protos
```

## Future: upstream Rust crates

If any of the vendored upstreams ever publishes Rust bindings (e.g., agent-payload
grows a `Cargo.toml` alongside the existing Go and Java bindings), its entries
here can be replaced with a plain dependency on the upstream crate.

## Usage

```rust
use pomsky_dd_protos::process::CollectorConnections;
use prost::Message;

let decoded = CollectorConnections::decode(&bytes[..])?;
for conn in &decoded.connections {
    // ...
}
```
