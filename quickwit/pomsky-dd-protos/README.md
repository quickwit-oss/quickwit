# pomsky-dd-protos

Vendored Datadog-external protobuf definitions and generated Rust types.

## Why this crate exists

Several Datadog-owned repos publish `.proto` files plus generated Go and Java
bindings, but do not publish Rust crates. The
[`agent-payload`](https://github.com/DataDog/agent-payload) README is typical:

> Other consumers may copy the `.proto` files into their repository and
> generate their own bindings.

This crate is Pomsky's single home for those vendored `.proto` files plus the
Rust types generated from them via `prost-build`. Grouping them here (rather
than one crate per upstream source) keeps the plumbing in one place; each
submodule below maps cleanly to its upstream.

## Contents

```
proto/
  └── process/            # from github.com/DataDog/agent-payload
      ├── agent.proto          # HTTPAggregations, DatabaseAggregations, DataStreamsAggregations, etc.
      └── connections.proto    # CollectorConnections, Connection, Addr, and related types
```

Add new files in their own subdirectory, one per upstream source, and update
the submodule list in `src/lib.rs` and the provenance table below.

## Provenance

| File | Upstream path | Vendored from |
|------|---------------|---------------|
| `proto/process/agent.proto` | `proto/process/agent.proto` | github.com/DataDog/agent-payload @ v5.0.184 |
| `proto/process/connections.proto` | `proto/process/connections.proto` | github.com/DataDog/agent-payload @ v5.0.184 |

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
