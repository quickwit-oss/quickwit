## Build & Test Commands

### Formatting & Linting
- **`make fmt`** — Format and validate code (requires nightly toolchain: `rustup toolchain install nightly`):
  1. Runs `cargo +nightly fmt`
  2. Checks license headers on `.rs`, `.ts`, `.proto` files
  3. Enforces log format policy: no trailing punctuation, no uppercase first character in log and error messages
- **`make fix`** — Runs clippy with `--fix`, then `make fmt`, then `make unused-deps`
- **`make unused-deps`** — Detects unused dependencies via `cargo-machete`

Log messages (`info!`, `warn!`, `error!`, `debug!`) must:
- Start with a **lowercase** letter
- Have **no trailing punctuation**

### Testing
- **Single crate test**: `cargo nextest run -p quickwit-search my_test_name`
- **Single test**: `cargo test -p quickwit-common my_test_name`
- **`make test-all`** — Starts Docker services (LocalStack S3, PostgreSQL, Pub/Sub emulator) and runs the full test suite with `cargo nextest run --all-features --retries 5`
- **`make test-failpoints`** — Runs failpoint tests only: `cargo nextest run --test failpoints --features fail/failpoints`
- Docker services: `make docker-compose-up` / `make docker-compose-down` (subset: `DOCKER_SERVICES=kafka,postgres`)

### Building
- **`make doc`** — Generates docs with `cargo doc --all-features` (warnings as errors)
- Rust toolchain: **1.93**

## Code Conventions

### Clippy Disallowed Methods
These methods are banned (see `clippy.toml`):
- `Path::exists` — (use try_exists)
- `Option::is_some_and`, `Option::is_none_or`, `Option::xor`
- `Option::map_or`, `Option::map_or_else` — use `.map(..).unwrap_or(..)` or `let Some(..) else {..}` instead

### Formatting Shortcut
Use `/fmt` to automatically run format checks.

## Architecture Overview

Quickwit is a cloud-native distributed search engine for observability data (logs, traces). It's organized as a ~38-crate Rust workspace.

### Key Layers

**Protocol & Types** — `quickwit-proto` defines all gRPC service contracts and message types via protobuf. Service traits are auto-generated.

**Actor System** — `quickwit-actors` is a custom lightweight actor framework. The indexing pipeline is fully actor-based:
```
Source → DocProcessor → Indexer → IndexSerializer → Packager → Uploader → Sequencer → Publisher
```
A parallel merge pipeline runs alongside.

**Search** — `quickwit-search` implements a root-leaf pattern: root servers parse queries and coordinate, leaf servers search their assigned splits in parallel, leaf results are merged at root.

**Storage** — `quickwit-storage` abstracts cloud storage (S3, Azure, GCS, local file, RAM) behind a `Storage` trait.

**Metastore** — `quickwit-metastore` manages index metadata with file-backed (dev) and PostgreSQL (production) backends.

**Cluster** — `quickwit-cluster` uses Chitchat gossip protocol for membership. `quickwit-control-plane` handles indexing task scheduling and placement.

**API Surface** — `quickwit-serve` hosts both REST and gRPC endpoints over the same service traits, plus serves the embedded React UI.

### Core Crates
| Crate | Purpose |
|-------|---------|
| `quickwit-cli` | CLI entry point and binary |
| `quickwit-serve` | REST/gRPC server |
| `quickwit-search` | Distributed search orchestration |
| `quickwit-indexing` | Actor-based indexing pipeline |
| `quickwit-ingest` | Distributed ingestion with replication |
| `quickwit-metastore` | Index metadata storage |
| `quickwit-storage` | Multi-cloud storage abstraction |
| `quickwit-config` | Configuration parsing/validation |
| `quickwit-doc-mapper` | Index schema and document mapping |
| `quickwit-query` | Query DSL parsing (ES-compatible) |
| `quickwit-cluster` | Cluster membership (Chitchat) |
| `quickwit-control-plane` | Indexing task scheduling |
| `quickwit-actors` | Actor framework |
| `quickwit-proto` | Protobuf definitions and gRPC traits |
| `quickwit-common` | Shared utilities and metrics |
| `quickwit-lambda-server` | AWS Lambda leaf search handler |
| `quickwit-lambda-client` | Lambda invocation with auto-deployment |

quickwit-common contains shared utilities about metrics, rate limited logging, reading from environment variables, etc.
It also contains the `run_cpu_intensive` that should be use to run CPU-intensive tasks from tokio tasks.

When the client is unlikely to match on an error, you can rely on the crate level Error or anyhow::Error. If you need to introduce a new Error type, use thiserror.

### Design Patterns
- **Trait-based services**: `SearchService`, `MetastoreService`, etc. — enables mocking and multiple implementations
- **Feature gates**: Cloud backends (`azure`, `gcs`), message sources (`kafka`, `kinesis`, `pulsar`, `sqs`, `gcp-pubsub`), `postgres` metastore, `multilang` tokenizers
- **Metrics**: `once_cell::sync::Lazy` statics with `quickwit_common::metrics::*` factories

### Key Dependencies
- **Tantivy**: Search engine library (custom fork)
- **Tonic/Prost**: gRPC framework and protobuf
- **Tokio**: Async runtime
- **SQLx**: PostgreSQL metastore

# Quickwit Claude Guidelines

When adding a new dependency, update license by running `make update-licenses`.
Prefer referring to the crate in workspace. 
Make sure to keep features minimal.

In other words, prefer
zip = { workspace = true, default-features = false, features=["deflate"] }
to
zip = "2"

## Code Formatting
### Quick Fix

Use `/fmt` to automatically run format checks and see issues.

## Coding Style
- Avoid single-letter variable names except for indices (i, j, k)
- Document all "hidden contracts" (implicit assumptions, invariants, preconditions)
- Try to avoid deep nesting. In particular, prefer early return style
- Avoid abusing iterator chaining with complex constructs like `.transpose()`
- Write type names explicitly when it aids readability
- Use `with_capacity` to hint container capacity when size is known
