# Codebase Structure

**Analysis Date:** 2026-01-22

## Directory Layout

```
quickhouse-pomsky/
├── quickwit/                        # Main Rust workspace root
│   ├── Cargo.toml                   # Workspace configuration
│   ├── quickwit-actors/             # Actor framework implementation
│   ├── quickwit-aws/                # AWS SDK integrations (S3, SQS, Kinesis)
│   ├── quickwit-cli/                # Command-line interface
│   ├── quickwit-cluster/            # Cluster coordination (Chitchat, discovery)
│   ├── quickwit-codegen/            # Macro code generation utilities
│   ├── quickwit-common/             # Shared utilities (metrics, retry, rate limit)
│   ├── quickwit-config/             # Configuration parsing and validation
│   ├── quickwit-control-plane/      # Cluster orchestration and scheduling
│   ├── quickwit-datetime/           # Date/time utilities and timezone handling
│   ├── quickwit-directories/        # File system directory management
│   ├── quickwit-doc-mapper/         # Schema enforcement and field mapping
│   ├── quickwit-index-management/   # Index lifecycle management
│   ├── quickwit-indexing/           # Document indexing pipeline
│   ├── quickwit-ingest/             # Document ingestion and WAL
│   ├── quickwit-integration-tests/  # Integration test suite
│   ├── quickwit-jaeger/             # Jaeger trace compatibility layer
│   ├── quickwit-janitor/            # Cleanup and maintenance tasks
│   ├── quickwit-macros/             # Procedural macros for derive traits
│   ├── quickwit-metastore/          # Metadata persistence (PostgreSQL/S3)
│   ├── quickwit-metrics-engine/     # Time-series metrics query engine
│   ├── quickwit-opentelemetry/      # OpenTelemetry integration
│   ├── quickwit-proto/              # Protocol Buffer definitions and codegen
│   ├── quickwit-query/              # Query parsing and compilation
│   ├── quickwit-remote-api/         # gRPC API servers
│   ├── quickwit-rest-client/        # REST client library
│   ├── quickwit-search/             # Search execution and result aggregation
│   ├── quickwit-serve/              # HTTP server and REST API handlers
│   ├── quickwit-storage/            # Storage abstraction layer
│   └── quickwit-ui/                 # React-based web UI
├── config/                          # Configuration templates and examples
├── docs/                            # Documentation files
├── k8s/                             # Kubernetes deployment manifests
├── distribution/                    # Docker/packaging configuration
└── scripts/                         # Utility scripts for development
```

## Directory Purposes

**quickwit/quickwit-actors:**
- Purpose: Custom actor framework for task orchestration
- Contains: Actor trait, mailbox implementation, supervisor, registry
- Key files: `actor.rs` (trait definition), `mailbox.rs` (message channel), `supervisor.rs` (monitoring)
- Responsibilities: Message passing, heartbeat detection, observable state tracking

**quickwit/quickwit-cli:**
- Purpose: Command-line interface and server startup
- Contains: CLI command parsing, server initialization, command routing
- Key files: `main.rs` (entry point), `cli/mod.rs` (command definitions)
- Responsibilities: Parse arguments, spawn services, coordinate execution

**quickwit/quickwit-ingest:**
- Purpose: Document ingestion, buffering, and replication
- Contains: IngestApiService, Ingester, write-ahead logging, shard management
- Key files: `ingest_v2/ingester.rs` (shard management), `ingest_api_service.rs` (HTTP API)
- Responsibilities: Accept documents, manage in-memory buffering, coordinate replication

**quickwit/quickwit-indexing:**
- Purpose: Transform documents into searchable indexes
- Contains: Indexing pipeline, document processing, merge scheduling
- Subdirectories:
  - `actors/` - Pipeline stages (doc_processor, indexer, uploader, etc.)
  - `source/` - Data source implementations (Kafka, Pulsar, HTTP, file, etc.)
  - `models/` - Statistics and pipeline state
  - `split_store/` - Split storage and retrieval
- Key files: `lib.rs` (service initialization), `actors/indexing_pipeline.rs` (pipeline orchestration)
- Responsibilities: Process documents, manage indexes, publish splits

**quickwit/quickwit-search:**
- Purpose: Execute queries and aggregate results
- Contains: Root searcher, leaf searcher, collectors, query optimization
- Key files: `root.rs` (root search execution), `leaf.rs` (leaf search execution)
- Responsibilities: Parse queries, distribute to leaves, merge results

**quickwit/quickwit-control-plane:**
- Purpose: Cluster orchestration and shard management
- Contains: ControlPlane actor, IndexingScheduler, IngestController
- Key files: `control_plane.rs` (main orchestrator), `ingest/ingest_controller.rs` (shard assignment)
- Responsibilities: Track cluster state, assign shards, schedule indexing tasks

**quickwit/quickwit-metastore:**
- Purpose: Persistent metadata store
- Contains: Metastore trait, PostgreSQL implementation, S3 implementation
- Key files: `metastore/postgres/metastore.rs` (DB implementation)
- Responsibilities: Store/retrieve indexes, splits, shards; manage state transitions

**quickwit/quickwit-storage:**
- Purpose: Abstract storage backend access
- Contains: Storage trait, S3/Azure/GCS/local implementations, caching layer
- Key files: `storage.rs` (trait definition), `object_storage/` (S3 implementation)
- Responsibilities: Read/write splits, handle retries, manage caches

**quickwit/quickwit-serve:**
- Purpose: HTTP and gRPC server implementation
- Contains: REST API handlers, gRPC server setup, middleware
- Key files: `lib.rs` (server initialization), API handler modules (search_api, ingest_api, etc.)
- Responsibilities: Accept requests, route to services, serialize responses

**quickwit/quickwit-common:**
- Purpose: Shared utilities
- Contains: Metrics, rate limiting, retry logic, tower middleware, logging setup
- Key files: `metrics.rs` (metric registration), `retry.rs` (retry policy)
- Responsibilities: Provide common functionality to all layers

**quickwit/quickwit-proto:**
- Purpose: Protocol definitions and code generation
- Contains: Protocol Buffer definitions, generated Rust code
- Key directories:
  - `protos/` - .proto files (service definitions, message types)
  - `src/codegen/` - Generated Rust code from protos
- Responsibilities: Define service contracts, message formats

**quickwit/quickwit-config:**
- Purpose: Configuration parsing and validation
- Contains: Config structs with serde, validation logic
- Key files: Config structs for nodes, indexes, sources, storage backends
- Responsibilities: Parse YAML/JSON configs, validate settings, provide defaults

**quickwit/quickwit-doc-mapper:**
- Purpose: Schema enforcement and field mapping
- Contains: DocMapper struct, field types, validation rules
- Responsibilities: Map document fields to schema, enforce types, handle defaults

**quickwit/quickwit-query:**
- Purpose: Query language parsing and compilation
- Contains: Query DSL parser, compilation to Tantivy/DataFusion queries
- Responsibilities: Parse queries, optimize, convert to backend-specific format

**quickwit/quickwit-ui:**
- Purpose: Web UI for cluster management and queries
- Contains: React components, service API clients, utility functions
- Key directories:
  - `src/components/` - React components (UI elements)
  - `src/views/` - Page-level components
  - `src/services/` - API client wrappers
  - `src/providers/` - Context providers (auth, API config)
- Responsibilities: Render UI, call REST APIs, display results

## Key File Locations

**Entry Points:**
- `quickwit/quickwit-cli/src/main.rs` - Binary entry point
- `quickwit/quickwit-serve/src/lib.rs` - Server initialization
- `quickwit/quickwit-ui/src/index.tsx` - UI entry point

**Configuration:**
- `quickwit/Cargo.toml` - Workspace manifest with all dependencies
- `config/quickwit.yaml` - Default server configuration
- `docker-compose.yml` - Local development environment setup

**Core Logic:**
- `quickwit/quickwit-indexing/src/actors/indexing_pipeline.rs` - Pipeline orchestration
- `quickwit/quickwit-search/src/root.rs` - Search execution
- `quickwit/quickwit-ingest/src/ingest_v2/ingester.rs` - Ingestion coordination
- `quickwit/quickwit-control-plane/src/control_plane.rs` - Cluster coordination

**Testing:**
- `quickwit/quickwit-integration-tests/` - End-to-end tests
- `quickwit/quickwit-ui/cypress/` - UI end-to-end tests
- Individual crates have `tests.rs` modules inline

## Naming Conventions

**Files:**
- Module files: `snake_case.rs` (e.g., `doc_processor.rs`, `indexing_pipeline.rs`)
- Test modules: `#[cfg(test)] mod tests` inline within source files
- Module directory: Named after public type (e.g., `ingest_v2/` contains Ingester logic)

**Directories:**
- Service crates: `quickwit-{service-name}` (e.g., `quickwit-search`, `quickwit-ingest`)
- Module subdivisions: Lowercase with hyphens (e.g., `ingest_v2/`, `object_storage/`)
- Metrics index pipelines: Separate modules (e.g., `metrics_doc_processor.rs`, `metrics_indexer.rs`)

**Type Names:**
- Actors: CamelCase, typically with "Service" or "Pipeline" suffix (e.g., `IndexingService`, `DocProcessor`)
- Errors: `{Context}Error` format (e.g., `IngestV2Error`, `MetastoreError`)
- Config structs: `{Component}Config` (e.g., `IndexerConfig`, `IngestApiConfig`)

**Message Types:**
- Requests/Responses follow `{Action}{Target}Request/Response` pattern
- State enums: Capitalized variants (e.g., `ActorState::Running`, `ShardState::Open`)

## Where to Add New Code

**New Feature - Log Indexing:**
- Primary code: `quickwit/quickwit-indexing/src/actors/`
- Add new actor struct implementing `Actor` trait
- Register in `quickwit-indexing/src/actors/mod.rs`
- Tests: Inline in same file or `quickwit-integration-tests/`

**New Feature - Search Optimization:**
- Primary code: `quickwit/quickwit-search/src/`
- Modify `root.rs` for root-level changes or `leaf.rs` for leaf-level changes
- Tests: Inline in source files

**New Source Integration:**
- Primary code: `quickwit/quickwit-indexing/src/source/{new_source_type}_source.rs`
- Register in `quickwit-indexing/src/source/mod.rs`
- Implement `Source` trait from `source/mod.rs`
- Tests: Inline in source file

**New Storage Backend:**
- Primary code: `quickwit/quickwit-storage/src/{backend_name}_storage.rs`
- Implement `Storage` trait from `storage.rs`
- Register in `lib.rs` public exports
- Tests: Inline in implementation file

**New REST API Endpoint:**
- Primary code: `quickwit/quickwit-serve/src/{api_domain}_api.rs`
- Register route in Warp router
- Handler function calls service mailbox
- Tests: Inline with `#[cfg(test)]` module

**New Configuration Option:**
- Primary code: `quickwit/quickwit-config/src/` - Add struct field
- Validation: Add to struct `validate()` method if needed
- Update `config/quickwit.yaml` with documentation

**Utilities and Helpers:**
- Shared helpers: `quickwit/quickwit-common/src/`
- Actor-specific helpers: Within the actor crate
- Service-specific helpers: Within the service module

## Special Directories

**quickwit/target/:**
- Purpose: Build artifacts and compiled binaries
- Generated: Yes
- Committed: No (in .gitignore)
- Contents: debug/ and release/ builds, deps, incremental compilation cache

**quickwit/quickwit-ui/build/:**
- Purpose: Compiled React bundle
- Generated: Yes
- Committed: No
- Generated by: `npm run build`

**config/tutorials/:**
- Purpose: Example configurations for specific use cases (Wikipedia, HDFS logs, OpenTelemetry)
- Generated: No
- Committed: Yes
- Usage: Reference for users setting up similar pipelines

**docs/:**
- Purpose: User-facing documentation
- Generated: Partial (some generated from code comments)
- Committed: Yes
- Format: Markdown

**.planning/codebase/:**
- Purpose: Machine-readable codebase analysis documents
- Generated: Yes (by GSD tools)
- Committed: No (in .gitignore)
- Contents: ARCHITECTURE.md, STRUCTURE.md, CONVENTIONS.md, etc.

---

*Structure analysis: 2026-01-22*
