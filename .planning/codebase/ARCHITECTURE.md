# Architecture

**Analysis Date:** 2026-01-22

## Pattern Overview

**Overall:** Actor-based distributed search and analytics engine with multi-pipeline architecture.

**Key Characteristics:**
- Asynchronous actor framework for task orchestration and supervision
- Modular pipeline architecture (indexing, search, ingestion, control plane)
- Separation of concerns: data ingestion, document processing, indexing, merging, searching, replication
- gRPC-based inter-node communication with Protocol Buffers
- Pluggable storage backends (S3, Azure, GCS, local filesystem)
- Metrics-first design with OpenTelemetry integration
- Support for both log indexing (Tantivy) and time-series metrics (DataFusion/Parquet)

## Layers

**Ingest Layer:**
- Purpose: Receives documents from external sources (Kafka, Pulsar, Kinesis, S3, GCP PubSub, HTTP API)
- Location: `quickwit/quickwit-ingest/src/`, `quickwit/quickwit-indexing/src/source/`
- Contains: IngestApiService, Ingester, source adapters (KafkaSource, PulsarSource, FileSource, etc.)
- Depends on: Storage, DocMapper, Protocol Buffers, Event Broker
- Used by: Indexing service, control plane

**Processing Layer:**
- Purpose: Transforms raw documents into indexable format
- Location: `quickwit/quickwit-indexing/src/actors/doc_processor.rs`, `quickwit/quickwit-indexing/src/actors/metrics_doc_processor.rs`
- Contains: DocProcessor (Tantivy/log), MetricsDocProcessor (DataFusion/metrics)
- Depends on: DocMapper, VRL expression engine
- Used by: Indexing pipeline

**Indexing Layer:**
- Purpose: Builds searchable indexes and manages document storage
- Location: `quickwit/quickwit-indexing/src/actors/`
- Contains: Indexer, IndexSerializer, Packager, Publisher, Uploader, Sequencer, MergeExecutor, MergePipeline
- Depends on: Tantivy (search), Storage, Metastore, Control Plane
- Used by: CLI, gRPC services

**Search Layer:**
- Purpose: Executes queries against indexes and returns results
- Location: `quickwit/quickwit-search/src/`
- Contains: RootSearcher, LeafSearcher, ClusterClient, Collectors (TopK, FindTraceIds)
- Depends on: Tantivy, DataFusion, Index metadata
- Used by: REST API, gRPC services

**Control Plane Layer:**
- Purpose: Orchestrates cluster state, shard assignment, and index lifecycle
- Location: `quickwit/quickwit-control-plane/src/`
- Contains: ControlPlane, IndexingScheduler, IngestController
- Depends on: Metastore, Cluster coordinator (Chitchat)
- Used by: All services, ingest, indexing

**Storage Layer:**
- Purpose: Abstracts access to various storage backends
- Location: `quickwit/quickwit-storage/src/`
- Contains: Storage trait implementations (S3, Azure, GCS, local filesystem, RAM)
- Depends on: AWS SDK, Azure SDK, OpenDAL
- Used by: All layers

**Protocol & Communication:**
- Purpose: Defines service contracts and cluster communication
- Location: `quickwit/quickwit-proto/`, gRPC services, REST API handlers
- Contains: Protocol Buffer definitions, gRPC stubs, REST handlers
- Depends on: Tonic (gRPC), Warp/Hyper (HTTP)
- Used by: All services

**Common Utilities:**
- Purpose: Shared functionality across services
- Location: `quickwit/quickwit-common/src/`
- Contains: Metrics, rate limiting, retry logic, tower middleware, pubsub event broker, logging
- Depends on: Tokio, Tracing, Prometheus
- Used by: All layers

## Data Flow

**Indexing Pipeline Flow:**

1. **Source** - External data arrives via Kafka, Pulsar, HTTP API, or file
   - Source actors pull/receive documents
   - Emit batches to next stage

2. **Doc Processor** - Validates and transforms documents
   - Applies schema mapping (DocMapper)
   - Executes VRL transformations (if configured)
   - Validates document structure
   - Output: Validated document batches

3. **Indexer** - Builds in-memory Tantivy/DataFusion indexes
   - For logs: Tantivy IndexWriter
   - For metrics: DataFusion table builder
   - Tracks indexing statistics
   - Signals when segment is full

4. **Index Serializer** - Prepares index for disk storage
   - Serializes Tantivy/Parquet segments
   - Compresses data

5. **Packager** - Bundles all segments and metadata
   - Creates .split files
   - Calculates split checksums
   - Prepares for upload

6. **Uploader** - Persists splits to storage
   - Uploads to S3/Azure/GCS/local storage
   - Retries on failure

7. **Sequencer** - Maintains sequence integrity
   - Ensures in-order processing

8. **Publisher** - Updates metastore with published splits
   - Registers splits in metastore
   - Updates index metadata

**Search Query Flow:**

1. **Root Searcher** - Entry point for search queries
   - Parses query language (SQL or Tantivy query syntax)
   - Fetches index metadata from metastore
   - Determines which leaf nodes have relevant splits
   - Distributes leaf search requests

2. **Leaf Searcher** - Executes query on local indexes
   - Loads splits from storage
   - Executes Tantivy/DataFusion query
   - Applies collectors (top-K, aggregations)
   - Returns partial results

3. **Merge Results** - Aggregates leaf results
   - Combines partial results
   - Final aggregation/sorting
   - Returns to client

**State Management:**

- **Index Metadata**: Stored in metastore (PostgreSQL/S3), cached in control plane
- **Split Metadata**: Persisted in metastore, used to locate data
- **Shard State**: Maintained in ingester, tracked by control plane
- **Cluster State**: Coordinated via Chitchat gossip protocol
- **In-Flight Documents**: Kept in ingester memory (WAL-backed for replication)

## Key Abstractions

**Actor Pattern:**
- Purpose: Encapsulates concurrent work with message passing
- Examples: `quickwit/quickwit-actors/src/actor.rs`, `quickwit-indexing/src/actors/`
- Pattern: Implement `Actor` trait, handle messages via `Handler<T>`, spawn with `Universe`
- Benefits: Built-in supervision, heartbeat monitoring, observable state

**Source Abstraction:**
- Purpose: Unified interface for data sources
- Examples: `quickwit/quickwit-indexing/src/source/mod.rs` defines `SourceActor`
- Pattern: Implement `Source` trait, fetch documents in batches
- Implementations: Kafka, Pulsar, HTTP ingest, file readers, Kinesis

**Storage Abstraction:**
- Purpose: Unified interface across storage backends
- Examples: `quickwit/quickwit-storage/src/storage.rs`
- Pattern: Implement `Storage` trait with get/put/delete operations
- Implementations: S3 (`S3CompatibleObjectStorage`), Azure (`AzureBlobStorage`), GCS (`GoogleCloudStorage`), local filesystem

**DocMapper:**
- Purpose: Schema enforcement and field mapping
- Examples: `quickwit/quickwit-doc-mapper/src/lib.rs`
- Pattern: Maps incoming documents to schema, handles type conversions
- Used by: Doc processors, search, indexing

**Metastore:**
- Purpose: Persistent metadata store for indexes, splits, shards
- Examples: `quickwit/quickwit-metastore/src/`
- Pattern: Abstraction over PostgreSQL/S3 backends
- Operations: Create/update indexes, list/publish splits, shard management

## Entry Points

**CLI Entry:**
- Location: `quickwit/quickwit-cli/src/main.rs`
- Triggers: `quickwit run` command for server, or subcommands (index, ingest, search)
- Responsibilities:
  - Parse CLI arguments
  - Setup logging and tracing
  - Initialize metrics collection
  - Spawn appropriate service actor (IndexingService, SearchService, etc.)
  - Route commands to handlers

**Server Entry:**
- Location: `quickwit/quickwit-serve/src/lib.rs`
- Triggers: Server startup via CLI
- Responsibilities:
  - Initialize cluster and gossip protocol
  - Spawn core services (ControlPlane, IndexingService, IngestService)
  - Start REST API handlers
  - Start gRPC servers
  - Orchestrate graceful shutdown

**REST API Entry:**
- Location: `quickwit/quickwit-serve/src/rest_api_response.rs` and various API modules
- Endpoints: `/api/v1/*` routes handled by Warp/Hyper
- Request flow: HTTP handler → API module → Service actor → Mailbox

**gRPC Entry:**
- Location: `quickwit/quickwit-serve/src/grpc.rs`
- Services: IndexingService, SearchService, IngestService, MetastoreService, ControlPlaneService
- Communication: Tonic framework with tower middleware for retry/rate-limit

## Error Handling

**Strategy:** Structured error propagation with `anyhow::Result` at boundaries, custom error types for service APIs.

**Patterns:**

**Actor Supervision:**
- Files: `quickwit/quickwit-actors/src/supervisor.rs`
- Pattern: Supervisors monitor actor heartbeats; on timeout, kill actor and propagate error
- Recovery: Pipeline respawns automatically with exponential backoff (see `wait_duration_before_retry`)

**Retry Logic:**
- Files: `quickwit/quickwit-common/src/retry.rs`
- Pattern: Configurable retry parameters, exponential backoff with jitter
- Applied to: Storage operations, metastore calls, gRPC requests

**Ingestion Error Handling:**
- Files: `quickwit/quickwit-ingest/src/error.rs`
- Pattern: `IngestV2Error` with variants (ResourceExhausted, Timeout, PersistenceError, etc.)
- Behavior: Failures reported to client, not silently dropped

**Search Error Handling:**
- Files: `quickwit/quickwit-search/src/error.rs`
- Pattern: Partial results on leaf failures, aggregated errors in response
- Behavior: Search continues on some node failures rather than full abort

## Cross-Cutting Concerns

**Logging:**
- Framework: `tracing` crate with `tracing-subscriber`
- Configuration: Via `RUST_LOG` env var, JSON output for production
- Approach: Structured logging with spans across actor boundaries

**Validation:**
- DocMapper: Schema enforcement on ingest
- Field validators: Type checking, required field validation
- Source validators: Connectivity checks before pipeline spawn (via `check_source_connectivity`)

**Authentication:**
- Current: Basic token auth via REST/gRPC headers
- Framework: Tower middleware in `quickwit-serve`
- Approach: Per-request validation in handler layer

**Metrics:**
- Framework: Prometheus metrics via `metrics` crate
- Exporters: Dogstatsd exporter for Datadog integration
- Collection: Per-actor state (observability), per-operation counters
- Location: `quickwit/quickwit-common/src/metrics.rs`

**Rate Limiting:**
- Location: `quickwit/quickwit-common/src/rate_limiter.rs`
- Approach: Token bucket, configurable per service
- Tower middleware: `RateLimitLayer`

**Cluster Coordination:**
- Protocol: Chitchat gossip protocol
- Location: `quickwit/quickwit-cluster/src/`
- Used for: Node discovery, cluster membership, shard assignment announcements

---

*Architecture analysis: 2026-01-22*
