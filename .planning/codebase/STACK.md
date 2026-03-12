# Technology Stack

**Analysis Date:** 2026-01-22

## Languages

**Primary:**
- Rust 2024 edition - All application code (`quickwit/Cargo.toml`)
- TypeScript 5.9.3 - UI application (`quickwit/quickwit-ui/package.json`)

**Secondary:**
- JavaScript - Build scripts, config files
- Protocol Buffers - Service definitions (`quickwit/quickwit-proto/`)
- YAML - Configuration files

## Runtime

**Environment:**
- Rust 1.91 toolchain - `quickwit/rust-toolchain.toml`
- Tokio 1.45 async runtime - `quickwit/Cargo.toml` (line 254)
- Node.js 20 LTS for UI - `Dockerfile` (line 1)

**Package Manager:**
- Cargo with workspace - `quickwit/Cargo.toml` (26 member crates)
- Lockfile: `Cargo.lock` present and committed
- Yarn for Node.js - `.yarnrc.yml`
- Lockfile: `yarn.lock` for reproducible builds

## Frameworks

**Core Engine:**
- Tantivy (fork from `quickwit-oss/tantivy`, rev 618e3bd) - Full-text search with custom Quickwit extensions for columnar storage
- DataFusion 51 - SQL query engine with Parquet support for metrics
- Apache Arrow 57 - Columnar in-memory data format
- Apache Parquet 57 - Columnar storage with compression (Zstandard, Snappy)

**Web/HTTP:**
- Hyper 1.7 - HTTP/1.1 and HTTP/2 server - `quickwit/Cargo.toml` (line 137)
- Warp 0.4 - REST API framework - `quickwit/Cargo.toml` (line 307)
- Tonic 0.13 - gRPC services with TLS and compression - `quickwit/Cargo.toml` (line 261)
- Tower 0.5 - Service middleware for retry, load balancing - `quickwit/Cargo.toml` (line 270)
- Tower-HTTP 0.6 - HTTP middleware for compression and CORS - `quickwit/Cargo.toml` (line 278)

**Actors & Concurrency:**
- quickwit-actors - Custom actor framework for distributed pipeline (`quickwit/quickwit-actors/`)
- Tokio 1.45 - Async runtime with full features
- Tokio-metrics 0.3 - Runtime instrumentation
- Tokio-stream 0.1 - Stream utilities
- Flume 0.11 - Multi-producer, single-consumer channel

**UI Framework:**
- React 19.2.0 - Frontend library
- Vite 7.2.2 - Build tool and dev server
- Material-UI (MUI) 7.3.5 - Component library with charts, date pickers, icons
- React Router 7.9.6 - Client-side routing
- Monaco Editor 0.54.0 - Code/query editor component
- Swagger UI React 5.30.2 - Interactive API documentation

**Testing:**
- Jest 30.2.0 - JavaScript test runner
- Cypress 13.3.2 - E2E testing for UI
- Tokio test - Async test utilities
- Criterion 0.5 - Benchmarking (`quickwit/Cargo.toml` line 108)
- Proptest 1 - Property-based testing
- Mockall 0.11 - Trait mocking
- Wiremock 0.6 - HTTP mocking

**Build/Dev:**
- Bazel - Build system (`.bazelrc`, `.bazelversion`)
- Cargo - Rust package management
- Cross - Cross-platform compilation
- Docker multi-stage builds
- Make - Build orchestration (`Makefile`)

## Key Dependencies

**Message Queues (Optional features):**
- `rdkafka` 0.38 - Apache Kafka consumer (feature `kafka`)
- `pulsar` 6.3 - Apache Pulsar consumer (feature `pulsar`)
- `aws-sdk-kinesis` 1.86 - AWS Kinesis consumer (feature `kinesis`)
- `aws-sdk-sqs` 1.82 - AWS SQS consumer (feature `sqs`)
- `google-cloud-pubsub` 0.18 - Google Cloud Pub/Sub consumer (feature `gcp-pubsub`)
- mrecordlog (fork) - Write-ahead log for durable ingestion

**Cloud Storage:**
- AWS SDK core 1.8 - AWS service configuration
- `aws-sdk-s3` 1.62 - S3-compatible storage (primary)
- Azure Blob Storage 0.21 - Azure support (feature `azure`)
- OpenDAL 0.53 - Storage abstraction with GCS support (feature `gcs`)
- Reqsign 0.16 - Request signing for cloud services

**Database:**
- SQLx 0.8 - Async SQL toolkit with compile-time checked queries
- Sea-query 0.32 - SQL query builder
- Sea-query-binder 0.7 - SQLx integration
- PostgreSQL 12+ - Metastore backend (dev: Docker service in `docker-compose.yml`)

**Serialization & Encoding:**
- Serde 1.0.219 - Serialization framework
- Serde JSON 1.0 - JSON support
- Postcard 1.0.4 - Binary serialization
- Prost 0.13 - Protocol Buffer code generation
- YAML 0.9 - YAML configuration parsing

**Observability:**
- OpenTelemetry 0.27 - Instrumentation API
- OpenTelemetry SDK 0.27 - OTLP implementation
- OpenTelemetry OTLP 0.27 - Protocol with Zstandard compression
- Prometheus 0.13 - Metrics exporter
- Metrics 0.24 - Metrics collection
- Metrics-exporter-dogstatsd 0.9 - Datadog integration
- Tracing 0.1 - Structured logging
- Tracing-OpenTelemetry 0.28 - OTLP integration
- Tracing-Subscriber 0.3 - Log subscriber with JSON output

**Data Processing:**
- VRL 0.27 - Vector Remap Language for transformations
- Lindera 0.27 - Japanese/Chinese/Korean tokenization
- Regex 1.11 - Pattern matching
- Nom 7.1 - Parser combinators

**Security:**
- Rustls 0.23 - Pure Rust TLS (no OpenSSL dependency)
- Hyper-rustls 0.27 - TLS integration
- Tokio-rustls 0.26 - Async TLS
- MD5, SHA hashing

**Async/Utilities:**
- Futures 0.3 - Async traits
- Arc-swap 1.7 - Atomic reference swapping
- Chrono 0.4 - DateTime with timezone support
- Time 0.3 - Date/time operations
- UUID 1.17 - Unique identifiers
- ULID 1.2 - Sortable unique identifiers
- Clap 4.5 - CLI argument parsing with env support

## Configuration

**Environment:**
- `.env.example` - Template for development environment variables
- Key variables: `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `LOCALSTACK_VERSION`, etc.
- `dotenvy` 0.15 - .env file parsing at runtime
- `QW_CONFIG` - Path to YAML configuration file
- `QW_DATA_DIR` - Data directory for Quickwit
- `QW_LISTEN_ADDRESS` - Server bind address
- `QW_DISABLE_DOCUMENT_VALIDATION` - Feature flag for validation

**Build Configuration:**
- `quickwit/Cargo.toml` - Workspace with 26 member crates
- `quickwit/rust-toolchain.toml` - Rust version pinning
- `quickwit/rustfmt.toml` - Code formatting rules
- `quickwit/clippy.toml` - Linting configuration
- `.bazelrc` - Bazel build configuration
- `Dockerfile` - Multi-stage build (Node.js UI → Rust binary → Runtime)

**Features:**
- Optional: `kafka`, `pulsar`, `gcp-pubsub`, `kinesis`, `sqs`, `azure`, `gcs`
- Integration test features: `kafka-broker-tests`, `pulsar-broker-tests`, `kinesis-localstack-tests`, etc.

## Development Services (Docker Compose)

**Infrastructure** (`docker-compose.yml`):
- **LocalStack** 3.5.0 - AWS S3, Kinesis, SQS emulation
- **PostgreSQL** 12.17 - Metastore database
- **Apache Pulsar** 3.0.0 - Message broker
- **Apache Kafka/Zookeeper** 7.0.9 - Message queue
- **Azure Storage Emulator** (Azurite) 3.24.0 - Azure Blob storage emulation
- **Google Cloud Pub/Sub Emulator** 455.0.0 - GCP pub/sub emulation
- **Fake GCS Server** 1.47.7 - GCS emulation
- **Jaeger** 1.48.0 - Distributed tracing
- **OpenTelemetry Collector** 0.84.0 - Telemetry collection
- **Prometheus** v2.43.0 - Metrics collection
- **Grafana** 10.4.1 - Metrics visualization
- **MinIO** - S3-compatible object storage

## Platform Requirements

**Development:**
- Rust 1.91 (via rustup from `rust-toolchain.toml`)
- Node.js 20.x (for UI development)
- PostgreSQL 12+ (for metastore)
- Docker & Docker Compose (for local infrastructure)
- Git, Make, Bazel

**Production:**
- Base image: `registry.ddbuild.io/images/base/gbi-ubuntu_2204` (Ubuntu 22.04)
- Dependencies: libssl3, ca-certificates, tzdata
- Object storage (S3, Azure, GCS, or compatible)
- PostgreSQL for metastore
- Kubernetes support (k8s manifests in `k8s/`)

**Architecture Support:**
- amd64 (x86-64)
- arm64 (aarch64)
- Cross-platform builds via Cargo and Docker

---

*Stack analysis: 2026-01-22*
