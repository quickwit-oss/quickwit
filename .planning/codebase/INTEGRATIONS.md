# External Integrations

**Analysis Date:** 2026-01-22

## APIs & External Services

**OpenTelemetry OTLP:**
- Ingest metrics, logs, and traces via gRPC and HTTP protocols
- SDK: OpenTelemetry 0.27, OpenTelemetry SDK 0.27
- Implementation: `quickwit/quickwit-opentelemetry/src/otlp/`
- Endpoints: gRPC on port 4317, HTTP on port 4318 (via OpenTelemetry Collector)
- Compression: Zstandard (zstd) support

**Datadog Integration:**
- PomChi - Datadog log parser library (`quickwit/Cargo.toml` line 179)
- Metrics export via Dogstatsd: `metrics-exporter-dogstatsd` 0.9
- REST handler: `quickwit/quickwit-serve/src/datadog_api/rest_handler.rs`
- Doc processor: `quickwit/quickwit-indexing/src/actors/doc_processor.rs`

**Jaeger:**
- Distributed tracing query and storage
- Crate: `quickwit/quickwit-jaeger/`
- Query API support for trace retrieval
- Configuration: `config/quickwit.yaml`

## Data Storage

**Databases:**

*PostgreSQL 12+:*
- Connection: `DATABASE_URL` environment variable (or `QW_TEST_DATABASE_URL` for tests)
- Client: SQLx 0.8 async SQL toolkit with compile-time checked queries
- Query builder: Sea-query 0.32, Sea-query-binder 0.7
- Implementation: `quickwit/quickwit-metastore/src/postgres.rs`
- Schema: Auto-migrated on startup
- Tables: `splits` (logs/traces metadata), `metrics_splits` (metrics metadata)
- Feature flag: `postgres` in quickwit-metastore and quickwit-proto

*In-Memory Alternative:*
- Used for testing and single-node deployments
- Implementation: `quickwit/quickwit-metastore/src/memory_metastore.rs`

**Object Storage:**

*AWS S3 (Primary):*
- SDK: `aws-sdk-s3` 1.62, `aws-config` 1.8
- Auth: IAM credentials via `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- Environment: `QW_S3_ENDPOINT` for custom S3-compatible endpoints, `QW_S3_FORCE_PATH_STYLE_ACCESS`
- Implementation: `quickwit/quickwit-storage/src/object_storage/s3_compatible_storage.rs`
- Supports: LocalStack, MinIO, Wasabi, DigitalOcean Spaces

*Azure Blob Storage:*
- SDK: `azure_storage_blobs` 0.21, `azure_core` 0.21, `azure_identity` 0.21
- Auth: Azure AD service principal or managed identity
- Optional feature: `azure` in quickwit-storage
- Implementation: `quickwit/quickwit-storage/src/object_storage/azure_blob_storage.rs`
- Emulation: Azurite for local testing (Docker service)

*Google Cloud Storage:*
- SDK: OpenDAL 0.53 with GCS service support, `google-cloud-auth` 0.12
- Auth: GCP service account JSON or Application Default Credentials
- Optional feature: `gcs` in quickwit-storage
- Implementation: Uses OpenDAL abstraction layer
- Emulation: Fake GCS Server for testing (Docker service)

*Local File System:*
- Implementation: `quickwit/quickwit-storage/src/local_file_storage.rs`
- Usage: Development and single-node deployments
- Location: `QW_DATA_DIR` environment variable

*RAM Storage:*
- Implementation: `quickwit/quickwit-storage/src/ram_storage.rs`
- Usage: Testing only

**Caching:**
- Not detected - Quickwit uses direct database and storage queries
- In-memory caching: LRU cache 0.13 for index splits

## Message Queues & Event Streaming

**Apache Kafka:**
- Client: `rdkafka` 0.38 (Rust bindings to librdkafka C library)
- Optional feature: `kafka` in quickwit-indexing
- Vendored build: `vendored-kafka` feature includes CMake build
- Authentication: SASL/SCRAM support via SASL2 (custom fork)
- Implementation: `quickwit/quickwit-indexing/src/source/queue_sources/` (via generic queue source)
- Dockerfile: Optional CMake, Clang, OpenSSL dev for Kafka support

**Apache Pulsar:**
- Client: `pulsar` 6.3 crate
- Optional feature: `pulsar` in quickwit-indexing
- Authentication: OAuth2 auth support
- Implementation: Source adapter in queue_sources module
- Docker service: Pulsar 3.0.0 standalone mode

**AWS SQS:**
- SDK: `aws-sdk-sqs` 1.82
- Optional feature: `sqs` in quickwit-indexing with `queue-sources`
- Auth: IAM credentials (same as S3)
- Implementation: `quickwit/quickwit-indexing/src/source/queue_sources/sqs_queue.rs`
- Emulation: LocalStack in Docker Compose

**AWS Kinesis:**
- SDK: `aws-sdk-kinesis` 1.86
- Optional feature: `kinesis` in quickwit-indexing with quickwit-aws
- Auth: IAM credentials
- Implementation: `quickwit/quickwit-indexing/src/source/kinesis/kinesis_source.rs`
- Features: Shard consumer, checkpointing, auto-scaling shard discovery
- Emulation: LocalStack in Docker Compose

**Google Cloud Pub/Sub:**
- SDK: `google-cloud-pubsub` 0.18 with `google-cloud-auth` 0.12, `google-cloud-gax` 0.15
- Optional feature: `gcp-pubsub` in quickwit-indexing
- Auth: GCP service account credentials
- Emulator support for testing: GCP Pub/Sub Emulator in Docker Compose

**Local Message Queue:**
- Implementation: `quickwit/quickwit-indexing/src/source/queue_sources/memory_queue.rs`
- Usage: Testing only

**Write-Ahead Log:**
- MRecord (mrecordlog) - Custom fork from quickwit-oss
- Purpose: Durable ingestion with recovery capability
- Implementation: `quickwit/quickwit-ingest/src/metrics/metrics_wal.rs`

## Authentication & Identity

**Cloud Provider Auth:**
- AWS: IAM via SDK credential chain (environment variables, instance roles, credential files)
- Azure: Azure AD service principal via environment or managed identity
- GCP: Service account JSON or Application Default Credentials
- No centralized auth gateway (relies on cloud provider identity)

**Cluster Communication:**
- Chitchat (custom fork) - Gossip protocol for cluster membership
- Node ID configuration in `quickwit.yaml`
- No built-in encryption for inter-node communication

**API Authentication:**
- No built-in API authentication (assumed behind network boundary)
- No OAuth2, JWT, or API key support in core
- Datadog authentication: Via Dogstatsd client credentials (if using Datadog exporter)

## Monitoring & Observability

**Metrics:**
- Prometheus 0.13 - Metrics export on `/metrics` endpoint
- Metrics collection: `metrics` 0.24 framework
- Datadog Dogstatsd: `metrics-exporter-dogstatsd` 0.9 for Datadog integration
- Custom metrics: DD_STATUS_CODES tracking in `quickwit/quickwit-common/src/dd_metrics.rs`

**Distributed Tracing:**
- OpenTelemetry 0.27 instrumentation API
- OpenTelemetry SDK 0.27 with Tokio runtime
- Tracing 0.1 structured logging bridge
- Tracing-OpenTelemetry 0.28 - OTLP export bridge
- Jaeger integration via OTLP protocol
- Configure OTLP exporter in `config/quickwit.yaml` with endpoint like `http://localhost:4317`

**Logging:**
- Tracing-subscriber 0.3 - Structured logging with JSON output support
- Env_logger 0.10 - Log level control via `RUST_LOG` environment variable
- No centralized logging system (stdout/stderr), suitable for container environments
- OpenTelemetry Collector can aggregate logs

**Error Tracking:**
- No Sentry integration
- Errors propagated through structured logs
- Panic handling via Tokio runtime

## CI/CD & Deployment

**Hosting:**
- Docker container image - `Dockerfile` (multi-stage build)
- Base image: `registry.ddbuild.io/images/base/gbi-ubuntu_2204` (Datadog internal)
- Multi-architecture builds: amd64, arm64 (via Cross and Bazel)
- Kubernetes deployment manifests: `k8s/` directory

**CI Pipeline:**
- GitLab CI: `.gitlab-ci.yml` with Docker build and push
- GitHub: `.github/dependabot.yml` for dependency updates
- Bazel: `BUILD.bazel`, `.bazelrc` for reproducible builds
- Integration tests with service emulators in Docker Compose

**Image Registry:**
- Datadog internal: `registry.ddbuild.io/` (configured in Dockerfile)
- Support for custom registry via build args

## Environment Configuration

**Required Environment Variables:**

*Database:*
- `DATABASE_URL` - PostgreSQL connection string for production
- `QW_TEST_DATABASE_URL` - PostgreSQL for tests (if running against real DB)
- `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` - For Docker Compose

*Storage:*
- `QW_S3_ENDPOINT` - Custom S3 endpoint (e.g., localhost:9000 for MinIO)
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` - AWS credentials
- `AWS_REGION` - AWS region (default: us-west-2)
- `QW_DATA_DIR` - Local data directory (e.g., `/quickwit/qwdata`)

*Server:*
- `QW_CONFIG` - Path to YAML config file (e.g., `/quickwit/config/quickwit.yaml`)
- `QW_LISTEN_ADDRESS` - Bind address (e.g., `0.0.0.0`)
- `QW_DISABLE_DOCUMENT_VALIDATION` - Toggle validation (env var check in code)

*Observability:*
- `RUST_LOG` - Log level filter (e.g., `info,quickwit=debug`)
- `OTEL_EXPORTER_OTLP_ENDPOINT` - OpenTelemetry collector endpoint

**Docker Compose Services:**
- All services configurable via `.env` file (see `.env.example`)
- Service versions: `LOCALSTACK_VERSION`, `POSTGRES_VERSION`, `PULSAR_VERSION`, `CP_VERSION`, `AZURITE_VERSION`, `JAEGER_VERSION`, `OTEL_VERSION`, `PROMETHEUS_VERSION`, `GRAFANA_VERSION`

**Configuration Files:**
- `config/quickwit.yaml` - Main config with node, storage, metastore, indexing settings
- `config/cloudprem/datadog.yaml` - Datadog-specific configuration
- `.env.example` - Environment variable template
- Feature compilation: Feature flags in `quickwit/Cargo.toml` (kafka, pulsar, gcp-pubsub, kinesis, sqs, azure, gcs)

## Secrets Management

**Development:**
- `.env.local` and `.env` (gitignored)
- Postgres credentials in environment
- AWS credentials via AWS SDK credential chain

**Production:**
- All secrets via environment variables (no hardcoded defaults)
- Kubernetes Secrets recommended for containerized deployments
- Support for AWS Secrets Manager integration via credential chain

## Webhooks & Callbacks

**Incoming:**
- None - Pull-based ingestion model only
- Ingest via Kafka, Pulsar, SQS, Kinesis, Pub/Sub, or Datadog API

**Outgoing:**
- None - Query-based access model only
- Metrics/logs retrieved via REST API or OpenTelemetry-compatible clients

---

*Integration audit: 2026-01-22*
*Update when adding/removing external services or changing integration methods*
