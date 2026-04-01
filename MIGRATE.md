# DataFusion Metrics Query Layer — Migration Guide

This document describes the full query architecture built in the Pomsky fork
(`github.com/DataDog/pomsky`) that needs to be ported into this OSS quickwit
repository. It is written for an agent that will do the porting directly.

---

## What Already Exists in OSS

The following are **already present** in this branch and do **not** need to be
ported:

| Component | Location | Notes |
|-----------|----------|-------|
| Parquet engine | `quickwit-parquet-engine/` | Ingest processor, schema, split metadata, ParquetWriter |
| Ingestion pipeline | `quickwit-indexing/src/actors/parquet_*.rs` | ParquetDocProcessor → ParquetIndexer → ParquetPackager → ParquetUploader |
| OTLP metrics endpoint | `quickwit-opentelemetry/src/otlp/otel_metrics.rs` | Receives OTLP, builds Arrow IPC with dynamic schema |
| Arrow batch builder | `quickwit-opentelemetry/src/otlp/arrow_metrics.rs` | Dynamic schema, dict-encoded columns |
| Metastore split APIs | `quickwit-metastore/src/metastore/` | `stage_metrics_splits`, `publish_metrics_splits`, `list_metrics_splits` in both Postgres and file-backed |
| Proto definitions | `quickwit-proto/` | `ListMetricsSplitsRequest`, `StageMetricsSplitsRequest`, etc. |

---

## What Needs to Be Ported

### 1. New Crate: `quickwit-datafusion`

Create a new crate at `quickwit/quickwit-datafusion/`. This is the **entire
query execution layer**. Add it to the workspace `Cargo.toml`.

#### `quickwit-datafusion/Cargo.toml`

```toml
[package]
name = "quickwit-datafusion"
description = "DataFusion-based query execution for Quickwit parquet metrics"
version.workspace = true
edition.workspace = true
# ... other workspace fields

[dependencies]
anyhow = { workspace = true }
async-trait = { workspace = true }
bytes = { workspace = true }
chrono = { workspace = true }
futures = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }
tokio = { workspace = true }
tracing = { workspace = true }
url = "2"

quickwit-common = { workspace = true }
quickwit-metastore = { workspace = true }
quickwit-parquet-engine = { workspace = true }
quickwit-proto = { workspace = true }
quickwit-search = { workspace = true }
quickwit-storage = { workspace = true }

arrow = { workspace = true }
arrow-flight = "57"
datafusion = "52"
datafusion-datasource = "52"
datafusion-physical-plan = "52"
datafusion-proto = "52"
datafusion-sql = "52"
datafusion-datasource-parquet = "52"
datafusion-substrait = "52"
datafusion-distributed = { git = "https://github.com/datafusion-contrib/datafusion-distributed" }
datafusion-variant = { git = "https://github.com/datafusion-contrib/datafusion-variant" }
object_store = "0.12"
parquet = { workspace = true }

[dev-dependencies]
quickwit-common = { workspace = true, features = ["testsuite"] }
quickwit-datafusion = { path = ".", features = ["testsuite"] }
tokio = { workspace = true, features = ["test-util", "macros"] }

[features]
testsuite = []
```

**Version note**: DataFusion 52 / Arrow 57. The workspace already uses
Arrow 57 for parquet-engine. DataFusion 52 is the version that aligns with
`datafusion-distributed` from `datafusion-contrib`.

#### Module structure

```
quickwit-datafusion/src/
├── lib.rs
├── catalog.rs          # MetricsIndexResolver trait, SimpleIndexResolver, MetricsSchemaProvider
├── session.rs          # MetricsSessionBuilder
├── table_provider.rs   # MetricsSplitProvider trait, MetricsTableProvider
├── predicate.rs        # DataFusion Expr → MetricsSplitQuery (with CAST fix)
├── resolver.rs         # MetricsWorkerResolver (SearcherPool → Flight URLs)
├── metastore_provider.rs  # MetastoreSplitProvider
├── metastore_resolver.rs  # MetastoreIndexResolver (production resolver)
├── storage.rs          # QuickwitObjectStore (Quickwit Storage → object_store)
├── task_estimator.rs   # MetricsTaskEstimator (split count → task count)
├── flight.rs           # MetricsWorkerSessionBuilder, build_metrics_worker
├── table_factory.rs    # MetricsTableProviderFactory (CREATE EXTERNAL TABLE)
└── test_utils.rs       # MetricsTestbed, TestSplitProvider, make_batch (cfg test)
```

---

## The Wide Schema Problem

**Critical difference from Pomsky**: The OSS ingestion pipeline
(`arrow_metrics.rs`) produces a **dynamic schema** where every tag key
becomes a top-level column. Example parquet file:

```
metric_name  BINARY  (dictionary)
metric_type  INT32   (UInt8)
timestamp_secs  INT64
value        DOUBLE
env          BINARY  (dictionary, nullable)  ← dynamic tag
service      BINARY  (dictionary, nullable)  ← dynamic tag
datacenter   BINARY  (dictionary, nullable)  ← dynamic tag
region       BINARY  (dictionary, nullable)  ← dynamic tag
host         BINARY  (dictionary, nullable)  ← dynamic tag
```

Pomsky used a **fixed 14-column schema** with `tag_service`, `tag_env`, etc.
The OSS approach is better: no fixed tag columns, tags are just columns.

### How to handle this in the query layer

The `MetricsTableProvider` cannot use a fixed schema. Instead:

1. **Schema is declared by the caller** via `CREATE EXTERNAL TABLE` DDL
   (handled by `MetricsTableProviderFactory`).
2. **Columns missing from a specific parquet file** are returned as NULLs
   by DataFusion's `PhysicalExprAdapterFactory` — this is automatic, no
   extra code needed.
3. **Guaranteed columns** (always present): `metric_name`, `metric_type`,
   `timestamp_secs`, `value` — these are required at ingest time.

**No `ParquetSchema::new()` for the table schema.** Pass whatever schema
the caller declared in the DDL.

### SQL pattern for clients

```sql
CREATE EXTERNAL TABLE "otel-metrics-v0_9" (
  metric_name    VARCHAR NOT NULL,
  timestamp_secs BIGINT  NOT NULL,
  value          DOUBLE  NOT NULL,
  metric_type    TINYINT,
  env            VARCHAR,
  service        VARCHAR,
  datacenter     VARCHAR,
  region         VARCHAR,
  host           VARCHAR
) STORED AS metrics LOCATION 'otel-metrics-v0_9';

SELECT metric_name, service, AVG(value)
FROM "otel-metrics-v0_9"
WHERE timestamp_secs >= 1773868000
  AND metric_name = 'test.parquet_pomsky.load_gen.metric_0'
GROUP BY metric_name, service
ORDER BY metric_name, service
```

Multi-statement SQL is handled by `execute_sql_statements()` in the handler
(described below).

---

## File-by-File Porting Instructions

### `catalog.rs`

Contains:
- `MetricsIndexResolver` trait — resolves index name → `(Arc<dyn MetricsSplitProvider>, Arc<dyn ObjectStore>, ObjectStoreUrl)`
- `SimpleIndexResolver` — test helper, same store/provider for all names
- `MetricsSchemaProvider` — implements DataFusion `SchemaProvider`; calls resolver lazily

**Key implementation detail**: `table_names()` on `SchemaProvider` is sync
but listing indexes from the metastore is async. Bridge with
`tokio::task::block_in_place` + `Handle::current().block_on(...)`.

**OSS schema difference**: `MetricsSchemaProvider::table()` should NOT
hardcode a schema. The `MetricsTableProvider` is created with whatever
schema comes from the resolver. When no DDL is present, use a minimal
base schema (just the 4 guaranteed columns).

Copy from Pomsky: `quickwit-datafusion/src/catalog.rs`

---

### `session.rs`

The `MetricsSessionBuilder`:
- Accepts `Arc<dyn MetricsIndexResolver>`
- Optional `SearcherPool` for distributed execution
- `build_session()` creates a `SessionContext` with:
  - `default_catalog = "quickwit"`, `default_schema = "public"`
  - `information_schema = true`
  - `create_default_catalog_and_schema = false`
  - `target_partitions = 1`
  - Registers `MetricsTableProviderFactory` for `"metrics"` file type
  - Registers `MetricsSchemaProvider` in catalog
  - If searcher_pool: registers `MetricsWorkerResolver` + `MetricsTaskEstimator` + `DistributedPhysicalOptimizerRule`
  - Registers all variant UDFs from `datafusion-variant`

**Variant UDFs to register**:
```rust
VariantGetUdf, VariantGetFieldUdf, VariantGetStrUdf, VariantGetIntUdf,
IsVariantNullUdf, VariantToJsonUdf, JsonToVariantUdf, CastToVariantUdf
```

Copy from Pomsky: `quickwit-datafusion/src/session.rs`

---

### `table_provider.rs`

`MetricsSplitProvider` trait:
```rust
#[async_trait]
pub trait MetricsSplitProvider: Send + Sync + fmt::Debug {
    async fn list_splits(&self, query: &MetricsSplitQuery) -> DFResult<Vec<MetricsSplitMetadata>>;
}
```

`MetricsTableProvider` implements DataFusion `TableProvider`:
- `schema()` → whatever arrow schema was passed at construction
- `supports_filters_pushdown()` → `Inexact` for: metric_name, timestamp_secs, and any tag columns that are in the schema
- `scan()`:
  1. `extract_split_filters(filters)` → `MetricsSplitQuery`
  2. `split_provider.list_splits(&query)` → pruned split list
  3. Register object store with runtime env
  4. Build `ParquetSource` with bloom filters + pushdown enabled
  5. Return `DataSourceExec::from_data_source(file_scan_config)`

**OSS difference vs Pomsky**: The tag column names are dynamic (env, service,
datacenter, region, host — not tag_service, tag_env, etc.).
`supports_filters_pushdown()` and `extract_split_filters()` need to handle
the actual column names from the schema, not hardcoded `tag_*` names.

Copy from Pomsky: `quickwit-datafusion/src/table_provider.rs`, then adapt
the filter pushdown column names.

---

### `predicate.rs`

Converts DataFusion `Expr` filters to `MetricsSplitQuery` for metastore pruning.

**CRITICAL BUG FIX — must include**: DataFusion inserts `CAST` nodes during
type coercion (e.g. `CAST(timestamp_secs AS Int64) >= 1000` for UInt64 vs
Int64 comparison). Without unwrapping these, the time filter is never pushed
to Postgres and ALL splits are fetched.

**The fix** (must be in `column_name()` and `scalar_u64()`):
```rust
fn column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(col) => Some(col.name().to_string()),
        // Unwrap CASTs inserted by DataFusion type coercion
        Expr::Cast(Cast { expr, .. }) | Expr::TryCast(TryCast { expr, .. }) => column_name(expr),
        _ => None,
    }
}

fn scalar_u64(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Literal(ScalarValue::UInt64(Some(v)), _) => Some(*v),
        Expr::Literal(ScalarValue::Int64(Some(v)), _) if *v >= 0 => Some(*v as u64),
        // Also unwrap cast on the literal side
        Expr::Cast(Cast { expr, .. }) | Expr::TryCast(TryCast { expr, .. }) => scalar_u64(expr),
        _ => None,
    }
}
```

**OSS difference**: Tag column names in the MetricsSplitQuery need to match
the actual OSS column names: `service`, `env`, `datacenter`, `region`, `host`
(not `tag_service`, `tag_env`, etc.). Update `set_tag_values()` accordingly.

Copy from Pomsky: `quickwit-datafusion/src/predicate.rs`, then update column
name mappings and ensure the full test suite is included (22 tests, including
CAST unwrapping tests).

---

### `resolver.rs`

`MetricsWorkerResolver` implements `datafusion_distributed::WorkerResolver`:
```rust
fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
    let addrs: Vec<SocketAddr> = self.searcher_pool.keys();
    // returns vec of "http://<addr>" URLs
}
```

Copy from Pomsky: `quickwit-datafusion/src/resolver.rs` — no changes needed.

---

### `metastore_provider.rs`

`MetastoreSplitProvider` calls `metastore.list_metrics_splits()` and returns
published splits only.

**OSS tag name difference**: In `to_metastore_query()`, the tag field names
used in `ListMetricsSplitsQuery` must match what the OSS metastore stores.
Look at `ListMetricsSplitsQuery` in `quickwit-metastore/src/metastore/mod.rs`
to see the actual field names (`tag_service`, `tag_env`, etc. may be stored
differently in OSS).

Copy from Pomsky: `quickwit-datafusion/src/metastore_provider.rs`, verify
field name mapping against OSS `ListMetricsSplitsQuery`.

---

### `metastore_resolver.rs`

Production `MetricsIndexResolver` backed by the Quickwit metastore + storage:
1. `index_metadata(IndexMetadataRequest::for_index_id(name))` → gets `index_uri`
2. `storage_resolver.resolve(&index_uri)` → `Arc<dyn Storage>`
3. Wraps in `QuickwitObjectStore`
4. Returns `(MetastoreSplitProvider, QuickwitObjectStore, ObjectStoreUrl("quickwit://<name>/"))`
5. `list_index_names()` → calls `list_indexes_metadata()` → returns all index IDs

Copy from Pomsky: `quickwit-datafusion/src/metastore_resolver.rs` — no
changes needed.

---

### `storage.rs`

Bridges `Arc<dyn quickwit_storage::Storage>` to DataFusion's `ObjectStore`.

Read-only. Implements:
- `get_opts()` → `storage.get_all()`
- `get_range(path, range: Range<u64>)` → `storage.get_slice(path, range as usize)`
- `head()` → `storage.file_num_bytes()`
- All write/list ops → `NotSupported`

Copy from Pomsky: `quickwit-datafusion/src/storage.rs` — no changes needed.

---

### `task_estimator.rs`

`MetricsTaskEstimator` implements `datafusion_distributed::TaskEstimator`.
Inspects `DataSourceExec` file groups to count splits, returns that as
desired task count.

Copy from Pomsky: `quickwit-datafusion/src/task_estimator.rs` — no changes
needed.

---

### `flight.rs`

`MetricsWorkerSessionBuilder` implements `WorkerSessionBuilder`:
- Registers metrics catalog on worker sessions (same as coordinator)
- Pre-registers object stores from `"x-metrics-indexes"` HTTP header
- Fallback: tries to resolve `"metrics"` index

`build_metrics_worker(resolver)` → `Worker::from_session_builder(...)`

**Important**: Workers receive serialized plan fragments with
`ObjectStoreUrl`s like `quickwit://index-name/` baked in. The object store
MUST be registered BEFORE DataFusion tries to execute the plan. The header
mechanism sends the index name(s) from coordinator to workers.

Copy from Pomsky: `quickwit-datafusion/src/flight.rs` — no changes needed.

---

### `table_factory.rs`

`MetricsTableProviderFactory` handles:
```sql
CREATE EXTERNAL TABLE "index-name" (col1 TYPE, ...) STORED AS metrics LOCATION 'index-name';
```

The file type string is `"metrics"` (const `METRICS_FILE_TYPE`).

- `LOCATION` field → index name for resolver (falls back to table name)
- `cmd.schema.as_arrow().clone()` → declared Arrow schema
- Resolves `(split_provider, object_store, url)` from index_resolver
- Creates `MetricsTableProvider` with declared schema

This enables the wide-schema pattern: callers declare exactly what columns
they want, DataFusion returns NULLs for columns missing from specific files.

Copy from Pomsky: `quickwit-datafusion/src/table_factory.rs` — no changes
needed.

---

### `test_utils.rs`

Test helpers gated on `#[cfg(any(test, feature = "testsuite"))]`:

- `make_batch(metric_name, timestamps, values, service)` → `RecordBatch`
  with the base 4 columns + service tag
- `TestSplitProvider` → in-memory split provider with pruning
- `MetricsTestbed` → writes parquet to InMemory store, builds test sessions
- `physical_plan_str()`, `execute()`, `total_rows()`

**OSS difference**: `make_batch()` should produce a schema matching the OSS
dynamic schema (service, env, etc. as top-level columns) rather than the
fixed 14-column Pomsky schema. Or keep both and alias.

Copy from Pomsky: `quickwit-datafusion/src/test_utils.rs`, update schema to
match OSS column names.

---

## Changes to Existing Crates

### `quickwit-serve/Cargo.toml`

Add:
```toml
quickwit-datafusion = { workspace = true }
datafusion = "52"
datafusion-sql = "52"
```

### `quickwit-serve/src/lib.rs`

#### 1. Add `storage_resolver` to `QuickwitServices`

```rust
struct QuickwitServices {
    // ... existing fields ...
    pub metrics_session_builder: Option<Arc<quickwit_datafusion::session::MetricsSessionBuilder>>,
    pub storage_resolver: quickwit_storage::StorageResolver,
}
```

#### 2. Change `setup_searcher` return type

```rust
async fn setup_searcher(...) -> anyhow::Result<(SearchJobPlacer, Arc<dyn SearchService>, SearcherPool)>
```

Return `searcher_pool` as a third element (it's created inside but not
currently returned).

#### 3. Construct `MetricsSessionBuilder` after `setup_searcher`

```rust
let (search_job_placer, search_service, searcher_pool) = setup_searcher(...).await?;

let metrics_session_builder = if node_config.is_service_enabled(QuickwitService::Searcher) {
    let index_resolver = Arc::new(
        quickwit_datafusion::metastore_resolver::MetastoreIndexResolver::new(
            metastore_through_control_plane.clone(),
            storage_resolver.clone(),
        ),
    );
    let builder = quickwit_datafusion::session::MetricsSessionBuilder::new(index_resolver)
        .with_searcher_pool(searcher_pool);
    Some(Arc::new(builder))
} else {
    None
};
```

#### 4. Add `create_metrics_index_if_configured` (local dev convenience)

Gated on `QW_CREATE_METRICS_INDEX=true` env var. Creates a simple
`"metrics"` index with `default_index_root_uri/metrics` as the URI.
Called at the end of `create_managed_indexes()`.

#### 5. In `QuickwitServices` construction

```rust
let quickwit_services = Arc::new(QuickwitServices {
    // ... existing fields ...
    metrics_session_builder,
    storage_resolver: storage_resolver.clone(),
});
```

---

### `quickwit-serve/src/grpc.rs`

#### CloudPrem service wiring

Pass `services.metrics_session_builder.clone()` instead of `None`:

```rust
let cloudprem_service_impl = CloudPremServiceImpl::new(
    search_service,
    services.metastore_client.clone(),
    services.cluster.clone(),
    services.node_config.default_index_root_uri.clone(),
    services.metrics_session_builder.clone(),  // was: None
);
```

#### Flight service wiring

```rust
let worker = if let Some(ref session_builder) = services.metrics_session_builder {
    quickwit_datafusion::flight::build_metrics_worker(
        Arc::clone(session_builder.index_resolver()),
    )
} else {
    Worker::default()
};
Some(FlightServiceServer::new(worker))
```

Also add `session_builder.index_resolver() -> &Arc<dyn MetricsIndexResolver>`
method to `MetricsSessionBuilder`.

---

### `quickwit-serve/src/cloudprem/service.rs`

This file contains the CloudPrem service implementation. If it doesn't
exist in the OSS fork, check whether there is a separate gRPC service for
DataFusion queries that needs to be created or adapted.

#### Add imports

```rust
use datafusion::prelude::SessionContext;
use datafusion_sql::parser::DFParserBuilder;
use quickwit_datafusion::session::MetricsSessionBuilder;
use quickwit_proto::cloudprem::{
    CloudpremSubstraitRequest, CloudpremSubstraitResponse,
    cloudprem_substrait_request,
};
```

#### Add `execute_sql_statements` helper

```rust
/// Executes one or more SQL statements sequentially.
/// DDL statements (CREATE EXTERNAL TABLE) are executed for side effects.
/// The last non-DDL DataFrame is returned.
async fn execute_sql_statements(
    ctx: &SessionContext,
    sql: &str,
) -> datafusion::error::Result<datafusion::dataframe::DataFrame> {
    let mut statements = DFParserBuilder::new(sql).build()?.parse_statements()?;
    if statements.is_empty() {
        return Err(datafusion::error::DataFusionError::Plan(
            "no SQL statements provided".to_string(),
        ));
    }
    let mut last_df = None;
    while let Some(stmt) = statements.pop_front() {
        let plan = ctx.state().statement_to_plan(stmt).await?;
        let df = ctx.execute_logical_plan(plan).await?;
        last_df = Some(df);
    }
    last_df.ok_or_else(|| {
        datafusion::error::DataFusionError::Plan("no query statement found".to_string())
    })
}
```

#### `substrait_search` RPC implementation

```rust
async fn substrait_search(
    &self,
    request: CloudpremSubstraitRequest,
) -> CloudPremResult<CloudpremSubstraitResponse> {
    let session_builder = self.metrics_session_builder.as_ref()
        .ok_or_else(|| CloudPremError::Internal("metrics datafusion not configured".to_string()))?;
    let ctx = session_builder.build_session()
        .map_err(|e| CloudPremError::Internal(format!("session error: {e}")))?;

    let df = match request.query {
        Some(cloudprem_substrait_request::Query::SqlQuery(sql)) => {
            execute_sql_statements(&ctx, &sql).await
                .map_err(|e| CloudPremError::Internal(format!("sql error: {e}")))?
        }
        Some(cloudprem_substrait_request::Query::SubstraitPlanBytes(_)) => {
            return Err(CloudPremError::Internal("substrait plans not yet supported".to_string()));
        }
        None => return Err(CloudPremError::Internal("missing query".to_string())),
    };

    // Optional: return physical plan text if settings["explain"] == "true"
    if request.settings.get("explain").is_some_and(|v| v == "true") {
        let plan = df.create_physical_plan().await
            .map_err(|e| CloudPremError::Internal(format!("plan error: {e}")))?;
        let plan_text = format!(
            "{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
        );
        // encode plan_text as single-row Arrow IPC with schema { plan: Utf8 }
        // ... (see Pomsky service.rs for full implementation)
        return Ok(CloudpremSubstraitResponse { arrow_ipc_bytes: ipc_buf.into() });
    }

    let batches = df.collect().await
        .map_err(|e| CloudPremError::Internal(format!("execution error: {e}")))?;

    // Encode as Arrow IPC StreamWriter format
    let mut ipc_buf = Vec::new();
    if let Some(batch) = batches.first() {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut ipc_buf, &batch.schema())
            .map_err(|e| CloudPremError::Internal(format!("ipc error: {e}")))?;
        for batch in &batches {
            writer.write(batch)
                .map_err(|e| CloudPremError::Internal(format!("ipc error: {e}")))?;
        }
        writer.finish()
            .map_err(|e| CloudPremError::Internal(format!("ipc error: {e}")))?;
    }
    Ok(CloudpremSubstraitResponse { arrow_ipc_bytes: ipc_buf.into() })
}
```

---

### `quickwit-serve/src/rest.rs`

Register the metrics ingest REST endpoint in `api_v1_routes()`:

```rust
.or(crate::metrics_ingest_api::metrics_ingest_handler(
    quickwit_services.metastore_client.clone(),
    quickwit_services.storage_resolver.clone(),
))
.boxed()
```

---

### `quickwit-serve/src/metrics_ingest_api.rs` (new file)

REST endpoint for local testing: `POST /api/v1/{index_id}/ingest-metrics`

Accepts JSON array of metric data points:
```json
[{
  "metric_name": "cpu.usage",
  "timestamp_secs": 1700000100,
  "value": 0.85,
  "service": "web",
  "env": "prod",
  "attributes": {"k8s.pod": "web-abc123"}
}]
```

Implementation:
1. Parse JSON → `Vec<MetricDataPoint>`
2. Get index metadata from metastore (for `index_uri`)
3. Resolve storage via `StorageResolver`
4. Build `RecordBatch` with the OSS dynamic schema
5. Write parquet via `ParquetWriter`
6. Build `MetricsSplitMetadata` with metric names, time range, tag values
7. Stage + publish via metastore
8. Return `{ num_metrics_ingested, split_id }`

**OSS schema difference**: The `MetricDataPoint` struct and `build_record_batch()`
should use the OSS column names (`service`, `env`, `datacenter`, `region`,
`host`) rather than `tag_service`, `tag_env`, etc.

The `MetricsSplitMetadata` tag storage also uses the OSS field names — check
`ListMetricsSplitsQuery` for the exact field names used in pruning.

Copy from Pomsky: `quickwit-serve/src/metrics_ingest_api.rs`, update column
and field names to match OSS.

---

## Integration Test Pattern

The integration tests use `ClusterSandbox` to start real Quickwit nodes.

Reference file: `quickwit-integration-tests/src/tests/` — create
`metrics_datafusion_tests.rs` with:

1. `start_metrics_sandbox()` — set env vars, start standalone sandbox
2. `metastore_client(&sandbox)` — connect to metastore gRPC
3. `create_metrics_index(metastore, id, data_dir)` — create index with `file://` URI
4. `write_and_publish_split(metastore, index_uid, data_dir, name, batch)` — write parquet + stage/publish
5. `substrait_sql(sandbox, sql)` — send SubstraitSearch RPC, decode Arrow IPC

Test cases:
- SELECT * (schema + row count)
- Metric name pruning (only matching splits scanned)
- Time range pruning (CAST fix critical here)
- Aggregation across splits
- GROUP BY across splits
- REST ingest → SubstraitSearch (end-to-end)

Add to `quickwit-integration-tests/Cargo.toml`:
```toml
arrow = { workspace = true }
bytesize = { workspace = true }
quickwit-datafusion = { workspace = true, features = ["testsuite"] }
quickwit-parquet-engine = { workspace = true }
```

---

## Source Files to Copy

All source files are in the Pomsky fork at:
```
github.com/DataDog/pomsky — branch: bianchi/forrealthistime
quickwit/quickwit-datafusion/src/
quickwit/quickwit-serve/src/metrics_ingest_api.rs
quickwit/quickwit-serve/src/cloudprem/service.rs  (substrait_search + execute_sql_statements)
quickwit/quickwit-integration-tests/src/tests/metrics_datafusion_tests.rs
```

---

## Required Schema Adaptations

The key difference between Pomsky and OSS is tag column naming:

| Pomsky | OSS |
|--------|-----|
| `tag_service` | `service` |
| `tag_env` | `env` |
| `tag_datacenter` | `datacenter` |
| `tag_region` | `region` |
| `tag_host` | `host` |

Files that need this adaptation:
- `table_provider.rs` — `supports_filters_pushdown()` column names
- `predicate.rs` — `set_tag_values()` column-to-field mapping
- `metastore_provider.rs` — `to_metastore_query()` tag field mapping
- `test_utils.rs` — `make_batch()` schema
- `metrics_ingest_api.rs` — `MetricDataPoint` struct + `build_record_batch()`

Also: the OSS schema does **not** have `metric_unit`, `start_timestamp_secs`,
`attributes`, `service_name`, or `resource_attributes` columns. The
`attributes` / `resource_attributes` VARIANT columns were a Pomsky addition
for OTel compatibility. The OSS schema is minimal: `metric_name`,
`metric_type`, `timestamp_secs`, `value`, plus dynamic tag columns.

Check `quickwit-parquet-engine/src/schema/fields.rs` for the current
`REQUIRED_FIELDS` and `SORT_ORDER` constants — these define the guaranteed
columns and sort order.

---

## Key Gotchas

1. **CAST Unwrapping** — the `column_name()` and `scalar_u64()` fix in
   `predicate.rs` is critical for performance. Without it, every query
   fetches ALL splits from Postgres regardless of time filter.

2. **Object store pre-registration on workers** — Flight workers receive
   serialized plan fragments with `quickwit://index-name/` baked in. The
   object store must be registered in the worker's RuntimeEnv before plan
   execution. `MetricsWorkerSessionBuilder` handles this via the
   `"x-metrics-indexes"` header.

3. **`block_in_place` in `table_names()`** — DataFusion's `SchemaProvider`
   trait has a sync `table_names()` method. Bridging to the async metastore
   requires `tokio::task::block_in_place`. This panics if not running in a
   multi-threaded tokio runtime.

4. **`datafusion-distributed` is a git dep** — it is not yet on crates.io.
   Cargo.lock must include the pinned commit. The crate provides
   `Worker`, `WorkerResolver`, `DistributedExt`, `DistributedPhysicalOptimizerRule`.

5. **Arrow Flight tonic conflict** — `arrow-flight 57` and the workspace
   both pull tonic but from different registries. Cannot implement a custom
   `FlightService` trait; must use `Worker` directly (it implements
   `FlightService` internally).

6. **Multi-statement SQL** — `ctx.sql()` rejects multiple statements.
   Use `DFParserBuilder::new(sql).build()?.parse_statements()` and execute
   each in a loop. This enables the DDL + SELECT pattern.

7. **`PhysicalExprAdapterFactory` for wide schema** — DataFusion
   automatically handles columns declared in the table schema but absent
   from specific parquet files: they become NULL columns. No extra code
   needed. This is the mechanism that makes the wide dynamic schema work.
