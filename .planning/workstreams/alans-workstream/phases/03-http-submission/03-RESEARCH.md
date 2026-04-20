# Phase 3: HTTP Submission - Research

**Researched:** 2026-04-20
**Domain:** HTTP client implementation, JSON wire format compatibility, mock-based testing
**Confidence:** HIGH

## Summary

Phase 3 implements the `FlushClient` struct that POSTs pending metric metadata to the SaaS `byoc-ingest-metadata-svc` endpoint. The Go reference service at `dd-source/domains/quickhouse/apps/byoc-metrics-metadata/internal/client/` defines the exact wire format: a JSON body with `org_id` (string) and `records` (array of `{metric_name, metric_type, interval}` objects), plus a `DD-API-KEY` header and `Content-Type: application/json`. The response contains a `succeeded_metrics` string array.

The existing Rust codebase already has `reqwest` (workspace 0.12 with `json` + `rustls-tls` features) as a dependency in `pomsky-intake/Cargo.toml`, and the workspace defines `wiremock = "0.6"` (resolved to 0.6.5), `serde_json = "1.0"` (resolved to 1.0.149), and `thiserror = "2"` for testing and error handling. The `quickwit-rest-client` crate already uses wiremock with `MockServer`, `Mock::given()`, matchers, and `ResponseTemplate` -- providing an in-repo pattern to follow. The existing `MetricTypeInfo` struct lacks `Serialize`/`Deserialize` derives, so Phase 3 must either add serde derives or create dedicated request/response serde structs.

**Primary recommendation:** Create a `flush_client.rs` module with a `FlushClient` struct holding `reqwest::Client`, config fields, and an async `flush_pending()` method. Define dedicated `UpsertRequest`/`UpsertResponse` serde structs matching the Go wire format exactly. Use `thiserror` for `FlushError` enum. Use `wiremock` for all tests. Add `serde_json`, `thiserror`, and `wiremock` to `pomsky-intake/Cargo.toml`.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** Exact wire format must match the Go service at `dd-source/domains/quickhouse/apps/byoc-metrics-metadata/`. Researcher reads the Go source to extract JSON request body structure, response shape, and `succeeded_metrics` field format.
- **D-02:** POST to `{metadata_svc_url}/api/unstable/byoc/ingest/metadata/metric-metadata` with `DD-API-KEY` header value from env and `org_id` in JSON body (per HTTP-01).
- **D-03:** Separate `FlushClient` struct in a new `flush_client.rs` module. Holds `reqwest::Client`, `api_key: String`, `metadata_svc_url: String`, `org_id: String`. Follows the module-per-responsibility pattern from Phase 2 (`known_metrics.rs`, `csv_persistence.rs`).
- **D-04:** `reqwest::Client` built once in `TransformConfig::build()` with timeout from `http_timeout_secs`. Stored in `FlushClient`. Connection pooling handled automatically by reqwest. No retry per design.
- **D-05:** `FlushClient` exposes an async `flush_pending()` method that takes the pending list and returns the set of succeeded metric names. Phase 4 calls this from the `select!` loop. Phase 3 tests call it directly -- no timer wiring needed.
- **D-06:** Phase 3 implements the flush logic only (HTTP call, response parsing, known-set update, failure handling). Phase 4 implements interval + batch-size trigger checking and the `select!` loop that calls `flush_pending()`.
- **D-07:** The `flush_pending()` method receives `&HashMap<String, MetricTypeInfo>` (the pending list), performs the POST, and returns `Result<Vec<String>, FlushError>` where `Ok(names)` are the succeeded metrics and `Err` means the entire flush failed (pending should be dropped).
- **D-08:** On HTTP failure (network error, 4xx, 5xx, timeout), log at `warn!` level with status code or error type, then return an error. The caller (Phase 4's `select!` loop) drops the pending list. This gives operators visibility without requiring OBS-01/OBS-02 metrics (deferred to v2).
- **D-09:** Use `wiremock` crate for mock HTTP server. Register expected requests with matchers, assert headers/body, return canned responses.
- **D-10:** Key test scenario: partial success -- response `succeeded_metrics` is a subset of what was sent. Only succeeded names get added to known set; others remain unknown for re-detection on next arrival.

### Claude's Discretion
- Exact error type design for `FlushError` (simple enum vs anyhow)
- Whether `flush_pending()` takes ownership or borrows the pending map
- Request serialization details (serde struct vs manual JSON construction)
- Additional edge-case tests beyond partial success (empty response, malformed response, etc.)

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| HTTP-01 | Pending metrics POSTed to `{metadata_svc_url}/api/unstable/byoc/ingest/metadata/metric-metadata` with `DD-API-KEY` header and org_id in body | Go wire format extracted: `UpsertMetricMetadataRequest{org_id, records[{metric_name, metric_type, interval}]}`, header `DD-API-KEY`, `Content-Type: application/json`. See Wire Format section. |
| HTTP-02 | Flush triggered by interval (default 15s) or pending list size (default 200), whichever comes first | Phase 3 implements flush method only; trigger logic is Phase 4. Config defaults already exist in `mod.rs`. |
| HTTP-03 | Only metrics in `succeeded_metrics` response are added to the known set with fresh TTL | Response type `UpsertMetricMetadataResponse{succeeded_metrics: Vec<String>}`. `KnownMetrics::insert()` already exists from Phase 2. |
| HTTP-04 | On flush failure, pending list is dropped; metrics will be re-detected on next arrival | `flush_pending()` returns `Result<Vec<String>, FlushError>` -- caller drops pending on Err. |
</phase_requirements>

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| HTTP POST to metadata service | FlushClient (flush_client.rs) | -- | Encapsulates reqwest::Client, URL construction, header injection, JSON serialization |
| Request body construction | FlushClient (flush_client.rs) | types.rs (MetricTypeInfo source data) | FlushClient maps HashMap<String, MetricTypeInfo> to UpsertMetricMetadataRequest serde struct |
| Response parsing + succeeded extraction | FlushClient (flush_client.rs) | -- | Deserializes UpsertMetricMetadataResponse, extracts succeeded_metrics Vec<String> |
| Error classification (timeout/4xx/5xx) | FlushClient (flush_client.rs) | -- | Maps reqwest errors and HTTP status codes to FlushError enum |
| Known-set update with succeeded metrics | Caller (Phase 4 select! loop) | KnownMetrics (known_metrics.rs) | FlushClient returns names; caller calls KnownMetrics::insert() per D-05/D-06 boundary |
| Pending list lifecycle (drop on failure) | Caller (Phase 4 select! loop) | -- | FlushClient returns Err; caller drops HashMap per D-07/D-08 |

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| reqwest | 0.12.28 | Async HTTP client for POST requests | Already workspace dependency in pomsky-intake; `json` feature enables `.json(&body)` [VERIFIED: Cargo.toml workspace definition + cargo metadata] |
| serde_json | 1.0.149 | JSON serialization for request/response structs | Workspace dependency, needed for serde derives on request/response types [VERIFIED: workspace Cargo.toml line 237] |
| serde | 1.0.228 | Derive Serialize/Deserialize for wire format structs | Already in pomsky-intake/Cargo.toml [VERIFIED: Cargo.toml] |
| thiserror | 2 | Typed error enum for FlushError | Workspace dependency [VERIFIED: workspace Cargo.toml line 258, `thiserror = "2"`] |
| tracing | (workspace) | `warn!` logging on HTTP failures per D-08 | Already in pomsky-intake/Cargo.toml [VERIFIED: Cargo.toml] |

### Supporting (dev-dependencies)
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| wiremock | 0.6.5 | Mock HTTP server for testing flush client | All flush_client tests; matches in-repo pattern from quickwit-rest-client [VERIFIED: workspace Cargo.toml line 322, cargo metadata] |
| serde_json | 1.0.149 | JSON assertions in tests (body inspection) | Test assertions on request body structure [VERIFIED: workspace Cargo.toml] |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| wiremock | mockall (trait mocking) | wiremock tests the real HTTP path including serialization; mockall would skip the network layer. D-09 locks wiremock. |
| serde structs for request | serde_json::json!() macro | Dedicated structs catch field name typos at compile time and document the wire format. Structs preferred. |
| thiserror for FlushError | anyhow::Error | thiserror gives typed variants (Timeout, HttpStatus, Network) enabling future pattern matching; anyhow is simpler but less structured. Recommend thiserror -- available in workspace as v2. |

**Dependencies to add to `pomsky-intake/Cargo.toml`:**
```toml
# In [dependencies]
serde_json = { workspace = true }
thiserror = { workspace = true }

# In [dev-dependencies]
wiremock = { workspace = true }
```

Note: `serde_json` is needed in production dependencies for the `#[derive(Deserialize)]` on the response type -- `reqwest`'s `.json::<T>()` method requires `serde_json` at the call site. [VERIFIED: reqwest "json" feature depends on serde_json]

## Project Constraints (from CLAUDE.md)

- **No `unwrap()` in library code** -- use `?` operator or proper error types
- **No `Path::exists()`** -- use fallible alternatives (not relevant to this phase)
- **No `tokio::sync::Mutex`** -- use actor model (not relevant; no shared mutable state in FlushClient)
- **No shadowing variable names** within a function
- **Log messages** must start with lowercase, no trailing punctuation
- **License headers** on all new `.rs` files (Apache 2.0)
- **Files under 500 lines** -- split by responsibility
- **`cargo clippy --workspace --all-features --tests`** must pass
- **`cargo +nightly fmt --all`** must pass
- **`cargo machete`** must pass (no unused dependencies)
- **Disallowed methods** in clippy.toml: `Option::is_some_and`, `is_none_or`, `xor`, `map_or`, `map_or_else`
- **Prefer workspace dependency references** with minimal features
- **Debug must NOT be derived on structs containing api_key** (already handled by MetricMetadataTransform not deriving Debug)

## Wire Format Specification (from Go Reference)

This section extracts the exact wire format from the Go service. The Rust implementation MUST produce identical HTTP requests. [VERIFIED: Go source code at dd-source/domains/quickhouse/apps/byoc-metrics-metadata/internal/client/]

### Request

**Method:** POST
**URL:** `{base_url}/api/unstable/byoc/ingest/metadata/metric-metadata`
**Headers:**
- `Content-Type: application/json`
- `DD-API-KEY: {api_key}`

**Body (JSON):**
```json
{
  "org_id": "string",
  "records": [
    {
      "metric_name": "system.cpu.user",
      "metric_type": "gauge",
      "interval": 0
    },
    {
      "metric_name": "request.rate",
      "metric_type": "rate",
      "interval": 10
    }
  ]
}
```

**Go struct (source of truth):**
```go
type UpsertMetricMetadataRequest struct {
    OrgID   string                     `json:"org_id"`
    Records []UpsertMetricMetadataItem `json:"records"`
}

type UpsertMetricMetadataItem struct {
    MetricName string `json:"metric_name"`
    MetricType string `json:"metric_type,omitempty"`
    Interval   int64  `json:"interval,omitempty"`
}
```

**Key observations:**
- `metric_type` and `interval` use `omitempty` in Go, meaning they are omitted from JSON when zero-valued (`""` for string, `0` for int64). The Rust serde equivalent is `#[serde(skip_serializing_if = "...")]`.
- `interval` is `int64` in Go. In the Rust `MetricTypeInfo`, `interval` is `u32`. The wire format uses `i64/int64`. For the request struct, use `i64` to match Go exactly; cast from `u32` at serialization time.
- `metric_type` serialization: The existing `MetadataMetricType` enum already serializes to lowercase strings via `#[serde(rename_all = "lowercase")]` with explicit `#[serde(rename = "ddsketch")]` for DdSketch. These match the Go service's expected string values ("count", "rate", "gauge", "ddsketch"). [VERIFIED: types.rs lines 24-33, Go tests use "gauge" string]

### Response

**Status:** 200 OK on success
**Body (JSON):**
```json
{
  "succeeded_metrics": ["system.cpu.user", "request.rate"]
}
```

**Go struct:**
```go
type UpsertMetricMetadataResponse struct {
    SucceededMetrics []string `json:"succeeded_metrics"`
}
```

**Edge cases from Go client tests:** [VERIFIED: client_test.go]
- `{"succeeded_metrics": []}` -- empty array, returns empty `Vec<String>`
- `{"succeeded_metrics": null}` -- null field, treat as empty
- `{}` -- missing field entirely, treat as empty
- The Rust `#[serde(default)] succeeded_metrics: Vec<String>` handles all three cases correctly: missing field -> empty vec, null -> empty vec (serde_json treats null as absent for `#[serde(default)]` containers), `[]` -> empty vec.

### Error Handling

**Go error classification:** [VERIFIED: client/errors.go, client/client.go]
- 401/403 -> Unauthorized
- 400-499 (except 401/403) -> BadRequest
- 500+ -> ServerError
- Network timeout -> Timeout (via `net.Error` check)
- Other network errors -> generic error

The Rust `FlushError` enum should mirror these categories for operator visibility in warn! logs per D-08.

## Architecture Patterns

### System Architecture Diagram

```
                          Phase 3 Scope
                     +----------------------+
                     |                      |
 HashMap<String,     |   FlushClient        |    reqwest::Client
 MetricTypeInfo>     |                      |         |
 (pending list)  --->|  flush_pending()     |-------->|  POST /api/unstable/
                     |    |                 |         |  byoc/ingest/metadata/
                     |    +- build request  |         |  metric-metadata
                     |    |  (serde struct) |         |
                     |    +- set headers    |         |  DD-API-KEY header
                     |    |  (DD-API-KEY,   |         |  Content-Type: json
                     |    |   Content-Type) |         |
                     |    +- send POST      |-------->|  --> SaaS endpoint
                     |    |                 |         |
                     |    +- check status   |<--------|  <-- HTTP response
                     |    |  (200 vs error) |         |
                     |    +- parse response |         |
                     |       (succeeded_    |         |
                     |        metrics)      |         |
                     |                      |         |
                     +---------+------------+         |
                               |                      |
                     Ok(Vec<String>)                   |
                     or Err(FlushError)                |
                               |
              +----------------+-----------------+
              | Phase 4 caller (select! loop)     |
              |  Ok: insert each name into        |
              |      KnownMetrics with fresh TTL  |
              |  Err: drop pending HashMap,       |
              |       warn! log, start fresh      |
              +-----------------------------------+
```

### Recommended Module Structure
```
src/transforms/metric_metadata/
+-- mod.rs              # Transform struct, config, build() -- add FlushClient field
+-- types.rs            # MetricTypeInfo, MetadataMetricType (existing)
+-- known_metrics.rs    # KnownMetrics (existing, no changes)
+-- csv_persistence.rs  # CSV read/write (existing, no changes)
+-- flush_client.rs     # NEW: FlushClient, FlushError, request/response serde types
```

### Pattern 1: Dedicated Wire Format Structs
**What:** Define Rust structs that mirror the Go `UpsertMetricMetadataRequest`, `UpsertMetricMetadataItem`, and `UpsertMetricMetadataResponse` exactly, separate from internal domain types.
**When to use:** Always -- decouples wire format from internal representation. If the Go API changes field names, only the serde structs change.
**Example:**
```rust
// Source: Go service types.go (verified)
use serde::{Deserialize, Serialize};

/// Request body for POST to metric-metadata endpoint.
/// Field names match Go `UpsertMetricMetadataRequest` exactly.
#[derive(Serialize)]
struct UpsertRequest {
    org_id: String,
    records: Vec<UpsertRecord>,
}

/// Single record within the upsert request.
/// Maps from internal `MetricTypeInfo` + metric name.
#[derive(Serialize)]
struct UpsertRecord {
    metric_name: String,
    /// Serialized as lowercase string ("count", "rate", "gauge", "ddsketch").
    metric_type: MetadataMetricType,
    /// Reporting interval in seconds. Omitted when 0 to match Go `omitempty`.
    #[serde(skip_serializing_if = "is_zero")]
    interval: i64,
}

fn is_zero(v: &i64) -> bool {
    *v == 0
}

/// Response body from metric-metadata endpoint.
#[derive(Deserialize)]
struct UpsertResponse {
    /// Names of metrics successfully upserted. May be null, empty, or a subset.
    #[serde(default)]
    succeeded_metrics: Vec<String>,
}
```

### Pattern 2: FlushClient with Owned reqwest::Client
**What:** `FlushClient` owns a `reqwest::Client` instance configured with timeout. Built once in `TransformConfig::build()`.
**When to use:** Per D-03/D-04.
**Example:**
```rust
use std::collections::HashMap;
use std::time::Duration;

/// HTTP client for flushing pending metric metadata to the SaaS endpoint.
///
/// NOTE: Debug is intentionally NOT derived -- the `api_key` field must not
/// appear in log output.
pub struct FlushClient {
    client: reqwest::Client,
    api_key: String,
    metadata_svc_url: String,
    org_id: String,
}

impl FlushClient {
    pub fn new(
        api_key: String,
        metadata_svc_url: String,
        org_id: String,
        timeout: Duration,
    ) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()?;
        Ok(Self { client, api_key, metadata_svc_url, org_id })
    }

    pub async fn flush_pending(
        &self,
        pending: &HashMap<String, MetricTypeInfo>,
    ) -> Result<Vec<String>, FlushError> {
        // ... build request, send, parse response
    }
}
```

### Pattern 3: FlushError Enum with thiserror
**What:** Typed error enum using thiserror v2, classifying failures by category for warn! log context.
**When to use:** Recommended for FlushError. thiserror v2 is available in the workspace. [VERIFIED: workspace Cargo.toml line 258]
**Example:**
```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FlushError {
    #[error("request build error: {0}")]
    RequestBuild(#[from] reqwest::Error),
    #[error("http {status}: {body}")]
    HttpStatus { status: u16, body: String },
    #[error("timeout: metadata service did not respond")]
    Timeout,
    #[error("network error: {0}")]
    Network(String),
    #[error("response parse error: {0}")]
    ResponseParse(String),
}
```

### Anti-Patterns to Avoid
- **Adding Serialize/Deserialize to MetricTypeInfo directly for wire format:** The internal type (`interval: u32`) differs from the wire type (`interval: i64`). Use dedicated serde structs.
- **Using `.unwrap()` on response parsing:** Always propagate errors via `?`.
- **Deriving Debug on FlushClient:** It holds `api_key: String` which must not appear in logs per T-01-02.
- **Constructing JSON with `serde_json::json!()` macro in production code:** Compile-time type safety is lost; field name typos become runtime bugs. Use typed serde structs.
- **Retrying on failure:** D-08 explicitly says no retry; return error and caller drops pending list.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| HTTP POST with JSON body | Manual hyper/TCP handling | `reqwest::Client::post().json(&body).header().send()` | Connection pooling, TLS, timeout, redirect handling built in [VERIFIED: reqwest docs] |
| JSON serialization | Manual string formatting | serde `#[derive(Serialize)]` + `reqwest` json feature | Type-safe, compile-checked field names, handles escaping |
| Mock HTTP server | Custom TCP listener in tests | `wiremock::MockServer` with matchers | Request matching, body inspection, response templating, auto port allocation [VERIFIED: Context7 wiremock-rs docs] |
| Error categorization | String matching on error messages | `reqwest::Error::is_timeout()` + `response.status()` | Structured error classification [VERIFIED: reqwest docs] |
| Error type boilerplate | Manual Display + Error impls | `thiserror::Error` derive macro | Reduces boilerplate, standard in workspace [VERIFIED: workspace Cargo.toml] |
| omitempty semantics | Custom serializer | `#[serde(skip_serializing_if = "...")]` | Standard serde pattern matching Go's omitempty [ASSUMED] |

**Key insight:** reqwest's `json` feature handles `Content-Type: application/json` header injection automatically when using `.json(&body)`, but we still need to explicitly set `DD-API-KEY` header. [VERIFIED: reqwest docs show `.json()` sets Content-Type]

## Common Pitfalls

### Pitfall 1: interval Field Type Mismatch
**What goes wrong:** Go uses `int64` for interval with `omitempty` (0 is omitted). Rust `MetricTypeInfo` uses `u32`. If the request struct uses `u32`, the JSON integer will be identical in most cases but the Go `omitempty` behavior means `0` is omitted from JSON entirely.
**Why it happens:** Different type widths between Go and Rust, plus Go's omitempty treating 0 as the zero value.
**How to avoid:** Use `i64` in the `UpsertRecord` serde struct with `#[serde(skip_serializing_if = "is_zero")]`. Cast `u32 -> i64` when building the request.
**Warning signs:** Tests pass but Go service logs unexpected fields; or 0-interval metrics have `"interval": 0` in JSON when Go omits it.

### Pitfall 2: Missing Content-Type Header
**What goes wrong:** Server rejects request with 400/415 because Content-Type is not set.
**Why it happens:** Using `.body()` instead of `.json()` on reqwest doesn't set Content-Type automatically.
**How to avoid:** Use `reqwest::Client::post(url).json(&body)` which automatically sets `Content-Type: application/json`. [VERIFIED: Go client explicitly sets `Content-Type: application/json`]
**Warning signs:** 400/415 responses in tests.

### Pitfall 3: succeeded_metrics Null/Missing Handling
**What goes wrong:** Response deserialization panics or returns error when `succeeded_metrics` is null or missing.
**Why it happens:** `Vec<String>` without `#[serde(default)]` requires the field to be present and non-null in JSON.
**How to avoid:** Use `#[serde(default)] succeeded_metrics: Vec<String>`. This handles: field missing -> empty vec, `null` -> empty vec (serde_json treats null as absent for `#[serde(default)]` containers), `[]` -> empty vec. [VERIFIED: Go client_test.go tests all three cases]
**Warning signs:** `ResponseParse` errors on successful 200 responses with empty or null succeeded_metrics.

### Pitfall 4: metric_type String Serialization
**What goes wrong:** `MetadataMetricType::DdSketch` serializes as `"DdSketch"` or `"dd_sketch"` instead of `"ddsketch"`.
**Why it happens:** Default serde rename_all might not produce the exact string.
**How to avoid:** `MetadataMetricType` already has `#[serde(rename_all = "lowercase")]` with explicit `#[serde(rename = "ddsketch")]` on the DdSketch variant. Reuse this type in `UpsertRecord.metric_type`. [VERIFIED: types.rs lines 24-33]
**Warning signs:** Go service doesn't recognize metric type; metrics fail to upsert.

### Pitfall 5: FlushClient Debug Leaking API Key
**What goes wrong:** `api_key` appears in log output or error messages.
**Why it happens:** Deriving `Debug` on `FlushClient` would include all fields.
**How to avoid:** Do NOT derive `Debug` on `FlushClient`. Already established pattern: `MetricMetadataTransform` intentionally skips Debug. [VERIFIED: mod.rs lines 204-205]
**Warning signs:** API key visible in test output or production logs.

### Pitfall 6: cargo-machete Flagging Unused serde_json
**What goes wrong:** `cargo machete` reports `serde_json` as unused if it's only used transitively via reqwest's `json` feature.
**Why it happens:** cargo-machete checks for direct `use` statements.
**How to avoid:** Ensure at least one file has `use serde_json` or the response deserialization explicitly uses serde_json. The `#[derive(Deserialize)]` on `UpsertResponse` should suffice since reqwest's `.json::<UpsertResponse>()` call requires serde_json. If machete still flags it, add to `[package.metadata.cargo-machete] ignored`. [ASSUMED]
**Warning signs:** `make unused-deps` fails.

## Code Examples

### Constructing the Request Body from Pending HashMap

```rust
// Source: derived from Go client/client.go SubmitMetrics() lines 41-53
fn build_request_body(
    org_id: &str,
    pending: &HashMap<String, MetricTypeInfo>,
) -> UpsertRequest {
    let records = pending
        .iter()
        .map(|(name, info)| UpsertRecord {
            metric_name: name.clone(),
            metric_type: info.metric_type,
            interval: i64::from(info.interval),
        })
        .collect();
    UpsertRequest {
        org_id: org_id.to_string(),
        records,
    }
}
```

### Full flush_pending Method

```rust
// Source: mirrors Go client/client.go SubmitMetrics() structure
pub async fn flush_pending(
    &self,
    pending: &HashMap<String, MetricTypeInfo>,
) -> Result<Vec<String>, FlushError> {
    let body = build_request_body(&self.org_id, pending);
    let url = format!(
        "{}/api/unstable/byoc/ingest/metadata/metric-metadata",
        self.metadata_svc_url
    );

    let response = self
        .client
        .post(&url)
        .header("DD-API-KEY", &self.api_key)
        .json(&body)
        .send()
        .await
        .map_err(|err| {
            if err.is_timeout() {
                FlushError::Timeout
            } else {
                FlushError::Network(err.to_string())
            }
        })?;

    let status = response.status();
    if !status.is_success() {
        let body_text = response.text().await.unwrap_or_default();
        return Err(FlushError::HttpStatus {
            status: status.as_u16(),
            body: body_text,
        });
    }

    let api_response: UpsertResponse = response
        .json()
        .await
        .map_err(|err| FlushError::ResponseParse(err.to_string()))?;

    Ok(api_response.succeeded_metrics)
}
```

### Wiremock Test Pattern (Partial Success)

```rust
// Source: pattern from quickwit-rest-client/src/rest_client.rs + wiremock Context7 docs
#[tokio::test]
async fn test_flush_partial_success() {
    let mock_server = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path(
            "/api/unstable/byoc/ingest/metadata/metric-metadata",
        ))
        .and(wiremock::matchers::header("DD-API-KEY", "test-key"))
        .and(wiremock::matchers::header("Content-Type", "application/json"))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({
                    "succeeded_metrics": ["cpu.user"]
                })),
        )
        .mount(&mock_server)
        .await;

    let client = FlushClient::new(
        "test-key".to_string(),
        mock_server.uri(),
        "org-123".to_string(),
        std::time::Duration::from_secs(5),
    )
    .expect("client build should succeed");

    let mut pending = HashMap::new();
    pending.insert("cpu.user".to_string(), MetricTypeInfo {
        metric_type: MetadataMetricType::Gauge,
        interval: 0,
    });
    pending.insert("mem.free".to_string(), MetricTypeInfo {
        metric_type: MetadataMetricType::Gauge,
        interval: 0,
    });

    let succeeded = client.flush_pending(&pending).await
        .expect("flush should succeed");

    // Only cpu.user succeeded; mem.free is NOT in the response
    assert_eq!(succeeded, vec!["cpu.user".to_string()]);
}
```

### Integration Point: Wiring FlushClient into mod.rs build()

```rust
// In TransformConfig::build(), after loading known_metrics:
let flush_client = FlushClient::new(
    api_key.clone(),
    self.metadata_svc_url.clone(),
    self.org_id.clone(),
    Duration::from_secs(self.http_timeout_secs),
)
.map_err(|err| format!("failed to build HTTP client: {err}"))?;

Ok(Transform::event_task(MetricMetadataTransform {
    config: self.clone(),
    api_key,
    known_metrics,
    pending: HashMap::new(),
    flush_client,  // NEW field
}))
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| reqwest 0.11 (hyper 0.14) | reqwest 0.12 (hyper 1.0) | Late 2023 | Workspace has both 0.11 and 0.12; pomsky-intake uses 0.12 (workspace default) [VERIFIED: cargo metadata] |
| wiremock 0.5 | wiremock 0.6 | 2024 | API largely unchanged; workspace pins 0.6.5 [VERIFIED: cargo metadata] |
| thiserror 1 | thiserror 2 | 2024 | Workspace uses v2; macro syntax unchanged for simple cases [VERIFIED: workspace Cargo.toml] |
| Manual JSON with serde_json::to_vec | reqwest .json(&body) feature | Stable since reqwest 0.9 | Sets Content-Type automatically, handles serialization [VERIFIED: reqwest docs] |

**Deprecated/outdated:**
- reqwest 0.11 pattern with `hyper::Body`: The workspace still has 0.11 for some crates, but pomsky-intake depends on the workspace default (0.12). Do not import reqwest 0.11 APIs.

## Assumptions Log

> List all claims tagged `[ASSUMED]` in this research. The planner and discuss-phase use this
> section to identify decisions that need user confirmation before execution.

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `#[serde(skip_serializing_if = "is_zero")]` is the correct Rust equivalent of Go's `omitempty` for int64 fields | Wire Format / Don't Hand-Roll | If wrong, 0-interval fields would appear in JSON when Go omits them; could cause API mismatch but SaaS likely tolerates the extra field |
| A2 | cargo-machete may flag serde_json as unused if only used transitively | Pitfall 6 | Low risk -- add to ignored list if needed |

## Open Questions (RESOLVED)

1. **Should `metric_type` omitempty be replicated for the empty-string case?**
   - What we know: Go uses `omitempty` on `metric_type` (string type), meaning empty string is omitted. In Rust, `MetadataMetricType` is an enum that always serializes to a non-empty string ("count", "rate", "gauge", "ddsketch"). So this case cannot occur.
   - What's unclear: Nothing -- this is resolved.
   - Recommendation: Do not add `skip_serializing_if` to `metric_type`. The enum always produces a value.

2. **Does reqwest's `.json()` set Content-Type even when DD-API-KEY header is added?**
   - What we know: reqwest's `.json()` calls `.header(CONTENT_TYPE, "application/json")` internally. Calling `.header("DD-API-KEY", ...)` should not override it since they are different header names.
   - What's unclear: Order-of-operations edge case.
   - Recommendation: Verify in tests with wiremock `header("Content-Type", "application/json")` matcher. If both headers present, confirmed.

## Environment Availability

Step 2.6: SKIPPED (no external dependencies identified). Phase 3 is pure Rust code with workspace dependencies. No external services, CLIs, or runtimes beyond the existing Rust toolchain are needed. Tests use wiremock (in-process mock server).

## Sources

### Primary (HIGH confidence)
- Go service source: `/Users/alan.gates/go/src/github.com/DataDog/dd-source/domains/quickhouse/apps/byoc-metrics-metadata/internal/client/` -- wire format types, HTTP client implementation, error types, comprehensive tests
- Existing Rust code: `pomsky-intake/src/transforms/metric_metadata/` -- all 4 modules read for integration points
- Workspace Cargo.toml: `quickwit/Cargo.toml` -- dependency versions and features verified (reqwest line 220, serde_json line 237, thiserror line 258, wiremock line 322)
- pomsky-intake Cargo.toml: existing dependencies confirmed
- cargo metadata output: resolved versions (reqwest 0.12.28, wiremock 0.6.5, serde_json 1.0.149)
- In-repo wiremock pattern: `quickwit-rest-client/src/rest_client.rs` lines 807-862

### Secondary (MEDIUM confidence)
- Context7 reqwest docs: POST with JSON, timeout configuration
- Context7 wiremock-rs docs: MockServer, matchers, JSON body matching, header matching

### Tertiary (LOW confidence)
- None

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all dependencies already in workspace, versions verified via cargo metadata
- Architecture: HIGH -- wire format extracted directly from Go source code with test coverage
- Pitfalls: HIGH -- derived from actual Go test cases and existing codebase patterns

**Research date:** 2026-04-20
**Valid until:** 2026-05-20 (stable -- wire format unlikely to change, dependencies are workspace-pinned)
