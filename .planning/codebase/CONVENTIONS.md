# Coding Conventions

**Analysis Date:** 2026-01-22

## Naming Patterns

**Files:**
- Lowercase snake_case: `rate_limiter.rs`, `doc_uid.rs`, `syslog_processor.rs`
- Test modules: tests contained within source file using `#[cfg(test)]` or separate test files in `tests/` directory
- Cargo workspace members use hyphenated names: `quickwit-common`, `quickwit-proto`, `quickwit-indexing`

**Functions:**
- Lowercase snake_case: `from_settings()`, `acquire_permits()`, `convert_tags()`, `create_or_purge_directory()`
- Constructor methods typically named `new()` or `from_*()`: `new()`, `from_settings()`, `from_test()`, `for_test()`
- Query/getter methods prefixed with verb: `available_permits()`, `next_doc_uid()`, `find_available_tcp_port()`
- Builder pattern methods: chainable methods return `Self` for fluent API (e.g., `connect_timeout()`, `timeout()`, `search_timeout()`)

**Variables:**
- Lowercase snake_case for local variables and fields: `available_permits`, `max_capacity`, `refill_period`, `base_url`
- Struct fields use snake_case: `burst_limit`, `rate_limit`, `refill_period`
- Constants use UPPER_SNAKE_CASE: `DEFAULT_BASE_URL`, `DEFAULT_CONTENT_TYPE`, `INGEST_CONTENT_LENGTH_LIMIT`, `QW_ERROR_HEADER_NAME`

**Types:**
- PascalCase for struct names: `RateLimiter`, `RateLimiterSettings`, `DocUid`, `DocUidGenerator`, `TagKV`, `Transport`, `QuickwitClientBuilder`
- PascalCase for trait names and enum variants: `ServiceError`, `AlreadyExists`, `BadRequest`, `NotFound`
- Newtype wrappers use PascalCase: `DocUid(Ulid)` wrapping underlying type

## Code Style

**Formatting:**
- rustfmt configured in `quickwit/rustfmt.toml`
- Key settings:
  - `comment_width = 120` - comment width limit
  - `format_strings = true` - format string literals
  - `group_imports = "StdExternalCrate"` - group std, external, then crate imports
  - `imports_granularity = "Module"` - group at module level
  - `normalize_comments = false` - don't normalize comment syntax
  - `where_single_line = true` - where clauses on single line if short
  - `wrap_comments = true` - wrap comments at width
  - Codegen files explicitly ignored: `ignore = ["**/codegen/**/*.rs"]`

**Linting:**
- Clippy lint `disallowed_methods` is denied at crate level: `#![deny(clippy::disallowed_methods)]` in `lib.rs`
- Selective allows applied: `#![allow(clippy::derive_partial_eq_without_eq)]`, `#![allow(rustdoc::invalid_html_tags)]`
- Individual method allows: `#[allow(clippy::unwrap_or_default)]` on specific functions when justified with comment

**Edition:**
- Rust edition 2024 (workspace configuration: `edition = "2024"`)
- MSRV managed at workspace level

## Import Organization

**Order:**
1. Standard library: `use std::collections::HashMap;`, `use std::time::{Duration, Instant};`
2. External crates: `use anyhow::...;`, `use bytes::Bytes;`, `use serde::{...};`
3. Local crate modules: `use crate::...;`
4. Test imports: `use super::*;` in test modules

**Path Aliases:**
- No wildcard aliases observed
- Full paths used: `use mockall::Sequence;`, `use wiremock::matchers;`
- Grouped imports when importing multiple items: `use quickwit_serve::{ListSplitsQueryParams, ListSplitsResponse, RestIngestResponse};`
- Re-exports use `pub use` when appropriate: `pub use quickwit_ingest::CommitType;`

**Example from `quickwit-rest-client/src/rest_client.rs`:**
```rust
use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use quickwit_cluster::ClusterSnapshot;
use quickwit_config::{ConfigFormat, SourceConfig};
use quickwit_indexing::actors::IndexingServiceCounters;
pub use quickwit_ingest::CommitType;
use quickwit_metastore::{IndexMetadata, Split, SplitInfo};
use quickwit_proto::ingest::Shard;
use quickwit_serve::{
    ListSplitsQueryParams, ListSplitsResponse, RestIngestResponse, SearchRequestQueryString,
};
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue};
use reqwest::tls::Certificate;
use reqwest::{ClientBuilder as ReqwestClientBuilder, Method, StatusCode, Url};

use crate::BatchLineReader;
use crate::error::Error;
use crate::models::{ApiResponse, IngestSource, SearchResponseRestClient, Timeout};
```

## Error Handling

**Patterns:**
- `Result<T, E>` return types used throughout all fallible operations
- Primary error handling approaches:
  1. `anyhow::Result<T>` for internal functions with context chaining via `anyhow::Context`
  2. Custom error enums implementing `ServiceError` trait for public APIs
  3. `io::Result<T>` for I/O operations (`std::io::Result`)
  4. `Result<(), Duration>` for rate limiter acquisition failures when duration matters

**Standard error handling:**
- Custom errors derive from `thiserror`: `#[derive(thiserror::Error, Debug)]`
- Service error codes enum in `quickwit-proto/src/error.rs` maps internal errors to gRPC status codes
- Context wrapping: `map_err(|error| Error::Custom(format!("message: {error}")))?`
- ServiceError trait provides error code mapping to HTTP and gRPC status codes

**Examples from codebase:**
- `pub fn acquire_with_duration(&mut self, num_permits: u64) -> Result<(), Duration>` - rate limiter returns Duration for wait time
- Serde deserialization: `Ulid::from_string(&doc_uid_str).map_err(D::Error::custom)?`
- File operations: `pub fn named_temp_child(&self, prefix: &str) -> io::Result<TempDirectory>`
- Async operations: `pub async fn send<Q: Serialize + ?Sized>(...) -> Result<ApiResponse, Error>`

## Logging

**Framework:** `tracing` crate for structured logging

**Usage patterns:**
- Explicit imports: `use tracing::{info, warn, error, debug, instrument};`
- Rate-limited tracing module for frequently called paths: `quickwit_common::rate_limited_tracing`
- Metrics integration: `metrics` crate for counters and gauges
- Instrumentation attribute macro: `#[instrument(skip(...))]` for async functions

**Datadog integration:**
- OpenTelemetry support: `opentelemetry`, `tracing-opentelemetry` for distributed tracing
- Metrics exporter: `metrics-exporter-dogstatsd` for Datadog DogStatsD protocol
- Automatic correlation: OpenTelemetry span context propagation

## Comments

**When to Comment:**
- Doc comments (`///`) for all public functions, structs, and traits
- Implementation comments (`//`) for complex algorithms or non-obvious logic
- Workaround comments: explain "why" not "what" (e.g., "Clippy insists on...", "This is needed because...")
- Field-level comments for non-obvious struct fields

**RustDoc/Documentation:**
- Triple-slash doc comments (`///`) on all public items
- Multiline docs for complex types:
  ```rust
  /// A bursty token-based rate limiter.
  ///
  /// Accumulates "credits" during inactivity up to burst limit.
  #[derive(Debug, Clone)]
  pub struct RateLimiter { ... }
  ```
- Parameter descriptions integrated into function doc:
  ```rust
  /// Acquires some permits from the rate limiter.
  /// If the permits are not available, returns the duration to wait before trying again.
  ///
  /// This method is currently only used in simian.
  pub fn acquire_with_duration(&mut self, num_permits: u64) -> Result<(), Duration>
  ```
- Struct field comments using `//` inline:
  ```rust
  pub struct RateLimiterSettings {
      // After a long period of inactivity, the rate limiter can accumulate some "credits"
      // up to what we call a `burst_limit`.
      pub burst_limit: u64,
      pub rate_limit: ConstantRate,
  }
  ```

## Function Design

**Size:** Functions kept reasonably sized (50-150 lines typical for non-trivial operations)

**Parameters:**
- Builder pattern for complex initialization: `QuickwitClientBuilder::new(endpoint)` with chainable methods
- Generic parameters constrained with trait bounds: `<Q: Serialize + ?Sized>`, `<D: Deserializer<'de>>`
- Ownership: parameters take `&self`, `&mut self`, or owned values based on needs
- Async traits: `async_trait` macro for trait methods that are async

**Return Values:**
- Public functions return `Result<T>` (often using anyhow) or `Result<T, CustomError>`
- Private functions may return `Option<T>` for fallible non-error cases
- Async functions return `impl Future<Output = Result<T, E>>`
- Builder methods return `Self` for chaining

## Module Design

**Exports:**
- Workspace-level `Cargo.toml` defines shared dependencies and configuration
- Individual crate `lib.rs` exports public modules explicitly:
  ```rust
  pub mod binary_heap;
  mod cpus;
  pub mod dd_metrics;
  pub mod fs;
  #[cfg(feature = "jemalloc-profiled")]
  pub mod jemalloc_profiled;
  ```
- Private modules use `mod` without `pub` keyword
- Re-exports use `pub use`: `pub use coolid::new_coolid;`, `pub use kill_switch::KillSwitch;`

**Barrel Files:**
- Not extensively used
- Simple modules exported directly from `lib.rs`
- Test utilities exported conditionally: `#[cfg(any(test, feature = "testsuite"))] pub mod test_utils;`

**Feature Flags:**
- Test-specific code gated: `#[cfg(any(test, feature = "testsuite"))]` for test utilities and Default implementations
- Features defined in Cargo.toml examples:
  - `testsuite = ["mockall", "futures"]` in quickwit-proto
  - `named_tasks = ["tokio/tracing"]` in quickwit-common
  - `jemalloc-profiled` with optional dependencies

**Module Structure (typical pattern):**
```rust
// quickwit-common/src/lib.rs
#![deny(clippy::disallowed_methods)]

mod cpus;
pub mod dd_metrics;
pub mod fs;
mod kill_switch;
pub mod metrics;
pub mod net;

pub use kill_switch::KillSwitch;
pub use cpus::num_cpus;
```

**Copyright Headers:**
- All source files include Apache 2.0 header:
  ```rust
  // Copyright 2021-Present Datadog, Inc.
  //
  // Licensed under the Apache License, Version 2.0 (the "License");
  // you may not use this file except in compliance with the License.
  // You may obtain a copy of the License at
  //
  //     http://www.apache.org/licenses/LICENSE-2.0
  //
  // Unless required by applicable law or agreed to in writing, software
  // distributed under the License is distributed on an "AS IS" BASIS,
  // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  // See the License for the specific language governing permissions and
  // limitations under the License.
  ```

---

*Convention analysis: 2026-01-22*
