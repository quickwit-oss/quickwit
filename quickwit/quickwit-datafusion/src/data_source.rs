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

//! `QuickwitDataSource` — the extension point for plugging data sources
//! (metrics, logs, traces, …) into the DataFusion session layer.
//!
//! ## Protocol compatibility note
//!
//! The worker communication protocol changed in datafusion-distributed PR #375
//! (commit 556a5de) from Arrow Flight to a custom `WorkerService` gRPC.
//! Any data source that needs to run distributed queries must be built against
//! the same protocol version.  The logs data source (PR #6160) uses the
//! pre-#375 Arrow Flight API and will require a protocol update before it
//! can share a `datafusion-distributed` pin with the metrics source.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::TableProviderFactory;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SessionState, SessionStateBuilder};

/// Extension point for plugging a data source into `DataFusionSessionBuilder`.
///
/// Implement this trait for each data type (metrics, logs, traces, …) that
/// should be queryable via DataFusion SQL.
///
/// ## Lifecycle
///
/// For each session (coordinator or worker), the builder calls these methods
/// in order:
///
/// 1. [`configure_session_state_builder`] — before `SessionStateBuilder::build()`.
///    Register codecs, UDFs, physical optimizer rules, opener factories, or any
///    other extension that must be present during plan construction/serialization.
///
/// 2. `SessionStateBuilder::build()` — called by the framework.
///
/// 3. [`register_for_worker`] — after `build()`.
///    Register runtime state that is available only after the session exists
///    (e.g., pre-warm object stores in the `RuntimeEnv`).
#[async_trait]
pub trait QuickwitDataSource: Send + Sync + Debug {
    // ── DDL support (optional) ───────────────────────────────────────

    /// The string used in `STORED AS <file_type>` DDL.
    ///
    /// Return `Some("metrics")` (or similar) to enable
    /// `CREATE [OR REPLACE] EXTERNAL TABLE … STORED AS <file_type>`.
    /// Return `None` (the default) if this source does not use DDL schema
    /// declaration — for example, the logs data source resolves schemas
    /// entirely from the metastore at query time.
    ///
    /// The session registers the factory under both this value and its
    /// uppercase equivalent because DataFusion uppercases the token.
    fn file_type(&self) -> Option<&str> {
        None
    }

    /// Creates the `TableProviderFactory` that handles DDL.
    ///
    /// Only called when [`file_type`] returns `Some(_)`.
    /// The default panics — override when `file_type()` is `Some`.
    fn create_table_provider_factory(&self) -> Arc<dyn TableProviderFactory> {
        panic!(
            "QuickwitDataSource::create_table_provider_factory() called but \
             file_type() returned None for {:?}",
            self
        )
    }

    // ── Session-state configuration (pre-build) ──────────────────────

    /// Configure the `SessionStateBuilder` before it is built.
    ///
    /// Use this hook to register anything that must be present **during**
    /// plan construction or serialization:
    /// - Physical extension codecs (e.g., `with_distributed_user_codec(TantivyCodec)`)
    /// - UDFs (e.g., `full_text_udf()` for the logs data source)
    /// - Physical optimizer rules specific to this source
    /// - Opener factories or other session-config extensions
    ///
    /// Called on both coordinator sessions (`build_session()`) and worker
    /// sessions (`QuickwitWorkerSessionBuilder::build_session_state()`).
    ///
    /// Default: returns `builder` unchanged (no-op for metrics).
    fn configure_session_state_builder(
        &self,
        builder: SessionStateBuilder,
    ) -> SessionStateBuilder {
        builder
    }

    // ── Default table resolution (schema-provider path) ─────────────

    /// Create a default `TableProvider` for `index_name` without DDL.
    ///
    /// Called by `QuickwitSchemaProvider::table(name)` when no DDL-registered
    /// table matches.  Returns `Ok(None)` if this source does not own the
    /// index — the schema provider will try the next registered source.
    ///
    /// The provider may use a minimal or inferred schema.  Callers that need
    /// a precise schema should use `CREATE [OR REPLACE] EXTERNAL TABLE` DDL.
    async fn create_default_table_provider(
        &self,
        index_name: &str,
    ) -> DFResult<Option<Arc<dyn TableProvider>>>;

    // ── Worker runtime setup (post-build) ────────────────────────────

    /// Register runtime state the worker needs after the session is built.
    ///
    /// Called by `QuickwitWorkerSessionBuilder` after `SessionStateBuilder::build()`.
    /// Use this to register resources in the `RuntimeEnv` (e.g., pre-warm
    /// object stores so parquet readers can reach them without a separate
    /// lookup on every query).
    ///
    /// Default: no-op.
    async fn register_for_worker(&self, _state: &SessionState) -> DFResult<()> {
        Ok(())
    }

    // ── Index enumeration ────────────────────────────────────────────

    /// Return all index names exposed by this source.
    ///
    /// Used by `QuickwitSchemaProvider::table_names()` to populate the
    /// `SHOW TABLES` / `information_schema` view.  Sources that cannot
    /// enumerate indexes cheaply (e.g., the logs source which has
    /// potentially thousands of indexes) may return an empty `Vec`.
    async fn list_index_names(&self) -> DFResult<Vec<String>>;
}
