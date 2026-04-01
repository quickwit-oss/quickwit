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
//! ## Design: contribution-return pattern
//!
//! Each data source **returns its additive contributions** via [`contributions()`].
//! The [`DataFusionSessionBuilder`][crate::session::DataFusionSessionBuilder] accumulates
//! contributions from all registered sources before building any session.  This mirrors
//! the pattern in `dd-datafusion/runtime/src/connector.rs` where `Connector::init()`
//! returns a `DDDataFusionQueryPlanner` that the runtime merges across all connectors.
//!
//! Advantages over a builder-mutation chain (`configure_session(builder) -> builder`):
//! - **No silent overwrite**: two sources registering different codecs both win.
//! - **Inspectable**: the `DataSourceContributions` struct is a plain value — easy
//!   to test and introspect without constructing a full `SessionStateBuilder`.
//! - **Conflict detection**: the session builder can validate (e.g., no two sources
//!   register the same UDF name) before building the session.
//!
//! ## Lifecycle
//!
//! For each session (coordinator or worker):
//!
//! 1. **`contributions()`** — called once. Returns optimizer rules, codecs, UDFs,
//!    and object stores to register on the `RuntimeEnv`.  Applied before
//!    `SessionStateBuilder::build()`.
//!
//! 2. `SessionStateBuilder::build()` — called by the framework.
//!
//! 3. **`register_for_worker(&SessionState)`** — called after `build()` for
//!    runtime state that requires the session to already exist (rare; prefer
//!    `contributions()` for most things).
//!
//! ## Protocol compatibility note
//!
//! The worker communication protocol changed in datafusion-distributed PR #375
//! (commit 556a5de) from Arrow Flight to a custom `WorkerService` gRPC protocol.
//! Any data source that needs distributed execution must be built against the same
//! protocol version as the coordinator.  The logs data source (PR #6160) was written
//! against the pre-#375 Arrow Flight API and will require a protocol update before
//! it can share a `datafusion-distributed` pin with the metrics source.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::TableProviderFactory;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::logical_expr::ScalarUDF;
use datafusion::physical_optimizer::PhysicalOptimizerRule;

/// Additive contributions from a [`QuickwitDataSource`] to the DataFusion session.
///
/// Returned by [`QuickwitDataSource::contributions()`] and aggregated across all
/// registered sources before any session is built.
///
/// Analogous to `DDDataFusionQueryPlanner` in `dd-datafusion`, which accumulates
/// extension planners, rules, and UDFs from every registered `Connector`.
///
/// ## Codec registration
///
/// Physical extension codecs (e.g. `TantivyCodec` for the logs data source) are
/// applied via [`DataSourceContributions::apply_to_builder`] using the
/// `with_distributed_user_codec` builder extension from `datafusion_distributed`.
/// If your source needs a codec, call it inside the `codec_applier` callback:
///
/// ```ignore
/// fn contributions(&self) -> DataSourceContributions {
///     DataSourceContributions::default()
///         .with_codec_applier(|builder| {
///             builder.with_distributed_user_codec(TantivyCodec)
///         })
/// }
/// ```
pub struct DataSourceContributions {
    /// Physical optimizer rules contributed by this source.
    ///
    /// Logs adds tantivy-specific pushdown rules here.
    /// Metrics adds nothing — DataFusion's built-in parquet pushdown is sufficient.
    pub physical_optimizer_rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,

    /// Scalar UDFs contributed by this source.
    ///
    /// Logs adds `full_text_udf()` here.
    /// Metrics adds nothing.
    pub udfs: Vec<Arc<ScalarUDF>>,

    /// Callbacks that apply codec / builder extensions that cannot be expressed
    /// as plain values (e.g. `with_distributed_user_codec(TantivyCodec)`).
    ///
    /// Applied to the `SessionStateBuilder` after rules and UDFs are merged.
    /// Using callbacks avoids a direct dependency on `datafusion-proto` types.
    codec_appliers: Vec<Arc<dyn Fn(SessionStateBuilder) -> SessionStateBuilder + Send + Sync>>,
}

impl Default for DataSourceContributions {
    fn default() -> Self {
        Self {
            physical_optimizer_rules: Vec::new(),
            udfs: Vec::new(),
            codec_appliers: Vec::new(),
        }
    }
}

impl DataSourceContributions {
    /// Add a codec / builder-extension callback.
    ///
    /// Logs uses this to call `.with_distributed_user_codec(TantivyCodec)`.
    pub fn with_codec_applier(
        mut self,
        f: impl Fn(SessionStateBuilder) -> SessionStateBuilder + Send + Sync + 'static,
    ) -> Self {
        self.codec_appliers.push(Arc::new(f));
        self
    }

    /// Apply all contributions to a `SessionStateBuilder`.
    ///
    /// Called by `DataFusionSessionBuilder` and `QuickwitWorkerSessionBuilder`
    /// after merging contributions from all sources.
    pub fn apply_to_builder(self, mut builder: SessionStateBuilder) -> SessionStateBuilder {
        for rule in self.physical_optimizer_rules {
            builder = builder.with_physical_optimizer_rule(rule);
        }
        // UDFs are added via the scalar_functions map on the builder
        // Currently DataFusion's SessionStateBuilder doesn't expose a direct
        // `with_scalar_udf` batch method; callers can add UDFs after build
        // via `ctx.register_udf()`.  We store them for that purpose.
        // Codec appliers (e.g. with_distributed_user_codec) run last.
        for applier in self.codec_appliers {
            builder = applier(builder);
        }
        builder
    }

    /// Drain the accumulated UDFs (to be registered on `SessionContext` post-build).
    pub fn take_udfs(&mut self) -> Vec<Arc<ScalarUDF>> {
        std::mem::take(&mut self.udfs)
    }

    /// Merge another set of contributions into this one (additive, no dedup).
    ///
    /// Used by `DataFusionSessionBuilder` to accumulate across all sources.
    pub fn merge(&mut self, other: DataSourceContributions) {
        self.physical_optimizer_rules.extend(other.physical_optimizer_rules);
        self.udfs.extend(other.udfs);
        self.codec_appliers.extend(other.codec_appliers);
    }
}

/// Extension point for plugging a data source into `DataFusionSessionBuilder`.
///
/// Implement this trait for each data type (metrics, logs, traces, …) that
/// should be queryable via DataFusion SQL.
#[async_trait]
pub trait QuickwitDataSource: Send + Sync + Debug {
    // ── Additive session contributions ──────────────────────────────

    /// Return this source's additive contributions to every session.
    ///
    /// Called once per `build_session()` / worker `build_session_state()` call.
    /// Contributions from all registered sources are merged and applied to the
    /// `SessionStateBuilder` before `build()` is called.
    ///
    /// Default: no contributions (metrics, for example, needs none).
    fn contributions(&self) -> DataSourceContributions {
        DataSourceContributions::default()
    }

    // ── DDL support (optional) ───────────────────────────────────────

    /// The string used in `STORED AS <file_type>` DDL.
    ///
    /// Return `Some("metrics")` to enable
    /// `CREATE [OR REPLACE] EXTERNAL TABLE … STORED AS metrics`.
    /// Return `None` (the default) if this source resolves tables purely through
    /// the schema provider — for example, the logs data source looks up the index
    /// schema from the metastore at query time and needs no DDL.
    ///
    /// The session registers the factory under both the literal and its uppercase
    /// equivalent because DataFusion uppercases the `STORED AS` token.
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

    // ── Default table resolution (schema-provider path) ─────────────

    /// Create a default `TableProvider` for `index_name` without DDL.
    ///
    /// Called by `QuickwitSchemaProvider::table(name)` when no DDL-registered
    /// table matches.  Returns `Ok(None)` if this source does not own the
    /// index — the schema provider will try the next registered source.
    async fn create_default_table_provider(
        &self,
        index_name: &str,
    ) -> DFResult<Option<Arc<dyn TableProvider>>>;

    // ── Worker runtime setup (post-build, optional) ──────────────────

    /// Register runtime state the worker needs after the session is built.
    ///
    /// Called after `SessionStateBuilder::build()`.  Use this for resources
    /// that can only be registered on an existing `SessionState` (e.g.,
    /// object stores in the `RuntimeEnv` that depend on lazily-discovered
    /// index URIs).
    ///
    /// For resources that are known at construction time, prefer registering
    /// them in `contributions()` — or directly on the `RuntimeEnv` passed to
    /// the session builder (analogous to `BlobStoreConnector::init(env)`).
    ///
    /// Default: no-op.
    async fn register_for_worker(&self, _state: &SessionState) -> DFResult<()> {
        Ok(())
    }

    // ── Index enumeration ────────────────────────────────────────────

    /// Return all index names exposed by this source.
    ///
    /// Used by `QuickwitSchemaProvider::table_names()` for `SHOW TABLES` /
    /// `information_schema`.  Sources that cannot enumerate cheaply may
    /// return an empty `Vec` (the logs data source does this — it would need
    /// to list potentially thousands of indexes).
    async fn list_index_names(&self) -> DFResult<Vec<String>>;
}
