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
//!    extension planners, and UDAFs to register.  Applied before
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
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};

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
    physical_optimizer_rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,

    /// Scalar UDFs contributed by this source.
    ///
    /// Logs adds `full_text_udf()` here.
    /// Metrics adds nothing.
    udfs: Vec<Arc<ScalarUDF>>,

    /// Aggregate UDFs (UDAFs) contributed by this source.
    ///
    /// Logs adds histogram UDAFs here.
    /// Metrics adds nothing.
    udafs: Vec<Arc<AggregateUDF>>,

    /// Extension planners contributed by this source.
    ///
    /// Logs adds the `TantivyExec` extension planner here so that the worker
    /// can execute plan fragments containing `TantivyExec` nodes.
    /// Metrics adds nothing.
    extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,

    /// Callbacks that apply codec / builder extensions that cannot be expressed
    /// as plain values (e.g. `with_distributed_user_codec(TantivyCodec)`).
    ///
    /// Applied to the `SessionStateBuilder` after rules and UDFs are merged.
    /// Using callbacks avoids a direct dependency on `datafusion-proto` types.
    ///
    /// These are `FnOnce` because `SessionStateBuilder` is consumed and returned;
    /// each applier can only run once.
    codec_appliers: Vec<Box<dyn FnOnce(SessionStateBuilder) -> SessionStateBuilder + Send + Sync>>,
}

impl Default for DataSourceContributions {
    fn default() -> Self {
        Self {
            physical_optimizer_rules: Vec::new(),
            udfs: Vec::new(),
            udafs: Vec::new(),
            extension_planners: Vec::new(),
            codec_appliers: Vec::new(),
        }
    }
}

impl DataSourceContributions {
    /// Add a physical optimizer rule.
    pub fn with_physical_optimizer_rule(
        mut self,
        rule: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
    ) -> Self {
        self.physical_optimizer_rules.push(rule);
        self
    }

    /// Add a scalar UDF.
    pub fn with_udf(mut self, udf: Arc<ScalarUDF>) -> Self {
        self.udfs.push(udf);
        self
    }

    /// Add multiple scalar UDFs at once.
    pub fn with_udf_batch(mut self, udfs: impl IntoIterator<Item = Arc<ScalarUDF>>) -> Self {
        self.udfs.extend(udfs);
        self
    }

    /// Add an aggregate UDF (UDAF).
    pub fn with_udaf(mut self, udaf: Arc<AggregateUDF>) -> Self {
        self.udafs.push(udaf);
        self
    }

    /// Add multiple aggregate UDFs (UDAFs) at once.
    pub fn with_udaf_batch(mut self, udafs: impl IntoIterator<Item = Arc<AggregateUDF>>) -> Self {
        self.udafs.extend(udafs);
        self
    }

    /// Add an extension planner.
    ///
    /// Extension planners are needed when the logical plan contains
    /// `LogicalPlan::Extension` nodes (e.g. `TantivyExec`).  Both the
    /// coordinator and workers must install the planner so that extension nodes
    /// can be translated to physical plans at execution time.
    pub fn with_extension_planner(
        mut self,
        planner: Arc<dyn ExtensionPlanner + Send + Sync>,
    ) -> Self {
        self.extension_planners.push(planner);
        self
    }

    /// Add a codec / builder-extension callback.
    ///
    /// Logs uses this to call `.with_distributed_user_codec(TantivyCodec)`.
    pub fn with_codec_applier(
        mut self,
        f: impl FnOnce(SessionStateBuilder) -> SessionStateBuilder + Send + Sync + 'static,
    ) -> Self {
        self.codec_appliers.push(Box::new(f));
        self
    }

    /// Returns the names of all registered scalar UDFs.
    ///
    /// Used by `DataFusionSessionBuilder::check_invariants()` to detect
    /// conflicting UDF registrations across sources.
    pub(crate) fn udaf_names(&self) -> Vec<String> {
        self.udafs.iter().map(|udaf| udaf.name().to_string()).collect()
    }

    pub(crate) fn udf_names(&self) -> Vec<String> {
        self.udfs.iter().map(|udf| udf.name().to_string()).collect()
    }

    /// Apply all contributions to a `SessionStateBuilder`.
    ///
    /// Called by `DataFusionSessionBuilder` and `QuickwitWorkerSessionBuilder`
    /// after merging contributions from all sources.
    ///
    /// Injects in order:
    /// 1. Physical optimizer rules
    /// 2. Extension planners (via a `ContributionQueryPlanner` wrapper if non-empty)
    /// 3. Scalar UDFs (into the builder's scalar function map)
    /// 4. Aggregate UDFs (into the builder's aggregate function map)
    /// 5. Codec appliers (consumed in order)
    pub fn apply_to_builder(self, mut builder: SessionStateBuilder) -> SessionStateBuilder {
        for rule in self.physical_optimizer_rules {
            builder = builder.with_physical_optimizer_rule(rule);
        }

        if !self.extension_planners.is_empty() {
            let planner = ContributionQueryPlanner {
                extension_planners: self.extension_planners,
            };
            builder = builder.with_query_planner(Arc::new(planner));
        }

        if !self.udfs.is_empty() {
            builder
                .scalar_functions()
                .get_or_insert_default()
                .extend(self.udfs);
        }

        if !self.udafs.is_empty() {
            builder
                .aggregate_functions()
                .get_or_insert_default()
                .extend(self.udafs);
        }

        for applier in self.codec_appliers {
            builder = applier(builder);
        }

        builder
    }

    /// Merge another set of contributions into this one (additive, no dedup).
    ///
    /// Used by `DataFusionSessionBuilder` to accumulate across all sources.
    pub fn merge(&mut self, other: DataSourceContributions) {
        self.physical_optimizer_rules
            .extend(other.physical_optimizer_rules);
        self.udfs.extend(other.udfs);
        self.udafs.extend(other.udafs);
        self.extension_planners.extend(other.extension_planners);
        self.codec_appliers.extend(other.codec_appliers);
    }
}

/// A `QueryPlanner` that delegates extension node planning to a set of
/// `ExtensionPlanner` implementations contributed by data sources.
///
/// Installed by `DataSourceContributions::apply_to_builder` when at least one
/// source contributes an extension planner.  Uses
/// `DefaultPhysicalPlanner::with_extension_planners` to handle the extension
/// nodes, falling back to DataFusion's default planner for all other nodes.
struct ContributionQueryPlanner {
    extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
}

impl std::fmt::Debug for ContributionQueryPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContributionQueryPlanner")
            .field("num_extension_planners", &self.extension_planners.len())
            .finish()
    }
}

#[async_trait]
impl datafusion::execution::context::QueryPlanner for ContributionQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &datafusion::logical_expr::LogicalPlan,
        session_state: &datafusion::execution::SessionState,
    ) -> DFResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        DefaultPhysicalPlanner::with_extension_planners(self.extension_planners.clone())
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

/// Extension point for plugging a data source into `DataFusionSessionBuilder`.
///
/// Implement this trait for each data type (metrics, logs, traces, …) that
/// should be queryable via DataFusion SQL.
#[async_trait]
pub trait QuickwitDataSource: Send + Sync + Debug {
    // ── Startup hook ─────────────────────────────────────────────────

    /// Called once when the source is registered via
    /// `DataFusionSessionBuilder::with_source()`.
    ///
    /// Receives the shared `RuntimeEnv` that all sessions built by this builder
    /// will use.  Sources that know their object-store URLs at construction time
    /// should register them here — analogous to `BlobStoreConnector::init` in
    /// `dd-datafusion`, which calls `env.register_object_store(url, store)` once
    /// at service startup so that every query can reach the store without any
    /// per-session registration.
    ///
    /// Sources whose URLs are only discoverable at query time (e.g. metrics,
    /// where indexes are listed from the metastore) should leave this as a no-op
    /// and perform lazy registration in `MetricsTableProvider::scan()`, which
    /// writes into the same shared `RuntimeEnv`.
    ///
    /// Default: no-op.
    fn init(&self, _env: &datafusion::execution::runtime_env::RuntimeEnv) {}

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

    /// Return the DDL file-type token and its `TableProviderFactory` together,
    /// or `None` if this source does not support DDL.
    ///
    /// When `Some((token, factory))` is returned:
    /// - `token` is the string used in `STORED AS <token>` DDL (e.g. `"metrics"`).
    /// - `factory` handles `CREATE [OR REPLACE] EXTERNAL TABLE … STORED AS <token>`.
    ///
    /// The session registers the factory under both the literal token and its
    /// uppercase equivalent because DataFusion uppercases the `STORED AS` token.
    ///
    /// Returning both pieces from a single method prevents the mismatch bug where
    /// `file_type()` and `create_table_provider_factory()` could disagree or
    /// create two different factory instances.
    ///
    /// Return `None` (the default) if this source resolves tables purely through
    /// the schema provider — for example, the logs data source looks up the index
    /// schema from the metastore at query time and needs no DDL.
    fn ddl_registration(&self) -> Option<(String, Arc<dyn TableProviderFactory>)> {
        None
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
    async fn register_for_worker(&self, _state: &datafusion::execution::SessionState) -> DFResult<()> {
        Ok(())
    }

    // ── Index enumeration ────────────────────────────────────────────

    /// Return all index names exposed by this source.
    ///
    /// Used by `QuickwitSchemaProvider::table_names()` for `SHOW TABLES` /
    /// `information_schema`.  Sources that cannot enumerate cheaply may
    /// return an empty `Vec` (the logs data source does this — it would need
    /// to list potentially thousands of indexes).
    ///
    /// # Threading note
    ///
    /// This method may be called from within a `tokio::task::block_in_place`
    /// context on the DataFusion query thread.  Implementations that call
    /// blocking I/O must ensure they are not already inside a `block_in_place`
    /// context (tokio panics on nested `block_in_place`).  If in doubt, use
    /// `tokio::task::spawn_blocking` or check
    /// `tokio::runtime::Handle::try_current()` before blocking.
    async fn list_index_names(&self) -> DFResult<Vec<String>>;
}
