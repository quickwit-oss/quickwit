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

//! Substrait plan consumption for Quickwit data sources.
//!
//! ## How this fits together
//!
//! [`QuickwitSubstraitConsumer`] implements the `SubstraitConsumer` trait from
//! `datafusion-substrait`.  It intercepts `ReadRel` nodes in an incoming
//! Substrait plan, routes them to whichever registered [`QuickwitDataSource`]
//! claims them, and falls back to the standard catalog lookup for everything
//! else.
//!
//! ## OSS path — standard Substrait (no custom protos)
//!
//! A producer targeting Quickwit OSS sends a completely vanilla Substrait plan:
//!
//! ```text
//! ReadRel {
//!     base_schema: <NamedStruct describing projected columns>,
//!     read_type: NamedTable { names: ["<index_name>"] },
//! }
//! ```
//!
//! [`MetricsDataSource`][crate::sources::metrics::MetricsDataSource] handles
//! this by resolving the index from the metastore and creating a
//! `MetricsTableProvider` with the schema declared in `base_schema`.  No
//! custom protobuf type or type URL is involved.
//!
//! ## Extension path — custom protos (Pomsky)
//!
//! Pomsky registers its own `QuickwitDataSource` implementation that decodes
//! DD-internal protos (e.g. `ExtensionTable<MetricRead>`).  The OSS code
//! simply calls the hook; the proto decoding stays in Pomsky.
//!
//! ## Entry point
//!
//! [`DataFusionSessionBuilder::execute_substrait`][crate::session::DataFusionSessionBuilder::execute_substrait]
//! builds a `QuickwitSubstraitConsumer` from the session state and sources,
//! converts the plan via `from_substrait_plan_with_consumer`, then executes it.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::common::TableReference;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{FunctionRegistry, SendableRecordBatchStream, SessionState};
use datafusion::logical_expr::LogicalPlan;
use datafusion_substrait::extensions::Extensions;
use datafusion_substrait::logical_plan::consumer::{
    SubstraitConsumer, from_read_rel, from_substrait_named_struct,
    from_substrait_plan_with_consumer,
};
use datafusion_substrait::substrait::proto::{
    Plan, ReadRel,
    read_rel::{ReadType, NamedTable as SubstraitNamedTable},
};

use crate::data_source::QuickwitDataSource;

/// `SubstraitConsumer` that routes `ReadRel` nodes to registered
/// [`QuickwitDataSource`]s before falling back to the standard catalog path.
///
/// Constructed by [`DataFusionSessionBuilder::execute_substrait`].
pub struct QuickwitSubstraitConsumer<'a> {
    extensions: &'a Extensions,
    state: &'a SessionState,
    sources: &'a [Arc<dyn QuickwitDataSource>],
}

impl<'a> QuickwitSubstraitConsumer<'a> {
    pub fn new(
        extensions: &'a Extensions,
        state: &'a SessionState,
        sources: &'a [Arc<dyn QuickwitDataSource>],
    ) -> Self {
        Self { extensions, state, sources }
    }
}

#[async_trait]
impl SubstraitConsumer for QuickwitSubstraitConsumer<'_> {
    // ── Required boilerplate ─────────────────────────────────────────

    /// Resolve a table reference via the quickwit catalog
    /// (`quickwit.public.<name>`).
    async fn resolve_table_ref(
        &self,
        table_ref: &TableReference,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let schema = self.state.schema_for_ref(table_ref.clone())?;
        schema.table(table_ref.table()).await
    }

    fn get_extensions(&self) -> &Extensions {
        self.extensions
    }

    fn get_function_registry(&self) -> &impl FunctionRegistry {
        self.state
    }

    // ── Custom ReadRel handling ───────────────────────────────────────

    /// Intercept `ReadRel` nodes and offer them to each registered source.
    ///
    /// 1. Convert `ReadRel.base_schema` → Arrow `SchemaRef` (the schema hint
    ///    the producer declared; sources use this for schema injection rather
    ///    than the minimal default).
    /// 2. Call each source's `try_consume_read_rel`.  The first source that
    ///    returns `Some((table_name, provider))` wins.
    /// 3. If a source claims the rel, build a temporary resolver that returns
    ///    the provider when `from_read_rel` performs its catalog lookup.
    ///    If the original rel used `ExtensionTable`, rewrite it to `NamedTable`
    ///    so `from_read_rel` can apply the standard filter/projection handling.
    /// 4. If no source claims the rel, fall through to the default path which
    ///    uses `resolve_table_ref` → quickwit catalog → `QuickwitSchemaProvider`.
    async fn consume_read(&self, rel: &ReadRel) -> DFResult<LogicalPlan> {
        // Convert base_schema to Arrow once so every source can use it without
        // re-parsing the Substrait types.
        let schema_hint: Option<SchemaRef> = if let Some(ns) = &rel.base_schema {
            Some(Arc::clone(from_substrait_named_struct(self, ns)?.inner()))
        } else {
            None
        };

        for source in self.sources {
            if let Some((table_name, provider)) =
                source.try_consume_read_rel(rel, schema_hint.clone()).await?
            {
                // Build a short-lived resolver that returns our provider for
                // this table name.  Everything else (filters, projections,
                // schema coercion) is handled by `from_read_rel`.
                let resolver = WithCustomProvider {
                    extensions: self.extensions,
                    state: self.state,
                    table_name: table_name.clone(),
                    provider: Arc::clone(&provider),
                };

                // If the rel uses ExtensionTable (custom proto), rewrite it to
                // NamedTable so `from_read_rel` resolves it via `resolve_table_ref`.
                let effective_rel = if matches!(rel.read_type, Some(ReadType::ExtensionTable(_))) {
                    let mut r = rel.clone();
                    r.read_type = Some(ReadType::NamedTable(SubstraitNamedTable {
                        names: vec![table_name],
                        ..Default::default()
                    }));
                    r
                } else {
                    rel.clone()
                };

                return from_read_rel(&resolver, &effective_rel).await;
            }
        }

        // No source claimed this rel — use the standard path (catalog lookup).
        from_read_rel(self, rel).await
    }
}

/// Short-lived `SubstraitConsumer` that overrides `resolve_table_ref` to
/// return a specific pre-built `TableProvider` for one table name, then
/// delegates everything else to the outer consumer's session/extensions.
///
/// Used by `QuickwitSubstraitConsumer::consume_read` so that `from_read_rel`
/// applies standard filter/projection handling against our custom provider.
struct WithCustomProvider<'a> {
    extensions: &'a Extensions,
    state: &'a SessionState,
    table_name: String,
    provider: Arc<dyn TableProvider>,
}

#[async_trait]
impl SubstraitConsumer for WithCustomProvider<'_> {
    async fn resolve_table_ref(
        &self,
        table_ref: &TableReference,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        if table_ref.table() == self.table_name.as_str() {
            return Ok(Some(Arc::clone(&self.provider)));
        }
        // Fall back to catalog for anything else
        let schema = self.state.schema_for_ref(table_ref.clone())?;
        schema.table(table_ref.table()).await
    }

    fn get_extensions(&self) -> &Extensions {
        self.extensions
    }

    fn get_function_registry(&self) -> &impl FunctionRegistry {
        self.state
    }
}

/// Convert a Substrait plan to batches using the registered data sources.
///
/// This is the entry point for external coordinators that send Substrait plans
/// to Quickwit.  It is called by
/// [`DataFusionSessionBuilder::execute_substrait`].
/// Convert a Substrait plan to batches.
///
/// Takes the full `SessionContext` (not just state) so that catalog
/// registrations made by `build_session()` — including the `quickwit.public`
/// schema provider — are visible during both plan conversion and execution.
/// Creating a fresh `SessionContext::new_with_state(state.clone())` loses
/// those registrations because `register_catalog` lives on the context, not
/// the state snapshot.
pub async fn execute_substrait_plan(
    plan: &Plan,
    ctx: &datafusion::prelude::SessionContext,
    sources: &[Arc<dyn QuickwitDataSource>],
) -> DFResult<Vec<arrow::array::RecordBatch>> {
    let state = ctx.state();
    let extensions = Extensions::try_from(&plan.extensions)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let consumer = QuickwitSubstraitConsumer::new(&extensions, &state, sources);
    let logical_plan = from_substrait_plan_with_consumer(&consumer, plan).await?;

    tracing::debug!(
        plan = %logical_plan.display_indent(),
        "substrait plan converted to DataFusion logical plan"
    );

    let df = ctx.execute_logical_plan(logical_plan).await?;
    let batches = df.collect().await?;
    tracing::debug!(num_batches = batches.len(), "substrait plan executed");
    Ok(batches)
}

/// Convert a Substrait plan to a streaming `RecordBatch` iterator.
///
/// Unlike [`execute_substrait_plan`], this function does **not** collect all
/// results into memory — it returns a [`SendableRecordBatchStream`] that the
/// caller can poll lazily.  This is the preferred path for gRPC streaming
/// responses and Arrow Flight handlers.
///
/// Takes the full `SessionContext` for the same reasons as
/// `execute_substrait_plan` — catalog registrations live on the context, not
/// the state snapshot.
pub async fn execute_substrait_plan_streaming(
    plan: &Plan,
    ctx: &datafusion::prelude::SessionContext,
    sources: &[Arc<dyn QuickwitDataSource>],
) -> DFResult<SendableRecordBatchStream> {
    let state = ctx.state();
    let extensions = Extensions::try_from(&plan.extensions)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let consumer = QuickwitSubstraitConsumer::new(&extensions, &state, sources);
    let logical_plan = from_substrait_plan_with_consumer(&consumer, plan).await?;

    tracing::debug!(
        plan = %logical_plan.display_indent(),
        "substrait plan converted to DataFusion logical plan for streaming execution"
    );

    let df = ctx.execute_logical_plan(logical_plan).await?;
    let stream = df.execute_stream().await?;
    Ok(stream)
}
