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

//! Narrow DataFusion extension traits used by Quickwit.
//!
//! The native OSS DataFusion integration points are:
//! - `RuntimeEnv`
//! - `SessionStateBuilder`
//! - `CatalogProviderList` / `CatalogProvider` / `SchemaProvider`
//! - `TableProviderFactory`
//! - `SubstraitConsumer`
//!
//! This module keeps Quickwit-specific abstractions aligned with those native
//! hooks by splitting runtime/session registration from Substrait read
//! interception. Catalog and schema registration stay in the native DataFusion
//! provider interfaces rather than on a broad "data source" trait.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow;
use datafusion::catalog::TableProviderFactory;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::ScalarUDF;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::prelude::SessionConfig;

type SessionConfigSetter = Arc<dyn Fn(&mut SessionConfig) + Send + Sync>;

/// Additive runtime/session registration emitted by a
/// [`QuickwitRuntimePlugin`].
///
/// This is the runtime-scoped analogue of `dd-datafusion`'s additive planner
/// registration. It carries the values that are native to the OSS
/// `SessionStateBuilder` API: config setters, UDFs, optimizer rules, and DDL
/// table factories.
#[derive(Default)]
pub struct QuickwitRuntimeRegistration {
    session_config_setters: Vec<SessionConfigSetter>,
    physical_optimizer_rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
    udfs: Vec<Arc<ScalarUDF>>,
    table_factories: Vec<(String, Arc<dyn TableProviderFactory>)>,
}

impl QuickwitRuntimeRegistration {
    pub fn with_session_config_setter(
        mut self,
        setter: impl Fn(&mut SessionConfig) + Send + Sync + 'static,
    ) -> Self {
        self.session_config_setters.push(Arc::new(setter));
        self
    }

    pub fn with_udf(mut self, udf: Arc<ScalarUDF>) -> Self {
        self.udfs.push(udf);
        self
    }

    pub fn with_physical_optimizer_rule(
        mut self,
        rule: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
    ) -> Self {
        self.physical_optimizer_rules.push(rule);
        self
    }

    pub fn with_table_factory(
        mut self,
        key: impl Into<String>,
        factory: Arc<dyn TableProviderFactory>,
    ) -> Self {
        self.table_factories.push((key.into(), factory));
        self
    }

    pub(crate) fn udf_names(&self) -> Vec<String> {
        self.udfs.iter().map(|udf| udf.name().to_string()).collect()
    }

    pub fn apply_to_config(&self, config: &mut SessionConfig) {
        for setter in &self.session_config_setters {
            setter(config);
        }
    }

    pub fn apply_to_builder(self, mut builder: SessionStateBuilder) -> SessionStateBuilder {
        for rule in self.physical_optimizer_rules {
            builder = builder.with_physical_optimizer_rule(rule);
        }

        if !self.udfs.is_empty() {
            builder
                .scalar_functions()
                .get_or_insert_default()
                .extend(self.udfs);
        }

        for (key, factory) in self.table_factories {
            builder = builder.with_table_factory(key.clone(), Arc::clone(&factory));
            let upper = key.to_uppercase();
            if upper != key {
                builder = builder.with_table_factory(upper, factory);
            }
        }

        builder
    }

    pub fn merge(&mut self, other: QuickwitRuntimeRegistration) {
        self.session_config_setters
            .extend(other.session_config_setters);
        self.physical_optimizer_rules
            .extend(other.physical_optimizer_rules);
        self.udfs.extend(other.udfs);
        self.table_factories.extend(other.table_factories);
    }
}

/// Runtime/session registration hook for Quickwit-specific functionality.
///
/// Implement this trait for components that need to:
/// - initialize the shared `RuntimeEnv`
/// - contribute native `SessionStateBuilder` state
/// - perform post-build worker setup
#[async_trait]
pub trait QuickwitRuntimePlugin: Send + Sync + Debug {
    /// Called once when the plugin is registered on a
    /// [`crate::session::DataFusionSessionBuilder`].
    fn init(&self, _env: &datafusion::execution::runtime_env::RuntimeEnv) {}

    /// Return this plugin's additive runtime/session registration.
    fn registration(&self) -> QuickwitRuntimeRegistration {
        QuickwitRuntimeRegistration::default()
    }

    /// Register runtime state the worker needs after the session is built.
    async fn register_for_worker(
        &self,
        _state: &datafusion::execution::SessionState,
    ) -> DFResult<()> {
        Ok(())
    }
}

/// Optional Substrait extension hook mirroring `dd-datafusion`'s
/// `SubstraitConsumerExt`.
#[async_trait]
pub trait QuickwitSubstraitConsumerExt: Send + Sync + Debug {
    /// Try to handle a Substrait `ReadRel`.
    ///
    /// Returning `Ok(Some((table_name, provider)))` claims the relation. The
    /// caller will rewrite any `ExtensionTable` to a `NamedTable` with that
    /// effective name so that the standard `from_read_rel` path can still apply
    /// filter and projection handling.
    async fn try_consume_read_rel(
        &self,
        _rel: &datafusion_substrait::substrait::proto::ReadRel,
        _schema_hint: Option<arrow::datatypes::SchemaRef>,
    ) -> DFResult<Option<(String, Arc<dyn TableProvider>)>> {
        Ok(None)
    }
}
