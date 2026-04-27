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

//! Generic DataFusion session builder.
//!
//! ## Runtime environment lifecycle
//!
//! `DataFusionSessionBuilder` maintains a shared `Arc<RuntimeEnv>` for every
//! session it builds. A shared `RuntimeEnv` lets object stores registered at
//! service-startup time be visible to all queries without any per-query
//! re-registration.
//!
//! ## Native OSS registration
//!
//! Catalogs, schemas, and DDL table factories are wired through the native OSS
//! `SessionStateBuilder` APIs:
//! - `with_catalog_list(...)`
//! - `with_table_factory(...)`
//!
//! Schema and catalog providers are created fresh per session so DDL state
//! remains session-local.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::catalog::{
    CatalogProvider, CatalogProviderList, MemoryCatalogProvider, MemoryCatalogProviderList,
    SchemaProvider,
};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::object_store::ObjectStoreRegistry;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_distributed::{
    DistributedExt, DistributedPhysicalOptimizerRule, TaskEstimator, WorkerResolver,
};

use crate::data_source::{
    QuickwitRuntimePlugin, QuickwitRuntimeRegistration, QuickwitSubstraitConsumerExt,
};
use crate::task_estimator::DataSourceExecPartitionEstimator;

type CatalogProviderFactory = Arc<dyn Fn() -> Arc<dyn CatalogProvider> + Send + Sync>;
type SchemaProviderFactory = Arc<dyn Fn() -> Arc<dyn SchemaProvider> + Send + Sync>;

#[derive(Clone)]
pub(crate) struct CatalogRegistration {
    name: String,
    factory: CatalogProviderFactory,
}

#[derive(Clone)]
pub(crate) struct SchemaRegistration {
    catalog_name: String,
    schema_name: String,
    factory: SchemaProviderFactory,
}

/// Builds `SessionContext`s for DataFusion queries over registered runtime
/// plugins, catalogs, and Substrait extensions.
pub struct DataFusionSessionBuilder {
    runtime_plugins: Vec<Arc<dyn QuickwitRuntimePlugin>>,
    substrait_extensions: Vec<Arc<dyn QuickwitSubstraitConsumerExt>>,
    catalog_registrations: Vec<CatalogRegistration>,
    schema_registrations: Vec<SchemaRegistration>,
    worker_resolver: Option<Arc<dyn WorkerResolver + Send + Sync>>,
    task_estimator: Arc<dyn TaskEstimator + Send + Sync>,
    memory_pool: Option<Arc<dyn MemoryPool>>,
    object_store_registry: Option<Arc<dyn ObjectStoreRegistry>>,
    runtime: Arc<RuntimeEnv>,
}

impl std::fmt::Debug for DataFusionSessionBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFusionSessionBuilder")
            .field("num_runtime_plugins", &self.runtime_plugins.len())
            .field("num_substrait_extensions", &self.substrait_extensions.len())
            .field("num_catalogs", &self.catalog_registrations.len())
            .field("num_schemas", &self.schema_registrations.len())
            .field("distributed", &self.worker_resolver.is_some())
            .finish()
    }
}

impl Default for DataFusionSessionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl DataFusionSessionBuilder {
    pub fn new() -> Self {
        Self {
            runtime_plugins: Vec::new(),
            substrait_extensions: Vec::new(),
            catalog_registrations: Vec::new(),
            schema_registrations: Vec::new(),
            worker_resolver: None,
            task_estimator: Arc::new(DataSourceExecPartitionEstimator),
            memory_pool: None,
            object_store_registry: None,
            runtime: Arc::new(RuntimeEnv::default()),
        }
    }

    fn rebuild_runtime(&mut self) -> DFResult<()> {
        let mut builder = RuntimeEnvBuilder::new();
        if let Some(memory_pool) = &self.memory_pool {
            builder = builder.with_memory_pool(Arc::clone(memory_pool));
        }
        if let Some(object_store_registry) = &self.object_store_registry {
            builder = builder.with_object_store_registry(Arc::clone(object_store_registry));
        }
        let runtime = builder.build_arc()?;
        for plugin in &self.runtime_plugins {
            plugin.init(&runtime);
        }
        self.runtime = runtime;
        Ok(())
    }

    pub(crate) fn merged_runtime_registration(&self) -> QuickwitRuntimeRegistration {
        let mut combined = QuickwitRuntimeRegistration::default();
        for plugin in &self.runtime_plugins {
            combined.merge(plugin.registration());
        }
        combined
    }

    pub(crate) fn build_catalog_list(&self) -> DFResult<Arc<dyn CatalogProviderList>> {
        let catalog_list = Arc::new(MemoryCatalogProviderList::new());

        for registration in &self.catalog_registrations {
            catalog_list.register_catalog(registration.name.clone(), (registration.factory)());
        }

        for registration in &self.schema_registrations {
            let catalog = if let Some(existing) = catalog_list.catalog(&registration.catalog_name) {
                existing
            } else {
                let catalog: Arc<dyn CatalogProvider> = Arc::new(MemoryCatalogProvider::new());
                catalog_list
                    .register_catalog(registration.catalog_name.clone(), Arc::clone(&catalog));
                catalog
            };

            catalog
                .register_schema(&registration.schema_name, (registration.factory)())
                .map_err(|e| {
                    DataFusionError::Internal(format!(
                        "failed to register schema '{}.{}': {e}",
                        registration.catalog_name, registration.schema_name
                    ))
                })?;
        }

        Ok(catalog_list as Arc<dyn CatalogProviderList>)
    }

    pub fn with_memory_limit(mut self, bytes: usize) -> DFResult<Self> {
        self.memory_pool = Some(Arc::new(GreedyMemoryPool::new(bytes)));
        self.rebuild_runtime()?;
        Ok(self)
    }

    pub fn with_object_store_registry(
        mut self,
        registry: Arc<dyn ObjectStoreRegistry>,
    ) -> DFResult<Self> {
        self.object_store_registry = Some(registry);
        self.rebuild_runtime()?;
        Ok(self)
    }

    pub fn with_runtime_plugin(mut self, plugin: Arc<dyn QuickwitRuntimePlugin>) -> Self {
        plugin.init(&self.runtime);
        self.runtime_plugins.push(plugin);
        self
    }

    pub fn with_substrait_consumer(
        mut self,
        extension: Arc<dyn QuickwitSubstraitConsumerExt>,
    ) -> Self {
        self.substrait_extensions.push(extension);
        self
    }

    pub fn with_catalog_provider_factory(
        mut self,
        catalog_name: impl Into<String>,
        factory: impl Fn() -> Arc<dyn CatalogProvider> + Send + Sync + 'static,
    ) -> Self {
        self.catalog_registrations.push(CatalogRegistration {
            name: catalog_name.into(),
            factory: Arc::new(factory),
        });
        self
    }

    pub fn with_schema_provider_factory(
        mut self,
        catalog_name: impl Into<String>,
        schema_name: impl Into<String>,
        factory: impl Fn() -> Arc<dyn SchemaProvider> + Send + Sync + 'static,
    ) -> Self {
        self.schema_registrations.push(SchemaRegistration {
            catalog_name: catalog_name.into(),
            schema_name: schema_name.into(),
            factory: Arc::new(factory),
        });
        self
    }

    pub fn with_worker_resolver(
        mut self,
        resolver: impl WorkerResolver + Send + Sync + 'static,
    ) -> Self {
        self.worker_resolver = Some(Arc::new(resolver));
        self
    }

    pub fn with_task_estimator(
        mut self,
        estimator: impl TaskEstimator + Send + Sync + 'static,
    ) -> Self {
        self.task_estimator = Arc::new(estimator);
        self
    }

    pub fn runtime(&self) -> &Arc<RuntimeEnv> {
        &self.runtime
    }

    pub fn runtime_plugins(&self) -> &[Arc<dyn QuickwitRuntimePlugin>] {
        &self.runtime_plugins
    }

    pub(crate) fn substrait_extensions(&self) -> &[Arc<dyn QuickwitSubstraitConsumerExt>] {
        &self.substrait_extensions
    }

    pub fn check_invariants(&self) -> DFResult<()> {
        let mut seen_udfs: HashSet<String> = HashSet::new();
        for plugin in &self.runtime_plugins {
            let registration = plugin.registration();
            for name in registration.udf_names() {
                if !seen_udfs.insert(name.clone()) {
                    return Err(DataFusionError::Configuration(format!(
                        "two runtime plugins both register a scalar UDF named '{name}'"
                    )));
                }
            }
        }
        Ok(())
    }

    pub async fn execute_substrait(
        &self,
        plan_bytes: &[u8],
    ) -> DFResult<datafusion::physical_plan::SendableRecordBatchStream> {
        use datafusion_substrait::substrait::proto::Plan;
        use prost::Message;

        let plan = Plan::decode(plan_bytes).map_err(|e| DataFusionError::External(Box::new(e)))?;

        let ctx = self.build_session()?;
        crate::substrait::execute_substrait_plan_streaming(&plan, &ctx, self.substrait_extensions())
            .await
    }

    pub fn build_session(&self) -> DFResult<SessionContext> {
        self.build_session_with_properties(&HashMap::new())
    }

    pub fn build_session_with_properties(
        &self,
        properties: &HashMap<String, String>,
    ) -> DFResult<SessionContext> {
        let registration = self.merged_runtime_registration();
        let mut config = SessionConfig::new();
        config.options_mut().catalog.default_catalog = "quickwit".to_string();
        config.options_mut().catalog.default_schema = "public".to_string();
        config.options_mut().catalog.information_schema = true;
        config
            .options_mut()
            .catalog
            .create_default_catalog_and_schema = false;
        registration.apply_to_config(&mut config);

        for (key, value) in properties {
            config.options_mut().set(key, value)?;
        }

        let mut builder = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_runtime_env(Arc::clone(&self.runtime))
            .with_catalog_list(self.build_catalog_list()?);

        builder = registration.apply_to_builder(builder);

        if let Some(resolver) = &self.worker_resolver {
            builder = builder
                .with_distributed_worker_resolver(ArcWorkerResolver(Arc::clone(resolver)))
                .with_distributed_task_estimator(ArcTaskEstimator(Arc::clone(&self.task_estimator)))
                .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule));
        }

        Ok(SessionContext::new_with_state(builder.build()))
    }
}

struct ArcWorkerResolver(Arc<dyn WorkerResolver + Send + Sync>);

impl WorkerResolver for ArcWorkerResolver {
    fn get_urls(&self) -> Result<Vec<url::Url>, DataFusionError> {
        self.0.get_urls()
    }
}

struct ArcTaskEstimator(Arc<dyn TaskEstimator + Send + Sync>);

impl std::fmt::Debug for ArcTaskEstimator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArcTaskEstimator").finish_non_exhaustive()
    }
}

impl TaskEstimator for ArcTaskEstimator {
    fn task_estimation(
        &self,
        plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        cfg: &datafusion::config::ConfigOptions,
    ) -> Option<datafusion_distributed::TaskEstimation> {
        self.0.task_estimation(plan, cfg)
    }

    fn scale_up_leaf_node(
        &self,
        plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        task_count: usize,
        cfg: &datafusion::config::ConfigOptions,
    ) -> Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        self.0.scale_up_leaf_node(plan, task_count, cfg)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use datafusion::execution::object_store::DefaultObjectStoreRegistry;
    use object_store::memory::InMemory;
    use url::Url;

    use super::*;

    #[derive(Debug)]
    struct TestSource {
        init_calls: Arc<AtomicUsize>,
        source_url: Url,
        batch_size: usize,
    }

    struct UrlLookup<'a>(&'a Url);

    impl AsRef<Url> for UrlLookup<'_> {
        fn as_ref(&self) -> &Url {
            self.0
        }
    }

    #[async_trait]
    impl QuickwitRuntimePlugin for TestSource {
        fn init(&self, env: &RuntimeEnv) {
            self.init_calls.fetch_add(1, Ordering::SeqCst);
            env.register_object_store(&self.source_url, Arc::new(InMemory::new()));
        }

        fn registration(&self) -> crate::data_source::QuickwitRuntimeRegistration {
            let batch_size = self.batch_size;
            crate::data_source::QuickwitRuntimeRegistration::default().with_session_config_setter(
                move |config| {
                    config.options_mut().execution.batch_size = batch_size;
                },
            )
        }
    }

    #[test]
    fn runtime_settings_compose_and_reinitialize_sources() {
        let source_url = Url::parse("test://source").unwrap();
        let registry_url = Url::parse("memory://registry").unwrap();
        let registry = Arc::new(DefaultObjectStoreRegistry::new());
        registry.register_store(&registry_url, Arc::new(InMemory::new()));

        let init_calls = Arc::new(AtomicUsize::new(0));
        let source = Arc::new(TestSource {
            init_calls: Arc::clone(&init_calls),
            source_url: source_url.clone(),
            batch_size: 512,
        });

        let builder = DataFusionSessionBuilder::new()
            .with_runtime_plugin(source)
            .with_object_store_registry(registry)
            .unwrap()
            .with_memory_limit(2048)
            .unwrap();

        assert_eq!(init_calls.load(Ordering::SeqCst), 3);
        assert_eq!(
            builder
                .runtime()
                .config_entries()
                .into_iter()
                .find(|entry| entry.key == "datafusion.runtime.memory_limit")
                .and_then(|entry| entry.value),
            Some("2K".to_string())
        );
        assert!(
            builder
                .runtime()
                .object_store(UrlLookup(&source_url))
                .is_ok()
        );
        assert!(
            builder
                .runtime()
                .object_store(UrlLookup(&registry_url))
                .is_ok()
        );
    }

    #[test]
    fn session_config_setters_are_additive_and_overridable() {
        let source = Arc::new(TestSource {
            init_calls: Arc::new(AtomicUsize::new(0)),
            source_url: Url::parse("test://source").unwrap(),
            batch_size: 512,
        });
        let builder = DataFusionSessionBuilder::new().with_runtime_plugin(source);

        let default_ctx = builder.build_session().unwrap();
        assert_eq!(
            default_ctx.state().config().options().execution.batch_size,
            512
        );

        let overrides = HashMap::from([(
            "datafusion.execution.batch_size".to_string(),
            "1024".to_string(),
        )]);
        let overridden_ctx = builder.build_session_with_properties(&overrides).unwrap();
        assert_eq!(
            overridden_ctx
                .state()
                .config()
                .options()
                .execution
                .batch_size,
            1024
        );
    }
}
