use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_distributed::{DistributedExt, DistributedPhysicalOptimizerRule};
use quickwit_metastore::SplitMetadata;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_proto::types::IndexUid;
use quickwit_search::SearcherPool;
use quickwit_storage::StorageResolver;
use tantivy_datafusion::{IndexOpener, TantivyCodec, full_text_udf};

use crate::catalog::QuickwitSchemaProvider;
use crate::resolver::QuickwitWorkerResolver;
use crate::split_opener::{SplitIndexOpener, SplitRegistry};
use crate::table_provider::{OpenerFactory, QuickwitTableProvider};

/// Everything needed to build a DataFusion session for a Quickwit node.
pub struct QuickwitSessionBuilder {
    metastore: MetastoreServiceClient,
    searcher_pool: SearcherPool,
    registry: Arc<SplitRegistry>,
    storage_resolver: Option<StorageResolver>,
    searcher_context: Option<Arc<quickwit_search::SearcherContext>>,
}

impl QuickwitSessionBuilder {
    pub fn new(
        metastore: MetastoreServiceClient,
        searcher_pool: SearcherPool,
        registry: Arc<SplitRegistry>,
    ) -> Self {
        Self {
            metastore,
            searcher_pool,
            registry,
            storage_resolver: None,
            searcher_context: None,
        }
    }

    /// Set the storage resolver for production split opening.
    /// When set, the catalog will create `StorageSplitOpener`s.
    pub fn with_storage(
        mut self,
        storage_resolver: StorageResolver,
        searcher_context: Arc<quickwit_search::SearcherContext>,
    ) -> Self {
        self.storage_resolver = Some(storage_resolver);
        self.searcher_context = Some(searcher_context);
        self
    }

    /// Build a `SessionContext` configured for distributed query execution.
    ///
    /// If `storage_resolver` and `searcher_context` are set, registers a
    /// [`QuickwitSchemaProvider`] that lazily resolves index names from
    /// the metastore. Otherwise (tests), tables must be registered manually.
    pub fn build_session(&self) -> SessionContext {
        let config = SessionConfig::new();
        let worker_resolver = QuickwitWorkerResolver::new(self.searcher_pool.clone());

        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_distributed_worker_resolver(worker_resolver)
            .with_distributed_user_codec(TantivyCodec)
            .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
            .build();

        let ctx = SessionContext::new_with_state(state);
        ctx.register_udf(full_text_udf());

        // If storage is available, register the Quickwit catalog so
        // that index names resolve automatically from the metastore.
        if let (Some(storage_resolver), Some(searcher_context)) =
            (&self.storage_resolver, &self.searcher_context)
        {
            let schema_provider = Arc::new(QuickwitSchemaProvider::new(
                self.metastore.clone(),
                storage_resolver.clone(),
                searcher_context.clone(),
            ));
            let catalog = Arc::new(MemoryCatalogProvider::new());
            catalog
                .register_schema("public", schema_provider)
                .expect("register quickwit schema");
            ctx.register_catalog("quickwit", catalog);
        }

        ctx
    }

    /// Register a Quickwit index as a DataFusion table (manual path).
    pub fn register_index(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        index_uid: IndexUid,
        tantivy_schema: &tantivy::schema::Schema,
    ) -> datafusion::common::Result<()> {
        let opener_factory = self.make_opener_factory(tantivy_schema);
        let provider = QuickwitTableProvider::new(
            index_uid,
            self.metastore.clone(),
            opener_factory,
            tantivy_schema,
        );
        ctx.register_table(table_name, Arc::new(provider))?;
        Ok(())
    }

    fn make_opener_factory(
        &self,
        tantivy_schema: &tantivy::schema::Schema,
    ) -> OpenerFactory {
        let registry = self.registry.clone();
        let schema = tantivy_schema.clone();
        Arc::new(move |meta: &SplitMetadata| {
            let segment_sizes = registry
                .get(&meta.split_id)
                .map(|entry| {
                    entry
                        .reader()
                        .map(|r| {
                            r.searcher()
                                .segment_readers()
                                .iter()
                                .map(|sr| sr.max_doc())
                                .collect()
                        })
                        .unwrap_or_default()
                })
                .unwrap_or_default();

            Arc::new(SplitIndexOpener::new(
                meta.split_id.clone(),
                registry.clone(),
                schema.clone(),
                segment_sizes,
            )) as Arc<dyn IndexOpener>
        })
    }
}
