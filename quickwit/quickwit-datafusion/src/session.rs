use std::sync::Arc;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_distributed::{DistributedExt, DistributedPhysicalOptimizerRule};
use quickwit_metastore::SplitMetadata;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_proto::types::IndexUid;
use quickwit_search::SearcherPool;
use tantivy_datafusion::{IndexOpener, OpenerMetadata, TantivyCodec, full_text_udf};

use crate::resolver::QuickwitWorkerResolver;
use crate::split_opener::{SplitIndexOpener, SplitRegistry};
use crate::table_provider::{OpenerFactory, QuickwitTableProvider};

/// Everything needed to build a DataFusion session for a Quickwit node.
pub struct QuickwitSessionBuilder {
    metastore: MetastoreServiceClient,
    searcher_pool: SearcherPool,
    registry: Arc<SplitRegistry>,
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
        }
    }

    /// Build a `SessionContext` configured for distributed query execution.
    ///
    /// The context has:
    /// - `TantivyCodec` (stateless) for serializing tantivy-df nodes
    /// - `QuickwitWorkerResolver` for discovering searcher nodes
    /// - `full_text()` UDF registered
    ///
    /// The opener factory is NOT set here — it lives on each worker's
    /// session config (set in `build_flight_service`). The coordinator
    /// doesn't need an opener factory because it never opens indexes.
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
        ctx
    }

    /// Register a Quickwit index as a DataFusion table.
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
