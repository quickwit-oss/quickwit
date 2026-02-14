//! A DataFusion [`SchemaProvider`] that lazily resolves Quickwit
//! indexes from the metastore.
//!
//! When DataFusion encounters a table name it doesn't know, the
//! catalog calls the metastore to look up the index, builds a
//! [`QuickwitTableProvider`], and returns it. No manual
//! `ctx.register_table()` needed.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::common::DataFusionError;
use datafusion::datasource::TableProvider;
use quickwit_config::build_doc_mapper;
use quickwit_metastore::{IndexMetadata, IndexMetadataResponseExt};
use quickwit_proto::metastore::{
    IndexMetadataRequest, MetastoreService, MetastoreServiceClient,
};
use quickwit_storage::StorageResolver;
use tantivy_datafusion::IndexOpener;
use tokio::sync::Mutex;

use crate::split_opener::StorageSplitOpener;
use crate::table_provider::{OpenerFactory, QuickwitTableProvider};

/// A [`SchemaProvider`] backed by the Quickwit metastore.
///
/// When DataFusion queries for a table by name, this provider treats
/// the name as a Quickwit index ID, fetches its metadata from the
/// metastore, and returns a [`QuickwitTableProvider`] that discovers
/// splits at scan time.
pub struct QuickwitSchemaProvider {
    metastore: Mutex<MetastoreServiceClient>,
    storage_resolver: StorageResolver,
    searcher_context: Arc<quickwit_search::SearcherContext>,
}

impl QuickwitSchemaProvider {
    pub fn new(
        metastore: MetastoreServiceClient,
        storage_resolver: StorageResolver,
        searcher_context: Arc<quickwit_search::SearcherContext>,
    ) -> Self {
        Self {
            metastore: Mutex::new(metastore),
            storage_resolver,
            searcher_context,
        }
    }

    async fn resolve_index(
        &self,
        index_id: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let metastore = self.metastore.lock().await;

        // Fetch index metadata.
        let request = IndexMetadataRequest::for_index_id(index_id.to_string());
        let index_metadata: IndexMetadata = match metastore
            .clone()
            .index_metadata(request)
            .await
        {
            Ok(response) => response
                .deserialize_index_metadata()
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
            Err(_) => return Ok(None), // index doesn't exist
        };

        let index_uid = index_metadata.index_uid.clone();
        let index_config = &index_metadata.index_config;

        // Build the doc mapper to get the tantivy schema and tokenizer manager.
        let doc_mapper = build_doc_mapper(
            &index_config.doc_mapping,
            &index_config.search_settings,
        )
        .map_err(|e| DataFusionError::Internal(format!("build doc mapper: {e}")))?;

        let tantivy_schema = doc_mapper.schema();
        let tokenizer_manager = doc_mapper.tokenizer_manager().clone();
        let storage_resolver = self.storage_resolver.clone();
        let searcher_context = self.searcher_context.clone();

        // Resolve the index storage URI.
        let storage = storage_resolver
            .resolve(&index_config.index_uri)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Build the opener factory that creates StorageSplitOpeners.
        let schema_for_factory = tantivy_schema.clone();
        let opener_factory: OpenerFactory = Arc::new(move |split_meta| {
            Arc::new(
                StorageSplitOpener::new(
                    split_meta.split_id.clone(),
                    schema_for_factory.clone(),
                    vec![], // segment sizes discovered at open time
                    searcher_context.clone(),
                    storage.clone(),
                    split_meta.footer_offsets.start,
                    split_meta.footer_offsets.end,
                )
                .with_tokenizer_manager(tokenizer_manager.clone()),
            ) as Arc<dyn IndexOpener>
        });

        let provider = QuickwitTableProvider::new(
            index_uid,
            metastore.clone(),
            opener_factory,
            &tantivy_schema,
        );

        Ok(Some(Arc::new(provider)))
    }
}

impl std::fmt::Debug for QuickwitSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuickwitSchemaProvider").finish()
    }
}

#[async_trait]
impl SchemaProvider for QuickwitSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// We can't enumerate all indexes cheaply. Return empty.
    /// Users must know their index names.
    fn table_names(&self) -> Vec<String> {
        Vec::new()
    }

    /// Lazily resolve a Quickwit index by name.
    async fn table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        self.resolve_index(name).await
    }

    fn table_exist(&self, _name: &str) -> bool {
        // We can't check synchronously. Return true and let table()
        // return None if it doesn't exist.
        true
    }
}
