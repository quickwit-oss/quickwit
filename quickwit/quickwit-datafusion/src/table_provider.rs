use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use quickwit_metastore::{
    ListSplitsQuery, ListSplitsRequestExt, MetastoreServiceStreamSplitsExt, SplitMetadata,
    SplitState,
};
use quickwit_proto::metastore::{
    ListSplitsRequest, MetastoreService, MetastoreServiceClient,
};
use quickwit_proto::types::IndexUid;
use tantivy_datafusion::{IndexOpener, TantivyTableProvider};
use tokio::sync::Mutex;

/// Factory that creates an [`IndexOpener`] from split metadata.
///
/// Called at scan time for each split discovered from the metastore.
/// The returned opener defers actual index opening to execution time.
pub type OpenerFactory =
    Arc<dyn Fn(&SplitMetadata) -> Arc<dyn IndexOpener> + Send + Sync>;

/// A DataFusion table provider backed by a Quickwit index.
///
/// At scan time, queries the metastore for published splits, creates
/// an [`IndexOpener`] per split via the provided factory, builds
/// per-split tantivy-df providers, and unions them.
pub struct QuickwitTableProvider {
    index_uid: IndexUid,
    metastore: Mutex<MetastoreServiceClient>,
    opener_factory: OpenerFactory,
    arrow_schema: SchemaRef,
}

impl QuickwitTableProvider {
    pub fn new(
        index_uid: IndexUid,
        metastore: MetastoreServiceClient,
        opener_factory: OpenerFactory,
        tantivy_schema: &tantivy::schema::Schema,
    ) -> Self {
        let arrow_schema = tantivy_datafusion::tantivy_schema_to_arrow(tantivy_schema);
        Self {
            index_uid,
            metastore: Mutex::new(metastore),
            opener_factory,
            arrow_schema,
        }
    }

    /// List published splits from the metastore.
    async fn list_splits(&self) -> Result<Vec<SplitMetadata>> {
        let metastore = self.metastore.lock().await;
        let query = ListSplitsQuery::for_index(self.index_uid.clone())
            .with_split_state(SplitState::Published);

        let request = ListSplitsRequest::try_from_list_splits_query(&query)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let splits = metastore
            .list_splits(request)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .collect_splits_metadata()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(splits)
    }
}

impl std::fmt::Debug for QuickwitTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuickwitTableProvider")
            .field("index_uid", &self.index_uid)
            .field("arrow_schema", &self.arrow_schema)
            .finish()
    }
}

#[async_trait]
impl TableProvider for QuickwitTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let splits = self.list_splits().await?;

        if splits.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "no published splits found for index {}",
                self.index_uid
            )));
        }

        let mut execs = Vec::with_capacity(splits.len());
        for split_meta in &splits {
            let opener = (self.opener_factory)(split_meta);
            let provider = TantivyTableProvider::from_opener(opener);
            let exec = provider.scan(state, projection, filters, limit).await?;
            execs.push(exec);
        }

        if execs.len() == 1 {
            return Ok(execs.into_iter().next().unwrap());
        }
        UnionExec::try_new(execs)
    }
}
