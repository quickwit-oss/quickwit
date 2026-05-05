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

//! `TableProviderFactory` for metrics indexes.
//!
//! Allows callers to declare the expected schema inline in SQL:
//!
//! ```sql
//! CREATE EXTERNAL TABLE "my-metrics" (
//!     metric_name  VARCHAR NOT NULL,
//!     timestamp_secs BIGINT NOT NULL,
//!     value        DOUBLE NOT NULL,
//!     service      VARCHAR,
//!     env          VARCHAR
//! ) STORED AS metrics LOCATION 'my-metrics';
//!
//! CREATE EXTERNAL TABLE "my-sketches" (
//!     metric_name    VARCHAR NOT NULL,
//!     timestamp_secs BIGINT UNSIGNED NOT NULL,
//!     count          BIGINT UNSIGNED NOT NULL,
//!     sum            DOUBLE  NOT NULL,
//!     min            DOUBLE  NOT NULL,
//!     max            DOUBLE  NOT NULL,
//!     flags          INT UNSIGNED NOT NULL,
//!     keys           ARRAY<SMALLINT> NOT NULL,
//!     counts         ARRAY<BIGINT UNSIGNED> NOT NULL
//! ) STORED AS sketches LOCATION 'my-sketches';
//! ```

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::arrow;
use datafusion::catalog::{Session, TableProviderFactory};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::CreateExternalTable;
use quickwit_parquet_engine::split::ParquetSplitKind;

use super::index_resolver::MetricsIndexResolver;
use super::table_provider::MetricsTableProvider;

/// The file type string used in `STORED AS metrics`.
pub const METRICS_FILE_TYPE: &str = "metrics";
/// The file type string used in `STORED AS sketches`.
pub const SKETCHES_FILE_TYPE: &str = "sketches";

/// Creates `MetricsTableProvider` instances from `CREATE EXTERNAL TABLE` DDL.
#[derive(Debug)]
pub struct MetricsTableProviderFactory {
    index_resolver: Arc<dyn MetricsIndexResolver>,
    split_kind: ParquetSplitKind,
}

impl MetricsTableProviderFactory {
    pub fn new(
        index_resolver: Arc<dyn MetricsIndexResolver>,
        split_kind: ParquetSplitKind,
    ) -> Self {
        Self {
            index_resolver,
            split_kind,
        }
    }
}

#[async_trait]
impl TableProviderFactory for MetricsTableProviderFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> DFResult<Arc<dyn datafusion::datasource::TableProvider>> {
        let index_name = if cmd.location.is_empty() {
            cmd.name.table().to_string()
        } else {
            cmd.location.clone()
        };

        let (split_provider, index_uri) = self
            .index_resolver
            .resolve(&index_name, self.split_kind)
            .await?;

        let arrow_schema: SchemaRef = Arc::new(cmd.schema.as_arrow().clone());

        if arrow_schema.fields().is_empty() {
            return Err(DataFusionError::Plan(format!(
                "CREATE EXTERNAL TABLE '{index_name}' must declare at least one column"
            )));
        }

        let provider = MetricsTableProvider::new(arrow_schema, split_provider, index_uri)?;

        Ok(Arc::new(provider))
    }
}
