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

//! Generic DataFusion catalog / schema provider.
//!
//! `QuickwitSchemaProvider` routes table resolution to whichever registered
//! `QuickwitDataSource` claims to own the index.  It knows nothing about
//! metrics, logs, or traces — those concerns live in each data source.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{MemorySchemaProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;

use crate::data_source::QuickwitDataSource;

/// DataFusion `SchemaProvider` that delegates table resolution to the
/// registered `QuickwitDataSource` implementations.
///
/// Resolution order for `table(name)`:
/// 1. Explicitly registered tables (from `CREATE EXTERNAL TABLE` DDL) — backed
///    by DataFusion's own [`MemorySchemaProvider`] which uses a lock-free
///    `DashMap` internally, the idiomatic choice for this role.
/// 2. Each source's `create_default_table_provider`, first non-None wins.
///
/// `register_table` / `deregister_table` delegate directly to the inner
/// `MemorySchemaProvider`, so `CREATE OR REPLACE EXTERNAL TABLE` works
/// correctly without any custom locking.
pub struct QuickwitSchemaProvider {
    sources: Vec<Arc<dyn QuickwitDataSource>>,
    /// DDL-registered tables (CREATE OR REPLACE EXTERNAL TABLE).
    /// Uses DataFusion's MemorySchemaProvider which is backed by DashMap —
    /// lock-free, concurrent-read-safe, and the standard DataFusion idiom.
    ddl_tables: MemorySchemaProvider,
}

impl QuickwitSchemaProvider {
    pub fn new(sources: Vec<Arc<dyn QuickwitDataSource>>) -> Self {
        Self {
            sources,
            ddl_tables: MemorySchemaProvider::new(),
        }
    }
}

impl std::fmt::Debug for QuickwitSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuickwitSchemaProvider")
            .field("num_sources", &self.sources.len())
            .field("num_ddl_tables", &self.ddl_tables.table_names().len())
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for QuickwitSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Lists all index names across all sources.
    ///
    /// `table_names()` is a sync DataFusion API, but enumerating sources is
    /// async. This uses `block_in_place`, which requires a multi-threaded
    /// Tokio runtime. Only called for `SHOW TABLES` / `information_schema`;
    /// not on the query hot path.
    fn table_names(&self) -> Vec<String> {
        let sources = &self.sources;
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut names = Vec::new();
                for source in sources {
                    if let Ok(mut source_names) = source.list_index_names().await {
                        names.append(&mut source_names);
                    }
                }
                // Deduplicate in case multiple sources claim the same name.
                names.dedup();
                names
            })
        })
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        // Resolution order:
        // 1. DDL-registered tables (CREATE OR REPLACE EXTERNAL TABLE)
        // 2. Each source's create_default_table_provider — first non-None wins.
        //    We do not pre-validate via table_names(); sources return None for
        //    unknown names and DataFusion emits "table not found". Avoids N+1.
        if let Some(provider) = self.ddl_tables.table(name).await? {
            return Ok(Some(provider));
        }

        for source in &self.sources {
            if let Some(provider) = source.create_default_table_provider(name).await? {
                return Ok(Some(provider));
            }
        }

        Ok(None)
    }

    /// Returns `true` if the table is present in the DDL cache.
    ///
    /// DataFusion's contract: `false` does not prevent `table()` from
    /// returning `Some`; it is a hint only. Checking only DDL tables keeps
    /// this method allocation-free and off the async hot path.
    fn table_exist(&self, name: &str) -> bool {
        self.ddl_tables.table_exist(name)
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        self.ddl_tables.register_table(name, table)
    }

    fn deregister_table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        self.ddl_tables.deregister_table(name)
    }
}
