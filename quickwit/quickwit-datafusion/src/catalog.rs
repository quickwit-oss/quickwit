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
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;

use crate::data_source::QuickwitDataSource;

/// DataFusion `SchemaProvider` that delegates table resolution to the
/// registered `QuickwitDataSource` implementations.
///
/// Resolution order for `table(name)`:
/// 1. Explicitly registered tables (from `CREATE EXTERNAL TABLE` DDL)
/// 2. Each source's `create_default_table_provider`, first non-None wins
///
/// `register_table` / `deregister_table` are implemented so that
/// `CREATE OR REPLACE EXTERNAL TABLE` works correctly.
pub struct QuickwitSchemaProvider {
    sources: Vec<Arc<dyn QuickwitDataSource>>,
    /// Tables explicitly registered via DDL (CREATE OR REPLACE EXTERNAL TABLE).
    registered_tables: Mutex<HashMap<String, Arc<dyn TableProvider>>>,
}

impl QuickwitSchemaProvider {
    pub fn new(sources: Vec<Arc<dyn QuickwitDataSource>>) -> Self {
        Self {
            sources,
            registered_tables: Mutex::new(HashMap::new()),
        }
    }
}

impl std::fmt::Debug for QuickwitSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuickwitSchemaProvider")
            .field("num_sources", &self.sources.len())
            .field(
                "registered_tables",
                &self.registered_tables.lock().map(|m| m.len()).unwrap_or(0),
            )
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
        // 1. Explicitly registered tables (from CREATE OR REPLACE EXTERNAL TABLE DDL)
        // 2. Each source's create_default_table_provider — first non-None wins.
        //    We do not call table_names() to pre-validate; sources return None for
        //    unknown names, and the schema provider returns None to DataFusion, which
        //    then emits a "table not found" planning error. This avoids any N+1 listing.
        if let Some(provider) = self
            .registered_tables
            .lock()
            .map_err(|_| datafusion::error::DataFusionError::Internal(
                "catalog mutex poisoned".to_string(),
            ))?
            .get(name)
            .cloned()
        {
            return Ok(Some(provider));
        }

        for source in &self.sources {
            if let Some(provider) = source.create_default_table_provider(name).await? {
                return Ok(Some(provider));
            }
        }

        Ok(None)
    }

    /// Returns `true` if the table is present in the in-memory DDL cache.
    ///
    /// DataFusion's contract: returning `false` does not prevent `table()` from
    /// returning `Some`; `true` is merely a hint. The planner always calls
    /// `table()` regardless. Checking only `registered_tables` keeps this
    /// method allocation-free and off the async hot path.
    fn table_exist(&self, name: &str) -> bool {
        self.registered_tables
            .lock()
            .map(|m| m.contains_key(name))
            .unwrap_or(false)
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let old = self
            .registered_tables
            .lock()
            .map_err(|_| datafusion::error::DataFusionError::Internal(
                "catalog mutex poisoned".to_string(),
            ))?
            .insert(name, table);
        Ok(old)
    }

    fn deregister_table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let removed = self
            .registered_tables
            .lock()
            .map_err(|_| datafusion::error::DataFusionError::Internal(
                "catalog mutex poisoned".to_string(),
            ))?
            .remove(name);
        Ok(removed)
    }
}
