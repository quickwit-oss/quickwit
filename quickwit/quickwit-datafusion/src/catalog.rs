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
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for QuickwitSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        // table_names() is sync; listing indexes is async.
        // Use block_in_place — only called for SHOW TABLES / information_schema.
        let sources = self.sources.clone();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                let mut names = Vec::new();
                for source in &sources {
                    if let Ok(mut source_names) = source.list_index_names().await {
                        names.append(&mut source_names);
                    }
                }
                names
            })
        })
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        // 1. Explicitly registered tables take precedence (DDL path)
        if let Some(provider) = self.registered_tables.lock().unwrap().get(name).cloned() {
            return Ok(Some(provider));
        }

        // 2. Ask each source — first one that claims the index wins
        for source in &self.sources {
            if let Some(provider) = source.create_default_table_provider(name).await? {
                return Ok(Some(provider));
            }
        }

        Ok(None)
    }

    fn table_exist(&self, name: &str) -> bool {
        if self.registered_tables.lock().unwrap().contains_key(name) {
            return true;
        }
        self.table_names().contains(&name.to_string())
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let old = self.registered_tables.lock().unwrap().insert(name, table);
        Ok(old)
    }

    fn deregister_table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        Ok(self.registered_tables.lock().unwrap().remove(name))
    }
}
