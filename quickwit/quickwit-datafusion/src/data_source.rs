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

//! `QuickwitDataSource` — the extension point for plugging data sources
//! (metrics, logs, traces, …) into the DataFusion session layer.
//!
//! Each data source is responsible for:
//! - Advertising the `STORED AS <file_type>` string used in DDL
//! - Producing a `TableProviderFactory` registered in the session
//! - Providing a default `TableProvider` for a given index name when no DDL
//!   schema is declared (schema is inferred or minimal)
//! - Pre-registering any runtime state (object stores, catalogs) needed by
//!   Flight workers before plan deserialization
//! - Listing the index names it exposes (for SHOW TABLES)

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::TableProviderFactory;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::execution::SessionState;

/// Extension point for plugging a data source into `DataFusionSessionBuilder`.
///
/// Implement this trait for each data type (metrics, logs, traces, …) that
/// should be queryable via DataFusion SQL. The session builder iterates over
/// all registered sources when building sessions and workers.
#[async_trait]
pub trait QuickwitDataSource: Send + Sync + Debug {
    /// The string used in `STORED AS <file_type>` DDL.
    ///
    /// The session registers the factory under both this value and its
    /// uppercase equivalent because DataFusion uppercases the token.
    fn file_type(&self) -> &str;

    /// Creates the `TableProviderFactory` that handles
    /// `CREATE [OR REPLACE] EXTERNAL TABLE … STORED AS <file_type>`.
    ///
    /// Called once per `build_session()` call.
    fn create_table_provider_factory(&self) -> Arc<dyn TableProviderFactory>;

    /// Create a default `TableProvider` for `index_name` without any
    /// DDL schema declaration.
    ///
    /// Returns `Ok(None)` if this source does not own the given index —
    /// `QuickwitSchemaProvider` will try the next registered source.
    ///
    /// The provider uses whatever minimal schema the source can derive
    /// on its own (e.g. the 4 required fields for metrics).  Callers
    /// that need a richer schema should use `CREATE EXTERNAL TABLE` DDL.
    async fn create_default_table_provider(
        &self,
        index_name: &str,
    ) -> DFResult<Option<Arc<dyn TableProvider>>>;

    /// Pre-register runtime state (object stores, catalogs) the Flight
    /// worker needs before deserializing and executing plan fragments.
    ///
    /// Called by `QuickwitWorkerSessionBuilder::build_session_state()` for
    /// every registered source when a new worker session is created.
    async fn register_for_worker(&self, state: &SessionState) -> DFResult<()>;

    /// Return all index names exposed by this source.
    ///
    /// Used by `QuickwitSchemaProvider::table_names()` to populate the
    /// `SHOW TABLES` / `information_schema` view.
    async fn list_index_names(&self) -> DFResult<Vec<String>>;
}
