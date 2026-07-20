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

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use quickwit_metrics::{Gauge, GaugeGuard, gauge, labels};
use sqlx::pool::PoolConnection;
use sqlx::pool::maybe::MaybePoolConnection;
use sqlx::{
    Acquire, Database, Describe, Either, Error, Execute, Executor, Pool, Postgres, Transaction,
};

use super::metrics::{ACQUIRE_CONNECTIONS, ACTIVE_CONNECTIONS, IDLE_CONNECTIONS, MetastoreKind};

#[derive(Clone, Debug)]
struct TrackedPoolMetrics {
    acquire_connections: Gauge,
    active_connections: Gauge,
    idle_connections: Gauge,
}

impl TrackedPoolMetrics {
    fn new(kind: MetastoreKind) -> Self {
        let kind_label = labels!("metastore_kind" => kind.as_str());
        Self {
            acquire_connections: gauge!(parent: ACQUIRE_CONNECTIONS, labels: [kind_label]),
            active_connections: gauge!(parent: ACTIVE_CONNECTIONS, labels: [kind_label]),
            idle_connections: gauge!(parent: IDLE_CONNECTIONS, labels: [kind_label]),
        }
    }
}

#[derive(Debug)]
pub(super) struct TrackedPool<DB: Database> {
    inner_pool: Pool<DB>,
    metrics: TrackedPoolMetrics,
}

impl TrackedPool<Postgres> {
    pub fn new(inner_pool: Pool<Postgres>, kind: MetastoreKind) -> Self {
        Self {
            inner_pool,
            metrics: TrackedPoolMetrics::new(kind),
        }
    }
}

impl<DB: Database> Clone for TrackedPool<DB> {
    fn clone(&self) -> Self {
        Self {
            inner_pool: self.inner_pool.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

impl<'a, DB: Database> Acquire<'a> for &TrackedPool<DB> {
    type Database = DB;

    type Connection = PoolConnection<DB>;

    fn acquire(self) -> BoxFuture<'static, Result<Self::Connection, Error>> {
        let acquire_conn_fut = self.inner_pool.acquire();

        self.metrics
            .active_connections
            .set(self.inner_pool.size() as f64);
        self.metrics
            .idle_connections
            .set(self.inner_pool.num_idle() as f64);
        let acquire_connections = self.metrics.acquire_connections.clone();

        Box::pin(async move {
            let _gauge_guard = GaugeGuard::new(&acquire_connections, 1.0);

            let conn = acquire_conn_fut.await?;
            Ok(conn)
        })
    }

    fn begin(self) -> BoxFuture<'static, Result<Transaction<'a, DB>, Error>> {
        let acquire_conn_fut = self.acquire();

        Box::pin(async move {
            Transaction::begin(
                MaybePoolConnection::PoolConnection(acquire_conn_fut.await?),
                None,
            )
            .await
        })
    }
}

impl<DB: Database> Executor<'_> for &TrackedPool<DB>
where for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>
{
    type Database = DB;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxStream<'e, Result<Either<DB::QueryResult, DB::Row>, Error>>
    where
        E: Execute<'q, Self::Database> + 'q,
    {
        self.inner_pool.fetch_many(query)
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<DB::Row>, Error>>
    where
        E: Execute<'q, Self::Database> + 'q,
    {
        self.inner_pool.fetch_optional(query)
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [<Self::Database as Database>::TypeInfo],
    ) -> BoxFuture<'e, Result<<Self::Database as Database>::Statement<'q>, Error>> {
        self.inner_pool.prepare_with(sql, parameters)
    }

    #[doc(hidden)]
    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> BoxFuture<'e, Result<Describe<Self::Database>, Error>> {
        self.inner_pool.describe(sql)
    }
}
