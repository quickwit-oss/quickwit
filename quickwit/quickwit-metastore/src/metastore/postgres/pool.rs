// Copyright (C) 2024 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use quickwit_common::metrics::GaugeGuard;
use sqlx::database::HasStatement;
use sqlx::pool::maybe::MaybePoolConnection;
use sqlx::pool::PoolConnection;
use sqlx::{
    Acquire, Database, Describe, Either, Error, Execute, Executor, Pool, Postgres, Transaction,
};

use super::metrics::POSTGRES_METRICS;

#[derive(Debug)]
pub(super) struct TrackedPool<DB: Database> {
    inner_pool: Pool<DB>,
}

impl TrackedPool<Postgres> {
    pub fn new(inner_pool: Pool<Postgres>) -> Self {
        Self { inner_pool }
    }
}

impl<DB: Database> Clone for TrackedPool<DB> {
    fn clone(&self) -> Self {
        Self {
            inner_pool: self.inner_pool.clone(),
        }
    }
}

impl<'a, DB: Database> Acquire<'a> for &TrackedPool<DB> {
    type Database = DB;

    type Connection = PoolConnection<DB>;

    fn acquire(self) -> BoxFuture<'static, Result<Self::Connection, Error>> {
        let acquire_conn_fut = self.inner_pool.acquire();

        POSTGRES_METRICS
            .active_connections
            .set(self.inner_pool.size() as i64);
        POSTGRES_METRICS
            .idle_connections
            .set(self.inner_pool.num_idle() as i64);

        Box::pin(async move {
            let mut gauge_guard = GaugeGuard::from_gauge(&POSTGRES_METRICS.acquire_connections);
            gauge_guard.add(1);

            let conn = acquire_conn_fut.await?;
            Ok(conn)
        })
    }

    fn begin(self) -> BoxFuture<'static, Result<Transaction<'a, DB>, Error>> {
        let acquire_conn_fut = self.acquire();

        Box::pin(async move {
            Transaction::begin(MaybePoolConnection::PoolConnection(acquire_conn_fut.await?)).await
        })
    }
}

impl<'p, DB: Database> Executor<'p> for &'_ TrackedPool<DB>
where for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>
{
    type Database = DB;

    fn fetch_many<'e, 'q: 'e, E: 'q>(
        self,
        query: E,
    ) -> BoxStream<'e, Result<Either<DB::QueryResult, DB::Row>, Error>>
    where
        E: Execute<'q, Self::Database>,
    {
        self.inner_pool.fetch_many(query)
    }

    fn fetch_optional<'e, 'q: 'e, E: 'q>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<DB::Row>, Error>>
    where
        E: Execute<'q, Self::Database>,
    {
        self.inner_pool.fetch_optional(query)
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [<Self::Database as Database>::TypeInfo],
    ) -> BoxFuture<'e, Result<<Self::Database as HasStatement<'q>>::Statement, Error>> {
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
