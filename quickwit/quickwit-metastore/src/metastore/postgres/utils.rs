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

use std::fmt::Display;
use std::ops::Bound;
use std::str::FromStr;
use std::time::Duration;

use quickwit_common::uri::Uri;
use quickwit_proto::metastore::{MetastoreError, MetastoreResult};
use sea_query::{Expr, Func, Order, SelectStatement, any};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{ConnectOptions, Postgres};
use tracing::error;
use tracing::log::LevelFilter;

use super::model::{Splits, ToTimestampFunc};
use super::pool::TrackedPool;
use super::tags::generate_sql_condition;
use crate::metastore::{FilterRange, SortBy};
use crate::{ListSplitsQuery, SplitMaturity, SplitMetadata};

/// Establishes a connection to the given database URI.
pub(super) async fn establish_connection(
    connection_uri: &Uri,
    min_connections: usize,
    max_connections: usize,
    acquire_timeout: Duration,
    idle_timeout_opt: Option<Duration>,
    max_lifetime_opt: Option<Duration>,
    read_only: bool,
) -> MetastoreResult<TrackedPool<Postgres>> {
    let pool_options = PgPoolOptions::new()
        .min_connections(min_connections as u32)
        .max_connections(max_connections as u32)
        .acquire_timeout(acquire_timeout)
        .idle_timeout(idle_timeout_opt)
        .max_lifetime(max_lifetime_opt);

    let mut connect_options: PgConnectOptions =
        PgConnectOptions::from_str(connection_uri.as_str())?
            .application_name("quickwit-metastore")
            .log_statements(LevelFilter::Info);

    if read_only {
        // this isn't a security mechanism, only a safeguard against involontary missuse
        connect_options = connect_options.options([("default_transaction_read_only", "on")]);
    }
    let sqlx_pool = pool_options
        .connect_with(connect_options)
        .await
        .map_err(|error| {
            error!(connection_uri=%connection_uri, error=?error, "failed to establish connection to database");
            MetastoreError::Connection {
                message: error.to_string(),
            }
        })?;
    let tracked_pool = TrackedPool::new(sqlx_pool);
    Ok(tracked_pool)
}

/// Extends an existing SQL string with the generated filter range appended to the query.
///
/// This method is **not** SQL injection proof and should not be used with user-defined values.
pub(super) fn append_range_filters<V: Display>(
    sql: &mut SelectStatement,
    field_name: Splits,
    filter_range: &FilterRange<V>,
    value_formatter: impl Fn(&V) -> Expr,
) {
    if let Bound::Included(value) = &filter_range.start {
        sql.cond_where(Expr::col(field_name).gte((value_formatter)(value)));
    };

    if let Bound::Excluded(value) = &filter_range.start {
        sql.cond_where(Expr::col(field_name).gt((value_formatter)(value)));
    };

    if let Bound::Included(value) = &filter_range.end {
        sql.cond_where(Expr::col(field_name).lte((value_formatter)(value)));
    };

    if let Bound::Excluded(value) = &filter_range.end {
        sql.cond_where(Expr::col(field_name).lt((value_formatter)(value)));
    };
}

pub(super) fn append_query_filters_and_order_by(
    sql: &mut SelectStatement,
    query: &ListSplitsQuery,
) {
    if let Some(index_uids) = &query.index_uids {
        // Note: `ListSplitsQuery` builder enforces a non empty `index_uids` list.
        // TODO we should explore IN VALUES, = ANY and similar constructs in case they perform
        // better.
        sql.cond_where(Expr::col(Splits::IndexUid).is_in(index_uids));
    }

    if let Some(node_id) = &query.node_id {
        sql.cond_where(Expr::col(Splits::NodeId).eq(node_id));
    };

    if !query.split_states.is_empty() {
        sql.cond_where(
            Expr::col(Splits::SplitState)
                .is_in(query.split_states.iter().map(|val| val.to_string())),
        );
    };

    if let Some(tags) = &query.tags {
        sql.cond_where(generate_sql_condition(tags));
    };

    if let Some(v) = query.max_time_range_end {
        sql.cond_where(Expr::col(Splits::TimeRangeEnd).lte(v));
    }

    match query.time_range.start {
        Bound::Included(v) => {
            sql.cond_where(any![
                Expr::col(Splits::TimeRangeEnd).gte(v),
                Expr::col(Splits::TimeRangeEnd).is_null()
            ]);
        }
        Bound::Excluded(v) => {
            sql.cond_where(any![
                Expr::col(Splits::TimeRangeEnd).gt(v),
                Expr::col(Splits::TimeRangeEnd).is_null()
            ]);
        }
        Bound::Unbounded => {}
    };

    match query.time_range.end {
        Bound::Included(v) => {
            sql.cond_where(any![
                Expr::col(Splits::TimeRangeStart).lte(v),
                Expr::col(Splits::TimeRangeStart).is_null()
            ]);
        }
        Bound::Excluded(v) => {
            sql.cond_where(any![
                Expr::col(Splits::TimeRangeStart).lt(v),
                Expr::col(Splits::TimeRangeStart).is_null()
            ]);
        }
        Bound::Unbounded => {}
    };

    match &query.mature {
        Bound::Included(evaluation_datetime) => {
            sql.cond_where(any![
                Expr::col(Splits::MaturityTimestamp)
                    .eq(Func::cust(ToTimestampFunc).arg(Expr::val(0))),
                Expr::col(Splits::MaturityTimestamp).lte(
                    Func::cust(ToTimestampFunc)
                        .arg(Expr::val(evaluation_datetime.unix_timestamp()))
                )
            ]);
        }
        Bound::Excluded(evaluation_datetime) => {
            sql.cond_where(Expr::col(Splits::MaturityTimestamp).gt(
                Func::cust(ToTimestampFunc).arg(Expr::val(evaluation_datetime.unix_timestamp())),
            ));
        }
        Bound::Unbounded => {}
    };
    append_range_filters(
        sql,
        Splits::UpdateTimestamp,
        &query.update_timestamp,
        |&val| Expr::expr(Func::cust(ToTimestampFunc).arg(Expr::val(val))),
    );
    append_range_filters(
        sql,
        Splits::CreateTimestamp,
        &query.create_timestamp,
        |&val| Expr::expr(Func::cust(ToTimestampFunc).arg(Expr::val(val))),
    );
    append_range_filters(sql, Splits::DeleteOpstamp, &query.delete_opstamp, |&val| {
        Expr::expr(val)
    });

    if let Some((index_uid, split_id)) = &query.after_split {
        sql.cond_where(
            Expr::tuple([
                Expr::col(Splits::IndexUid).into(),
                Expr::col(Splits::SplitId).into(),
            ])
            .gt(Expr::tuple([Expr::value(index_uid), Expr::value(split_id)])),
        );
    }

    match query.sort_by {
        SortBy::Staleness => {
            sql.order_by(Splits::DeleteOpstamp, Order::Asc)
                .order_by(Splits::PublishTimestamp, Order::Asc);
        }
        SortBy::IndexUid => {
            sql.order_by(Splits::IndexUid, Order::Asc)
                .order_by(Splits::SplitId, Order::Asc);
        }
        SortBy::None => (),
    }

    if let Some(limit) = query.limit {
        sql.limit(limit as u64);
    }

    if let Some(offset) = query.offset {
        sql.order_by(Splits::SplitId, Order::Asc)
            .offset(offset as u64);
    }
}

/// Returns the unix timestamp at which the split becomes mature.
/// If the split is mature (`SplitMaturity::Mature`), we return 0
/// as we don't want the maturity to depend on datetime.
pub(super) fn split_maturity_timestamp(split_metadata: &SplitMetadata) -> i64 {
    match split_metadata.maturity {
        SplitMaturity::Mature => 0,
        SplitMaturity::Immature { maturation_period } => {
            split_metadata.create_timestamp + maturation_period.as_secs() as i64
        }
    }
}
