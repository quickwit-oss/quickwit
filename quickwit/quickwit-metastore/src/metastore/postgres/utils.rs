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

use std::fmt::Display;
use std::ops::Bound;
use std::str::FromStr;
use std::time::Duration;

use quickwit_common::uri::Uri;
use quickwit_proto::metastore::{MetastoreError, MetastoreResult};
use sea_query::{any, Cond, Expr, Func, Order, SelectStatement};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{ConnectOptions, Pool, Postgres};
use tracing::error;
use tracing::log::LevelFilter;

use super::model::{Splits, ToTimestampFunc};
use super::tags_filter_expression_helper;
use crate::metastore::FilterRange;
use crate::{ListSplitsQuery, SplitMaturity, SplitMetadata};

/// Establishes a connection to the given database URI.
pub(super) async fn establish_connection(
    connection_uri: &Uri,
    min_connections: usize,
    max_connections: usize,
    acquire_timeout: Duration,
    idle_timeout_opt: Option<Duration>,
    max_lifetime_opt: Option<Duration>,
) -> MetastoreResult<Pool<Postgres>> {
    let pool_options = PgPoolOptions::new()
        .min_connections(min_connections as u32)
        .max_connections(max_connections as u32)
        .acquire_timeout(acquire_timeout)
        .idle_timeout(idle_timeout_opt)
        .max_lifetime(max_lifetime_opt);
    let connect_options: PgConnectOptions = PgConnectOptions::from_str(connection_uri.as_str())?
        .application_name("quickwit-metastore")
        .log_statements(LevelFilter::Info);
    pool_options
        .connect_with(connect_options)
        .await
        .map_err(|error| {
            error!(connection_uri=%connection_uri, error=?error, "failed to establish connection to database");
            MetastoreError::Connection {
                message: error.to_string(),
            }
        })
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

pub(super) fn append_query_filters(sql: &mut SelectStatement, query: &ListSplitsQuery) {
    // Note: `ListSplitsQuery` builder enforces a non empty `index_uids` list.

    let or_condition = query
        .index_uids
        .iter()
        .fold(Cond::any(), |cond, index_uid| {
            cond.add(Expr::col(Splits::IndexUid).eq(Expr::val(index_uid.to_string())))
        });
    sql.cond_where(or_condition);

    if !query.split_states.is_empty() {
        sql.cond_where(
            Expr::col(Splits::SplitState)
                .is_in(query.split_states.iter().map(|val| val.to_string())),
        );
    };

    if let Some(tags) = query.tags.as_ref() {
        sql.cond_where(tags_filter_expression_helper(tags));
    };

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
