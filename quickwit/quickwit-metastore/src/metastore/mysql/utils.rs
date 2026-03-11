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
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use sqlx::{ConnectOptions, Executor, MySql};
use tracing::log::LevelFilter;
use tracing::{error, info};

use super::auth::{generate_rds_iam_token, spawn_token_refresh_task};
use super::model::{FromUnixTimeFunc, Splits};
use super::pool::TrackedPool;
use super::tags::generate_sql_condition;
use crate::metastore::{FilterRange, SortBy};
use crate::{ListSplitsQuery, SplitMaturity, SplitMetadata};

/// Establishes a connection to the given database URI.
#[allow(clippy::too_many_arguments)]
pub(super) async fn establish_connection(
    connection_uri: &Uri,
    min_connections: usize,
    max_connections: usize,
    acquire_timeout: Duration,
    idle_timeout_opt: Option<Duration>,
    max_lifetime_opt: Option<Duration>,
    read_only: bool,
    auth_mode: &quickwit_config::MysqlAuthMode,
    aws_region: Option<&str>,
) -> MetastoreResult<TrackedPool<MySql>> {
    let mut connect_options: MySqlConnectOptions =
        MySqlConnectOptions::from_str(connection_uri.as_str())?.log_statements(LevelFilter::Info);

    let aws_config_opt = match auth_mode {
        quickwit_config::MysqlAuthMode::AwsIam => {
            let mut aws_config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
            if let Some(region) = aws_region {
                aws_config_loader =
                    aws_config_loader.region(aws_config::Region::new(region.to_string()));
            }
            let aws_config = aws_config_loader.load().await;

            let region = aws_config
                .region()
                .map(|region| region.as_ref().to_string())
                .ok_or_else(|| MetastoreError::Internal {
                    message: "failed to determine AWS region for RDS IAM auth".to_string(),
                    cause: "no region configured; set AWS_REGION, AWS_DEFAULT_REGION, or \
                            metastore.mysql.aws_region"
                        .to_string(),
                })?;

            let host = connect_options.get_host().to_string();
            let port = connect_options.get_port();
            let user = connect_options.get_username().to_string();

            let token = generate_rds_iam_token(&host, port, &user, &region, &aws_config).await?;

            connect_options = connect_options
                .password(&token)
                .enable_cleartext_plugin(true);

            info!(host=%host, port=%port, user=%user, region=%region, "connecting to Aurora MySQL with IAM auth");
            Some((aws_config, host, port, user, region))
        }
        quickwit_config::MysqlAuthMode::Password => None,
    };

    let pool_options = MySqlPoolOptions::new()
        .min_connections(min_connections as u32)
        .max_connections(max_connections as u32)
        .acquire_timeout(acquire_timeout)
        .idle_timeout(idle_timeout_opt)
        .max_lifetime(max_lifetime_opt)
        .after_connect(move |conn, _meta| {
            Box::pin(async move {
                // Ensure all sessions use UTC for timestamp consistency.
                conn.execute("SET time_zone = '+00:00'").await?;
                if read_only {
                    conn.execute("SET SESSION TRANSACTION READ ONLY").await?;
                }
                Ok(())
            })
        });

    let sqlx_pool = pool_options
        .connect_with(connect_options.clone())
        .await
        .map_err(|error| {
            error!(connection_uri=%connection_uri, error=?error, "failed to establish connection to database");
            MetastoreError::Connection {
                message: error.to_string(),
            }
        })?;
    let tracked_pool = TrackedPool::new(sqlx_pool);

    if let Some((aws_config, host, port, user, region)) = aws_config_opt {
        spawn_token_refresh_task(
            tracked_pool.clone(),
            connect_options,
            host,
            port,
            user,
            region,
            aws_config,
        );
    }

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
                    .eq(Func::cust(FromUnixTimeFunc).arg(Expr::val(0))),
                Expr::col(Splits::MaturityTimestamp).lte(
                    Func::cust(FromUnixTimeFunc)
                        .arg(Expr::val(evaluation_datetime.unix_timestamp()))
                )
            ]);
        }
        Bound::Excluded(evaluation_datetime) => {
            sql.cond_where(Expr::col(Splits::MaturityTimestamp).gt(
                Func::cust(FromUnixTimeFunc).arg(Expr::val(evaluation_datetime.unix_timestamp())),
            ));
        }
        Bound::Unbounded => {}
    };
    append_range_filters(
        sql,
        Splits::UpdateTimestamp,
        &query.update_timestamp,
        |&val| Expr::expr(Func::cust(FromUnixTimeFunc).arg(Expr::val(val))),
    );
    append_range_filters(
        sql,
        Splits::CreateTimestamp,
        &query.create_timestamp,
        |&val| Expr::expr(Func::cust(FromUnixTimeFunc).arg(Expr::val(val))),
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
        // MySQL requires LIMIT before OFFSET; use a large limit if none was set.
        if query.limit.is_none() {
            sql.limit(u64::MAX);
        }
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
