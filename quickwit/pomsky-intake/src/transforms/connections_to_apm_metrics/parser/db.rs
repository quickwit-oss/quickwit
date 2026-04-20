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

//! Extracts Postgres + Redis `ProtoStat` records from the agent's opaque
//! `DatabaseAggregations` payload.
//!
//! Port of `parser/db.go`.

use prost::Message;

use super::super::types::{Operation, ProtoStat};
use super::http::{optional_bytes, optional_f64};
use crate::protos::process::database_stats::DbStats;
use crate::protos::process::{
    DatabaseAggregations, PostgresOperation, PostgresStats, RedisCommand, RedisErrorType,
    RedisStats,
};

pub(in crate::transforms::connections_to_apm_metrics) fn parse_database_aggregations(
    data: &[u8],
) -> Vec<ProtoStat> {
    let agg = match DatabaseAggregations::decode(data) {
        Ok(agg) => agg,
        Err(_) => return Vec::new(),
    };
    let mut out: Vec<ProtoStat> = Vec::new();
    for stats in agg.aggregations {
        match stats.db_stats {
            Some(DbStats::Postgres(pg)) => {
                if let Some(ps) = parse_postgres(&pg) {
                    out.push(ps);
                }
            }
            Some(DbStats::Redis(rd)) => {
                out.extend(parse_redis(&rd));
            }
            None => {}
        }
    }
    out
}

fn parse_postgres(pg: &PostgresStats) -> Option<ProtoStat> {
    if pg.count == 0 {
        return None;
    }
    let op = postgres_operation(pg.operation)?;
    if pg.table_name.is_empty() {
        return None;
    }
    Some(ProtoStat {
        operation: Operation::Postgres,
        resource: format!("{op}/{}", pg.table_name),
        status: 0,
        hits: pg.count,
        errors: 0,
        latencies: optional_bytes(&pg.latencies),
        first_latency_sample: optional_f64(pg.first_latency_sample),
    })
}

fn parse_redis(redis: &RedisStats) -> Vec<ProtoStat> {
    if redis.error_to_stats.is_empty() {
        return Vec::new();
    }
    let Some(command) = redis_command(redis.command) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(redis.error_to_stats.len());
    for (error_type, entry) in &redis.error_to_stats {
        if entry.count == 0 {
            continue;
        }
        let is_error = RedisErrorType::try_from(*error_type)
            .map(|e| e != RedisErrorType::RedisNoError)
            .unwrap_or(true);
        out.push(ProtoStat {
            operation: Operation::Redis,
            resource: command.to_string(),
            status: *error_type,
            hits: entry.count,
            errors: if is_error { entry.count } else { 0 },
            latencies: optional_bytes(&entry.latencies),
            first_latency_sample: optional_f64(entry.first_latency_sample),
        });
    }
    out
}

fn postgres_operation(v: i32) -> Option<&'static str> {
    match PostgresOperation::try_from(v).ok()? {
        PostgresOperation::PostgresSelectOp => Some("SELECT"),
        PostgresOperation::PostgresInsertOp => Some("INSERT"),
        PostgresOperation::PostgresUpdateOp => Some("UPDATE"),
        PostgresOperation::PostgresDeleteOp => Some("DELETE"),
        PostgresOperation::PostgresAlterOp => Some("ALTER"),
        PostgresOperation::PostgresCreateOp => Some("CREATE"),
        PostgresOperation::PostgresDropOp => Some("DROP"),
        PostgresOperation::PostgresTruncateOp => Some("TRUNCATE"),
        PostgresOperation::PostgresShowOp => Some("SHOW"),
        PostgresOperation::PostgresUnknownOp => None,
    }
}

fn redis_command(v: i32) -> Option<&'static str> {
    match RedisCommand::try_from(v).ok()? {
        RedisCommand::RedisGetCommand => Some("GET"),
        RedisCommand::RedisSetCommand => Some("SET"),
        RedisCommand::RedisPingCommand => Some("PING"),
        RedisCommand::RedisDelCommand => Some("DEL"),
        RedisCommand::RedisIncrCommand => Some("INCR"),
        RedisCommand::RedisExpireCommand => Some("EXPIRE"),
        RedisCommand::RedisExistsCommand => Some("EXISTS"),
        RedisCommand::RedisHGetCommand => Some("HGET"),
        RedisCommand::RedisHSetCommand => Some("HSET"),
        RedisCommand::RedisLPushCommand => Some("LPUSH"),
        RedisCommand::RedisUnknownCommand => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protos::process::{DatabaseStats, RedisStatsEntry};

    #[test]
    fn postgres_select_stats() {
        let pg = PostgresStats {
            table_name: "users".into(),
            operation: PostgresOperation::PostgresSelectOp as i32,
            latencies: Vec::new(),
            first_latency_sample: 0.0,
            count: 10,
        };
        let agg = DatabaseAggregations {
            aggregations: vec![DatabaseStats {
                db_stats: Some(DbStats::Postgres(pg)),
            }],
        };
        let stats = parse_database_aggregations(&agg.encode_to_vec());
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].resource, "SELECT/users");
        assert_eq!(stats[0].hits, 10);
    }

    #[test]
    fn postgres_unknown_op_skipped() {
        let pg = PostgresStats {
            table_name: "users".into(),
            operation: PostgresOperation::PostgresUnknownOp as i32,
            latencies: Vec::new(),
            first_latency_sample: 0.0,
            count: 5,
        };
        let agg = DatabaseAggregations {
            aggregations: vec![DatabaseStats {
                db_stats: Some(DbStats::Postgres(pg)),
            }],
        };
        assert!(parse_database_aggregations(&agg.encode_to_vec()).is_empty());
    }

    #[test]
    fn redis_error_to_stats_map() {
        let mut error_to_stats = std::collections::HashMap::new();
        error_to_stats.insert(
            RedisErrorType::RedisNoError as i32,
            RedisStatsEntry {
                latencies: Vec::new(),
                first_latency_sample: 0.0,
                count: 3,
            },
        );
        error_to_stats.insert(
            RedisErrorType::RedisErrErr as i32,
            RedisStatsEntry {
                latencies: Vec::new(),
                first_latency_sample: 0.0,
                count: 2,
            },
        );
        let rd = RedisStats {
            command: RedisCommand::RedisGetCommand as i32,
            key_name: "k".into(),
            truncated: false,
            error_to_stats,
        };
        let agg = DatabaseAggregations {
            aggregations: vec![DatabaseStats {
                db_stats: Some(DbStats::Redis(rd)),
            }],
        };
        let stats = parse_database_aggregations(&agg.encode_to_vec());
        assert_eq!(stats.len(), 2);
        let success = stats
            .iter()
            .find(|s| s.status == RedisErrorType::RedisNoError as i32)
            .unwrap();
        assert_eq!(success.errors, 0);
        let err = stats
            .iter()
            .find(|s| s.status == RedisErrorType::RedisErrErr as i32)
            .unwrap();
        assert_eq!(err.errors, 2);
    }

    #[test]
    fn empty_oneof_ignored() {
        let agg = DatabaseAggregations {
            aggregations: vec![DatabaseStats { db_stats: None }],
        };
        assert!(parse_database_aggregations(&agg.encode_to_vec()).is_empty());
    }
}
