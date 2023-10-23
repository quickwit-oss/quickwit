// Copyright (C) 2023 Quickwit, Inc.
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

use std::ops::RangeBounds;

use mrecordlog::MultiRecordLog;
use quickwit_proto::ingest::ShardState;

use super::models::{Position, PrimaryShard, ReplicaShard};

pub(super) trait MultiRecordLogTestExt {
    fn assert_records_eq<R>(&self, queue_id: &str, range: R, expected_records: &[(u64, &str)])
    where R: RangeBounds<u64> + 'static;
}

impl MultiRecordLogTestExt for MultiRecordLog {
    #[track_caller]
    fn assert_records_eq<R>(&self, queue_id: &str, range: R, expected_records: &[(u64, &str)])
    where R: RangeBounds<u64> + 'static {
        let records = self
            .range(queue_id, range)
            .unwrap()
            .map(|(position, record)| (position, String::from_utf8(record.into_owned()).unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(
            records.len(),
            expected_records.len(),
            "expected {} records, got {}",
            expected_records.len(),
            records.len()
        );
        for ((position, record), (expected_position, expected_record)) in
            records.iter().zip(expected_records.iter())
        {
            assert_eq!(
                position, expected_position,
                "expected record at position `{expected_position}`, got `{position}`",
            );
            assert_eq!(
                record, expected_record,
                "expected record `{expected_record}`, got `{record}`",
            );
        }
    }
}

pub(super) trait PrimaryShardTestExt {
    fn assert_positions(
        &self,
        expected_primary_position: impl Into<Position>,
        expected_replica_position: Option<impl Into<Position>>,
    );

    fn assert_publish_position(&self, expected_publish_position: impl Into<Position>);

    fn assert_is_open(&self, expected_position: impl Into<Position>);
}

impl PrimaryShardTestExt for PrimaryShard {
    #[track_caller]
    fn assert_positions(
        &self,
        expected_primary_position: impl Into<Position>,
        expected_replica_position: Option<impl Into<Position>>,
    ) {
        let expected_primary_position = expected_primary_position.into();
        let expected_replica_position =
            expected_replica_position.map(|replication_position| replication_position.into());

        assert_eq!(
            self.primary_position_inclusive, expected_primary_position,
            "expected primary position at `{:?}`, got `{:?}`",
            expected_primary_position, self.primary_position_inclusive
        );
        assert_eq!(
            self.replica_position_inclusive_opt, expected_replica_position,
            "expected replica position at `{:?}`, got `{:?}`",
            expected_replica_position, self.replica_position_inclusive_opt
        );
    }

    #[track_caller]
    fn assert_publish_position(&self, expected_publish_position: impl Into<Position>) {
        let expected_publish_position = expected_publish_position.into();

        assert_eq!(
            self.publish_position_inclusive, expected_publish_position,
            "expected publish position at `{:?}`, got `{:?}`",
            expected_publish_position, self.publish_position_inclusive
        );
        assert_eq!(
            self.shard_status_tx.borrow().publish_position_inclusive,
            expected_publish_position,
            "expected publish position at `{:?}`, got `{:?}`",
            expected_publish_position,
            self.publish_position_inclusive
        );
    }

    #[track_caller]
    fn assert_is_open(&self, expected_replication_position: impl Into<Position>) {
        let expected_replication_position = expected_replication_position.into();
        let shard_status = self.shard_status_tx.borrow();
        assert_eq!(
            shard_status.shard_state,
            ShardState::Open,
            "expected open primary shard, got closed one",
        );
        assert_eq!(
            shard_status.replication_position_inclusive, expected_replication_position,
            "expected open primary shard at `{expected_replication_position:?}`, got `{:?}`",
            shard_status.replication_position_inclusive
        );
    }
}

pub(super) trait ReplicaShardTestExt {
    fn assert_position(&self, expected_replica_position: impl Into<Position>);

    fn assert_is_open(&self, expected_position: impl Into<Position>);
}

impl ReplicaShardTestExt for ReplicaShard {
    #[track_caller]
    fn assert_position(&self, expected_replica_position: impl Into<Position>) {
        let expected_replica_position = expected_replica_position.into();
        assert_eq!(
            self.replica_position_inclusive, expected_replica_position,
            "expected replica position at `{:?}`, got `{:?}`",
            expected_replica_position, self.replica_position_inclusive
        );
    }

    #[track_caller]
    fn assert_is_open(&self, expected_replication_position: impl Into<Position>) {
        let expected_replication_position = expected_replication_position.into();
        let shard_status = self.shard_status_tx.borrow();
        assert_eq!(
            shard_status.shard_state,
            ShardState::Open,
            "expected open replica shard, got closed one",
        );
        assert_eq!(
            shard_status.replication_position_inclusive, expected_replication_position,
            "expected open replica shard at `{expected_replication_position:?}`, got `{:?}`",
            shard_status.replication_position_inclusive
        );
    }
}
