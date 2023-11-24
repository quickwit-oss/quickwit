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

use quickwit_proto::ingest::ShardState;
use quickwit_proto::types::{NodeId, Position};
use tokio::sync::watch;

#[derive(Debug, Clone, Eq, PartialEq)]
pub(super) enum IngesterShardType {
    /// A primary shard hosted on a leader and replicated on a follower.
    Primary { follower_id: NodeId },
    /// A replica shard hosted on a follower.
    Replica { leader_id: NodeId },
    /// A shard hosted on a single node when the replication factor is set to 1.
    Solo,
}

#[derive(Debug)]
pub(super) struct IngesterShard {
    pub shard_type: IngesterShardType,
    pub shard_state: ShardState,
    /// Position of the last record written in the shard's mrecordlog queue.
    pub replication_position_inclusive: Position,
    /// Position up to which the shard has been truncated.
    pub truncation_position_inclusive: Position,
    pub new_records_tx: watch::Sender<()>,
    pub new_records_rx: watch::Receiver<()>,
}

impl IngesterShard {
    pub fn new_primary(
        follower_id: NodeId,
        shard_state: ShardState,
        replication_position_inclusive: Position,
        truncation_position_inclusive: Position,
    ) -> Self {
        let (new_records_tx, new_records_rx) = watch::channel(());
        Self {
            shard_type: IngesterShardType::Primary { follower_id },
            shard_state,
            replication_position_inclusive,
            truncation_position_inclusive,
            new_records_tx,
            new_records_rx,
        }
    }

    pub fn new_replica(
        leader_id: NodeId,
        shard_state: ShardState,
        replication_position_inclusive: Position,
        truncation_position_inclusive: Position,
    ) -> Self {
        let (new_records_tx, new_records_rx) = watch::channel(());
        Self {
            shard_type: IngesterShardType::Replica { leader_id },
            shard_state,
            replication_position_inclusive,
            truncation_position_inclusive,
            new_records_tx,
            new_records_rx,
        }
    }

    pub fn new_solo(
        shard_state: ShardState,
        replication_position_inclusive: Position,
        truncation_position_inclusive: Position,
    ) -> Self {
        let (new_records_tx, new_records_rx) = watch::channel(());
        Self {
            shard_type: IngesterShardType::Solo,
            shard_state,
            replication_position_inclusive,
            truncation_position_inclusive,
            new_records_tx,
            new_records_rx,
        }
    }

    pub fn is_replica(&self) -> bool {
        matches!(self.shard_type, IngesterShardType::Replica { .. })
    }

    pub fn notify_new_records(&mut self) {
        // `new_records_tx` is guaranteed to be open because `self` also holds a receiver.
        self.new_records_tx
            .send(())
            .expect("channel should be open");
    }

    pub fn set_replication_position_inclusive(&mut self, replication_position_inclusive: Position) {
        if self.replication_position_inclusive == replication_position_inclusive {
            return;
        }
        self.replication_position_inclusive = replication_position_inclusive;
        self.notify_new_records();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl IngesterShard {
        #[track_caller]
        pub fn assert_is_solo(&self) {
            assert!(matches!(self.shard_type, IngesterShardType::Solo { .. }))
        }

        #[track_caller]
        pub fn assert_is_primary(&self) {
            assert!(matches!(self.shard_type, IngesterShardType::Primary { .. }))
        }

        #[track_caller]
        pub fn assert_is_replica(&self) {
            assert!(matches!(self.shard_type, IngesterShardType::Replica { .. }))
        }

        #[track_caller]
        pub fn assert_is_open(&self) {
            assert!(self.shard_state.is_open())
        }

        #[track_caller]
        pub fn assert_is_closed(&self) {
            assert!(self.shard_state.is_closed())
        }

        #[track_caller]
        pub fn assert_replication_position(
            &self,
            expected_replication_position: impl Into<Position>,
        ) {
            let expected_replication_position = expected_replication_position.into();

            assert_eq!(
                self.replication_position_inclusive, expected_replication_position,
                "expected replication position at `{:?}`, got `{:?}`",
                expected_replication_position, self.replication_position_inclusive
            );
        }

        #[track_caller]
        pub fn assert_truncation_position(
            &self,
            expected_truncation_position: impl Into<Position>,
        ) {
            let expected_truncation_position = expected_truncation_position.into();

            assert_eq!(
                self.truncation_position_inclusive, expected_truncation_position,
                "expected truncation position at `{:?}`, got `{:?}`",
                expected_truncation_position, self.truncation_position_inclusive
            );
        }
    }

    #[test]
    fn test_new_primary_shard() {
        let primary_shard = IngesterShard::new_primary(
            "test-follower".into(),
            ShardState::Closed,
            Position::from(42u64),
            Position::Eof,
        );
        assert!(matches!(
            primary_shard.shard_type,
            IngesterShardType::Primary { .. }
        ));
        assert_eq!(primary_shard.shard_state, ShardState::Closed);
        assert_eq!(primary_shard.truncation_position_inclusive, Position::Eof);
        assert_eq!(
            primary_shard.replication_position_inclusive,
            Position::from(42u64)
        );
    }

    #[test]
    fn test_new_replica_shard() {
        let solo_shard = IngesterShard::new_solo(ShardState::Closed, 42u64.into(), Position::Eof);
        assert_eq!(solo_shard.shard_type, IngesterShardType::Solo);
        assert_eq!(solo_shard.shard_state, ShardState::Closed);
        assert_eq!(solo_shard.truncation_position_inclusive, Position::Eof);
        assert_eq!(
            solo_shard.replication_position_inclusive,
            Position::from(42u64)
        );
    }

    #[test]
    fn test_new_solo_shard() {
        let solo_shard =
            IngesterShard::new_solo(ShardState::Closed, Position::from(42u64), Position::Eof);
        assert_eq!(solo_shard.shard_type, IngesterShardType::Solo);
        assert_eq!(solo_shard.shard_state, ShardState::Closed);
        assert_eq!(solo_shard.truncation_position_inclusive, Position::Eof);
        assert_eq!(
            solo_shard.replication_position_inclusive,
            Position::from(42u64)
        );
    }
}
