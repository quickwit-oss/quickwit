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

/// Status of a shard: state + position of the last record written.
pub(super) type ShardStatus = (ShardState, Position);

#[derive(Debug)]
pub(super) struct IngesterShard {
    pub shard_type: IngesterShardType,
    pub shard_state: ShardState,
    /// Position of the last record written in the shard's mrecordlog queue.
    pub replication_position_inclusive: Position,
    /// Position up to which the shard has been truncated.
    pub truncation_position_inclusive: Position,
    pub shard_status_tx: watch::Sender<ShardStatus>,
    pub shard_status_rx: watch::Receiver<ShardStatus>,
}

impl IngesterShard {
    pub fn new_primary(
        follower_id: NodeId,
        shard_state: ShardState,
        replication_position_inclusive: Position,
        truncation_position_inclusive: Position,
    ) -> Self {
        let shard_status = (shard_state, replication_position_inclusive.clone());
        let (shard_status_tx, shard_status_rx) = watch::channel(shard_status);
        Self {
            shard_type: IngesterShardType::Primary { follower_id },
            shard_state,
            replication_position_inclusive,
            truncation_position_inclusive,
            shard_status_tx,
            shard_status_rx,
        }
    }

    pub fn new_replica(
        leader_id: NodeId,
        shard_state: ShardState,
        replication_position_inclusive: Position,
        truncation_position_inclusive: Position,
    ) -> Self {
        let shard_status = (shard_state, replication_position_inclusive.clone());
        let (shard_status_tx, shard_status_rx) = watch::channel(shard_status);
        Self {
            shard_type: IngesterShardType::Replica { leader_id },
            shard_state,
            replication_position_inclusive,
            truncation_position_inclusive,
            shard_status_tx,
            shard_status_rx,
        }
    }

    pub fn new_solo(
        shard_state: ShardState,
        replication_position_inclusive: Position,
        truncation_position_inclusive: Position,
    ) -> Self {
        let shard_status = (shard_state, replication_position_inclusive.clone());
        let (shard_status_tx, shard_status_rx) = watch::channel(shard_status);
        Self {
            shard_type: IngesterShardType::Solo,
            shard_state,
            replication_position_inclusive,
            truncation_position_inclusive,
            shard_status_tx,
            shard_status_rx,
        }
    }

    pub fn is_indexed(&self) -> bool {
        self.shard_state.is_closed() && self.truncation_position_inclusive.is_eof()
    }

    pub fn is_replica(&self) -> bool {
        matches!(self.shard_type, IngesterShardType::Replica { .. })
    }

    pub fn follower_id_opt(&self) -> Option<&NodeId> {
        match &self.shard_type {
            IngesterShardType::Primary { follower_id } => Some(follower_id),
            IngesterShardType::Replica { .. } => None,
            IngesterShardType::Solo => None,
        }
    }

    pub fn notify_shard_status(&self) {
        // `shard_status_tx` is guaranteed to be open because `self` also holds a receiver.
        let shard_status = (
            self.shard_state,
            self.replication_position_inclusive.clone(),
        );
        self.shard_status_tx
            .send(shard_status)
            .expect("channel should be open");
    }

    pub fn set_replication_position_inclusive(&mut self, replication_position_inclusive: Position) {
        if self.replication_position_inclusive == replication_position_inclusive {
            return;
        }
        self.replication_position_inclusive = replication_position_inclusive;
        self.notify_shard_status();
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
        pub fn assert_replication_position(&self, expected_replication_position: Position) {
            assert_eq!(
                self.replication_position_inclusive, expected_replication_position,
                "expected replication position at `{:?}`, got `{:?}`",
                expected_replication_position, self.replication_position_inclusive
            );
        }

        #[track_caller]
        pub fn assert_truncation_position(&self, expected_truncation_position: Position) {
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
            Position::offset(42u64),
            Position::Beginning,
        );
        assert!(matches!(
            &primary_shard.shard_type,
            IngesterShardType::Primary { follower_id } if *follower_id == "test-follower"
        ));
        assert!(!primary_shard.is_replica());
        assert_eq!(primary_shard.shard_state, ShardState::Closed);
        assert_eq!(
            primary_shard.replication_position_inclusive,
            Position::offset(42u64)
        );
        assert_eq!(
            primary_shard.truncation_position_inclusive,
            Position::Beginning
        );
    }

    #[test]
    fn test_new_replica_shard() {
        let replica_shard = IngesterShard::new_replica(
            "test-leader".into(),
            ShardState::Closed,
            Position::offset(42u64),
            Position::Beginning,
        );
        assert!(matches!(
            &replica_shard.shard_type,
            IngesterShardType::Replica { leader_id } if *leader_id == "test-leader"
        ));
        assert!(replica_shard.is_replica());
        assert_eq!(replica_shard.shard_state, ShardState::Closed);
        assert_eq!(
            replica_shard.replication_position_inclusive,
            Position::offset(42u64)
        );
        assert_eq!(
            replica_shard.truncation_position_inclusive,
            Position::Beginning
        );
    }

    #[test]
    fn test_new_solo_shard() {
        let solo_shard = IngesterShard::new_solo(
            ShardState::Closed,
            Position::offset(42u64),
            Position::Beginning,
        );
        assert_eq!(solo_shard.shard_type, IngesterShardType::Solo);
        assert!(!solo_shard.is_replica());
        assert_eq!(solo_shard.shard_state, ShardState::Closed);
        assert_eq!(
            solo_shard.replication_position_inclusive,
            Position::offset(42u64)
        );
        assert_eq!(
            solo_shard.truncation_position_inclusive,
            Position::Beginning
        );
    }
}
