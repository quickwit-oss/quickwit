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

use std::cmp;

use quickwit_proto::ingest::ShardState;
use quickwit_proto::NodeId;
use tokio::sync::watch;

#[derive(Debug, Clone, Eq, PartialEq)]
pub(super) struct ShardStatus {
    /// Current state of the shard.
    pub shard_state: ShardState,
    /// Position up to which indexers have indexed and published the records stored in the shard.
    pub publish_position_inclusive: Position,
    /// Position up to which the follower has acknowledged replication of the records written in
    /// its log.
    pub replication_position_inclusive: Position,
}

impl Default for ShardStatus {
    fn default() -> Self {
        Self {
            shard_state: ShardState::Open,
            publish_position_inclusive: Position::default(),
            replication_position_inclusive: Position::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(super) enum Position {
    #[default]
    Beginning,
    Offset(u64),
}

impl Position {
    pub fn offset(&self) -> Option<u64> {
        match self {
            Position::Beginning => None,
            Position::Offset(offset) => Some(*offset),
        }
    }
}

impl PartialEq<u64> for Position {
    fn eq(&self, other: &u64) -> bool {
        match self {
            Position::Beginning => false,
            Position::Offset(offset) => offset == other,
        }
    }
}

impl PartialOrd<u64> for Position {
    fn partial_cmp(&self, other: &u64) -> Option<cmp::Ordering> {
        match self {
            Position::Beginning => Some(cmp::Ordering::Less),
            Position::Offset(offset) => offset.partial_cmp(other),
        }
    }
}

impl From<u64> for Position {
    fn from(offset: u64) -> Self {
        Position::Offset(offset)
    }
}

impl From<Option<u64>> for Position {
    fn from(offset_opt: Option<u64>) -> Self {
        match offset_opt {
            Some(offset) => Position::Offset(offset),
            None => Position::Beginning,
        }
    }
}

impl From<String> for Position {
    fn from(position_str: String) -> Self {
        if position_str.is_empty() {
            Position::Beginning
        } else {
            let offset = position_str
                .parse::<u64>()
                .expect("position should be a u64");
            Position::Offset(offset)
        }
    }
}

/// Records the state of a primary shard managed by a leader.
pub(super) struct PrimaryShard {
    /// Node ID of the ingester on which the replica shard is hosted. `None` if the replication
    /// factor is 1.
    pub follower_id_opt: Option<NodeId>,
    /// Current state of the shard.
    pub shard_state: ShardState,
    /// Position up to which indexers have indexed and published the data stored in the shard.
    /// It is updated asynchronously in a best effort manner by the indexers and indicates the
    /// position up to which the log can be safely truncated. When the shard is closed, the
    /// publish position has reached the replication position, and the deletion grace period has
    /// passed, the shard can be safely deleted (see [`GcTask`] more details about the deletion
    /// logic).
    pub publish_position_inclusive: Position,
    /// Position up to which the leader has written records in its log.
    pub primary_position_inclusive: Position,
    /// Position up to which the follower has acknowledged replication of the records written in
    /// its log.
    pub replica_position_inclusive_opt: Option<Position>,
    /// Channel to notify readers that new records have been written to the shard.
    pub shard_status_tx: watch::Sender<ShardStatus>,
    pub shard_status_rx: watch::Receiver<ShardStatus>,
}

impl PrimaryShard {
    pub fn is_gc_candidate(&self) -> bool {
        self.shard_state.is_closed()
            && self.publish_position_inclusive >= self.primary_position_inclusive
    }

    pub fn set_publish_position_inclusive(
        &mut self,
        publish_position_inclusive: impl Into<Position>,
    ) {
        self.publish_position_inclusive = publish_position_inclusive.into();
        self.shard_status_tx.send_modify(|shard_status| {
            shard_status.publish_position_inclusive = self.publish_position_inclusive;
        });
    }

    pub fn set_primary_position_inclusive(
        &mut self,
        primary_position_inclusive: impl Into<Position>,
    ) {
        self.primary_position_inclusive = primary_position_inclusive.into();

        // Notify readers if the replication factor is 1.
        if self.follower_id_opt.is_none() {
            self.shard_status_tx.send_modify(|shard_status| {
                shard_status.replication_position_inclusive = self.primary_position_inclusive
            })
        }
    }

    pub fn set_replica_position_inclusive(
        &mut self,
        replica_position_inclusive: impl Into<Position>,
    ) {
        assert!(self.follower_id_opt.is_some());

        let replica_position_inclusive = replica_position_inclusive.into();
        self.replica_position_inclusive_opt = Some(replica_position_inclusive);

        self.shard_status_tx.send_modify(|shard_status| {
            shard_status.replication_position_inclusive = replica_position_inclusive
        })
    }
}

/// Records the state of a replica shard managed by a follower. See [`PrimaryShard`] for more
/// details about the fields.
pub(super) struct ReplicaShard {
    pub leader_id: NodeId,
    pub shard_state: ShardState,
    pub publish_position_inclusive: Position,
    pub replica_position_inclusive: Position,
    pub shard_status_tx: watch::Sender<ShardStatus>,
    pub shard_status_rx: watch::Receiver<ShardStatus>,
}

impl ReplicaShard {
    pub fn is_gc_candidate(&self) -> bool {
        self.shard_state.is_closed()
            && self.publish_position_inclusive >= self.replica_position_inclusive
    }

    pub fn set_publish_position_inclusive(
        &mut self,
        publish_position_inclusive: impl Into<Position>,
    ) {
        self.publish_position_inclusive = publish_position_inclusive.into();
        self.shard_status_tx.send_modify(|shard_status| {
            shard_status.publish_position_inclusive = self.publish_position_inclusive;
        });
    }

    pub fn set_replica_position_inclusive(
        &mut self,
        replica_position_inclusive: impl Into<Position>,
    ) {
        self.replica_position_inclusive = replica_position_inclusive.into();
        self.shard_status_tx.send_modify(|shard_status| {
            shard_status.replication_position_inclusive = self.replica_position_inclusive
        });
    }
}

#[cfg(test)]
impl PrimaryShard {
    pub(crate) fn for_test(
        follower_id_opt: Option<&str>,
        shard_state: ShardState,
        publish_position_inclusive: impl Into<Position>,
        primary_position_inclusive: impl Into<Position>,
        replica_position_inclusive_opt: Option<impl Into<Position>>,
    ) -> Self {
        let publish_position_inclusive: Position = publish_position_inclusive.into();
        let primary_position_inclusive: Position = primary_position_inclusive.into();
        let replica_position_inclusive_opt: Option<Position> =
            replica_position_inclusive_opt.map(Into::into);
        let replication_position_inclusive =
            replica_position_inclusive_opt.unwrap_or(primary_position_inclusive);

        let shard_status = ShardStatus {
            shard_state,
            publish_position_inclusive,
            replication_position_inclusive,
        };
        let (shard_status_tx, shard_status_rx) = watch::channel(shard_status);

        Self {
            follower_id_opt: follower_id_opt.map(Into::into),
            shard_state,
            publish_position_inclusive,
            primary_position_inclusive,
            replica_position_inclusive_opt,
            shard_status_tx,
            shard_status_rx,
        }
    }
}

#[cfg(test)]
impl ReplicaShard {
    pub(crate) fn for_test(
        leader_id: &str,
        shard_state: ShardState,
        publish_position_inclusive: impl Into<Position>,
        replica_position_inclusive: impl Into<Position>,
    ) -> Self {
        let publish_position_inclusive: Position = publish_position_inclusive.into();
        let replica_position_inclusive: Position = replica_position_inclusive.into();

        let shard_status = ShardStatus {
            shard_state,
            publish_position_inclusive,
            replication_position_inclusive: replica_position_inclusive,
        };
        let (shard_status_tx, shard_status_rx) = watch::channel(shard_status);

        Self {
            leader_id: leader_id.into(),
            shard_state,
            publish_position_inclusive,
            replica_position_inclusive,
            shard_status_tx,
            shard_status_rx,
        }
    }
}
