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

use std::fmt;

use quickwit_proto::ingest::ShardState;
use quickwit_proto::types::{NodeId, Position};
use tokio::sync::watch;

/// Shard hosted on a leader node and replicated on a follower node.
pub(super) struct PrimaryShard {
    pub follower_id: NodeId,
    pub shard_state: ShardState,
    /// Position of the last record written in the shard's mrecordlog queue.
    pub replication_position_inclusive: Position,
    pub new_records_tx: watch::Sender<()>,
    pub new_records_rx: watch::Receiver<()>,
}

impl fmt::Debug for PrimaryShard {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PrimaryShard")
            .field("follower_id", &self.follower_id)
            .field("shard_state", &self.shard_state)
            .finish()
    }
}

impl PrimaryShard {
    pub fn new(follower_id: NodeId) -> Self {
        let (new_records_tx, new_records_rx) = watch::channel(());
        Self {
            follower_id,
            shard_state: ShardState::Open,
            replication_position_inclusive: Position::Beginning,
            new_records_tx,
            new_records_rx,
        }
    }
}

/// Shard hosted on a follower node and replicated from a leader node.
pub(super) struct ReplicaShard {
    pub leader_id: NodeId,
    pub shard_state: ShardState,
    /// Position of the last record written in the shard's mrecordlog queue.
    pub replication_position_inclusive: Position,
    pub new_records_tx: watch::Sender<()>,
    pub new_records_rx: watch::Receiver<()>,
}

impl fmt::Debug for ReplicaShard {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ReplicaShard")
            .field("leader_id", &self.leader_id)
            .field("shard_state", &self.shard_state)
            .finish()
    }
}

impl ReplicaShard {
    pub fn new(leader_id: NodeId) -> Self {
        let (new_records_tx, new_records_rx) = watch::channel(());
        Self {
            leader_id,
            shard_state: ShardState::Open,
            replication_position_inclusive: Position::Beginning,
            new_records_tx,
            new_records_rx,
        }
    }
}

/// A shard hosted on a single node when the replication factor is set to 1. When a shard is
/// recovered after a node failure, it is always recreated as a solo shard in closed state.
pub(super) struct SoloShard {
    pub shard_state: ShardState,
    /// Position of the last record written in the shard's mrecordlog queue.
    pub replication_position_inclusive: Position,
    pub new_records_tx: watch::Sender<()>,
    pub new_records_rx: watch::Receiver<()>,
}

impl fmt::Debug for SoloShard {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("SoloShard")
            .field("shard_state", &self.shard_state)
            .finish()
    }
}

impl SoloShard {
    pub fn new(shard_state: ShardState, replication_position_inclusive: Position) -> Self {
        let (new_records_tx, new_records_rx) = watch::channel(());
        Self {
            shard_state,
            replication_position_inclusive,
            new_records_tx,
            new_records_rx,
        }
    }
}

#[derive(Debug)]
pub(super) enum IngesterShard {
    /// A primary shard hosted on a leader and replicated on a follower.
    Primary(PrimaryShard),
    /// A replica shard hosted on a follower.
    Replica(ReplicaShard),
    /// A shard hosted on a single node when the replication factor is set to 1.
    Solo(SoloShard),
}

impl IngesterShard {
    pub fn is_closed(&self) -> bool {
        match self {
            IngesterShard::Primary(primary_shard) => &primary_shard.shard_state,
            IngesterShard::Replica(replica_shard) => &replica_shard.shard_state,
            IngesterShard::Solo(solo_shard) => &solo_shard.shard_state,
        }
        .is_closed()
    }

    pub fn close(&mut self) {
        let shard_state = match self {
            IngesterShard::Primary(primary_shard) => &mut primary_shard.shard_state,
            IngesterShard::Replica(replica_shard) => &mut replica_shard.shard_state,
            IngesterShard::Solo(solo_shard) => &mut solo_shard.shard_state,
        };
        *shard_state = ShardState::Closed;
    }

    pub fn replication_position_inclusive(&self) -> Position {
        match self {
            IngesterShard::Primary(primary_shard) => &primary_shard.replication_position_inclusive,
            IngesterShard::Replica(replica_shard) => &replica_shard.replication_position_inclusive,
            IngesterShard::Solo(solo_shard) => &solo_shard.replication_position_inclusive,
        }
        .clone()
    }

    pub fn set_replication_position_inclusive(&mut self, replication_position_inclusive: Position) {
        if self.replication_position_inclusive() == replication_position_inclusive {
            return;
        }
        match self {
            IngesterShard::Primary(primary_shard) => {
                primary_shard.replication_position_inclusive = replication_position_inclusive;
            }
            IngesterShard::Replica(replica_shard) => {
                replica_shard.replication_position_inclusive = replication_position_inclusive;
            }
            IngesterShard::Solo(solo_shard) => {
                solo_shard.replication_position_inclusive = replication_position_inclusive;
            }
        };
        self.notify_new_records();
    }

    pub fn new_records_rx(&self) -> watch::Receiver<()> {
        match self {
            IngesterShard::Primary(primary_shard) => &primary_shard.new_records_rx,
            IngesterShard::Replica(replica_shard) => &replica_shard.new_records_rx,
            IngesterShard::Solo(solo_shard) => &solo_shard.new_records_rx,
        }
        .clone()
    }

    pub fn notify_new_records(&self) {
        match self {
            IngesterShard::Primary(primary_shard) => &primary_shard.new_records_tx,
            IngesterShard::Replica(replica_shard) => &replica_shard.new_records_tx,
            IngesterShard::Solo(solo_shard) => &solo_shard.new_records_tx,
        }
        .send(())
        .expect("channel should be open");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_solo_shard() {
        let solo_shard = SoloShard::new(ShardState::Closed, Position::from(42u64));
        assert_eq!(solo_shard.shard_state, ShardState::Closed);
        assert_eq!(
            solo_shard.replication_position_inclusive,
            Position::from(42u64)
        );
    }
}
