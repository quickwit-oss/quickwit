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

use std::sync::Arc;
use std::time::{Duration, Instant};

use quickwit_doc_mapper::DocMapper;
use quickwit_proto::ingest::ShardState;
use quickwit_proto::types::{NodeId, Position};
use tokio::sync::watch;

#[derive(Debug, Clone)]
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
    /// Whether the shard should be advertised to other nodes (routers) via gossip.
    ///
    /// Because shards  are created in multiple steps, (e.g., init shard on leader, create shard in
    /// metastore), we must receive a "signal" from the control plane confirming that a shard
    /// was successfully opened before advertising it. Currently, this confirmation comes in the
    /// form of `PersistRequest` or `FetchRequest`.
    pub is_advertisable: bool,
    /// Document mapper for the shard. Replica shards and closed solo shards do not have one.
    pub doc_mapper_opt: Option<Arc<DocMapper>>,
    /// Whether to validate documents in this shard. True if no preprocessing (VRL) will happen
    /// before indexing.
    pub validate_docs: bool,
    pub shard_status_tx: watch::Sender<ShardStatus>,
    pub shard_status_rx: watch::Receiver<ShardStatus>,
    /// Instant at which the shard was last written to.
    pub last_write_instant: Instant,
}

impl IngesterShard {
    pub fn new_primary(
        follower_id: NodeId,
        shard_state: ShardState,
        replication_position_inclusive: Position,
        truncation_position_inclusive: Position,
        doc_mapper: Arc<DocMapper>,
        now: Instant,
        validate_docs: bool,
    ) -> Self {
        let shard_status = (shard_state, replication_position_inclusive.clone());
        let (shard_status_tx, shard_status_rx) = watch::channel(shard_status);
        Self {
            shard_type: IngesterShardType::Primary { follower_id },
            shard_state,
            replication_position_inclusive,
            truncation_position_inclusive,
            is_advertisable: false,
            doc_mapper_opt: Some(doc_mapper),
            validate_docs,
            shard_status_tx,
            shard_status_rx,
            last_write_instant: now,
        }
    }

    pub fn new_replica(
        leader_id: NodeId,
        shard_state: ShardState,
        replication_position_inclusive: Position,
        truncation_position_inclusive: Position,
        now: Instant,
    ) -> Self {
        let shard_status = (shard_state, replication_position_inclusive.clone());
        let (shard_status_tx, shard_status_rx) = watch::channel(shard_status);
        Self {
            shard_type: IngesterShardType::Replica { leader_id },
            shard_state,
            replication_position_inclusive,
            truncation_position_inclusive,
            // This is irrelevant for replica shards since they are not advertised via gossip
            // anyway.
            is_advertisable: false,
            doc_mapper_opt: None,
            validate_docs: false,
            shard_status_tx,
            shard_status_rx,
            last_write_instant: now,
        }
    }

    pub fn new_solo(
        shard_state: ShardState,
        replication_position_inclusive: Position,
        truncation_position_inclusive: Position,
        doc_mapper_opt: Option<Arc<DocMapper>>,
        now: Instant,
        validate_docs: bool,
    ) -> Self {
        let shard_status = (shard_state, replication_position_inclusive.clone());
        let (shard_status_tx, shard_status_rx) = watch::channel(shard_status);
        Self {
            shard_type: IngesterShardType::Solo,
            shard_state,
            replication_position_inclusive,
            truncation_position_inclusive,
            is_advertisable: false,
            doc_mapper_opt,
            validate_docs,
            shard_status_tx,
            shard_status_rx,
            last_write_instant: now,
        }
    }

    pub fn follower_id_opt(&self) -> Option<&NodeId> {
        match &self.shard_type {
            IngesterShardType::Primary { follower_id, .. } => Some(follower_id),
            IngesterShardType::Replica { .. } => None,
            IngesterShardType::Solo => None,
        }
    }

    pub fn close(&mut self) {
        self.shard_state = ShardState::Closed;
        self.notify_shard_status();
    }

    pub fn is_closed(&self) -> bool {
        self.shard_state.is_closed()
    }

    pub fn is_open(&self) -> bool {
        self.shard_state.is_open()
    }

    pub fn is_idle(&self, now: Instant, idle_timeout: Duration) -> bool {
        now.duration_since(self.last_write_instant) >= idle_timeout
    }

    pub fn is_indexed(&self) -> bool {
        self.shard_state.is_closed() && self.truncation_position_inclusive.is_eof()
    }

    pub fn is_replica(&self) -> bool {
        matches!(self.shard_type, IngesterShardType::Replica { .. })
    }

    pub fn notify_shard_status(&self) {
        let shard_status = (
            self.shard_state,
            self.replication_position_inclusive.clone(),
        );
        // `shard_status_tx` is guaranteed to be open because `self` also holds a receiver.
        self.shard_status_tx
            .send(shard_status)
            .expect("channel should be open");
    }

    pub fn set_replication_position_inclusive(
        &mut self,
        replication_position_inclusive: Position,
        now: Instant,
    ) {
        if self.replication_position_inclusive == replication_position_inclusive {
            return;
        }
        self.replication_position_inclusive = replication_position_inclusive;
        self.last_write_instant = now;
        self.notify_shard_status();
    }
}

#[cfg(test)]
mod tests {
    use quickwit_config::{DocMapping, SearchSettings, build_doc_mapper};

    use super::*;

    impl IngesterShard {
        #[track_caller]
        pub fn assert_is_solo(&self) {
            assert!(matches!(self.shard_type, IngesterShardType::Solo))
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
        let doc_mapping: DocMapping = serde_json::from_str("{}").unwrap();
        let search_settings = SearchSettings::default();
        let doc_mapper = build_doc_mapper(&doc_mapping, &search_settings).unwrap();

        let primary_shard = IngesterShard::new_primary(
            "test-follower".into(),
            ShardState::Closed,
            Position::offset(42u64),
            Position::Beginning,
            doc_mapper,
            Instant::now(),
            true,
        );
        assert!(matches!(
            &primary_shard.shard_type,
            IngesterShardType::Primary { follower_id, .. } if *follower_id == "test-follower"
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
        assert!(!primary_shard.is_advertisable);
    }

    #[test]
    fn test_new_replica_shard() {
        let replica_shard = IngesterShard::new_replica(
            "test-leader".into(),
            ShardState::Closed,
            Position::offset(42u64),
            Position::Beginning,
            Instant::now(),
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
        assert!(!replica_shard.is_advertisable);
    }

    #[test]
    fn test_new_solo_shard() {
        let solo_shard = IngesterShard::new_solo(
            ShardState::Closed,
            Position::offset(42u64),
            Position::Beginning,
            None,
            Instant::now(),
            false,
        );
        solo_shard.assert_is_solo();
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
        assert!(!solo_shard.is_advertisable);
    }
}
