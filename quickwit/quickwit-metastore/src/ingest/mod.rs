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

// TODO: The shard API is declared in this crate to work around circular dependencies between the
// `quickwit-ingest` and `quickwit-metastore` crates. Find a way to declare the shard API in the
// `quickwit-ingest` crate and leave the implementation in this one.

use std::fmt;

use prost_types::Timestamp;
use quickwit_common::timestamp::TimestampExt;
use quickwit_common::{opt_contains, opt_exists};
use quickwit_proto::metastore_api::{
    ListShardsResponse as ProtoBufListShardsResponse, Shard as ProtoBufShard,
    ShardDelta as ProtoBufShardDelta, ShardState,
};
use quickwit_types::NodeId;
use serde::{Deserialize, Serialize};
use tonic::Status;

use crate::checkpoint::Position;

#[derive(Debug, Clone, thiserror::Error, Eq, PartialEq)]
#[error("{0}")]
pub struct ShardError(String);

/// Shard ID.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct ShardId(u64);

impl fmt::Display for ShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:0>12}", self.0)
    }
}

impl PartialEq<ShardId> for u64 {
    fn eq(&self, other: &ShardId) -> bool {
        self == &other.0
    }
}

impl From<u64> for ShardId {
    fn from(shard_id: u64) -> Self {
        Self(shard_id)
    }
}

impl Into<u64> for ShardId {
    fn into(self) -> u64 {
        self.0
    }
}

/// TODO: Replace with [`PartitionDelta`].
/// (`from_position`, `to_position`]
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ShardDelta {
    ///
    pub from_position: Position,
    ///
    pub to_position: Position,
}

impl fmt::Display for ShardDelta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "({}..{}]",
            self.from_position.as_str(),
            self.to_position.as_str()
        )
    }
}

impl TryFrom<ProtoBufShardDelta> for ShardDelta {
    type Error = Status;

    fn try_from(protobuf_shard_delta: ProtoBufShardDelta) -> Result<Self, Self::Error> {
        let from_position = protobuf_shard_delta
            .from_position
            .ok_or_else(|| Status::invalid_argument("The field `from_position` is required."))?
            .into();
        let to_position = protobuf_shard_delta
            .to_position
            .ok_or_else(|| Status::invalid_argument("The field `to_position` is required."))?
            .into();
        let shard_delta = Self {
            from_position,
            to_position,
        };
        Ok(shard_delta)
    }
}

///
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Shard {
    /// Index ID of the index this shard belongs to.
    pub index_id: String,
    /// Source ID of the source this shard belongs to.
    pub source_id: String,
    /// Shard ID.
    pub shard_id: ShardId,
    /// Node ID of the leader (primary writer) for this shard.
    pub leader_id: NodeId,
    /// Node ID of the follower (secondary writer) for this shard.
    pub follower_id: Option<NodeId>,
    /// Shard state, e.g. open, or closed.
    pub shard_state: ShardState,
    /// Start position of the shard.
    pub start_position: Position,
    /// End position of the shard.
    pub end_position: Option<Position>,
    /// Publish position of the shard.
    pub publish_position: Option<Position>,
    ///
    #[serde(with = "quickwit_common::timestamp_serde")]
    pub create_timestamp: Timestamp,
    ///
    #[serde(with = "quickwit_common::timestamp_opt_serde")]
    pub publish_timestamp: Option<Timestamp>,
    ///
    #[serde(with = "quickwit_common::timestamp_opt_serde")]
    pub close_timestamp: Option<Timestamp>,
}

impl Shard {
    /// Instantiates a new open shard.
    pub fn new(
        index_id: String,
        source_id: String,
        shard_id: ShardId,
        leader_id: NodeId,
        follower_id: Option<NodeId>,
        start_position: Option<Position>,
    ) -> Self {
        Self {
            index_id,
            source_id,
            shard_id,
            leader_id,
            follower_id,
            shard_state: ShardState::Open,
            start_position: start_position.unwrap_or_default(),
            end_position: None,
            publish_position: None,
            create_timestamp: Timestamp::utc_now(),
            publish_timestamp: None,
            close_timestamp: None,
        }
    }

    /// Attempts to apply a shard delta to the shard. Returns whether a mutation occurred.
    pub fn try_apply_delta(&mut self, shard_delta: ShardDelta) -> Result<bool, ShardError> {
        if self.start_position > shard_delta.from_position {
            return Err(ShardError(format!(
                "Failed to apply shard delta `{}` to shard `{}`: from position is less than start \
                 position.",
                shard_delta, self.shard_id
            )));
        }

        if opt_exists(&self.publish_position, |publish_position| {
            publish_position > &shard_delta.from_position
        }) {
            return Err(ShardError(format!(
                "Failed to apply shard delta `{}` to shard `{}`: from position is less than \
                 publish position.",
                shard_delta, self.shard_id
            )));
        }
        if opt_contains(&self.publish_position, &shard_delta.to_position) {
            return Ok(false);
        }
        self.publish_position = Some(shard_delta.to_position);
        self.publish_timestamp = Some(Timestamp::utc_now());
        Ok(true)
    }

    /// Closes the shard: producers can no longer write to it. Returns whether a mutation occurred.
    pub fn close(&mut self, end_position: Position) -> Result<bool, ShardError> {
        if self.shard_state == ShardState::Open {
            if self.start_position > end_position {
                return Err(ShardError(format!(
                    "Failed to close shard `{}`: start position is greater than end position.",
                    self.shard_id
                )));
            }
            self.shard_state = ShardState::Closed;
            self.end_position = Some(end_position);
            self.close_timestamp = Some(Timestamp::utc_now());
            return Ok(true);
        }
        if self.shard_state == ShardState::Closed
            && !opt_contains(&self.end_position, &end_position)
        {
            return Err(ShardError(format!(
                "Failed to close shard `{}`: shard is already closed but end positions do not \
                 match.",
                self.shard_id
            )));
        }
        Ok(false)
    }

    /// Deletes the shard.
    pub fn delete(&self) -> Result<(), ShardError> {
        if self.shard_state == ShardState::Open {
            return Err(ShardError(format!(
                "Failed to delete shard `{}`: shard is open.",
                self.shard_id
            )));
        }
        if self.end_position != self.publish_position {
            return Err(ShardError(format!(
                "Failed to delete shard `{}`: shard contains unpublished documents.",
                self.shard_id
            )));
        }
        Ok(())
    }
}

impl TryFrom<ProtoBufShard> for Shard {
    type Error = Status;

    fn try_from(protobuf_shard: ProtoBufShard) -> Result<Self, Self::Error> {
        let shard_state =
            ShardState::from_i32(protobuf_shard.shard_state).unwrap_or(ShardState::Unknown);
        let start_position = protobuf_shard
            .start_position
            .ok_or_else(|| Status::invalid_argument("The field `start_position` is required."))?
            .into();
        let create_timestamp = protobuf_shard
            .create_timestamp
            .ok_or_else(|| Status::invalid_argument("The field `create_timestamp` is required."))?
            .into();
        let shard = Self {
            index_id: protobuf_shard.index_id,
            source_id: protobuf_shard.source_id,
            shard_id: protobuf_shard.shard_id.into(),
            leader_id: protobuf_shard.leader_id.into(),
            follower_id: protobuf_shard
                .follower_id
                .map(|follower_id| follower_id.into()),
            shard_state,
            start_position,
            end_position: protobuf_shard.end_position.map(|position| position.into()),
            publish_position: protobuf_shard
                .publish_position
                .map(|position| position.into()),
            create_timestamp,
            publish_timestamp: protobuf_shard.publish_timestamp,
            close_timestamp: protobuf_shard.close_timestamp,
        };
        Ok(shard)
    }
}

impl Into<ProtoBufShard> for Shard {
    fn into(self) -> ProtoBufShard {
        let start_position = Some(self.start_position.into());
        let end_position = self.end_position.map(|position| position.into());
        let publish_position = self.publish_position.map(|position| position.into());
        let create_timestamp = Some(self.create_timestamp);
        ProtoBufShard {
            index_id: self.index_id,
            source_id: self.source_id,
            shard_id: self.shard_id.into(),
            leader_id: self.leader_id.into(),
            follower_id: self.follower_id.map(|follower_id| follower_id.into()),
            shard_state: self.shard_state as i32,
            start_position,
            end_position,
            publish_position,
            create_timestamp,
            publish_timestamp: self.publish_timestamp,
            close_timestamp: self.close_timestamp,
        }
    }
}

pub struct ListShardsResponse {
    pub shards: Vec<Shard>,
    pub next_shard_id: ShardId,
}

impl Into<ProtoBufListShardsResponse> for ListShardsResponse {
    fn into(self) -> ProtoBufListShardsResponse {
        let shards = self
            .shards
            .into_iter()
            .map(|shard| shard.into())
            .collect::<Vec<_>>();
        let next_shard_id = self.next_shard_id.into();
        ProtoBufListShardsResponse {
            shards,
            next_shard_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_id() {
        let shard_id = ShardId::from(42u64);
        assert_eq!(shard_id.to_string(), "000000000042");
    }

    #[test]
    fn test_try_apply_delta() {
        let mut shard = Shard::new(
            "index_id".to_string(),
            "source_id".to_string(),
            ShardId::from(0),
            "leader_id".into(),
            Some("follower_id".into()),
            Some(Position::from(42u64)),
        );
        // We cannot apply a shard delta before the start position.
        shard
            .try_apply_delta(ShardDelta {
                from_position: Position::from(12u64),
                to_position: Position::from(42u64),
            })
            .unwrap_err();
        assert_eq!(shard.publish_position, None);
        assert_eq!(shard.publish_timestamp, None);

        // We can apply a delta at the start position.
        shard
            .try_apply_delta(ShardDelta {
                from_position: Position::from(42u64),
                to_position: Position::from(42u64),
            })
            .unwrap();
        assert_eq!(shard.publish_position, Some(Position::from(42u64)));

        // We can apply a delta after the start position.
        shard
            .try_apply_delta(ShardDelta {
                from_position: Position::from(42u64),
                to_position: Position::from(43u64),
            })
            .unwrap();
        assert_eq!(shard.publish_position, Some(Position::from(43u64)));
    }

    #[test]
    fn test_close_shard() {
        let mut shard = Shard::new(
            "index_id".to_string(),
            "source_id".to_string(),
            ShardId::from(0),
            "leader_id".into(),
            Some("follower_id".into()),
            Some(Position::from(42u64)),
        );
        // We cannot close a shard before its start position.
        shard.close(Position::from(12u64)).unwrap_err();
        assert_eq!(shard.shard_state, ShardState::Open);
        assert_eq!(shard.end_position, None);

        shard.close(Position::from(42u64)).unwrap();
        assert_eq!(shard.shard_state, ShardState::Closed);
        assert_eq!(shard.end_position, Some(Position::from(42u64)));

        let close_timestamp = shard.close_timestamp.clone().unwrap();
        assert_eq!(
            close_timestamp.as_millis(),
            Timestamp::utc_now().as_millis()
        );
        // We can close a shard multiple times at the same end position.
        shard.close(Position::from(42u64)).unwrap();
        assert_eq!(shard.close_timestamp, Some(close_timestamp));

        shard.close(Position::from(1337u64)).unwrap_err();
    }

    #[test]
    fn test_delete_shard() {
        let mut shard = Shard::new(
            "index_id".to_string(),
            "source_id".to_string(),
            ShardId::from(0),
            "leader_id".into(),
            Some("follower_id".into()),
            Some(Position::from(42u64)),
        );
        // We cannot delete an open shard.
        shard.delete().unwrap_err();

        // We cannot delete a shard with unpublished documents.
        shard.close(Position::from(1337u64)).unwrap();
        shard.delete().unwrap_err();

        shard.publish_position = Some(Position::from(1337u64));
        shard.delete().unwrap();
    }
}
