// Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use serde::ser::SerializeMap;
use serde::Deserialize;
use serde::Serialize;
use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use thiserror::Error;
use tracing::info;
use tracing::warn;

/// PartitionId identifies a partition for a given source.
#[derive(Debug, Default, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct PartitionId(pub Arc<String>);

impl From<String> for PartitionId {
    fn from(partition_id_str: String) -> Self {
        PartitionId(Arc::new(partition_id_str))
    }
}

impl From<&str> for PartitionId {
    fn from(partition_id_str: &str) -> Self {
        PartitionId(Arc::new(partition_id_str.to_string()))
    }
}

impl From<u16> for PartitionId {
    fn from(partition_id: u16) -> Self {
        let partition_id_str = format!("{:0>5}", partition_id);
        PartitionId(Arc::new(partition_id_str))
    }
}

impl From<u64> for PartitionId {
    fn from(partition_id: u64) -> Self {
        let partition_id_str = format!("{:0>20}", partition_id);
        PartitionId(Arc::new(partition_id_str))
    }
}

/// Marks a position within a specific partition of a source.
///
/// The nature of the position may very depending on the source.
/// Each source needs to encode it as a String in such a way that
/// the lexicographical order matches the natural order of the
/// position.
///
/// For instance, for u64 a 0-left-padded decimal representation
/// can be used. Alternatively a base64 representation of their
/// Big Endian representation can be used.
///
/// The empty string can be used to represent the beginning of the source,
/// if no position makes sense. It can be built via `Position::default()`.
#[derive(Debug, Default, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct Position(pub Arc<String>);

impl Position {
    /// String representation of the position
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<u64> for Position {
    fn from(pos: u64) -> Self {
        let pos_str = format!("{:0>20}", pos);
        Position(Arc::new(pos_str))
    }
}

impl From<String> for Position {
    fn from(position_str: String) -> Self {
        Position(Arc::new(position_str))
    }
}

impl<'a> From<&'a str> for Position {
    fn from(position_str: &'a str) -> Self {
        Position(Arc::new(position_str.to_string()))
    }
}

/// A checkpoint is a map of the last processed position for every partition.
///
/// If a partition is missing, it implicitely means that none of its message
/// has been processed.
#[derive(Default, Clone)]
pub struct Checkpoint {
    per_partition: BTreeMap<PartitionId, Position>,
}

impl Serialize for Checkpoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.per_partition.len()))?;
        for (partition, position) in &self.per_partition {
            map.serialize_entry(&*partition.0, &*position.0)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for Checkpoint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let string_to_string_map: BTreeMap<String, String> = BTreeMap::deserialize(deserializer)?;
        let per_partition: BTreeMap<PartitionId, Position> = string_to_string_map
            .into_iter()
            .map(|(partition_id, position)| {
                (PartitionId::from(partition_id), Position::from(position))
            })
            .collect();
        Ok(Checkpoint { per_partition })
    }
}

/// Error returned when trying to apply a checkpoint delta to a checkpoint that is not
/// compatible. ie: the checkpoint delta starts from a point anterior to
/// the checkpoint.
#[derive(Error, Debug)]
#[error("IncompatibleChkpt at partition: {partition_id:?} cur_pos:{current_position:?} delta_pos:{delta_position_from:?}")]
pub struct IncompatibleCheckpoint {
    /// One PartitionId for which the incompatibility has been detected.
    pub partition_id: PartitionId,
    /// The current position within this partition.
    pub current_position: Position,
    /// The origin position for the delta.
    pub delta_position_from: Position,
}

impl Checkpoint {
    /// Returns an iterator with the reached position for each given partition.
    pub fn iter(&self) -> impl Iterator<Item = (PartitionId, Position)> + '_ {
        self.per_partition
            .iter()
            .map(|(partition_id, position)| (partition_id.clone(), position.clone()))
    }

    fn check_compatibility(&self, delta: &CheckpointDelta) -> Result<(), IncompatibleCheckpoint> {
        info!(delta=?delta, checkpoint=?self);
        for (delta_partition, delta_position) in &delta.per_partition {
            let position = if let Some(position) = self.per_partition.get(delta_partition) {
                position
            } else {
                continue;
            };
            match position.cmp(&delta_position.from) {
                Ordering::Equal => {}
                Ordering::Less => {
                    warn!(cur_pos=?position, delta_pos_from=?delta_position.from,partition=?delta_partition, "Some positions were skipped.");
                }
                Ordering::Greater => {
                    return Err(IncompatibleCheckpoint {
                        partition_id: delta_partition.clone(),
                        current_position: position.clone(),
                        delta_position_from: delta_position.from.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Try and apply a delta.
    ///
    /// We accept a delta as long as it comes after the current checkpoint,
    /// for all partitions.
    ///
    /// We accept a delta that is not perfected chained after a checkpoint,
    /// as gaps may happen. For instance, assuming a Kafka source, if the indexing
    /// pipeline is down for more than the retention period.
    ///
    ///    |    Checkpoint & Delta        | Outcome                     |
    ///    |------------------------------|-----------------------------|
    ///    |  (..a] (b..c] with a = b     | Compatible                  |
    ///    |  (..a] (b..c] with b > a     | Compatible                  |
    ///    |  (..a] (b..c]  with b < a    | Incompatible                |
    ///
    /// If the delta is compatible, returns an error without modifying the original checkpoint.
    pub fn try_apply_delta(
        &mut self,
        delta: CheckpointDelta,
    ) -> Result<(), IncompatibleCheckpoint> {
        self.check_compatibility(&delta)?;
        for (partition_id, partition_position) in delta.per_partition {
            self.per_partition
                .insert(partition_id, partition_position.to);
        }
        Ok(())
    }
}

impl fmt::Debug for Checkpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Ckpt(")?;
        for (i, (partition_id, position)) in self.per_partition.iter().enumerate() {
            f.write_str(&partition_id.0)?;
            f.write_str(":")?;
            f.write_str(&position.0)?;
            let is_last = i == self.per_partition.len() - 1;
            if !is_last {
                f.write_str(" ")?;
            }
        }
        f.write_str(")")?;
        Ok(())
    }
}

/// A partition delta represents an interval (from, to] over a partition of a source.
#[derive(Debug, Clone, Eq, PartialEq)]
struct PartitionDelta {
    pub from: Position,
    pub to: Position,
}

/// A checkpoint delta represents a checkpoint update.
///
/// It is shipped as part of a split to convey the update
/// that should be applied to the index checkpoint once the split
/// is published.
///
/// The `CheckpointDelta` not only ships for each
/// partition not only a new position, but also an expected
/// `from` position. This makes it possible to defensively check that
/// we are not trying to add records to the index that were already indexed.
#[derive(Default, Clone, Eq, PartialEq)]
pub struct CheckpointDelta {
    per_partition: BTreeMap<PartitionId, PartitionDelta>,
}

impl fmt::Debug for CheckpointDelta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("∆(")?;
        for (i, (partition_id, partition_delta)) in self.per_partition.iter().enumerate() {
            write!(
                f,
                "{}:({}..{}]",
                partition_id.0, partition_delta.from.0, partition_delta.to.0
            )?;
            if i != self.per_partition.len() - 1 {
                f.write_str(" ")?;
            }
        }
        f.write_str(")")?;
        Ok(())
    }
}

impl From<Range<u64>> for CheckpointDelta {
    fn from(range: Range<u64>) -> Self {
        let mut delta = CheckpointDelta::default();
        // Checkpoint delta are expressed as (fromt, to] intervals while ranges
        // are [start, end) intervals
        let from_position = if range.start == 0 {
            Position::default()
        } else {
            Position::from(range.start as u64 - 1u64)
        };
        let to_position = if range.end == 0 {
            Position::default()
        } else {
            Position::from(range.end as u64 - 1u64)
        };
        delta.add_partition(PartitionId::default(), from_position, to_position);
        delta
    }
}

impl CheckpointDelta {
    /// Record a `(from, to]` position interval for a given partition..
    pub fn add_partition(
        &mut self,
        partition_id: PartitionId,
        from_position: Position,
        to_position: Position,
    ) {
        assert!(from_position <= to_position);
        let partition_delta = PartitionDelta {
            from: from_position,
            to: to_position,
        };
        self.per_partition.insert(partition_id, partition_delta);
    }

    /// Extend the given checkpoint_delta with another checkpoint delta.
    ///
    /// Contrary to checkpoint update, the two deltas here need to chain perfectly.
    pub fn add(&mut self, delta: CheckpointDelta) -> Result<(), IncompatibleCheckpoint> {
        for (partition_id, partition_delta) in delta.per_partition {
            let entry = self.per_partition.entry(partition_id.clone());
            match entry {
                Entry::Occupied(mut occupied_entry) => {
                    if occupied_entry.get().to == partition_delta.from {
                        occupied_entry.insert(partition_delta);
                    } else {
                        return Err(IncompatibleCheckpoint {
                            partition_id,
                            current_position: occupied_entry.get().to.clone(),
                            delta_position_from: partition_delta.from,
                        });
                    }
                }
                Entry::Vacant(vacant_entry) => {
                    vacant_entry.insert(partition_delta);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_from_range() {
        let checkpoint_delta = CheckpointDelta::from(0..3);
        assert_eq!(
            format!("{:?}", checkpoint_delta),
            "∆(:(..00000000000000000002])"
        );
        let checkpoint_delta = CheckpointDelta::from(1..4);
        assert_eq!(
            format!("{:?}", checkpoint_delta),
            "∆(:(00000000000000000000..00000000000000000003])"
        );
    }

    #[test]
    fn test_checkpoint_simple() {
        let mut checkpoint = Checkpoint::default();
        assert_eq!(format!("{:?}", checkpoint), "Ckpt()");
        let delta1 = {
            let mut delta = CheckpointDelta::default();
            delta.add_partition(
                PartitionId::from("a"),
                Position::from(123u64),
                Position::from(128u64),
            );
            delta.add_partition(
                PartitionId::from("b"),
                Position::from(60002u64),
                Position::from(60187u64),
            );
            delta
        };
        assert!(checkpoint.try_apply_delta(delta1).is_ok());
        assert_eq!(
            format!("{:?}", checkpoint),
            "Ckpt(a:00000000000000000128 b:00000000000000060187)"
        );
    }

    #[test]
    fn test_partially_incompatible_does_not_update() {
        let mut checkpoint = Checkpoint::default();
        let delta1 = {
            let mut delta = CheckpointDelta::default();
            delta.add_partition(
                PartitionId::from("a"),
                Position::from("00123"),
                Position::from("00128"),
            );
            delta.add_partition(
                PartitionId::from("b"),
                Position::from("60002"),
                Position::from("60187"),
            );
            delta
        };
        assert!(checkpoint.try_apply_delta(delta1).is_ok());
        let delta2 = {
            let mut delta = CheckpointDelta::default();
            delta.add_partition(
                PartitionId::from("a"),
                Position::from("00128"),
                Position::from("00128"),
            );
            delta.add_partition(
                PartitionId::from("b"),
                Position::from("50099"),
                Position::from("60002"),
            );
            delta
        };
        assert!(matches!(
            checkpoint.try_apply_delta(delta2),
            Err(IncompatibleCheckpoint { .. })
        ));
        // checkpoint was unchanged
        assert_eq!(format!("{:?}", checkpoint), "Ckpt(a:00128 b:60187)");
    }

    #[test]
    fn test_adding_new_partition() {
        let mut checkpoint = Checkpoint::default();
        let delta1 = {
            let mut delta = CheckpointDelta::default();
            delta.add_partition(
                PartitionId::from("a"),
                Position::from("00123"),
                Position::from("00128"),
            );
            delta.add_partition(
                PartitionId::from("b"),
                Position::from("60002"),
                Position::from("60187"),
            );
            delta
        };
        assert!(checkpoint.try_apply_delta(delta1).is_ok());
        let delta3 = {
            let mut delta = CheckpointDelta::default();
            delta.add_partition(
                PartitionId::from("b"),
                Position::from("60187"),
                Position::from("60190"),
            );
            delta.add_partition(
                PartitionId::from("c"),
                Position::from("20001"),
                Position::from("20008"),
            );
            delta
        };
        assert!(checkpoint.try_apply_delta(delta3).is_ok());
        assert_eq!(format!("{:?}", checkpoint), "Ckpt(a:00128 b:60190 c:20008)");
    }

    #[test]
    fn test_position_u64() {
        let pos = Position::from(4u64);
        assert_eq!(pos.as_str(), "00000000000000000004");
    }
}
