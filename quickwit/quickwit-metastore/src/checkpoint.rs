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

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fmt;
use std::iter::FromIterator;
use std::ops::Range;
use std::sync::Arc;

use quickwit_proto::types::{Position, SourceId};
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize};
/// Updates running indexing tasks in chitchat cluster state.
use thiserror::Error;
use tracing::{debug, warn};

/// A `PartitionId` uniquely identifies a partition for a given source.
#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct PartitionId(pub Arc<String>);

impl PartitionId {
    /// Returns the partition ID as a `i64`.
    pub fn as_i64(&self) -> Option<i64> {
        self.0.parse::<i64>().ok()
    }

    /// Returns the partition ID as a `u64`.
    pub fn as_u64(&self) -> Option<u64> {
        self.0.parse().ok()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PartitionId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", &self.0)
    }
}

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

impl From<u64> for PartitionId {
    fn from(partition_id: u64) -> Self {
        let partition_id_str = format!("{partition_id:0>20}");
        PartitionId(Arc::new(partition_id_str))
    }
}

impl From<i64> for PartitionId {
    fn from(partition_id: i64) -> Self {
        let partition_id_str = format!("{partition_id:0>20}");
        PartitionId(Arc::new(partition_id_str))
    }
}

/// A partition delta represents an interval (from, to] over a partition of a source.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PartitionDelta {
    pub from: Position,
    pub to: Position,
}

#[derive(Default, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexCheckpoint {
    #[serde(flatten)]
    per_source: BTreeMap<SourceId, SourceCheckpoint>,
}

impl fmt::Debug for IndexCheckpoint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let json = serde_json::to_string_pretty(&self).map_err(|_| fmt::Error)?;
        write!(f, "{json}")?;
        Ok(())
    }
}

impl From<BTreeMap<SourceId, SourceCheckpoint>> for IndexCheckpoint {
    fn from(per_source: BTreeMap<SourceId, SourceCheckpoint>) -> Self {
        Self { per_source }
    }
}

impl IndexCheckpoint {
    /// Updates a checkpoint in place. Returns whether the checkpoint was modified.
    ///
    /// If the checkpoint delta is not compatible with the
    /// current checkpoint, an error is returned, and the
    /// checkpoint remains unchanged.
    ///
    /// See [`SourceCheckpoint::try_apply_delta`] for more details.
    pub fn try_apply_delta(
        &mut self,
        delta: IndexCheckpointDelta,
    ) -> Result<bool, IncompatibleCheckpointDelta> {
        if delta.is_empty() {
            return Ok(false);
        }
        self.per_source
            .entry(delta.source_id)
            .or_default()
            .try_apply_delta(delta.source_delta)?;
        Ok(true)
    }

    /// Resets the checkpoint of the source identified by `source_id`. Returns whether a mutation
    /// occurred.
    pub(crate) fn reset_source(&mut self, source_id: &str) -> bool {
        self.per_source.remove(source_id).is_some()
    }

    /// Returns the checkpoint associated with a given source.
    ///
    /// All registered source have an associated checkpoint (that is possibly empty).
    ///
    /// Some non-registered source may also have checkpoint (due to backward compatibility
    /// and the ingest command).
    pub fn source_checkpoint(&self, source_id: &str) -> Option<&SourceCheckpoint> {
        self.per_source.get(source_id)
    }

    /// Adds a new source. If the source was already here, this
    /// method returns successfully and does not override the existing checkpoint.
    pub fn add_source(&mut self, source_id: &str) {
        self.per_source.entry(source_id.to_string()).or_default();
    }

    /// Removes a source.
    /// Returns successfully regardless of whether the source was present or not.
    pub fn remove_source(&mut self, source_id: &str) {
        self.per_source.remove(source_id);
    }

    /// Returns [`true`] if the checkpoint is empty.
    pub fn is_empty(&self) -> bool {
        self.per_source.is_empty()
    }
}

/// A source checkpoint is a map of the last processed position for every partition.
///
/// If a partition is missing, it implicitly means that none of its message
/// has been processed.
#[derive(Default, Clone, Eq, PartialEq)]
pub struct SourceCheckpoint {
    per_partition: BTreeMap<PartitionId, Position>,
}
impl SourceCheckpoint {
    /// Adds a partition to the checkpoint.
    pub fn add_partition(&mut self, partition_id: PartitionId, position: Position) {
        self.per_partition.insert(partition_id, position);
    }

    /// Returns the number of partitions covered by the checkpoint.
    pub fn num_partitions(&self) -> usize {
        self.per_partition.len()
    }

    /// Returns [`true`] if the checkpoint is empty.
    pub fn is_empty(&self) -> bool {
        self.per_partition.is_empty()
    }
}

/// Creates a checkpoint from an iterator of `(PartitionId, Position)` tuples.
/// ```
/// use quickwit_metastore::checkpoint::{SourceCheckpoint, PartitionId};
/// use quickwit_proto::types::Position;
///
/// let checkpoint: SourceCheckpoint = [(0u64, 0u64), (1u64, 2u64)]
///     .into_iter()
///     .map(|(partition_id, offset)| {
///         (PartitionId::from(partition_id), Position::offset(offset))
///     })
///     .collect();
/// ```
impl FromIterator<(PartitionId, Position)> for SourceCheckpoint {
    fn from_iter<I>(iter: I) -> SourceCheckpoint
    where I: IntoIterator<Item = (PartitionId, Position)> {
        SourceCheckpoint {
            per_partition: iter.into_iter().collect(),
        }
    }
}

impl Serialize for SourceCheckpoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let mut map = serializer.serialize_map(Some(self.per_partition.len()))?;
        for (partition, position) in &self.per_partition {
            map.serialize_entry(&*partition.0, position)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for SourceCheckpoint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        let string_to_string_map: BTreeMap<String, String> = BTreeMap::deserialize(deserializer)?;
        let per_partition: BTreeMap<PartitionId, Position> = string_to_string_map
            .into_iter()
            .map(|(partition_id, position)| {
                (PartitionId::from(partition_id), Position::from(position))
            })
            .collect();
        Ok(SourceCheckpoint { per_partition })
    }
}

/// Error returned when trying to apply a checkpoint delta to a checkpoint that is not
/// compatible. ie: the checkpoint delta starts from a point anterior to
/// the checkpoint.
#[derive(Clone, Debug, Error, Eq, PartialEq, Serialize, Deserialize)]
#[error(
    "incompatible checkpoint delta at partition `{partition_id}`: end position is \
     `{partition_position:?}` (inclusive), whereas delta starts at `{delta_from_position:?}` \
     (exclusive)"
)]
pub struct IncompatibleCheckpointDelta {
    /// The partition ID for which the incompatibility has been detected.
    pub partition_id: PartitionId,
    /// The current position (inclusive) within this partition.
    pub partition_position: Position,
    /// The start position (exclusive) for the delta.
    pub delta_from_position: Position,
}

#[derive(Clone, Debug, Error, Serialize, Deserialize, PartialEq, Eq)]
pub enum PartitionDeltaError {
    #[error(transparent)]
    IncompatibleCheckpointDelta(#[from] IncompatibleCheckpointDelta),
    #[error(
        "empty or negative delta at partition `{partition_id}`: {from_position:?} >= \
         {to_position:?}"
    )]
    EmptyOrNegativeDelta {
        /// One PartitionId for which the negative delta has been detected.
        partition_id: PartitionId,
        /// Delta from position.
        from_position: Position,
        /// Delta to position.
        to_position: Position,
    },
}

impl SourceCheckpoint {
    /// Returns the position reached for a given partition.
    pub fn position_for_partition(&self, partition_id: &PartitionId) -> Option<&Position> {
        self.per_partition.get(partition_id)
    }

    /// Returns an iterator with the reached position for each partition.
    pub fn iter(&self) -> impl Iterator<Item = (PartitionId, Position)> + '_ {
        self.per_partition
            .iter()
            .map(|(partition_id, position)| (partition_id.clone(), position.clone()))
    }

    pub fn check_compatibility(
        &self,
        delta: &SourceCheckpointDelta,
    ) -> Result<(), IncompatibleCheckpointDelta> {
        for (delta_partition, delta_position) in &delta.per_partition {
            let Some(position) = self.per_partition.get(delta_partition) else {
                continue;
            };
            match position.cmp(&delta_position.from) {
                Ordering::Equal => {}
                Ordering::Less => {
                    warn!(cur_pos=?position, delta_pos_from=?delta_position.from,partition=?delta_partition, "some positions were skipped");
                }
                Ordering::Greater => {
                    return Err(IncompatibleCheckpointDelta {
                        partition_id: delta_partition.clone(),
                        partition_position: position.clone(),
                        delta_from_position: delta_position.from.clone(),
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
    ///   |    Checkpoint & Delta        | Outcome                     |
    ///   |------------------------------|-----------------------------|
    ///   |  (..a] (b..c] with a = b     | Compatible                  |
    ///   |  (..a] (b..c] with b > a     | Compatible                  |
    ///   |  (..a] (b..c] with b < a     | Incompatible                |
    ///
    /// If the delta is incompatible, returns an error without modifying the original checkpoint.
    pub fn try_apply_delta(
        &mut self,
        delta: SourceCheckpointDelta,
    ) -> Result<(), IncompatibleCheckpointDelta> {
        self.check_compatibility(&delta)?;
        debug!(delta=?delta, checkpoint=?self, "applying delta to checkpoint");
        for (partition_id, partition_position) in delta.per_partition {
            self.per_partition
                .insert(partition_id, partition_position.to);
        }
        Ok(())
    }
}

impl fmt::Debug for SourceCheckpoint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("Ckpt(")?;
        for (i, (partition_id, position)) in self.per_partition.iter().enumerate() {
            f.write_str(&partition_id.0)?;
            f.write_str(":")?;
            write!(f, "{position}")?;
            let is_last = i == self.per_partition.len() - 1;
            if !is_last {
                f.write_str(" ")?;
            }
        }
        f.write_str(")")?;
        Ok(())
    }
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
/// we are not trying to add documents to the index that were already indexed.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexCheckpointDelta {
    pub source_id: SourceId,
    pub source_delta: SourceCheckpointDelta,
}

impl IndexCheckpointDelta {
    pub fn is_empty(&self) -> bool {
        self.source_delta.is_empty()
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test(source_id: &str, pos_range: Range<u64>) -> Self {
        Self {
            source_id: source_id.to_string(),
            source_delta: SourceCheckpointDelta::from_range(pos_range),
        }
    }
}

impl fmt::Debug for IndexCheckpointDelta {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{:?}", &self.source_id, self.source_delta)?;
        Ok(())
    }
}

#[derive(Default, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceCheckpointDelta {
    per_partition: BTreeMap<PartitionId, PartitionDelta>,
}

impl fmt::Debug for SourceCheckpointDelta {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("∆(")?;
        for (i, (partition_id, partition_delta)) in self.per_partition.iter().enumerate() {
            write!(
                f,
                "{}:({}..{}]",
                partition_id.0, partition_delta.from, partition_delta.to,
            )?;
            if i != self.per_partition.len() - 1 {
                f.write_str(" ")?;
            }
        }
        f.write_str(")")?;
        Ok(())
    }
}

impl TryFrom<Range<u64>> for SourceCheckpointDelta {
    type Error = PartitionDeltaError;

    fn try_from(range: Range<u64>) -> Result<Self, Self::Error> {
        // Checkpoint delta are expressed as (from, to] intervals while ranges
        // are [start, end) intervals
        let from_position = if range.start == 0 {
            Position::Beginning
        } else {
            Position::offset(range.start - 1)
        };
        let to_position = if range.end == 0 {
            Position::Beginning
        } else {
            Position::offset(range.end - 1)
        };
        SourceCheckpointDelta::from_partition_delta(
            PartitionId::default(),
            from_position,
            to_position,
        )
    }
}

impl SourceCheckpointDelta {
    /// Used for tests only.
    /// Panics if the range is not strictly increasing.
    #[cfg(any(test, feature = "testsuite"))]
    pub fn from_range(range: Range<u64>) -> Self {
        SourceCheckpointDelta::try_from(range).expect("Invalid position range")
    }

    /// Creates a new checkpoint delta initialized with a single partition delta.
    pub fn from_partition_delta(
        partition_id: PartitionId,
        from_position: Position,
        to_position: Position,
    ) -> Result<Self, PartitionDeltaError> {
        let mut delta = SourceCheckpointDelta::default();
        delta.record_partition_delta(partition_id, from_position, to_position)?;
        Ok(delta)
    }

    /// Returns the checkpoint associated with the endpoint of the delta.
    pub fn get_source_checkpoint(&self) -> SourceCheckpoint {
        let mut source_checkpoint = SourceCheckpoint::default();
        source_checkpoint.try_apply_delta(self.clone()).unwrap();
        source_checkpoint
    }

    /// Returns an iterator of partition IDs and associated deltas.
    pub fn iter(&self) -> impl Iterator<Item = (PartitionId, PartitionDelta)> + '_ {
        self.per_partition
            .iter()
            .map(|(partition_id, partition_delta)| (partition_id.clone(), partition_delta.clone()))
    }

    /// Records a `(from, to]` partition delta for a given partition.
    pub fn record_partition_delta(
        &mut self,
        partition_id: PartitionId,
        from_position: Position,
        to_position: Position,
    ) -> Result<(), PartitionDeltaError> {
        // `from_position == to_position` means delta is empty.
        if from_position >= to_position {
            return Err(PartitionDeltaError::EmptyOrNegativeDelta {
                partition_id,
                from_position,
                to_position,
            });
        }
        let entry = self.per_partition.entry(partition_id);
        match entry {
            Entry::Occupied(mut occupied_entry) => {
                if occupied_entry.get().to == from_position {
                    occupied_entry.get_mut().to = to_position;
                } else {
                    return Err(PartitionDeltaError::from(IncompatibleCheckpointDelta {
                        partition_id: occupied_entry.key().clone(),
                        partition_position: occupied_entry.get().to.clone(),
                        delta_from_position: from_position,
                    }));
                }
            }
            Entry::Vacant(vacant_entry) => {
                let partition_delta = PartitionDelta {
                    from: from_position,
                    to: to_position,
                };
                vacant_entry.insert(partition_delta);
            }
        }
        Ok(())
    }

    /// Extends the current checkpoint delta in-place with the provided checkpoint delta.
    ///
    /// Contrary to checkpoint update, the two deltas here need to chain perfectly.
    pub fn extend(&mut self, delta: SourceCheckpointDelta) -> Result<(), PartitionDeltaError> {
        for (partition_id, partition_delta) in delta.per_partition {
            self.record_partition_delta(partition_id, partition_delta.from, partition_delta.to)?;
        }
        Ok(())
    }

    /// Returns the number of partitions covered by the checkpoint delta.
    pub fn num_partitions(&self) -> usize {
        self.per_partition.len()
    }

    /// Returns an iterator over the partition_ids.
    pub fn partitions(&self) -> impl Iterator<Item = &PartitionId> {
        self.per_partition.keys()
    }

    /// Returns `true` if the checkpoint delta is empty.
    pub fn is_empty(&self) -> bool {
        self.per_partition.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_from_range() {
        let checkpoint_delta = SourceCheckpointDelta::from_range(0..3);
        assert_eq!(
            format!("{checkpoint_delta:?}"),
            "∆(:(..00000000000000000002])"
        );
        let checkpoint_delta = SourceCheckpointDelta::from_range(1..4);
        assert_eq!(
            format!("{checkpoint_delta:?}"),
            "∆(:(00000000000000000000..00000000000000000003])"
        );
    }

    #[test]
    fn test_checkpoint_simple() {
        let mut checkpoint = SourceCheckpoint::default();
        assert_eq!(format!("{checkpoint:?}"), "Ckpt()");

        let delta = {
            let mut delta = SourceCheckpointDelta::from_partition_delta(
                PartitionId::from("a"),
                Position::offset(123u64),
                Position::offset(128u64),
            )
            .unwrap();
            delta
                .record_partition_delta(
                    PartitionId::from("b"),
                    Position::offset(60002u64),
                    Position::offset(60187u64),
                )
                .unwrap();
            delta
        };
        checkpoint.try_apply_delta(delta.clone()).unwrap();
        assert_eq!(
            format!("{checkpoint:?}"),
            "Ckpt(a:00000000000000000128 b:00000000000000060187)"
        );
        // `try_apply_delta` is not idempotent.
        checkpoint.try_apply_delta(delta).unwrap_err();
        assert_eq!(
            format!("{checkpoint:?}"),
            "Ckpt(a:00000000000000000128 b:00000000000000060187)"
        );
    }

    #[test]
    fn test_partially_incompatible_does_not_update() -> anyhow::Result<()> {
        let mut checkpoint = SourceCheckpoint::default();
        let delta1 = {
            let mut delta = SourceCheckpointDelta::from_partition_delta(
                PartitionId::from("a"),
                Position::offset("00123"),
                Position::offset("00128"),
            )
            .unwrap();
            delta.record_partition_delta(
                PartitionId::from("b"),
                Position::offset("60002"),
                Position::offset("60187"),
            )?;
            delta
        };
        assert!(checkpoint.try_apply_delta(delta1).is_ok());
        let delta2 = {
            let mut delta = SourceCheckpointDelta::from_partition_delta(
                PartitionId::from("a"),
                Position::offset("00128"),
                Position::offset("00129"),
            )
            .unwrap();
            delta.record_partition_delta(
                PartitionId::from("b"),
                Position::offset("50099"),
                Position::offset("60002"),
            )?;
            delta
        };
        assert!(matches!(
            checkpoint.try_apply_delta(delta2),
            Err(IncompatibleCheckpointDelta { .. })
        ));
        // checkpoint was unchanged
        assert_eq!(format!("{checkpoint:?}"), "Ckpt(a:00128 b:60187)");
        Ok(())
    }

    #[test]
    fn test_adding_new_partition() -> anyhow::Result<()> {
        let mut checkpoint = SourceCheckpoint::default();
        let delta1 = {
            let mut delta = SourceCheckpointDelta::from_partition_delta(
                PartitionId::from("a"),
                Position::offset("00123"),
                Position::offset("00128"),
            )
            .unwrap();
            delta.record_partition_delta(
                PartitionId::from("b"),
                Position::offset("60002"),
                Position::offset("60187"),
            )?;
            delta
        };
        assert!(checkpoint.try_apply_delta(delta1).is_ok());
        let delta3 = {
            let mut delta = SourceCheckpointDelta::from_partition_delta(
                PartitionId::from("b"),
                Position::offset("60187"),
                Position::offset("60190"),
            )
            .unwrap();
            delta.record_partition_delta(
                PartitionId::from("c"),
                Position::offset("20001"),
                Position::offset("20008"),
            )?;
            delta
        };
        assert!(checkpoint.try_apply_delta(delta3).is_ok());
        assert_eq!(format!("{checkpoint:?}"), "Ckpt(a:00128 b:60190 c:20008)");
        Ok(())
    }

    #[test]
    fn test_extend_checkpoint_delta() {
        let mut delta1 = {
            let mut delta = SourceCheckpointDelta::from_partition_delta(
                PartitionId::from("a"),
                Position::offset("00123"),
                Position::offset("00128"),
            )
            .unwrap();
            delta
                .record_partition_delta(
                    PartitionId::from("b"),
                    Position::offset("60002"),
                    Position::offset("60187"),
                )
                .unwrap();
            delta
        };
        let delta2 = {
            let mut delta = SourceCheckpointDelta::from_partition_delta(
                PartitionId::from("b"),
                Position::offset("60187"),
                Position::offset("60348"),
            )
            .unwrap();
            delta
                .record_partition_delta(
                    PartitionId::from("c"),
                    Position::offset("20001"),
                    Position::offset("20008"),
                )
                .unwrap();
            delta
        };
        let delta3 = {
            let mut delta = SourceCheckpointDelta::from_partition_delta(
                PartitionId::from("a"),
                Position::offset("00123"),
                Position::offset("00128"),
            )
            .unwrap();
            delta
                .record_partition_delta(
                    PartitionId::from("b"),
                    Position::offset("60002"),
                    Position::offset("60348"),
                )
                .unwrap();
            delta
                .record_partition_delta(
                    PartitionId::from("c"),
                    Position::offset("20001"),
                    Position::offset("20008"),
                )
                .unwrap();
            delta
        };
        delta1.extend(delta2).unwrap();
        assert_eq!(delta1, delta3);

        let delta4 = SourceCheckpointDelta::from_partition_delta(
            PartitionId::from("a"),
            Position::offset("00130"),
            Position::offset("00142"),
        )
        .unwrap();
        let result = delta1.extend(delta4);
        assert_eq!(
            result,
            Err(PartitionDeltaError::from(IncompatibleCheckpointDelta {
                partition_id: PartitionId::from("a"),
                partition_position: Position::offset("00128"),
                delta_from_position: Position::offset("00130")
            }))
        );
    }

    #[test]
    fn test_record_negative_partition_delta_is_failing() {
        {
            let delta_error = SourceCheckpointDelta::from_partition_delta(
                PartitionId::from("a"),
                Position::offset("20"),
                Position::offset("20"),
            )
            .unwrap_err();
            matches!(
                delta_error,
                PartitionDeltaError::EmptyOrNegativeDelta { .. }
            );
        }
        {
            let mut delta = SourceCheckpointDelta::from_range(10..20);
            let delta_error = delta
                .record_partition_delta(
                    PartitionId::from("a"),
                    Position::offset("20"),
                    Position::offset("10"),
                )
                .unwrap_err();
            matches!(
                delta_error,
                PartitionDeltaError::EmptyOrNegativeDelta { .. }
            );
        }
    }

    #[test]
    fn test_index_checkpoint() {
        let mut index_checkpoint = IndexCheckpoint::default();
        assert!(
            index_checkpoint
                .source_checkpoint("missing_source")
                .is_none()
        );
        index_checkpoint.add_source("existing_source_with_empty_checkpoint");
        assert!(
            index_checkpoint
                .source_checkpoint("existing_source_with_empty_checkpoint")
                .is_some()
        );
        index_checkpoint.remove_source("missing_source"); //< we just check this does not fail
        assert!(
            index_checkpoint
                .source_checkpoint("missing_source")
                .is_none()
        );
        assert!(
            index_checkpoint
                .source_checkpoint("existing_source_with_empty_checkpoint")
                .is_some()
        );
        index_checkpoint.remove_source("existing_source_with_empty_checkpoint"); //< we just check this does not fail
        assert!(
            index_checkpoint
                .source_checkpoint("existing_source_with_empty_checkpoint")
                .is_none()
        );
    }

    #[test]
    fn test_get_source_checkpoint() {
        let partition = PartitionId::from("a");
        let delta = SourceCheckpointDelta::from_partition_delta(
            partition.clone(),
            Position::offset(42u64),
            Position::offset(43u64),
        )
        .unwrap();
        let checkpoint: SourceCheckpoint = delta.get_source_checkpoint();
        assert_eq!(
            checkpoint.position_for_partition(&partition).unwrap(),
            &Position::offset(43u64)
        );
    }
}
