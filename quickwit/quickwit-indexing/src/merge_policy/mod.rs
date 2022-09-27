// Copyright (C) 2022 Quickwit, Inc.
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

mod no_merge_policy;
mod stable_log_merge_policy;

use std::fmt;
use std::sync::Arc;

pub use no_merge_policy::NoMergePolicy;
use quickwit_config::IndexingSettings;
use quickwit_metastore::SplitMetadata;
pub(crate) use stable_log_merge_policy::StableLogMergePolicy;

use crate::new_split_id;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MergeOperationType {
    Merge,
    DeleteAndMerge,
}

impl fmt::Display for MergeOperationType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone)]
pub struct MergeOperation {
    pub merge_split_id: String,
    pub splits: Vec<SplitMetadata>,
    pub operation_type: MergeOperationType,
}

impl MergeOperation {
    pub fn new_merge_operation(splits: Vec<SplitMetadata>) -> Self {
        Self {
            merge_split_id: new_split_id(),
            splits,
            operation_type: MergeOperationType::Merge,
        }
    }

    pub fn new_delete_and_merge_operation(split: SplitMetadata) -> Self {
        Self {
            merge_split_id: new_split_id(),
            splits: vec![split],
            operation_type: MergeOperationType::DeleteAndMerge,
        }
    }

    pub fn splits_as_slice(&self) -> &[SplitMetadata] {
        self.splits.as_slice()
    }
}

impl fmt::Debug for MergeOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Merge(operation_type={}, merged_split_id={},splits=[",
            self.operation_type, self.merge_split_id
        )?;
        for split in &self.splits {
            write!(f, "{},", split.split_id())?;
        }
        write!(f, "])")?;
        Ok(())
    }
}

/// A merge policy wraps the logic that decide what should be merged.
/// The SplitMetadata must be extracted from the splits `Vec`.
///
/// It is called by the merge planner whenever a new split is added.
pub trait MergePolicy: Send + Sync + fmt::Debug {
    /// Returns the list of merge operations that should be performed.
    fn operations(&self, splits: &mut Vec<SplitMetadata>) -> Vec<MergeOperation>;
    /// A mature split is a split that won't undergo a merge operation in the future.
    fn is_mature(&self, split: &SplitMetadata) -> bool;
}

pub fn merge_policy_from_settings(indexing_settings: &IndexingSettings) -> Arc<dyn MergePolicy> {
    if !indexing_settings.merge_enabled {
        return Arc::new(NoMergePolicy);
    }
    let stable_log_merge_policy = StableLogMergePolicy {
        merge_factor: indexing_settings.merge_policy.merge_factor,
        max_merge_factor: indexing_settings.merge_policy.max_merge_factor,
        split_num_docs_target: indexing_settings.split_num_docs_target,
        ..Default::default()
    };
    Arc::new(stable_log_merge_policy)
}

pub fn default_merge_policy() -> Arc<dyn MergePolicy> {
    merge_policy_from_settings(&IndexingSettings::default())
}

struct SplitShortDebug<'a>(&'a SplitMetadata);

impl<'a> fmt::Debug for SplitShortDebug<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Split")
            .field("split_id", &self.0.split_id())
            .field("num_docs", &self.0.num_docs)
            .finish()
    }
}

fn splits_short_debug(splits: &[SplitMetadata]) -> Vec<SplitShortDebug> {
    splits.iter().map(SplitShortDebug).collect()
}

#[cfg(test)]
pub mod tests {

    use std::ops::RangeInclusive;

    use super::*;

    pub(crate) fn create_splits(num_docs_vec: Vec<usize>) -> Vec<SplitMetadata> {
        let num_docs_with_timestamp = num_docs_vec
            .into_iter()
            // we give the same timestamp to all of them and rely on stable sort to keep the split
            // order.
            .map(|num_docs| (num_docs, (1630563067..=1630564067)))
            .collect();
        create_splits_with_timestamps(num_docs_with_timestamp)
    }

    pub(crate) fn create_splits_with_timestamps(
        num_docs_vec: Vec<(usize, RangeInclusive<i64>)>,
    ) -> Vec<SplitMetadata> {
        num_docs_vec
            .into_iter()
            .enumerate()
            .map(|(split_ord, (num_docs, time_range))| SplitMetadata {
                split_id: format!("split_{:02}", split_ord),
                num_docs,
                time_range: Some(time_range),
                ..Default::default()
            })
            .collect()
    }
}
