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

use std::fmt;

use crate::merge_policy::MergePolicy;

/// The NoMergePolicy simply... does not run any merges!
#[derive(Debug)]
pub struct NoMergePolicy;

impl fmt::Display for NoMergePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl MergePolicy for NoMergePolicy {
    fn operations(
        &self,
        _splits: &mut Vec<quickwit_metastore::SplitMetadata>,
    ) -> Vec<super::MergeOperation> {
        Vec::new()
    }

    fn is_mature(&self, _split: &quickwit_metastore::SplitMetadata) -> bool {
        // With the no merge policy, all splits are mature as they will never undergo any merge.
        true
    }
}

#[cfg(test)]
mod tests {
    use crate::merge_policy::{MergePolicy, NoMergePolicy};

    #[test]
    pub fn test_no_merge_policy_is_mature() {
        let split = super::super::tests::create_splits(vec![1])
            .into_iter()
            .next()
            .unwrap();
        // All splits are mature when merge is disabled.
        assert!(NoMergePolicy.is_mature(&split));
    }

    #[test]
    pub fn test_no_merge_policy_operations() {
        let mut splits = super::super::tests::create_splits(vec![1; 100]);
        assert!(NoMergePolicy.operations(&mut splits).is_empty());
        assert_eq!(splits.len(), 100);
    }
}
