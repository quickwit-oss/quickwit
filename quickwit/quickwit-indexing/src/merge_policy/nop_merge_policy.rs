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

use std::fmt;

use quickwit_metastore::SplitMaturity;

use crate::merge_policy::MergePolicy;

/// The NopMergePolicy, as the name suggests, is no-op and does not perform any merges.
/// <https://en.wikipedia.org/wiki/NOP_(code)>
#[derive(Debug)]
pub struct NopMergePolicy;

impl fmt::Display for NopMergePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl MergePolicy for NopMergePolicy {
    fn operations(
        &self,
        _splits: &mut Vec<quickwit_metastore::SplitMetadata>,
    ) -> Vec<super::MergeOperation> {
        Vec::new()
    }

    fn split_maturity(&self, _split_num_docs: usize, _split_num_merge_ops: usize) -> SplitMaturity {
        // With the no merge policy, all splits are mature immediately as they will never undergo
        // any merge.
        SplitMaturity::Mature
    }
}

#[cfg(test)]
mod tests {

    use quickwit_metastore::SplitMaturity;

    use crate::merge_policy::{MergePolicy, NopMergePolicy};

    #[test]
    pub fn test_no_merge_policy_maturity_timestamp() {
        // All splits are always mature for `NopMergePolicy`.
        assert_eq!(NopMergePolicy.split_maturity(10, 0), SplitMaturity::Mature);
    }

    #[test]
    pub fn test_no_merge_policy_operations() {
        let mut splits = super::super::tests::create_splits(&NopMergePolicy, vec![1; 100]);
        assert!(NopMergePolicy.operations(&mut splits).is_empty());
        assert_eq!(splits.len(), 100);
    }
}
