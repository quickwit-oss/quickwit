// Copyright (C) 2021 Quickwit, Inc.
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

use quickwit_metastore::checkpoint::CheckpointDelta;
use quickwit_metastore::SplitMetadata;

#[derive(Clone)]
pub enum PublishOperation {
    /// Publish a new split, coming from the indexer.
    PublishNewSplit {
        new_split: SplitMetadata,
        checkpoint_delta: CheckpointDelta,
    },
    /// Publish a merge, replacing several splits (typically 10)
    /// by a single larger split.
    ReplaceSplits {
        new_splits: Vec<SplitMetadata>,
        replaced_split_ids: Vec<String>,
    },
}

impl fmt::Debug for PublishOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::PublishNewSplit {
                new_split: new_split_id,
                checkpoint_delta,
            } => f
                .debug_struct("PublishNewSplit")
                .field("new_split_id", &new_split_id.split_id)
                .field("checkpoint_delta", checkpoint_delta)
                .finish(),
            Self::ReplaceSplits {
                new_splits,
                replaced_split_ids,
            } => {
                let new_split_ids: Vec<String> = new_splits
                    .iter()
                    .map(|split| split.split_id.clone())
                    .collect();
                f.debug_struct("ReplaceSplits")
                    .field("new_split_ids", &new_split_ids)
                    .field("replaced_split_ids", replaced_split_ids)
                    .finish()
            }
        }
    }
}

impl PublishOperation {
    pub fn extract_new_splits(self) -> Vec<SplitMetadata> {
        match self {
            PublishOperation::PublishNewSplit {
                new_split: new_split_id,
                ..
            } => vec![new_split_id],
            PublishOperation::ReplaceSplits {
                new_splits: new_split_ids,
                ..
            } => new_split_ids,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PublisherMessage {
    pub index_id: String,
    pub operation: PublishOperation,
}
