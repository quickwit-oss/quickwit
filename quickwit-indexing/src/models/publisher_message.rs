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
use std::time::Instant;

use quickwit_metastore::checkpoint::CheckpointDelta;
use quickwit_metastore::SplitMetadata;

/// Publish a new split, coming from the indexer.
pub struct PublishNewSplit {
    pub index_id: String,
    pub new_split: SplitMetadata,
    pub checkpoint_delta: CheckpointDelta,
    pub split_date_of_birth: Instant, // for logging
}

impl fmt::Debug for PublishNewSplit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PublishNewSplit")
            .field("index_id", &self.index_id)
            .field("new_split_id", &self.new_split.split_id())
            .field("checkpoint_delta", &self.checkpoint_delta)
            .field(
                "tts_in_secs",
                &self.split_date_of_birth.elapsed().as_secs_f32(),
            )
            .finish()
    }
}

/// Publish a merge, replacing several splits (typically 10)
/// by a single larger split.
pub struct ReplaceSplits {
    pub index_id: String,
    pub new_splits: Vec<SplitMetadata>,
    pub replaced_split_ids: Vec<String>,
}

impl fmt::Debug for ReplaceSplits {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let new_split_ids: Vec<String> = self
            .new_splits
            .iter()
            .map(|split| split.split_id().to_string())
            .collect();
        f.debug_struct("ReplaceSplits")
            .field("new_split_ids", &new_split_ids)
            .field("replaced_split_ids", &self.replaced_split_ids)
            .finish()
    }
}

#[derive(Debug)]
pub enum PublisherMessage {
    NewSplit(PublishNewSplit),
    ReplaceSplits(ReplaceSplits),
}
