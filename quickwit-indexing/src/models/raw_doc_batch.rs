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

#[derive(Default, Clone)]
pub struct RawDocBatch {
    pub docs: Vec<String>,
    pub checkpoint_delta: CheckpointDelta,
}

impl fmt::Debug for RawDocBatch {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("RawDocBatch")
            .field("docs_len", &self.docs.len())
            .field("checkpoint_delta", &self.checkpoint_delta)
            .finish()
    }
}
