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

use quickwit_types::NodeId;

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct IndexingPipelineId {
    pub index_id: String,
    pub source_id: String,
    pub node_id: NodeId,
    pub pipeline_ord: usize,
}

#[cfg(test)]
impl IndexingPipelineId {
    pub fn for_test() -> Self {
        Self {
            index_id: "test-index".to_string(),
            source_id: "test-source".to_string(),
            node_id: NodeId::from("test-node"),
            pipeline_ord: 0,
        }
    }
}
