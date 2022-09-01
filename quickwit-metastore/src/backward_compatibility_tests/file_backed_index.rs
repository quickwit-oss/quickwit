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

use super::index_metadata::{sample_index_metadata_for_regression, test_index_metadata_eq};
use super::split_metadata::sample_split_metadata_for_regression;
use crate::file_backed_metastore::file_backed_index::FileBackedIndex;
use crate::{DeleteQuery, DeleteTask, Split, SplitState};

fn sample_file_backed_index_for_regression() -> FileBackedIndex {
    let index_metadata = sample_index_metadata_for_regression();
    let split_metadata = sample_split_metadata_for_regression();
    let split = Split {
        split_state: SplitState::Published,
        split_metadata,
        update_timestamp: 1789,
        publish_timestamp: Some(1789),
    };
    let splits = vec![split];
    let delete_task = DeleteTask {
        create_timestamp: 0,
        opstamp: 10,
        delete_query: DeleteQuery {
            index_id: "index".to_string(),
            start_timestamp: None,
            end_timestamp: None,
            query: "Harry Potter".to_string(),
            search_fields: Vec::new(),
        },
    };
    FileBackedIndex::new(index_metadata, splits, vec![delete_task])
}

#[test]
fn test_file_backed_index_backward_compatibility() -> anyhow::Result<()> {
    let sample_file_backed_index = sample_file_backed_index_for_regression();
    super::test_json_backward_compatibility_helper(
        "file-backed-index",
        |deserialized: &FileBackedIndex, expected: &FileBackedIndex| {
            test_index_metadata_eq(deserialized.metadata(), expected.metadata());
            assert_eq!(deserialized.splits(), expected.splits());
        },
        sample_file_backed_index,
    )
}
