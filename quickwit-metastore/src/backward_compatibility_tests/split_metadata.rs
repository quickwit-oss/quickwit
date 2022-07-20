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

use crate::SplitMetadata;

/// Creates a split metadata object that will be
/// used to check for non-regression
pub(crate) fn sample_split_metadata_for_regression() -> SplitMetadata {
    SplitMetadata {
        split_id: "split".to_string(),
        num_docs: 12303,
        uncompressed_docs_size_in_bytes: 234234,
        time_range: Some(121000..=130198),
        create_timestamp: 3,
        tags: ["234".to_string(), "aaa".to_string()].into_iter().collect(),
        demux_num_ops: 1,
        footer_offsets: 1000..2000,
    }
}

#[test]
fn test_split_metadata_backward_compatibility() -> anyhow::Result<()> {
    let sample_split_metdata = sample_split_metadata_for_regression();
    super::test_json_backward_compatibility_helper(
        "split-metadata",
        |deserialized: &SplitMetadata, expected: &SplitMetadata| {
            assert_eq!(deserialized, expected);
        },
        sample_split_metdata,
    )
}
