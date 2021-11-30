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

use std::sync::Arc;

use quickwit_index_config::WikipediaIndexConfig;
use quickwit_metastore::checkpoint::{Checkpoint, CheckpointDelta, PartitionId, Position};
use quickwit_metastore::{IndexMetadata, SplitMetadata, SplitMetadataAndFooterOffsets, SplitState};

/// Creates a new [`IndexMetadata`] object against which backward compatibility tests will be run.
fn sample_index_metadata_for_regression() -> IndexMetadata {
    let mut checkpoint = Checkpoint::default();
    let delta = CheckpointDelta::from_partition_delta(
        PartitionId::from(0),
        Position::Beginning,
        Position::from(42),
    );
    checkpoint.try_apply_delta(delta).unwrap();

    IndexMetadata {
        index_id: "my-index".to_string(),
        index_uri: "s3://quickwit-indexes/my-index".to_string(),
        index_config: Arc::new(WikipediaIndexConfig::new()),
        checkpoint,
    }
}

fn save_index_metadata_test_files() -> anyhow::Result<()> {
    let index_metadata = sample_index_metadata_for_regression();
    let index_metadata_value = serde_json::to_value(&index_metadata)?;
    let mut index_metadata_json = serde_json::to_string_pretty(&index_metadata_value)?;
    index_metadata_json.push('\n');

    for extension in [".json", ".expected.json"] {
        std::fs::write(
            format!("test-data/index-metadata/v0-noversion{}", extension),
            index_metadata_json.as_bytes(),
        )?;
    }
    Ok(())
}

/// Creates a split metadata object that will be
/// used to check for non-regression
fn sample_split_metadata_for_regression() -> SplitMetadataAndFooterOffsets {
    SplitMetadataAndFooterOffsets {
        split_metadata: SplitMetadata {
            split_id: "split".to_string(),
            num_docs: 12303,
            size_in_bytes: 234234,
            time_range: Some(121000..=130198),
            split_state: SplitState::Published,
            create_timestamp: 3,
            update_timestamp: 1,
            tags: ["234".to_string(), "aaa".to_string()].into_iter().collect(),
            demux_num_ops: 1,
        },
        footer_offsets: 1000..2000,
    }
}

fn save_split_metadata_test_files() -> anyhow::Result<()> {
    let split_metadata = sample_split_metadata_for_regression();
    let split_metadata_value = serde_json::to_value(&split_metadata)?;
    let version: &str = split_metadata_value
        .as_object()
        .unwrap()
        .get("version")
        .expect("Missing version")
        .as_str()
        .expect("version should be a string");
    let mut split_metadata_json = serde_json::to_string_pretty(&split_metadata_value)?;
    split_metadata_json.push('\n');
    let md5sum = md5::compute(&split_metadata_json);
    let test_name = format!("test-data/split-metadata/v{}-{:x}", version, md5sum);
    let file_regression_test_path = format!("{}.json", test_name);
    std::fs::write(&file_regression_test_path, split_metadata_json.as_bytes())?;
    let file_regression_expected_path = format!("{}.expected.json", test_name);
    std::fs::write(
        &file_regression_expected_path,
        split_metadata_json.as_bytes(),
    )?;
    Ok(())
}

fn main() -> anyhow::Result<()> {
    save_index_metadata_test_files()?;
    save_split_metadata_test_files()?;
    Ok(())
}
