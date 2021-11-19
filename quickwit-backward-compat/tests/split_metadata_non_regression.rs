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

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::str::FromStr;

use anyhow::Context;
use once_cell::sync::OnceCell;
use quickwit_metastore::{SplitMetadata, SplitMetadataAndFooterOffsets, SplitState};

fn check_version(
    version: usize,
    split_metadata: &SplitMetadataAndFooterOffsets,
) -> anyhow::Result<()> {
    // Feel free to enrich this function if we start introducing default values that are not
    // deterministic. (e.g. depends on time.)
    static EXPECTED_DESERIALIZED_SPLIT_METADATA: OnceCell<
        HashMap<usize, SplitMetadataAndFooterOffsets>,
    > = OnceCell::new();
    let expected_split_metadata = EXPECTED_DESERIALIZED_SPLIT_METADATA
        .get_or_init(|| {
            let mut expected_split_metadata = HashMap::default();
            expected_split_metadata.insert(
                0,
                SplitMetadataAndFooterOffsets {
                    split_metadata: SplitMetadata {
                        split_id: "split".to_string(),
                        num_records: 12303,
                        size_in_bytes: 234234,
                        time_range: Some(121000..=130198),
                        split_state: SplitState::Published,
                        create_timestamp: 3,
                        update_timestamp: 1,
                        tags: ["234".to_string(), "aaa".to_string()].into_iter().collect(),
                        demux_num_ops: 1,
                    },
                    footer_offsets: 1000..2000,
                },
            );
            expected_split_metadata
        })
        .get(&version)
        .with_context(|| {
            format!(
                "Could not find expected split metadata for version {}",
                version
            )
        })?;
    assert_eq!(split_metadata, expected_split_metadata);
    Ok(())
}

fn extract_version(path: &Path) -> anyhow::Result<usize> {
    let path_str = path
        .file_name()
        .with_context(|| "Expect a filepath")?
        .to_string_lossy();
    let version_str = path_str
        .split('-')
        .nth(1)
        .with_context(|| "Failed to parse non-regression filepath")?;
    let version = usize::from_str(version_str)?;
    Ok(version)
}

fn test_split_metadata_version(path: &Path) -> anyhow::Result<()> {
    println!("---\nTest deserializaton of {}", path.display());
    let version = extract_version(path)?;
    println!("version: {}", version);
    let split_metadata_bytes = std::fs::read(path)?;
    let split: SplitMetadataAndFooterOffsets = serde_json::from_slice(&split_metadata_bytes)?;
    check_version(version, &split)?;
    Ok(())
}

#[test]
fn test_backward_compatibility() -> anyhow::Result<()> {
    for entry in fs::read_dir("./test-data")? {
        let entry = entry?;
        let path = entry.path();
        test_split_metadata_version(&path)?;
    }
    Ok(())
}
