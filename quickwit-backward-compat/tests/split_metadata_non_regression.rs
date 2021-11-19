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

use std::fs;
use std::path::Path;

use quickwit_metastore::SplitMetadataAndFooterOffsets;

fn read_split_metadata(path: &Path) -> anyhow::Result<SplitMetadataAndFooterOffsets> {
    let split_metadata_bytes = std::fs::read(path)?;
    let split: SplitMetadataAndFooterOffsets = serde_json::from_slice(&split_metadata_bytes)?;
    Ok(split)
}

fn test_split_metadata_version(path: &Path) -> anyhow::Result<()> {
    println!("---\nTest deserializaton of {}", path.display());
    let split_metadata = read_split_metadata(path)?;
    let expected_path = path.to_string_lossy().replace(".json", ".expected.json");
    let expected_split_metadata = read_split_metadata(Path::new(&expected_path))?;
    assert_eq!(split_metadata, expected_split_metadata);
    Ok(())
}

#[test]
fn test_backward_compatibility() -> anyhow::Result<()> {
    for entry in fs::read_dir("./test-data")? {
        let entry = entry?;
        let path = entry.path();
        if path.ends_with(".expected.json") {
            continue;
        }
        test_split_metadata_version(&path)?;
    }
    Ok(())
}
