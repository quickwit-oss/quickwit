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

use quickwit_metastore::{IndexMetadata, SplitMetadata};

fn read_index_metadata(path: &Path) -> anyhow::Result<IndexMetadata> {
    let index_metadata_bytes = std::fs::read(path)?;
    let index_metadata: IndexMetadata = serde_json::from_slice(&index_metadata_bytes)?;
    Ok(index_metadata)
}

fn test_index_metadata_deser(path: &Path) -> anyhow::Result<()> {
    println!(
        "---\nTest deserialization of index-metadata/{}",
        path.display()
    );
    let index_metadata = read_index_metadata(path)?;
    let expected_path = path.to_string_lossy().replace(".json", ".expected.json");
    let expected_index_metadata = read_index_metadata(Path::new(&expected_path))?;
    assert_eq!(index_metadata.index_id, expected_index_metadata.index_id);
    assert_eq!(index_metadata.index_uri, expected_index_metadata.index_uri);
    assert_eq!(
        index_metadata.checkpoint,
        expected_index_metadata.checkpoint
    );
    assert_eq!(
        index_metadata
            .doc_mapping
            .field_mappings
            .iter()
            .map(|field_mapping| &field_mapping.name)
            .collect::<Vec<_>>(),
        expected_index_metadata
            .doc_mapping
            .field_mappings
            .iter()
            .map(|field_mapping| &field_mapping.name)
            .collect::<Vec<_>>(),
    );
    assert_eq!(
        index_metadata.doc_mapping.tag_fields,
        expected_index_metadata.doc_mapping.tag_fields,
    );
    assert_eq!(
        index_metadata.doc_mapping.store_source,
        expected_index_metadata.doc_mapping.store_source,
    );
    assert_eq!(
        index_metadata.indexing_settings,
        expected_index_metadata.indexing_settings
    );
    assert_eq!(
        index_metadata.search_settings,
        expected_index_metadata.search_settings
    );
    assert_eq!(
        index_metadata
            .sources
            .iter()
            .map(|source| &source.source_id)
            .collect::<Vec<_>>(),
        expected_index_metadata
            .sources
            .iter()
            .map(|source| &source.source_id)
            .collect::<Vec<_>>(),
    );
    Ok(())
}

fn read_split_metadata(path: &Path) -> anyhow::Result<SplitMetadata> {
    let split_metadata_bytes = std::fs::read(path)?;

    let split: SplitMetadata = serde_json::from_slice(&split_metadata_bytes)?;
    Ok(split)
}

fn test_split_metadata_deser(path: &Path) -> anyhow::Result<()> {
    println!(
        "---\nTest deserialization of split-metadata/{}",
        path.display()
    );
    let split_metadata = read_split_metadata(path)?;
    let expected_path = path.to_string_lossy().replace(".json", ".expected.json");
    let expected_split_metadata = read_split_metadata(Path::new(&expected_path))?;
    assert_eq!(split_metadata, expected_split_metadata);
    Ok(())
}

#[test]
fn test_index_metadata_backward_compatibility() -> anyhow::Result<()> {
    for entry in fs::read_dir("./test-data/index-metadata")? {
        let entry = entry?;
        let path = entry.path();
        if path.to_string_lossy().ends_with(".expected.json") {
            continue;
        }
        test_index_metadata_deser(&path)?;
    }
    Ok(())
}

#[test]
fn test_split_metadata_backward_compatibility() -> anyhow::Result<()> {
    for entry in fs::read_dir("./test-data/split-metadata")? {
        let entry = entry?;
        let path = entry.path();
        if path.to_string_lossy().ends_with(".expected.json") {
            continue;
        }
        test_split_metadata_deser(&path)?;
    }
    Ok(())
}
