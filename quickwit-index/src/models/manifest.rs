// Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use quickwit_metastore::SplitMetadata;
use serde::{Deserialize, Serialize};
use tantivy::SegmentId;

#[derive(Debug, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub file_name: String,
    pub file_size_in_bytes: u64,
}

impl ManifestEntry {
    fn new(file_name: &str, file_size_in_bytes: u64) -> Self {
        ManifestEntry {
            file_name: file_name.to_string(),
            file_size_in_bytes,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub split_metadata: SplitMetadata,
    pub split_size_in_bytes: u64, //< this is already in the split metadata
    pub num_files: u64,
    pub files: Vec<ManifestEntry>,
    pub segments: Vec<SegmentId>,
}

impl Manifest {
    pub fn new(split_metadata: SplitMetadata) -> Self {
        Manifest {
            split_metadata,
            split_size_in_bytes: 0,
            num_files: 0,
            files: Vec::new(),
            segments: Vec::new(),
        }
    }

    pub fn push(&mut self, file_name: &str, file_size_in_bytes: u64) {
        self.num_files += 1;
        self.split_size_in_bytes += file_size_in_bytes;
        self.files
            .push(ManifestEntry::new(file_name, file_size_in_bytes))
    }

    pub fn to_json(&self) -> anyhow::Result<String> {
        serde_json::to_string(self).map_err(|error| anyhow::anyhow!(error))
    }

    pub fn file_statistics(&self) -> FileStatistics {
        if self.files.is_empty() {
            return FileStatistics::empty();
        }
        let mut file_statistics = FileStatistics::from_manifest_entry(&self.files[0]);

        for file in &self.files[1..] {
            if file.file_size_in_bytes < file_statistics.min_file_size_in_bytes {
                file_statistics.min_file_size_in_bytes = file.file_size_in_bytes;
            }

            if file.file_size_in_bytes > file_statistics.min_file_size_in_bytes {
                file_statistics.max_file_size_in_bytes = file.file_size_in_bytes;
            }

            file_statistics.avg_file_size_in_bytes += file.file_size_in_bytes;
        }

        file_statistics.avg_file_size_in_bytes /= self.num_files;
        file_statistics
    }
}

#[derive(Default)]
pub struct FileStatistics {
    pub min_file_size_in_bytes: u64,
    pub max_file_size_in_bytes: u64,
    pub avg_file_size_in_bytes: u64,
}

impl FileStatistics {
    fn from_manifest_entry(manifest_entry: &ManifestEntry) -> Self {
        FileStatistics {
            min_file_size_in_bytes: manifest_entry.file_size_in_bytes,
            max_file_size_in_bytes: manifest_entry.file_size_in_bytes,
            avg_file_size_in_bytes: manifest_entry.file_size_in_bytes,
        }
    }

    fn empty() -> Self {
        Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest() {
        let mut manifest = Manifest::new(SplitMetadata::new("split-one".to_string()));
        assert_eq!(manifest.split_metadata.num_records, 0);
        assert_eq!(manifest.files.len(), 0);
        assert_eq!(manifest.split_size_in_bytes, 0);
        assert_eq!(manifest.num_files, 0);

        manifest.push("foo", 50);
        assert_eq!(manifest.files.len(), 1);
        assert_eq!(manifest.split_size_in_bytes, 50);
        assert_eq!(manifest.num_files, 1);

        manifest.push("bar", 100);
        assert_eq!(manifest.files.len(), 2);
        assert_eq!(manifest.split_size_in_bytes, 150);
        assert_eq!(manifest.num_files, 2);

        manifest.push("qux", 150);
        assert_eq!(manifest.files.len(), 3);
        assert_eq!(manifest.split_size_in_bytes, 300);
        assert_eq!(manifest.num_files, 3);
    }

    #[test]
    fn test_manifest_file_statistics() {
        let mut manifest = Manifest::new(SplitMetadata::new("split-two".to_string()));
        let empty_file_statistics = manifest.file_statistics();

        assert_eq!(manifest.split_metadata.num_records, 0);
        assert_eq!(manifest.segments.len(), 0);
        assert_eq!(manifest.split_size_in_bytes, 0);
        assert_eq!(manifest.num_files, 0);
        assert_eq!(empty_file_statistics.min_file_size_in_bytes, 0);
        assert_eq!(empty_file_statistics.max_file_size_in_bytes, 0);
        assert_eq!(empty_file_statistics.avg_file_size_in_bytes, 0);

        manifest.push("foo", 50);
        let foo_file_statistics = manifest.file_statistics();

        assert_eq!(manifest.split_size_in_bytes, 50);
        assert_eq!(manifest.num_files, 1);
        assert_eq!(foo_file_statistics.min_file_size_in_bytes, 50);
        assert_eq!(foo_file_statistics.max_file_size_in_bytes, 50);
        assert_eq!(foo_file_statistics.avg_file_size_in_bytes, 50);

        manifest.push("bar", 100);
        let bar_file_statistics = manifest.file_statistics();

        assert_eq!(manifest.split_size_in_bytes, 150);
        assert_eq!(manifest.num_files, 2);
        assert_eq!(bar_file_statistics.min_file_size_in_bytes, 50);
        assert_eq!(bar_file_statistics.max_file_size_in_bytes, 100);
        assert_eq!(bar_file_statistics.avg_file_size_in_bytes, 75);

        manifest.push("qux", 150);
        let qux_file_statistics = manifest.file_statistics();

        assert_eq!(manifest.split_size_in_bytes, 300);
        assert_eq!(manifest.num_files, 3);
        assert_eq!(qux_file_statistics.min_file_size_in_bytes, 50);
        assert_eq!(qux_file_statistics.max_file_size_in_bytes, 150);
        assert_eq!(qux_file_statistics.avg_file_size_in_bytes, 100);
    }
}
