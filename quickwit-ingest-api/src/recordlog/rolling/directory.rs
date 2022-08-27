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

use std::collections::BTreeSet;
use std::io;
use std::path::Path;
use std::path::PathBuf;

use tokio::fs::File;
use tokio::fs::OpenOptions;

pub struct Directory {
    dir: PathBuf,
    // First position in files.
    file_set: BTreeSet<u64>,
}

fn filename_to_position(file_name: &str) -> Option<u64> {
    if file_name.len() != 24 {
        return None;
    }
    if !file_name.starts_with("wal-") {
        return None;
    }
    let seq_number_str = &file_name[4..];
    if !seq_number_str
        .as_bytes()
        .iter()
        .all(|b| (b'0'..=b'9').contains(b))
    {
        return None;
    }
    file_name[4..].parse::<u64>().ok()
}

impl Directory {
    pub async fn open(dir_path: &Path) -> io::Result<Directory> {
        let mut seq_numbers: BTreeSet<u64> = Default::default();
        let mut read_dir = tokio::fs::read_dir(dir_path).await?;
        while let Some(dir_entry) = read_dir.next_entry().await? {
            if !dir_entry.file_type().await?.is_file() {
                continue;
            }
            let file_name = if let Some(file_name) = dir_entry.file_name().to_str() {
                file_name.to_string()
            } else {
                continue;
            };
            if let Some(seq_number) = filename_to_position(&file_name) {
                seq_numbers.insert(seq_number);
            }
        }
        Ok(Directory {
            dir: dir_path.to_path_buf(),
            file_set: seq_numbers,
        })
    }

    pub fn num_files(&self) -> usize {
        self.file_set.len()
    }

    pub async fn truncate(&mut self, position: u64) -> io::Result<()> {
        if let Some(&first_file_to_retain) = self.file_set.range(..=position).last() {
            let mut removed_files = Vec::new();
            for &position in self.file_set.range(..first_file_to_retain) {
                let filepath = self.filepath(position);
                tokio::fs::remove_file(&filepath).await?;
                removed_files.push(position);
            }
            for position in removed_files {
                self.file_set.remove(&position);
            }
        }
        Ok(())
    }

    pub fn file_paths<'a>(&'a self) -> impl Iterator<Item = PathBuf> + 'a {
        self.file_set
            .iter()
            .copied()
            .map(move |seq_number| self.filepath(seq_number))
    }

    fn filepath(&self, seq_number: u64) -> PathBuf {
        self.dir.join(&format!("wal-{seq_number:020}"))
    }

    pub async fn new_file(&mut self, position: u64) -> io::Result<File> {
        assert!(self
            .file_set
            .iter()
            .last()
            .copied()
            .map(|last_position| last_position < position)
            .unwrap_or(true));
        self.file_set.insert(position);
        let new_filepath = self.filepath(position);
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&new_filepath)
            .await
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt;

    use super::*;

    #[test]
    fn test_filename_to_seq_number_invalid_prefix_rejected() {
        assert_eq!(filename_to_position("fil-00000000000000000001"), None);
    }

    #[test]
    fn test_filename_to_seq_number_invalid_padding_rejected() {
        assert_eq!(filename_to_position("wal-0000000000000000001"), None);
    }

    #[test]
    fn test_filename_to_seq_number_invalid_len_rejected() {
        assert_eq!(filename_to_position("wal-000000000000000000011"), None);
    }

    #[test]
    fn test_filename_to_seq_number_simple() {
        assert_eq!(filename_to_position("wal-00000000000000000001"), Some(1u64));
    }

    #[test]
    fn test_filename_to_seq_number() {
        assert_eq!(filename_to_position("wal-00000000000000000001"), Some(1u64));
    }

    fn test_directory_file_aux(directory: &Directory, dir_path: &Path) -> Vec<String> {
        directory
            .file_paths()
            .map(|filepath| {
                assert_eq!(filepath.parent().unwrap(), dir_path);
                filepath.file_name().unwrap().to_str().unwrap().to_string()
            })
            .collect::<Vec<String>>()
    }

    #[tokio::test]
    async fn test_directory() {
        let tmp_dir = tempfile::tempdir().unwrap();
        {
            let mut directory = Directory::open(tmp_dir.path()).await.unwrap();
            let mut file = directory.new_file(0u64).await.unwrap();
            file.write_all(b"hello").await.unwrap();
            file.flush().await.unwrap();
        }
        {
            let mut directory = Directory::open(tmp_dir.path()).await.unwrap();
            let filepaths = test_directory_file_aux(&directory, tmp_dir.path());
            assert_eq!(&filepaths, &["wal-00000000000000000000"]);
            let mut file = directory.new_file(3u64).await.unwrap();
            file.write_all(b"hello2").await.unwrap();
            file.flush().await.unwrap()
        }
        {
            let directory = Directory::open(tmp_dir.path()).await.unwrap();
            let filepaths = test_directory_file_aux(&directory, tmp_dir.path());
            assert_eq!(
                &filepaths,
                &["wal-00000000000000000000", "wal-00000000000000000003"]
            );
        }
    }

    #[tokio::test]
    async fn test_directory_truncate() {
        let tmp_dir = tempfile::tempdir().unwrap();
        {
            let mut directory = Directory::open(tmp_dir.path()).await.unwrap();
            let mut file = directory.new_file(0u64).await.unwrap();
            file.write_all(b"hello").await.unwrap();
            file.flush().await.unwrap();
        }
        {
            let mut directory = Directory::open(tmp_dir.path()).await.unwrap();
            let filepaths = test_directory_file_aux(&directory, tmp_dir.path());
            assert_eq!(&filepaths, &["wal-00000000000000000000"]);
            let mut file = directory.new_file(3u64).await.unwrap();
            file.write_all(b"hello2").await.unwrap();
            file.flush().await.unwrap();
            file.write_all(b"hello3").await.unwrap();
            file.flush().await.unwrap()
        }
        {
            let directory = Directory::open(tmp_dir.path()).await.unwrap();
            let filepaths = test_directory_file_aux(&directory, tmp_dir.path());
            assert_eq!(
                &filepaths,
                &["wal-00000000000000000000", "wal-00000000000000000003"]
            );
        }
        {
            let mut directory = Directory::open(tmp_dir.path()).await.unwrap();
            directory.truncate(3).await.unwrap();
            let filepaths = test_directory_file_aux(&directory, tmp_dir.path());
            assert_eq!(&filepaths, &["wal-00000000000000000003"]);
        }
    }
}
