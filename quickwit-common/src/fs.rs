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

use std::path::Path;

use tokio;

/// Deletes the contents of a directory.
pub async fn empty_dir<P: AsRef<Path>>(path: P) -> anyhow::Result<()> {
    let mut entries = tokio::fs::read_dir(path).await?;
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_dir() {
            tokio::fs::remove_dir_all(entry.path()).await?
        } else {
            tokio::fs::remove_file(entry.path()).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile;

    use super::*;

    #[tokio::test]
    async fn test_empty_dir() -> anyhow::Result<()> {
        let tempdir = tempfile::tempdir()?;

        let file_path = tempdir.path().join("file");
        tokio::fs::File::create(file_path).await?;

        let subdir = tempdir.path().join("subdir");
        tokio::fs::create_dir(&subdir).await?;

        let subfile_path = subdir.join("subfile");
        tokio::fs::File::create(subfile_path).await?;

        empty_dir(tempdir.path()).await?;
        assert!(tokio::fs::read_dir(tempdir.path())
            .await?
            .next_entry()
            .await?
            .is_none());
        Ok(())
    }
}
