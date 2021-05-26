/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use anyhow::Context;
use async_trait::async_trait;
use std::path::PathBuf;
use tokio::io::AsyncBufReadExt;
use tokio::io::{self, BufReader};
use tokio::{
    fs::File,
    io::{Lines, Stdin},
};

#[cfg(test)]
use std::io::Cursor;

/// A trait for retrieving input content online at a time
#[async_trait]
pub trait DocumentRetriever {
    /// Reads one line from underlying input buffer.
    async fn next_document(&mut self) -> anyhow::Result<Option<String>>;
}

/// Retrieves new-line delimited json documents from StdIn or from a local File
pub enum FileOrStdInDocumentRetriever {
    StdIn(Lines<BufReader<Stdin>>),
    File(Lines<BufReader<File>>),
}

impl FileOrStdInDocumentRetriever {
    /// creates an instance of [`DocumentRetriever`]
    pub async fn create(input_uri: &Option<PathBuf>) -> anyhow::Result<Self> {
        match input_uri {
            Some(path_buf) => {
                let file = File::open(path_buf).await?;
                let reader = BufReader::new(file);
                Ok(Self::File(reader.lines()))
            }
            None => {
                let file = io::stdin();
                let reader = BufReader::new(file);
                Ok(Self::StdIn(reader.lines()))
            }
        }
    }
}

#[async_trait]
impl DocumentRetriever for FileOrStdInDocumentRetriever {
    /// Reads one line from this [`DocumentRetriever`] input (File or StdIn)
    async fn next_document(&mut self) -> anyhow::Result<Option<String>> {
        match self {
            Self::StdIn(stdin_lines) => stdin_lines
                .next_line()
                .await
                .with_context(|| "Failed to read next line from StdIn".to_string()),
            Self::File(file_lines) => file_lines
                .next_line()
                .await
                .with_context(|| "Failed to read next line from File".to_string()),
        }
    }
}

#[cfg(test)]
pub struct CursorDocumentRetriever {
    lines: Lines<BufReader<Cursor<&'static str>>>,
}

#[cfg(test)]
impl CursorDocumentRetriever {
    /// Creates an instance of this doc retriever
    pub fn new(data: &'static str) -> Self {
        let cursor = Cursor::new(data);
        let reader = BufReader::new(cursor);
        Self {
            lines: reader.lines(),
        }
    }
}

#[cfg(test)]
#[async_trait]
impl DocumentRetriever for CursorDocumentRetriever {
    async fn next_document(&mut self) -> anyhow::Result<Option<String>> {
        self.lines
            .next_line()
            .await
            .with_context(|| "Failed to read next line from cursor".to_string())
    }
}
