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

use async_trait::async_trait;
use std::io;
use tokio::io::AsyncBufReadExt;
use tokio::io::Lines;

/// A trait for retrieving input content one line at a time
#[async_trait]
pub trait DocumentSource: 'static {
    /// Reads one line from underlying input buffer.
    async fn next_document(&mut self) -> io::Result<Option<String>>;
}

#[async_trait]
impl<I: AsyncBufReadExt + 'static + Unpin + Send> DocumentSource for Lines<I> {
    async fn next_document(&mut self) -> io::Result<Option<String>> {
        self.next_line().await
    }
}

#[cfg(test)]
struct TestDocuments<I>(pub I);

#[cfg(test)]
#[async_trait]
impl<I> DocumentSource for TestDocuments<I>
where
    I: Iterator<Item = serde_json::Value> + 'static + Send,
{
    async fn next_document(&mut self) -> tokio::io::Result<Option<String>> {
        if let Some(doc_json) = self.0.next() {
            let doc_json_line = serde_json::to_string(&doc_json)
                .map_err(|serde_err| io::Error::new(io::ErrorKind::Other, serde_err))?;
            Ok(Some(doc_json_line))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
pub fn test_documents<I>(doc_jsons: I) -> Box<dyn DocumentSource>
where
    I: IntoIterator<Item = serde_json::Value> + 'static,
    I::IntoIter: Send,
{
    Box::new(TestDocuments(doc_jsons.into_iter()))
}
