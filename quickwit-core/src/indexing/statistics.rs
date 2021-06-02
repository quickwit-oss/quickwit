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

use crate::counter::AtomicCounter;

/// A Struct that holds all statistical data about indexing
#[derive(Debug, Default)]
pub struct IndexingStatistics {
    /// Number of document processed
    pub num_docs: AtomicCounter,
    /// Number of document parse error
    pub num_parse_errors: AtomicCounter,
    /// Number of created split
    pub num_local_splits: AtomicCounter,
    /// Number of staged splits
    pub num_staged_splits: AtomicCounter,
    /// Number of uploaded splits
    pub num_uploaded_splits: AtomicCounter,
    ///Number of published splits
    pub num_published_splits: AtomicCounter,
    /// Size in byte of document processed
    pub total_bytes_processed: AtomicCounter,
    /// Size in bytes of resulting split
    pub total_size_splits: AtomicCounter,
}
