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
#![warn(missing_docs)]
#![allow(clippy::bool_assert_comparison)]

/*! `quickwit-core` provides all the core functions used in quickwit cli:
- `create_index` for creating a new index
- `index_data` for indexing new-line delimited json documents
- `search_index` for searching an index
- `delete_index` for deleting an index
*/

mod index;
mod test_utils;

pub use index::{create_index, delete_index, garbage_collect_index, reset_index};
pub use test_utils::TestSandbox;

#[derive(Debug)]
pub struct FileEntry {
    pub file_name: String,
    pub file_size_in_bytes: u64,
}
