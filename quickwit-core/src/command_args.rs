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

use std::path::PathBuf;

pub struct CreateIndexArgs {
    pub index_uri: PathBuf,
    pub timestamp_field: Option<String>,
    pub overwrite: bool,
}

#[derive(Debug, Clone)]
pub struct IndexDataArgs {
    pub index_uri: PathBuf,
    pub input_uri: Option<PathBuf>,
    pub temp_dir: PathBuf,
    pub num_threads: usize,
    pub heap_size: u64,
    pub overwrite: bool,
}

pub struct SearchIndexArgs {
    pub index_uri: PathBuf,
    pub query: String,
    pub max_hits: usize,
    pub start_offset: usize,
    pub search_fields: Option<Vec<String>>,
    pub start_timestamp: Option<i64>,
    pub end_timestamp: Option<i64>,
}

pub struct DeleteIndexArgs {
    pub index_uri: PathBuf,
    pub dry_run: bool,
}
