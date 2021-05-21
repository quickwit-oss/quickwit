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

use serde::{Deserialize, Serialize};
use std::ops::Range;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SplitMetaData {
    // The split id
    pub split_id: Uuid,

    // Split uri. In spirit, this uri should be self sufficient
    // to identify a split.
    // In reality, some information may be implicitly configure
    // in the store uri resolver, such as the Amazon S3 region.
    pub split_uri: String,

    // Number of records (or documents) in the split
    pub num_records: usize,

    // Number records (or documents) we could not parse for the index
    pub num_parsing_errors: usize,

    // Weight in bytes of the split
    pub size_in_bytes: usize,

    // if a time field is available, the min / max timestamp in the segment.
    pub time_range: Option<Range<u64>>,

    // Number of merge this segment has been subjected to during its lifetime.
    pub generation: usize,
}

impl SplitMetaData {
    pub fn new(split_id: Uuid, index_uri: String) -> Self {
        let split_datetime = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let split_path = format!("{}/{}", split_datetime, split_id);
        let split_uri = format!("{}{}", index_uri, split_path);
        Self {
            split_id,
            split_uri,
            num_records: 0,
            num_parsing_errors: 0,
            size_in_bytes: 0,
            time_range: None,
            generation: 0,
        }
    }
}
