//  Quickwit
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

mod cluster;
mod quickwit;

#[macro_use]
extern crate serde;

pub use cluster::*;
pub use quickwit::*;

impl From<SearchStreamRequest> for SearchRequest {
    fn from(item: SearchStreamRequest) -> Self {
        Self {
            index_id: item.index_id.clone(),
            query: item.query.clone(),
            search_fields: item.search_fields.clone(),
            start_timestamp: item.start_timestamp,
            end_timestamp: item.end_timestamp,
            max_hits: 0,
            start_offset: 0,
            tag: item.tag,
        }
    }
}
