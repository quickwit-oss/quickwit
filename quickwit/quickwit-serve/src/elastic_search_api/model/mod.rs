// Copyright (C) 2023 Quickwit, Inc.
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

mod bulk_body;
mod bulk_query_params;
mod error;
mod multi_search;
mod search_body;
mod search_query_params;

pub use bulk_body::{BulkAction, BulkActionMeta};
pub use bulk_query_params::{ElasticIngestOptions, ElasticRefresh};
pub use error::ElasticSearchError;
pub use multi_search::{
    MultiSearchHeader, MultiSearchQueryParams, MultiSearchResponse, MultiSearchSingleResponse,
};
pub use search_body::SearchBody;
pub use search_query_params::SearchQueryParams;
