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

use serde::{Deserialize, Serialize};
use tantivy::Order as TantivyOrder;

// TODO: Move to `quickwit-config` when `quickwit-config` no longer depends on
// `quickwit-doc-mapper`.

/// Sort order, either ascending or descending.
/// Use `SortOrder::Desc` for top-k queries.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SortOrder {
    /// Ascending order.
    Asc,
    /// Descending order, the default.
    Desc,
}

impl Default for SortOrder {
    fn default() -> Self {
        Self::Desc
    }
}

impl From<SortOrder> for TantivyOrder {
    fn from(order: SortOrder) -> Self {
        match order {
            SortOrder::Asc => TantivyOrder::Asc,
            SortOrder::Desc => TantivyOrder::Desc,
        }
    }
}

/// Specifies how documents are sorted.
/// In case of a tie, the documents are ordered according to descending `(split_id, segment_ord,
/// doc_id)`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SortBy {
    /// Sort by document ID.
    DocId,
    /// Sort by a specific field. The field must be a fast field.
    FastField {
        /// Name of the field to sort by.
        field_name: String,
        /// Order to sort by. A usual top-k search implies a descending order.
        order: SortOrder,
    },
}
