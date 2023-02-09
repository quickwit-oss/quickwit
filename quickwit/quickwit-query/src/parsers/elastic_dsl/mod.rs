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

use elasticsearch_dsl::Search;

use crate::{SearchInputAst, SearchInputLeaf};

pub fn elastic_search_input_to_search_ast(
    _elastic_search_input: &Search,
) -> anyhow::Result<SearchInputAst> {
    // TODO: transform
    Ok(SearchInputAst::leaf(SearchInputLeaf::literal(
        Some("body".to_string()),
        "foo".to_string(),
        0,
    )))
}
