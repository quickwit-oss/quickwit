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

mod ast;
mod parsers;
mod validations;

pub use ast::*;
pub use parsers::{elastic_search_input_to_search_ast, parse_quickwit_dsl};
pub use validations::{
    extract_field_with_ranges, extract_term_set_query_fields, needs_default_search_field,
    resolve_fields, validate_requested_snippet_fields, validate_sort_by_field,
};

#[cfg(test)]
mod test {
    use serde_json::json;

    use crate::ast::{SearchInputAst, SearchInputLeaf};

    #[test]
    fn test_serialize() -> anyhow::Result<()> {
        let search_input_ast = SearchInputAst::leaf(SearchInputLeaf::literal(
            Some("foo".to_string()),
            "bar".to_string(),
            1,
        ));
        let expected_obj = json!({
            "Leaf":{
                "Literal":{
                    "field_name_opt":"foo",
                    "phrase":"bar",
                    "slop":1
                }
            }
        });

        let json_obj: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&search_input_ast)?)?;
        let json_str = serde_json::to_string(&json_obj)?;

        let expected_str = serde_json::to_string(&expected_obj)?;

        assert_eq!(json_str, expected_str);
        Ok(())
    }
}
