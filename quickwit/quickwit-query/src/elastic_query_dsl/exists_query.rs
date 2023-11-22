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

use serde::Deserialize;

use crate::elastic_query_dsl::ConvertableToQueryAst;
use crate::query_ast::{self, QueryAst};

#[derive(Deserialize, Clone, Eq, PartialEq, Debug)]
pub struct ExistsQuery {
    field: String,
}

impl ConvertableToQueryAst for ExistsQuery {
    fn convert_to_query_ast(self) -> anyhow::Result<QueryAst> {
        Ok(QueryAst::FieldPresence(query_ast::FieldPresenceQuery {
            field: self.field,
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::elastic_query_dsl::exists_query::ExistsQuery;

    #[test]
    fn test_dsl_exists_query_deserialize_simple() {
        let exists_query_json = r#"{
           "field": "privileged"
        }"#;
        let bool_query: ExistsQuery = serde_json::from_str(exists_query_json).unwrap();
        assert_eq!(
            &bool_query,
            &ExistsQuery {
                field: "privileged".to_string(),
            }
        );
    }
}
