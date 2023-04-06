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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tantivy::schema::Schema;

use super::{IntoTantivyAst, QueryAst};
use crate::quickwit_query_ast::TantivyQueryAst;

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
pub struct TermQuery {
    pub field: String,
    pub value: String,
}

impl From<TermQuery> for QueryAst {
    fn from(term_query: TermQuery) -> Self {
        Self::Term(term_query)
    }
}

impl TermQuery {
    #[cfg(test)]
    pub fn from_field_value(field: impl ToString, value: impl ToString) -> Self {
        Self {
            field: field.to_string(),
            value: value.to_string(),
        }
    }
}

impl IntoTantivyAst for TermQuery {
    fn into_tantivy_ast(&self, schema: &Schema) -> anyhow::Result<TantivyQueryAst> {
        let query_ast = crate::quickwit_query_ast::utils::compute_query(
            &self.field,
            &self.value,
            false,
            schema,
        );
        Ok(query_ast)
    }
}

// Private struct used for serialization.
// It represents the value of a term query. in the json form : `{field: <TermQueryValue>}`.
#[derive(Serialize, Deserialize)]
struct TermQueryValue {
    value: String,
}

impl From<TermQuery> for (String, TermQueryValue) {
    fn from(term_query: TermQuery) -> Self {
        (
            term_query.field,
            TermQueryValue {
                value: term_query.value,
            },
        )
    }
}

impl From<(String, TermQueryValue)> for TermQuery {
    fn from((field, term_query_value): (String, TermQueryValue)) -> Self {
        Self {
            field,
            value: term_query_value.value,
        }
    }
}

impl TryFrom<HashMap<String, TermQueryValue>> for TermQuery {
    type Error = &'static str;

    fn try_from(map: HashMap<String, TermQueryValue>) -> Result<Self, Self::Error> {
        if map.len() > 1 {
            return Err("TermQuery must have exactly one entry");
        }
        Ok(TermQuery::from(map.into_iter().next().unwrap())) // unwrap justified by the if
                                                             // statementabove.
    }
}

impl From<TermQuery> for HashMap<String, TermQueryValue> {
    fn from(term_query: TermQuery) -> HashMap<String, TermQueryValue> {
        let (field, term_query_value) = term_query.into();
        let mut map = HashMap::with_capacity(1);
        map.insert(field, term_query_value);
        map
    }
}
