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

use crate::elastic_query_dsl::{ConvertableToQueryAst, ElasticQueryDslInner};
use crate::not_nan_f32::NotNaNf32;
use crate::quickwit_query_ast;

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
#[serde(
    into = "HashMap<String, TermQueryValue>",
    try_from = "HashMap<String, TermQueryValue>"
)]
pub struct TermQuery {
    pub field: String,
    pub value: String,
    #[serde(default)]
    pub boost: Option<NotNaNf32>,
}

impl TermQuery {
    #[cfg(test)]
    pub fn from_field_value(field: impl ToString, value: impl ToString) -> Self {
        Self {
            field: field.to_string(),
            value: value.to_string(),
            boost: None,
        }
    }
}

impl From<TermQuery> for ElasticQueryDslInner {
    fn from(term_query: TermQuery) -> Self {
        Self::Term(term_query)
    }
}

impl ConvertableToQueryAst for TermQuery {
    fn convert_to_query_ast(
        self,
        _default_search_fields: &[&str],
    ) -> anyhow::Result<crate::quickwit_query_ast::QueryAst> {
        let term_ast = quickwit_query_ast::TermQuery {
            field: self.field,
            value: self.value,
        };
        Ok(term_ast.into())
    }
}

// Private struct used for serialization.
// It represents the value of a term query. in the json form : `{field: <TermQueryValue>}`.
#[derive(Serialize, Deserialize)]
struct TermQueryValue {
    value: String,
}

impl TryFrom<HashMap<String, TermQueryValue>> for TermQuery {
    type Error = &'static str;

    fn try_from(map: HashMap<String, TermQueryValue>) -> Result<Self, Self::Error> {
        if map.len() != 1 {
            return Err("TermQuery must have exactly one entry");
        }
        let (field, term_query_value) = map.into_iter().next().unwrap(); //< Safe. See checked above.
        Ok(Self {
            field,
            value: term_query_value.value,
            boost: None,
        })
    }
}

impl From<TermQuery> for HashMap<String, TermQueryValue> {
    fn from(term_query: TermQuery) -> HashMap<String, TermQueryValue> {
        let (field, term_query_value) = (
            term_query.field,
            TermQueryValue {
                value: term_query.value,
            },
        );
        let mut map = HashMap::with_capacity(1);
        map.insert(field, term_query_value);
        map
    }
}

#[cfg(test)]
mod tests {
    use super::TermQuery;

    #[test]
    fn test_term_query_simple() {
        let term_query_json = r#"{ "product_id": { "value": "61809" } }"#;
        let term_query: TermQuery = serde_json::from_str(term_query_json).unwrap();
        assert_eq!(
            &term_query,
            &TermQuery::from_field_value("product_id", "61809")
        );
    }
}
