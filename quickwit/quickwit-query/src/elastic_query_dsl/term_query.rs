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

use serde::{Deserialize, Serialize};

use crate::elastic_query_dsl::one_field_map::OneFieldMap;
use crate::elastic_query_dsl::{ConvertableToQueryAst, ElasticQueryDslInner};
use crate::not_nan_f32::NotNaNf32;
use crate::query_ast::{self, QueryAst};

pub type TermQuery = OneFieldMap<TermQueryValue>;

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct TermQueryValue {
    pub value: String,
    #[serde(default)]
    pub boost: Option<NotNaNf32>,
}

#[cfg(test)]
pub fn term_query_from_field_value(field: impl ToString, value: impl ToString) -> TermQuery {
    TermQuery {
        field: field.to_string(),
        value: TermQueryValue {
            value: value.to_string(),
            boost: None,
        },
    }
}

impl From<TermQuery> for ElasticQueryDslInner {
    fn from(term_query: TermQuery) -> Self {
        Self::Term(term_query)
    }
}

impl ConvertableToQueryAst for TermQuery {
    fn convert_to_query_ast(self) -> anyhow::Result<QueryAst> {
        let TermQueryValue { value, boost } = self.value;
        let term_ast: QueryAst = query_ast::TermQuery {
            field: self.field,
            value,
        }
        .into();
        Ok(term_ast.boost(boost))
    }
}

#[cfg(test)]
mod tests {
    use super::TermQuery;
    use crate::elastic_query_dsl::term_query::term_query_from_field_value;

    #[test]
    fn test_term_query_simple() {
        let term_query_json = r#"{ "product_id": { "value": "61809" } }"#;
        let term_query: TermQuery = serde_json::from_str(term_query_json).unwrap();
        assert_eq!(
            &term_query,
            &term_query_from_field_value("product_id", "61809")
        );
    }
}
