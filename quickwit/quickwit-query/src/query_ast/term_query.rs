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
use tantivy::schema::Schema as TantivySchema;

use super::{BuildTantivyAst, QueryAst};
use crate::query_ast::TantivyQueryAst;
use crate::InvalidQuery;

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

impl BuildTantivyAst for TermQuery {
    fn build_tantivy_ast_impl(
        &self,
        schema: &TantivySchema,
        _search_fields: &[String],
        _with_validation: bool,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        crate::query_ast::utils::full_text_query(&self.field, &self.value, 0, false, schema)
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

#[cfg(test)]
mod tests {
    use tantivy::schema::{Schema, INDEXED};

    use crate::query_ast::{BuildTantivyAst, TermQuery};

    #[test]
    fn test_term_query_with_ipaddr_ipv4() {
        let term_query = TermQuery {
            field: "ip".to_string(),
            value: "127.0.0.1".to_string(),
        };
        let mut schema_builder = Schema::builder();
        schema_builder.add_ip_addr_field("ip", INDEXED);
        let schema = schema_builder.build();
        let tantivy_query_ast = term_query
            .build_tantivy_ast_call(&schema, &[], true)
            .unwrap();
        let leaf = tantivy_query_ast.as_leaf().unwrap();
        assert_eq!(
            &format!("{:?}", leaf),
            "TermQuery(Term(field=0, type=IpAddr, ::ffff:127.0.0.1))"
        );
    }

    #[test]
    fn test_term_query_with_ipaddr_compressed_ipv6() {
        let term_query = TermQuery {
            field: "ip".to_string(),
            value: "2001:db8:85a3::8a2e:370:7334".to_string(), //< note the ::. This is a compressed form
        };
        let mut schema_builder = Schema::builder();
        schema_builder.add_ip_addr_field("ip", INDEXED);
        let schema = schema_builder.build();
        let tantivy_query_ast = term_query
            .build_tantivy_ast_call(&schema, &[], true)
            .unwrap();
        let leaf = tantivy_query_ast.as_leaf().unwrap();
        assert_eq!(
            &format!("{:?}", leaf),
            "TermQuery(Term(field=0, type=IpAddr, 2001:db8:85a3::8a2e:370:7334))"
        );
    }

    #[test]
    fn test_term_query_bytes_with_padding() {
        let term_query = TermQuery {
            field: "bytes".to_string(),
            value: "bGlnaHQgdw==".to_string(),
        };
        let mut schema_builder = Schema::builder();
        schema_builder.add_bytes_field("bytes", INDEXED);
        let schema = schema_builder.build();
        let tantivy_query_ast = term_query
            .build_tantivy_ast_call(&schema, &[], true)
            .unwrap();
        let leaf = tantivy_query_ast.as_leaf().unwrap();
        assert_eq!(
            &format!("{:?}", leaf),
            "TermQuery(Term(field=0, type=Bytes, [108, 105, 103, 104, 116, 32, 119]))"
        );
    }

    #[test]
    fn test_term_query_bytes_without_padding() {
        let term_query = TermQuery {
            field: "bytes".to_string(),
            value: "bGlnaHQgdw".to_string(),
        };
        let mut schema_builder = Schema::builder();
        schema_builder.add_bytes_field("bytes", INDEXED);
        let schema = schema_builder.build();
        let tantivy_query_ast = term_query
            .build_tantivy_ast_call(&schema, &[], true)
            .unwrap();
        let leaf = tantivy_query_ast.as_leaf().unwrap();
        assert_eq!(
            &format!("{:?}", leaf),
            "TermQuery(Term(field=0, type=Bytes, [108, 105, 103, 104, 116, 32, 119]))"
        );
    }
}
