// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::{BuildTantivyAst, QueryAst};
use crate::query_ast::{BuildTantivyAstContext, FullTextParams, TantivyQueryAst};
use crate::{BooleanOperand, InvalidQuery};

/// The TermQuery acts exactly like a FullTextQuery with
/// a raw tokenizer.
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
        context: &BuildTantivyAstContext,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let full_text_params = FullTextParams {
            tokenizer: Some("raw".to_string()),
            // The parameter below won't matter, since we will have only one term
            mode: BooleanOperand::Or.into(),
            zero_terms_query: Default::default(),
        };
        crate::query_ast::utils::full_text_query(
            &self.field,
            &self.value,
            &full_text_params,
            context.schema,
            context.tokenizer_manager,
            false,
        )
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
    use tantivy::schema::{INDEXED, Schema};

    use crate::query_ast::{BuildTantivyAst, BuildTantivyAstContext, TermQuery};

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
            .build_tantivy_ast_call(&BuildTantivyAstContext::for_test(&schema))
            .unwrap();
        let leaf = tantivy_query_ast.as_leaf().unwrap();
        assert_eq!(
            &format!("{leaf:?}"),
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
            .build_tantivy_ast_call(&BuildTantivyAstContext::for_test(&schema))
            .unwrap();
        let leaf = tantivy_query_ast.as_leaf().unwrap();
        assert_eq!(
            &format!("{leaf:?}"),
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
            .build_tantivy_ast_call(&BuildTantivyAstContext::for_test(&schema))
            .unwrap();
        let leaf = tantivy_query_ast.as_leaf().unwrap();
        assert_eq!(
            &format!("{leaf:?}"),
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
            .build_tantivy_ast_call(&BuildTantivyAstContext::for_test(&schema))
            .unwrap();
        let leaf = tantivy_query_ast.as_leaf().unwrap();
        assert_eq!(
            &format!("{leaf:?}"),
            "TermQuery(Term(field=0, type=Bytes, [108, 105, 103, 104, 116, 32, 119]))"
        );
    }

    #[test]
    fn test_term_query_with_date_nanosecond() {
        let term_query = TermQuery {
            field: "timestamp".to_string(),
            value: "2025-08-07T14:49:21.831343Z".to_string(),
        };
        let mut schema_builder = Schema::builder();
        schema_builder.add_date_field("timestamp", INDEXED);
        let schema = schema_builder.build();
        let tantivy_query_ast = term_query
            .build_tantivy_ast_call(&BuildTantivyAstContext::for_test(&schema))
            .unwrap();
        let leaf = tantivy_query_ast.as_leaf().unwrap();
        // The date should have been truncated to seconds precision.
        assert_eq!(
            &format!("{leaf:?}"),
            "TermQuery(Term(field=0, type=Date, 2025-08-07T14:49:21Z))"
        );
    }
}
