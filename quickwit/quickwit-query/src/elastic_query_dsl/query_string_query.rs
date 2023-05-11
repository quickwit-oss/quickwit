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

use crate::elastic_query_dsl::ConvertableToQueryAst;
use crate::not_nan_f32::NotNaNf32;
use crate::query_ast::UserInputQuery;
use crate::DefaultOperator;

fn is_default<T: Default + Eq>(val: &T) -> bool {
    *val == Default::default()
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct QueryStringQuery {
    query: String,
    /// Limitation. We do not support * at the moment.
    /// We do not support JSON field either.
    ///
    /// Note that following elastic, we do not support "string" and require an array here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fields: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "is_default")]
    default_operator: DefaultOperator,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    boost: Option<NotNaNf32>,
}

impl ConvertableToQueryAst for QueryStringQuery {
    fn convert_to_query_ast(self) -> anyhow::Result<crate::query_ast::QueryAst> {
        let user_text_query = UserInputQuery {
            user_text: self.query,
            default_fields: self.fields,
            default_operator: self.default_operator,
        };
        Ok(user_text_query.into())
    }
}

#[cfg(test)]
mod tests {
    use crate::elastic_query_dsl::{ConvertableToQueryAst, QueryStringQuery};
    use crate::query_ast::{QueryAst, UserInputQuery};
    use crate::DefaultOperator;

    #[test]
    fn test_build_query_string_query_with_default_field_non_empty() {
        let query_string_query = crate::elastic_query_dsl::QueryStringQuery {
            query: "hello world".to_string(),
            fields: Some(vec!["hello".to_string()]),
            default_operator: crate::DefaultOperator::Or,
            boost: None,
        };
        let QueryAst::UserInput(user_input_query) = query_string_query.convert_to_query_ast().unwrap() else {
            panic!();
        };
        assert_eq!(user_input_query.default_operator, DefaultOperator::Or);
        assert_eq!(
            user_input_query.default_fields.unwrap(),
            vec!["hello".to_string()]
        );
    }

    #[test]
    fn test_build_query_string_query_with_default_operand_and() {
        let query_string_query = crate::elastic_query_dsl::QueryStringQuery {
            query: "hello world".to_string(),
            fields: Some(Vec::new()),
            default_operator: crate::DefaultOperator::And,
            boost: None,
        };
        let QueryAst::UserInput(user_input_query) = query_string_query.convert_to_query_ast().unwrap() else {
            panic!();
        };
        assert_eq!(user_input_query.default_operator, DefaultOperator::And);
    }

    #[test]
    fn test_build_query_string_query_with_empty_default_field() {
        let query_string_query = crate::elastic_query_dsl::QueryStringQuery {
            query: "hello world".to_string(),
            fields: Some(Vec::new()),
            default_operator: crate::DefaultOperator::Or,
            boost: None,
        };
        let QueryAst::UserInput(user_input_query) = query_string_query.convert_to_query_ast().unwrap() else {
            panic!();
        };
        assert_eq!(user_input_query.default_operator, DefaultOperator::Or);
        assert!(user_input_query.default_fields.unwrap().is_empty());
    }

    #[test]
    fn test_build_query_string_query_no_default_fields() {
        let query_string_query = crate::elastic_query_dsl::QueryStringQuery {
            query: "hello world".to_string(),
            fields: None,
            default_operator: crate::DefaultOperator::Or,
            boost: None,
        };
        let QueryAst::UserInput(user_input_query) = query_string_query.convert_to_query_ast().unwrap() else {
            panic!();
        };
        assert!(user_input_query.default_fields.is_none());
    }

    #[test]
    fn test_build_query_string_default_operator() {
        let query_string_query: QueryStringQuery =
            serde_json::from_str(r#"{ "query": "hello world", "fields": ["text"] }"#).unwrap();
        // By default the default operator is OR in elasticsearch and opensearch.
        assert_eq!(query_string_query.default_operator, DefaultOperator::Or);
        assert_eq!(query_string_query.fields, Some(vec!["text".to_string()]));
        assert_eq!(&query_string_query.query, "hello world");
        assert_eq!(query_string_query.boost, None);
        let query_ast: QueryAst = query_string_query.convert_to_query_ast().unwrap();
        assert!(matches!(query_ast, QueryAst::UserInput(UserInputQuery {
            user_text,
            default_fields,
            default_operator
        }) if user_text == "hello world"
            && default_operator == DefaultOperator::Or
            && default_fields == Some(vec!["text".to_string()])));
    }
}
