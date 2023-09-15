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
use crate::not_nan_f32::NotNaNf32;
use crate::query_ast::UserInputQuery;
use crate::BooleanOperand;

#[derive(Deserialize, Debug, Eq, PartialEq, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct QueryStringQuery {
    query: String,
    /// Limitation. We do not support * at the moment.
    /// We do not support JSON field either.
    ///
    /// Note that following elastic, we do not support "string" and require an array here.
    #[serde(default)]
    fields: Option<Vec<String>>,
    #[serde(default)]
    default_field: Option<String>,
    #[serde(default)]
    default_operator: BooleanOperand,
    #[serde(default)]
    boost: Option<NotNaNf32>,
    // Regardless of this option Quickwit behaves in elasticsearch definition of
    // lenient. We include this property here just to accept user queries containing
    // this option.
    #[serde(default, rename = "lenient")]
    _lenient: bool,
}

impl ConvertableToQueryAst for QueryStringQuery {
    fn convert_to_query_ast(self) -> anyhow::Result<crate::query_ast::QueryAst> {
        if self.default_field.is_some() && self.fields.is_some() {
            anyhow::bail!("fields and default_field cannot be both set in `query_string` queries");
        }
        let default_fields: Option<Vec<String>> = self
            .default_field
            .map(|default_field| vec![default_field])
            .or(self.fields);
        let user_text_query = UserInputQuery {
            user_text: self.query,
            default_fields,
            default_operator: self.default_operator,
        };
        Ok(user_text_query.into())
    }
}

#[cfg(test)]
mod tests {
    use crate::elastic_query_dsl::{ConvertableToQueryAst, QueryStringQuery};
    use crate::query_ast::{QueryAst, UserInputQuery};
    use crate::BooleanOperand;

    #[test]
    fn test_build_query_string_query_with_fields_non_empty() {
        let query_string_query = crate::elastic_query_dsl::QueryStringQuery {
            query: "hello world".to_string(),
            fields: Some(vec!["hello".to_string()]),
            default_operator: crate::BooleanOperand::Or,
            default_field: None,
            boost: None,
            _lenient: false,
        };
        let QueryAst::UserInput(user_input_query) =
            query_string_query.convert_to_query_ast().unwrap()
        else {
            panic!();
        };
        assert_eq!(user_input_query.default_operator, BooleanOperand::Or);
        assert_eq!(
            user_input_query.default_fields.unwrap(),
            vec!["hello".to_string()]
        );
    }

    #[test]
    fn test_build_query_string_query_with_default_field_non_empty() {
        let query_string_query = crate::elastic_query_dsl::QueryStringQuery {
            query: "hello world".to_string(),
            fields: None,
            default_operator: crate::BooleanOperand::Or,
            default_field: Some("hello".to_string()),
            boost: None,
            _lenient: false,
        };
        let QueryAst::UserInput(user_input_query) =
            query_string_query.convert_to_query_ast().unwrap()
        else {
            panic!();
        };
        assert_eq!(user_input_query.default_operator, BooleanOperand::Or);
        assert_eq!(
            user_input_query.default_fields.unwrap(),
            vec!["hello".to_string()]
        );
    }

    #[test]
    fn test_build_query_string_query_with_both_default_fields_and_field_yield_an_error() {
        let query_string_query = crate::elastic_query_dsl::QueryStringQuery {
            query: "hello world".to_string(),
            fields: Some(vec!["hello".to_string()]),
            default_operator: crate::BooleanOperand::Or,
            default_field: Some("hello".to_string()),
            boost: None,
            _lenient: false,
        };
        let err_msg = query_string_query
            .convert_to_query_ast()
            .unwrap_err()
            .to_string();
        assert!(err_msg.contains("cannot be both set"));
    }

    #[test]
    fn test_build_query_string_query_with_default_operand_and() {
        let query_string_query = crate::elastic_query_dsl::QueryStringQuery {
            query: "hello world".to_string(),
            fields: Some(Vec::new()),
            default_field: None,
            default_operator: crate::BooleanOperand::And,
            boost: None,
            _lenient: false,
        };
        let QueryAst::UserInput(user_input_query) =
            query_string_query.convert_to_query_ast().unwrap()
        else {
            panic!();
        };
        assert_eq!(user_input_query.default_operator, BooleanOperand::And);
    }

    #[test]
    fn test_build_query_string_query_with_empty_default_field() {
        let query_string_query = crate::elastic_query_dsl::QueryStringQuery {
            query: "hello world".to_string(),
            fields: Some(Vec::new()),
            default_field: None,
            default_operator: crate::BooleanOperand::Or,
            boost: None,
            _lenient: false,
        };
        let QueryAst::UserInput(user_input_query) =
            query_string_query.convert_to_query_ast().unwrap()
        else {
            panic!();
        };
        assert_eq!(user_input_query.default_operator, BooleanOperand::Or);
        assert!(user_input_query.default_fields.unwrap().is_empty());
    }

    #[test]
    fn test_build_query_string_query_no_default_fields() {
        let query_string_query = crate::elastic_query_dsl::QueryStringQuery {
            query: "hello world".to_string(),
            fields: None,
            default_field: None,
            default_operator: crate::BooleanOperand::Or,
            boost: None,
            _lenient: false,
        };
        let QueryAst::UserInput(user_input_query) =
            query_string_query.convert_to_query_ast().unwrap()
        else {
            panic!();
        };
        assert!(user_input_query.default_fields.is_none());
    }

    #[test]
    fn test_build_query_string_default_operator() {
        let query_string_query: QueryStringQuery =
            serde_json::from_str(r#"{ "query": "hello world", "fields": ["text"] }"#).unwrap();
        // By default the default operator is OR in elasticsearch and opensearch.
        assert_eq!(query_string_query.default_operator, BooleanOperand::Or);
        assert_eq!(query_string_query.fields, Some(vec!["text".to_string()]));
        assert_eq!(&query_string_query.query, "hello world");
        assert_eq!(query_string_query.boost, None);
        let query_ast: QueryAst = query_string_query.convert_to_query_ast().unwrap();
        assert!(matches!(query_ast, QueryAst::UserInput(UserInputQuery {
            user_text,
            default_fields,
            default_operator
        }) if user_text == "hello world"
            && default_operator == BooleanOperand::Or
            && default_fields == Some(vec!["text".to_string()])));
    }
}
