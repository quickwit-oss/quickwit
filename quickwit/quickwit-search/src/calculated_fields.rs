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

//! Calculated fields: fields computed from fast fields with a jitexpr expression.
//!
//! They are defined in [`SearchRequest::calculated_fields`] and can be used as field names in
//! the aggregation request. A calculated field shadows a fast field with the same name.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use quickwit_proto::search::{CalculatedField, SearchRequest};
use tantivy::aggregation::{JitExprValueSourceProvider, ValueSourceRegistry};
use tantivy::jitexpr::ast::{UntypedExpr, infer_types};

use crate::SearchError;

/// The calculated fields of a search request, parsed and type checked.
#[derive(Default)]
pub(crate) struct CalculatedFields {
    expressions: HashMap<String, UntypedExpr>,
}

impl CalculatedFields {
    /// Parses and type checks the calculated fields of `search_request`.
    pub fn from_search_request(search_request: &SearchRequest) -> crate::Result<CalculatedFields> {
        Self::parse(&search_request.calculated_fields)
    }

    fn parse(calculated_fields: &[CalculatedField]) -> crate::Result<CalculatedFields> {
        let mut expressions: HashMap<String, UntypedExpr> =
            HashMap::with_capacity(calculated_fields.len());
        for calculated_field in calculated_fields {
            if calculated_field.name.is_empty() {
                return Err(SearchError::InvalidArgument(
                    "calculated field name must not be empty".to_string(),
                ));
            }
            let expression: UntypedExpr = tantivy::jitexpr::ast::deserialize(
                &calculated_field.expression,
            )
            .map_err(|deserialize_error| {
                SearchError::InvalidArgument(format!(
                    "invalid expression for calculated field `{}`: {deserialize_error}",
                    calculated_field.name
                ))
            })?;
            if let Err(type_error) = infer_types(&expression) {
                return Err(SearchError::InvalidArgument(format!(
                    "invalid expression for calculated field `{}`: {type_error}",
                    calculated_field.name
                )));
            }
            if expressions
                .insert(calculated_field.name.clone(), expression)
                .is_some()
            {
                return Err(SearchError::InvalidArgument(format!(
                    "calculated field `{}` is defined several times",
                    calculated_field.name
                )));
            }
        }
        Ok(CalculatedFields { expressions })
    }

    pub fn is_empty(&self) -> bool {
        self.expressions.is_empty()
    }

    /// Builds the registry through which tantivy aggregations resolve calculated fields.
    pub fn value_source_registry(&self) -> crate::Result<Arc<ValueSourceRegistry>> {
        let mut registry = ValueSourceRegistry::default();
        for (name, expression) in &self.expressions {
            // Cannot fail in practice: the expression was type checked in `parse`.
            let provider =
                JitExprValueSourceProvider::new(expression.clone()).map_err(|type_error| {
                    SearchError::InvalidArgument(format!(
                        "invalid expression for calculated field `{name}`: {type_error}"
                    ))
                })?;
            registry.register(name, Arc::new(provider));
        }
        Ok(Arc::new(registry))
    }

    /// Replaces the calculated fields of `field_names` by the fast fields their expressions read.
    ///
    /// Used to warm up the columns actually read by aggregations.
    pub fn resolve_fast_field_names(&self, field_names: HashSet<String>) -> HashSet<String> {
        let mut fast_field_names: HashSet<String> = HashSet::with_capacity(field_names.len());
        for field_name in field_names {
            let Some(expression) = self.expressions.get(&field_name) else {
                fast_field_names.insert(field_name);
                continue;
            };
            // Cannot fail: the expression was type checked in `parse`.
            let Ok(inferred_types) = infer_types(expression) else {
                continue;
            };
            fast_field_names.extend(
                inferred_types
                    .into_keys()
                    .map(|variable_name| variable_name.to_string()),
            );
        }
        fast_field_names
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn calculated_field(name: &str, expression: &str) -> CalculatedField {
        CalculatedField {
            name: name.to_string(),
            expression: expression.to_string(),
        }
    }

    #[test]
    fn test_parse_calculated_fields() {
        let calculated_fields = CalculatedFields::parse(&[
            calculated_field("doubled", "(MULTIPLY latency 2i64)"),
            calculated_field("upper", "(UPPER service)"),
        ])
        .unwrap();
        assert!(calculated_fields.expressions.contains_key("doubled"));
        assert!(calculated_fields.expressions.contains_key("upper"));
        assert!(!calculated_fields.expressions.contains_key("latency"));
        calculated_fields.value_source_registry().unwrap();
    }

    #[test]
    fn test_parse_calculated_fields_errors() {
        for (calculated_fields, expected_message) in [
            (
                vec![calculated_field("", "(UPPER service)")],
                "must not be empty",
            ),
            (
                vec![calculated_field("bad", "(UPPER service")],
                "invalid expression for calculated field `bad`",
            ),
            (
                vec![calculated_field("bad", "(UPPER 1i64)")],
                "invalid expression for calculated field `bad`",
            ),
            (
                vec![
                    calculated_field("twice", "(UPPER service)"),
                    calculated_field("twice", "(LOWER service)"),
                ],
                "defined several times",
            ),
        ] {
            let Err(error) = CalculatedFields::parse(&calculated_fields) else {
                panic!("expected an error for {calculated_fields:?}");
            };
            assert!(
                error.to_string().contains(expected_message),
                "{calculated_fields:?}: {error}"
            );
        }
    }

    #[test]
    fn test_resolve_fast_field_names() {
        let calculated_fields =
            CalculatedFields::parse(&[calculated_field("ratio", "(DIVIDE latency duration)")])
                .unwrap();
        let field_names: HashSet<String> = ["ratio".to_string(), "service".to_string()].into();
        let fast_field_names = calculated_fields.resolve_fast_field_names(field_names);
        let expected: HashSet<String> = [
            "latency".to_string(),
            "duration".to_string(),
            "service".to_string(),
        ]
        .into();
        assert_eq!(fast_field_names, expected);
    }
}
