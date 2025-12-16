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

use std::ops::Bound;

use serde::{Deserialize, Serialize};
use tantivy::fastfield::FastValue;
use tantivy::query::FastFieldRangeQuery;
use tantivy::tokenizer::TextAnalyzer;
use tantivy::{DateTime, Term};

use super::QueryAst;
use super::tantivy_query_ast::TantivyBoolQuery;
use crate::json_literal::InterpretUserInput;
use crate::query_ast::{BuildTantivyAst, BuildTantivyAstContext, TantivyQueryAst};
use crate::{InvalidQuery, JsonLiteral};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RangeQuery {
    pub field: String,
    pub lower_bound: Bound<JsonLiteral>,
    pub upper_bound: Bound<JsonLiteral>,
}

/// Converts a given bound JsonLiteral bound into a bound of type T.
fn convert_bound<'a, T>(bound: &'a Bound<JsonLiteral>) -> Option<Bound<T>>
where T: InterpretUserInput<'a> {
    match bound {
        Bound::Included(val) => {
            let val = T::interpret_json(val)?;
            Some(Bound::Included(val))
        }
        Bound::Excluded(val) => {
            let val = T::interpret_json(val)?;
            Some(Bound::Excluded(val))
        }
        Bound::Unbounded => Some(Bound::Unbounded),
    }
}

/// Converts a given bound JsonLiteral bound into a bound of type T.
fn convert_bounds<'a, T>(
    lower_bound: &'a Bound<JsonLiteral>,
    upper_bound: &'a Bound<JsonLiteral>,
    field_name: &str,
) -> Result<(Bound<T>, Bound<T>), InvalidQuery>
where
    T: InterpretUserInput<'a>,
{
    let invalid_query = || InvalidQuery::InvalidBoundary {
        expected_value_type: T::name(),
        field_name: field_name.to_string(),
    };
    let lower_bound = convert_bound(lower_bound).ok_or_else(invalid_query)?;
    let upper_bound = convert_bound(upper_bound).ok_or_else(invalid_query)?;
    Ok((lower_bound, upper_bound))
}

/// Converts a given bound JsonLiteral bound into a bound of type T.
impl From<RangeQuery> for QueryAst {
    fn from(range_query: RangeQuery) -> Self {
        QueryAst::Range(range_query)
    }
}

fn term_with_fastval<T: FastValue>(term: &Term, val: T) -> Term {
    let mut term = term.clone();
    term.append_type_and_fast_value(val);
    term
}

fn query_from_fast_val_range<T: FastValue>(
    empty_term: &Term,
    range: (Bound<T>, Bound<T>),
) -> FastFieldRangeQuery {
    let (lower_bound, upper_bound) = range;
    FastFieldRangeQuery::new(
        lower_bound.map(|val| term_with_fastval(empty_term, val)),
        upper_bound.map(|val| term_with_fastval(empty_term, val)),
    )
}

fn get_normalized_text(normalizer: &mut Option<TextAnalyzer>, text: &str) -> String {
    if let Some(normalizer) = normalizer {
        let mut token_stream = normalizer.token_stream(text);
        let mut tokens = Vec::new();
        token_stream.process(&mut |token| {
            tokens.push(token.text.clone());
        });
        tokens[0].to_string()
    } else {
        text.to_string()
    }
}

impl BuildTantivyAst for RangeQuery {
    fn build_tantivy_ast_impl(
        &self,
        context: &BuildTantivyAstContext,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let (field, field_entry, json_path) =
            super::utils::find_field_or_hit_dynamic(&self.field, context.schema).ok_or_else(
                || InvalidQuery::FieldDoesNotExist {
                    full_path: self.field.clone(),
                },
            )?;
        if !field_entry.is_fast() {
            return Err(InvalidQuery::SchemaError(format!(
                "range queries are only supported for fast fields. (`{}` is not a fast field)",
                field_entry.name()
            )));
        }
        Ok(match field_entry.field_type() {
            tantivy::schema::FieldType::Str(options) => {
                let mut normalizer =
                    options
                        .get_fast_field_tokenizer_name()
                        .and_then(|tokenizer_name| {
                            context.tokenizer_manager.get_normalizer(tokenizer_name)
                        });

                let (lower_bound, upper_bound) =
                    convert_bounds(&self.lower_bound, &self.upper_bound, field_entry.name())?;

                FastFieldRangeQuery::new(
                    lower_bound.map(|text| {
                        Term::from_field_text(field, &get_normalized_text(&mut normalizer, text))
                    }),
                    upper_bound.map(|text| {
                        Term::from_field_text(field, &get_normalized_text(&mut normalizer, text))
                    }),
                )
                .into()
            }
            tantivy::schema::FieldType::U64(_) => {
                let (lower_bound, upper_bound) =
                    convert_bounds(&self.lower_bound, &self.upper_bound, field_entry.name())?;
                FastFieldRangeQuery::new(
                    lower_bound.map(|val| Term::from_field_u64(field, val)),
                    upper_bound.map(|val| Term::from_field_u64(field, val)),
                )
                .into()
            }
            tantivy::schema::FieldType::I64(_) => {
                let (lower_bound, upper_bound) =
                    convert_bounds(&self.lower_bound, &self.upper_bound, field_entry.name())?;
                FastFieldRangeQuery::new(
                    lower_bound.map(|val| Term::from_field_i64(field, val)),
                    upper_bound.map(|val| Term::from_field_i64(field, val)),
                )
                .into()
            }
            tantivy::schema::FieldType::F64(_) => {
                let (lower_bound, upper_bound) =
                    convert_bounds(&self.lower_bound, &self.upper_bound, field_entry.name())?;
                FastFieldRangeQuery::new(
                    lower_bound.map(|val| Term::from_field_f64(field, val)),
                    upper_bound.map(|val| Term::from_field_f64(field, val)),
                )
                .into()
            }
            tantivy::schema::FieldType::Bool(_) => {
                return Err(InvalidQuery::RangeQueryNotSupportedForField {
                    value_type: "bool",
                    field_name: field_entry.name().to_string(),
                });
            }
            tantivy::schema::FieldType::Date(date_options) => {
                let (lower_bound, upper_bound) =
                    convert_bounds(&self.lower_bound, &self.upper_bound, field_entry.name())?;
                let truncate_datetime =
                    |date: &DateTime| date.truncate(date_options.get_precision());
                let lower_bound = map_bound(&lower_bound, truncate_datetime);
                let upper_bound = map_bound(&upper_bound, truncate_datetime);
                FastFieldRangeQuery::new(
                    lower_bound.map(|val| Term::from_field_date(field, val)),
                    upper_bound.map(|val| Term::from_field_date(field, val)),
                )
                .into()
            }
            tantivy::schema::FieldType::Facet(_) => {
                return Err(InvalidQuery::RangeQueryNotSupportedForField {
                    value_type: "facet",
                    field_name: field_entry.name().to_string(),
                });
            }
            tantivy::schema::FieldType::Bytes(_) => todo!(),
            tantivy::schema::FieldType::JsonObject(options) => {
                let mut sub_queries: Vec<TantivyQueryAst> = Vec::new();
                let empty_term =
                    Term::from_field_json_path(field, json_path, options.is_expand_dots_enabled());
                // Try to convert the bounds into numerical values in following order i64, u64,
                // f64. Tantivy will convert to the correct numerical type of the column if it
                // doesn't match.
                let bounds_range_i64: Option<(Bound<i64>, Bound<i64>)> =
                    convert_bound(&self.lower_bound).zip(convert_bound(&self.upper_bound));
                let bounds_range_u64: Option<(Bound<u64>, Bound<u64>)> =
                    convert_bound(&self.lower_bound).zip(convert_bound(&self.upper_bound));
                let bounds_range_f64: Option<(Bound<f64>, Bound<f64>)> =
                    convert_bound(&self.lower_bound).zip(convert_bound(&self.upper_bound));
                if let Some(range) = bounds_range_i64 {
                    sub_queries.push(query_from_fast_val_range(&empty_term, range).into());
                } else if let Some(range) = bounds_range_u64 {
                    sub_queries.push(query_from_fast_val_range(&empty_term, range).into());
                } else if let Some(range) = bounds_range_f64 {
                    sub_queries.push(query_from_fast_val_range(&empty_term, range).into());
                }

                let mut normalizer =
                    options
                        .get_fast_field_tokenizer_name()
                        .and_then(|tokenizer_name| {
                            context.tokenizer_manager.get_normalizer(tokenizer_name)
                        });

                let bounds_range_str: Option<(Bound<&str>, Bound<&str>)> =
                    convert_bound(&self.lower_bound).zip(convert_bound(&self.upper_bound));
                if let Some(range) = bounds_range_str {
                    let str_query = FastFieldRangeQuery::new(
                        range.0.map(|val| {
                            let val = get_normalized_text(&mut normalizer, val);
                            let mut term = empty_term.clone();
                            term.append_type_and_str(&val);
                            term
                        }),
                        range.1.map(|val| {
                            let val = get_normalized_text(&mut normalizer, val);
                            let mut term = empty_term.clone();
                            term.append_type_and_str(&val);
                            term
                        }),
                    )
                    .into();
                    sub_queries.push(str_query);
                }
                if sub_queries.is_empty() {
                    return Err(InvalidQuery::InvalidBoundary {
                        expected_value_type: "i64, u64, f64, str",
                        field_name: field_entry.name().to_string(),
                    });
                }
                if sub_queries.len() == 1 {
                    return Ok(sub_queries.pop().unwrap());
                }

                let bool_query = TantivyBoolQuery {
                    should: sub_queries,
                    ..Default::default()
                };
                bool_query.into()
            }
            tantivy::schema::FieldType::IpAddr(_) => {
                let (lower_bound, upper_bound) =
                    convert_bounds(&self.lower_bound, &self.upper_bound, field_entry.name())?;
                FastFieldRangeQuery::new(
                    lower_bound.map(|val| Term::from_field_ip_addr(field, val)),
                    upper_bound.map(|val| Term::from_field_ip_addr(field, val)),
                )
                .into()
            }
        })
    }
}

fn map_bound<TFrom, TTo>(bound: &Bound<TFrom>, transform: impl Fn(&TFrom) -> TTo) -> Bound<TTo> {
    match bound {
        Bound::Excluded(from_val) => Bound::Excluded(transform(from_val)),
        Bound::Included(from_val) => Bound::Included(transform(from_val)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use tantivy::schema::{DateOptions, DateTimePrecision, FAST, STORED, Schema, TEXT};

    use super::RangeQuery;
    use crate::query_ast::{BuildTantivyAst, BuildTantivyAstContext};
    use crate::{InvalidQuery, JsonLiteral, MatchAllOrNone};

    fn make_schema(dynamic_mode: bool) -> Schema {
        let mut schema_builder = Schema::builder();
        schema_builder.add_i64_field("my_i64_field", FAST);
        schema_builder.add_u64_field("my_u64_field", FAST);
        schema_builder.add_f64_field("my_f64_field", FAST);
        schema_builder.add_text_field("my_str_field", FAST);
        let date_options = DateOptions::default()
            .set_fast()
            .set_precision(DateTimePrecision::Milliseconds);
        schema_builder.add_date_field("my_date_field", date_options);
        schema_builder.add_u64_field("my_u64_not_fastfield", STORED);
        if dynamic_mode {
            schema_builder.add_json_field("_dynamic", TEXT | STORED | FAST);
        }
        schema_builder.build()
    }

    fn test_range_query_typed_field_util(
        field: &str,
        lower_value: JsonLiteral,
        upper_value: JsonLiteral,
        expected: &str,
    ) {
        let schema = make_schema(false);
        let range_query = RangeQuery {
            field: field.to_string(),
            lower_bound: Bound::Included(lower_value),
            upper_bound: Bound::Included(upper_value),
        };
        let tantivy_ast = range_query
            .build_tantivy_ast_call(&BuildTantivyAstContext::for_test(&schema))
            .unwrap()
            .simplify();
        let leaf = tantivy_ast.as_leaf().unwrap();
        let leaf_str = format!("{leaf:?}");
        assert_eq!(leaf_str, expected);
    }

    #[test]
    fn test_range_query_typed_field() {
        test_range_query_typed_field_util(
            "my_i64_field",
            JsonLiteral::String("1980".to_string()),
            JsonLiteral::String("1989".to_string()),
            "FastFieldRangeQuery { bounds: BoundsRange { lower_bound: Included(Term(field=0, \
             type=I64, 1980)), upper_bound: Included(Term(field=0, type=I64, 1989)) } }",
        );
        test_range_query_typed_field_util(
            "my_u64_field",
            JsonLiteral::String("1980".to_string()),
            JsonLiteral::String("1989".to_string()),
            "FastFieldRangeQuery { bounds: BoundsRange { lower_bound: Included(Term(field=1, \
             type=U64, 1980)), upper_bound: Included(Term(field=1, type=U64, 1989)) } }",
        );
        test_range_query_typed_field_util(
            "my_f64_field",
            JsonLiteral::String("1980".to_string()),
            JsonLiteral::String("1989".to_string()),
            "FastFieldRangeQuery { bounds: BoundsRange { lower_bound: Included(Term(field=2, \
             type=F64, 1980.0)), upper_bound: Included(Term(field=2, type=F64, 1989.0)) } }",
        );
    }

    #[test]
    fn test_range_query_missing_field() {
        let schema = make_schema(false);
        let range_query = RangeQuery {
            field: "missing_field.toto".to_string(),
            lower_bound: Bound::Included(JsonLiteral::String("1980".to_string())),
            upper_bound: Bound::Included(JsonLiteral::String("1989".to_string())),
        };
        // with validation
        let invalid_query: InvalidQuery = range_query
            .build_tantivy_ast_call(&BuildTantivyAstContext::for_test(&schema))
            .unwrap_err();
        assert!(
            matches!(invalid_query, InvalidQuery::FieldDoesNotExist { full_path } if full_path == "missing_field.toto")
        );
        // without validation
        assert_eq!(
            range_query
                .build_tantivy_ast_call(
                    &BuildTantivyAstContext::for_test(&schema).without_validation()
                )
                .unwrap()
                .const_predicate(),
            Some(MatchAllOrNone::MatchNone)
        );
    }

    #[test]
    fn test_range_dynamic() {
        let range_query = RangeQuery {
            field: "hello".to_string(),
            lower_bound: Bound::Included(JsonLiteral::String("1980".to_string())),
            upper_bound: Bound::Included(JsonLiteral::String("1989".to_string())),
        };
        let schema = make_schema(true);
        let tantivy_ast = range_query
            .build_tantivy_ast_call(&BuildTantivyAstContext::for_test(&schema))
            .unwrap();
        assert_eq!(
            format!("{tantivy_ast:?}"),
            "Bool(TantivyBoolQuery { must: [], must_not: [], should: [Leaf(FastFieldRangeQuery { \
             bounds: BoundsRange { lower_bound: Included(Term(field=6, type=Json, path=hello, \
             type=I64, 1980)), upper_bound: Included(Term(field=6, type=Json, path=hello, \
             type=I64, 1989)) } }), Leaf(FastFieldRangeQuery { bounds: BoundsRange { lower_bound: \
             Included(Term(field=6, type=Json, path=hello, type=Str, \"1980\")), upper_bound: \
             Included(Term(field=6, type=Json, path=hello, type=Str, \"1989\")) } })], filter: \
             [], minimum_should_match: None })"
        );
    }

    #[test]
    fn test_range_query_not_fast_field() {
        let range_query = RangeQuery {
            field: "my_u64_not_fastfield".to_string(),
            lower_bound: Bound::Included(JsonLiteral::String("1980".to_string())),
            upper_bound: Bound::Included(JsonLiteral::String("1989".to_string())),
        };
        let schema = make_schema(false);
        let err = range_query
            .build_tantivy_ast_call(&BuildTantivyAstContext::for_test(&schema))
            .unwrap_err();
        assert!(matches!(err, InvalidQuery::SchemaError { .. }));
    }
}
