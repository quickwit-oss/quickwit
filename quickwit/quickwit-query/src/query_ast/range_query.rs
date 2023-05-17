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

use std::ops::Bound;

use serde::{Deserialize, Serialize};
use tantivy::query::{
    FastFieldRangeWeight as TantivyFastFieldRangeQuery, RangeQuery as TantivyRangeQuery,
};
use tantivy::schema::Schema as TantivySchema;

use super::QueryAst;
use crate::json_literal::InterpretUserInput;
use crate::query_ast::tantivy_query_ast::{TantivyBoolQuery, TantivyQueryAst};
use crate::query_ast::BuildTantivyAst;
use crate::{InvalidQuery, JsonLiteral};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RangeQuery {
    pub field: String,
    pub lower_bound: Bound<JsonLiteral>,
    pub upper_bound: Bound<JsonLiteral>,
}

struct NumericalBoundaries {
    i64_range: (Bound<i64>, Bound<i64>),
    u64_range: (Bound<u64>, Bound<u64>),
    f64_range: (Bound<f64>, Bound<f64>),
}

fn extract_boundary_value<T>(bound: &Bound<T>) -> Option<&T> {
    match bound {
        Bound::Included(val) | Bound::Excluded(val) => Some(val),
        Bound::Unbounded => None,
    }
}

trait IntType {
    fn min() -> Self;
    fn max() -> Self;
    fn to_f64(self) -> f64;
    fn from_f64(val: f64) -> Self;
}
impl IntType for i64 {
    fn min() -> Self {
        Self::MIN
    }
    fn max() -> Self {
        Self::MAX
    }
    fn to_f64(self) -> f64 {
        self as f64
    }
    fn from_f64(val: f64) -> Self {
        val as Self
    }
}
impl IntType for u64 {
    fn min() -> Self {
        Self::MIN
    }
    fn max() -> Self {
        Self::MAX
    }
    fn to_f64(self) -> f64 {
        self as f64
    }
    fn from_f64(val: f64) -> Self {
        val as Self
    }
}

fn convert_lower_bound<'a, T: IntType + InterpretUserInput<'a>>(
    lower_bound_f64: f64,
    lower_bound: &'a Bound<JsonLiteral>,
) -> Bound<T> {
    convert_bound(lower_bound).unwrap_or_else(|| {
        if lower_bound_f64 <= T::min().to_f64() {
            // All value should match
            return Bound::Unbounded;
        }
        if lower_bound_f64 > T::max().to_f64() {
            // No values should match
            return Bound::Excluded(T::max());
        }
        // The miss was due to a decimal number.
        Bound::Included(T::from_f64(lower_bound_f64.ceil()))
    })
}

fn convert_upper_bound<'a, T: IntType + InterpretUserInput<'a>>(
    upper_bound_f64: f64,
    lower_bound: &'a Bound<JsonLiteral>,
) -> Bound<T> {
    convert_bound(lower_bound).unwrap_or_else(|| {
        if upper_bound_f64 >= T::max().to_f64() {
            // All value should match
            return Bound::Unbounded;
        }
        if upper_bound_f64 < T::min().to_f64() {
            // No values should match
            return Bound::Excluded(T::max());
        }
        // The miss was due to a decimal number.
        Bound::Included(T::from_f64(upper_bound_f64.floor()))
    })
}

/// This function interprets the lower_bound and upper_bound as numerical boundaries
/// for JSON field.
fn compute_numerical_boundaries(
    lower_bound: &Bound<JsonLiteral>,
    upper_bound: &Bound<JsonLiteral>,
) -> Option<NumericalBoundaries> {
    // Let's check that this range can be interpret (as in both, bounds),
    // as a numerical range.
    // Note that if interpreting as a f64 range fails, we consider the boundary non
    // numerical and do not attempt to build u64/i64 boundaries.
    let lower_bound_f64: Bound<f64> = convert_bound(lower_bound)?;
    let upper_bound_f64: Bound<f64> = convert_bound(upper_bound)?;

    let lower_bound_i64: Bound<i64>;
    let lower_bound_u64: Bound<u64>;
    if let Some(lower_bound_f64) = extract_boundary_value(&lower_bound_f64).copied() {
        lower_bound_i64 = convert_lower_bound(lower_bound_f64, lower_bound);
        lower_bound_u64 = convert_lower_bound(lower_bound_f64, lower_bound);
    } else {
        lower_bound_i64 = Bound::Unbounded;
        lower_bound_u64 = Bound::Unbounded;
    }

    let upper_bound_i64: Bound<i64>;
    let upper_bound_u64: Bound<u64>;
    if let Some(upper_bound_f64) = extract_boundary_value(&upper_bound_f64).copied() {
        upper_bound_i64 = convert_upper_bound(upper_bound_f64, upper_bound);
        upper_bound_u64 = convert_upper_bound(upper_bound_f64, upper_bound);
    } else {
        upper_bound_i64 = Bound::Unbounded;
        upper_bound_u64 = Bound::Unbounded;
    }
    Some(NumericalBoundaries {
        i64_range: (lower_bound_i64, upper_bound_i64),
        u64_range: (lower_bound_u64, upper_bound_u64),
        f64_range: (lower_bound_f64, upper_bound_f64),
    })
}

/// Converts a given bound JsonLiteral bound into a bound of type T.
fn convert_bound<'a, T>(bound: &'a Bound<JsonLiteral>) -> Option<Bound<T>>
where T: InterpretUserInput<'a> {
    match bound {
        Bound::Included(val) => {
            let val = T::interpret(val)?;
            Some(Bound::Included(val))
        }
        Bound::Excluded(val) => {
            let val = T::interpret(val)?;
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

/// Return
/// - Some(true) if the range is guaranteed to be empty
/// - Some(false) if the range is not empty
/// - None if we cannot judge easily.
fn is_empty<T: Ord>(boundaries: &(Bound<T>, Bound<T>)) -> Option<bool> {
    match boundaries {
        (Bound::Included(lower), Bound::Included(upper)) => Some(lower > upper),
        (Bound::Included(lower), Bound::Excluded(upper))
        | (Bound::Excluded(lower), Bound::Included(upper)) => Some(lower >= upper),
        (Bound::Unbounded, Bound::Included(_)) | (Bound::Included(_), Bound::Unbounded) => {
            Some(false)
        }
        _ => None,
    }
}

impl BuildTantivyAst for RangeQuery {
    fn build_tantivy_ast_impl(
        &self,
        schema: &TantivySchema,
        _search_fields: &[String],
        _with_validation: bool,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let (_field, field_entry, _path) =
            super::utils::find_field_or_hit_dynamic(&self.field, schema)?;
        if !field_entry.is_fast() {
            return Err(InvalidQuery::SchemaError(format!(
                "Range queries are only supported for fast fields. (`{}` is not a fast field)",
                field_entry.name()
            )));
        }
        Ok(match field_entry.field_type() {
            tantivy::schema::FieldType::Str(_) => {
                return Err(InvalidQuery::RangeQueryNotSupportedForField {
                    value_type: "str",
                    field_name: field_entry.name().to_string(),
                });
            }
            tantivy::schema::FieldType::U64(_) => {
                let (lower_bound, upper_bound) =
                    convert_bounds(&self.lower_bound, &self.upper_bound, field_entry.name())?;
                TantivyFastFieldRangeQuery::new::<u64>(self.field.clone(), lower_bound, upper_bound)
                    .into()
            }
            tantivy::schema::FieldType::I64(_) => {
                let (lower_bound, upper_bound) =
                    convert_bounds(&self.lower_bound, &self.upper_bound, field_entry.name())?;
                TantivyFastFieldRangeQuery::new::<i64>(self.field.clone(), lower_bound, upper_bound)
                    .into()
            }
            tantivy::schema::FieldType::F64(_) => {
                let (lower_bound, upper_bound) =
                    convert_bounds(&self.lower_bound, &self.upper_bound, field_entry.name())?;
                TantivyFastFieldRangeQuery::new::<f64>(self.field.clone(), lower_bound, upper_bound)
                    .into()
            }
            tantivy::schema::FieldType::Bool(_) => {
                return Err(InvalidQuery::RangeQueryNotSupportedForField {
                    value_type: "bool",
                    field_name: field_entry.name().to_string(),
                });
            }
            tantivy::schema::FieldType::Date(_) => {
                let (lower_bound, upper_bound) =
                    convert_bounds(&self.lower_bound, &self.upper_bound, field_entry.name())?;
                TantivyRangeQuery::new_date_bounds(self.field.clone(), lower_bound, upper_bound)
                    .into()
            }
            tantivy::schema::FieldType::Facet(_) => {
                return Err(InvalidQuery::RangeQueryNotSupportedForField {
                    value_type: "facet",
                    field_name: field_entry.name().to_string(),
                });
            }
            tantivy::schema::FieldType::Bytes(_) => todo!(),
            tantivy::schema::FieldType::JsonObject(_) => {
                let full_path = self.field.clone();
                let mut sub_queries: Vec<TantivyQueryAst> = Vec::new();
                if let Some(NumericalBoundaries {
                    i64_range,
                    u64_range,
                    f64_range,
                }) = compute_numerical_boundaries(&self.lower_bound, &self.upper_bound)
                {
                    // Adding the f64 range.
                    sub_queries.push(
                        TantivyFastFieldRangeQuery::new(
                            full_path.clone(),
                            f64_range.0,
                            f64_range.1,
                        )
                        .into(),
                    );
                    // Adding the i64 range.
                    if !is_empty(&i64_range).unwrap_or(false) {
                        sub_queries.push(
                            TantivyFastFieldRangeQuery::new(
                                full_path.clone(),
                                i64_range.0,
                                i64_range.1,
                            )
                            .into(),
                        );
                    }
                    // Adding the u64 range.
                    if !is_empty(&u64_range).unwrap_or(false) {
                        sub_queries.push(
                            TantivyFastFieldRangeQuery::new(full_path, u64_range.0, u64_range.1)
                                .into(),
                        );
                    }
                }
                // TODO add support for str range queries.
                let bool_query = TantivyBoolQuery {
                    should: sub_queries,
                    ..Default::default()
                };
                bool_query.into()
            }
            tantivy::schema::FieldType::IpAddr(_) => {
                let (lower_bound, upper_bound) =
                    convert_bounds(&self.lower_bound, &self.upper_bound, field_entry.name())?;
                TantivyRangeQuery::new_ip_bounds(self.field.clone(), lower_bound, upper_bound)
                    .into()
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use tantivy::schema::{Schema, FAST, STORED, TEXT};

    use super::RangeQuery;
    use crate::query_ast::tantivy_query_ast::{MatchAllOrNone, TantivyBoolQuery};
    use crate::query_ast::BuildTantivyAst;
    use crate::{InvalidQuery, JsonLiteral};

    fn make_schema(dynamic_mode: bool) -> Schema {
        let mut schema_builder = Schema::builder();
        schema_builder.add_i64_field("my_i64_field", FAST);
        schema_builder.add_u64_field("my_u64_field", FAST);
        schema_builder.add_f64_field("my_f64_field", FAST);
        schema_builder.add_text_field("my_str_field", FAST);
        schema_builder.add_u64_field("my_u64_not_fastfield", STORED);
        if dynamic_mode {
            schema_builder.add_json_field("_dynamic", TEXT | STORED | FAST);
        }
        schema_builder.build()
    }

    fn test_range_query_typed_field_util(field: &str, expected: &str) {
        let schema = make_schema(false);
        let range_query = RangeQuery {
            field: field.to_string(),
            lower_bound: Bound::Included(JsonLiteral::String("1980".to_string())),
            upper_bound: Bound::Included(JsonLiteral::String("1989".to_string())),
        };
        let tantivy_ast = range_query
            .build_tantivy_ast_call(&schema, &[], true)
            .unwrap()
            .simplify();
        let leaf = tantivy_ast.as_leaf().unwrap();
        let leaf_str = format!("{:?}", leaf);
        assert_eq!(leaf_str, expected);
    }

    #[test]
    fn test_range_query_typed_field() {
        test_range_query_typed_field_util(
            "my_i64_field",
            "FastFieldRangeWeight { field: \"my_i64_field\", lower_bound: \
             Included(9223372036854777788), upper_bound: Included(9223372036854777797), \
             column_type_opt: Some(I64) }",
        );
        test_range_query_typed_field_util(
            "my_u64_field",
            "FastFieldRangeWeight { field: \"my_u64_field\", lower_bound: Included(1980), \
             upper_bound: Included(1989), column_type_opt: Some(U64) }",
        );
        test_range_query_typed_field_util(
            "my_f64_field",
            "FastFieldRangeWeight { field: \"my_f64_field\", lower_bound: \
             Included(13879794984393113600), upper_bound: Included(13879834566811713536), \
             column_type_opt: Some(F64) }",
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
            .build_tantivy_ast_call(&schema, &[], true)
            .unwrap_err();
        assert!(
            matches!(invalid_query, InvalidQuery::FieldDoesNotExist { full_path } if full_path == "missing_field.toto")
        );
        // without validation
        assert_eq!(
            range_query
                .build_tantivy_ast_call(&schema, &[], false)
                .unwrap()
                .const_predicate(),
            Some(MatchAllOrNone::MatchNone)
        );
    }

    #[test]
    fn test_range_query_field_unsupported_type_field() {
        let schema = make_schema(false);
        let range_query = RangeQuery {
            field: "my_str_field".to_string(),
            lower_bound: Bound::Included(JsonLiteral::String("1980".to_string())),
            upper_bound: Bound::Included(JsonLiteral::String("1989".to_string())),
        };
        // with validation
        let invalid_query: InvalidQuery = range_query
            .build_tantivy_ast_call(&schema, &[], true)
            .unwrap_err();
        assert!(matches!(
            invalid_query,
            InvalidQuery::RangeQueryNotSupportedForField { .. }
        ));
        // without validation
        assert_eq!(
            range_query
                .build_tantivy_ast_call(&schema, &[], false)
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
            .build_tantivy_ast_call(&schema, &[], true)
            .unwrap();
        let TantivyBoolQuery {
            must,
            must_not,
            should,
            filter,
        } = tantivy_ast.as_bool_query().unwrap();
        assert!(must.is_empty());
        assert!(must_not.is_empty());
        assert!(filter.is_empty());
        assert_eq!(should.len(), 3);
        let range_queries: Vec<&dyn tantivy::query::Query> = should
            .iter()
            .map(|ast| ast.as_leaf().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            format!("{:?}", range_queries[0]),
            "FastFieldRangeWeight { field: \"hello\", lower_bound: \
             Included(13879794984393113600), upper_bound: Included(13879834566811713536), \
             column_type_opt: Some(F64) }"
        );
        assert_eq!(
            format!("{:?}", range_queries[1]),
            "FastFieldRangeWeight { field: \"hello\", lower_bound: Included(9223372036854777788), \
             upper_bound: Included(9223372036854777797), column_type_opt: Some(I64) }"
        );
        assert_eq!(
            format!("{:?}", range_queries[2]),
            "FastFieldRangeWeight { field: \"hello\", lower_bound: Included(1980), upper_bound: \
             Included(1989), column_type_opt: Some(U64) }"
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
            .build_tantivy_ast_call(&schema, &[], true)
            .unwrap_err();
        assert!(matches!(err, InvalidQuery::SchemaError { .. }));
    }
}
