use std::collections::{BTreeSet, HashMap};
use std::ops::Bound;

use prost::{DecodeError, Message};
use quickwit_proto::cloudprem::query_node::Node;
use quickwit_proto::cloudprem::{
    AttributePrefixQueryNode, AttributeQuotedQueryNode, AttributeRangeQueryNode,
    AttributeSearchQueryNode, AttributeTermInQueryNode, AttributeWildcardQueryNode,
    BooleanOperator, QueryNode, SearchQueryMode, WildcardPattern,
};
use serde_json::Number;

use super::{missing_required, unsupported_query_error};
use crate::query_ast::{
    BoolQuery, FieldPresenceQuery, FullTextMode, FullTextParams, FullTextQuery, PhrasePrefixQuery,
    QueryAst, RangeQuery, TermSetQuery, WildcardQuery,
};
use crate::{InvalidQuery, JsonLiteral};

const EVP_DEFAULT_FIELD: &str = "_default_";
const EVP_RANDOM_DRAW: &str = "random_draw";
const EVP_WES_FIELD: &str = "*";

const QW_ERROR_FIELD: &str = "error";
const QW_MESSAGE_FIELD: &str = "message";
const QW_TIEBREAKER: &str = "tiebreaker";
const QW_WES_FIELD: &str = "all";

pub fn parse_query(raw_message: prost_types::Any) -> Result<QueryNode, DecodeError> {
    // TODO this can be cleaner once we upgrade to prost 0.12+
    QueryNode::decode(raw_message.value.as_ref())
}

// path is only used to provide a better error message.
fn value_to_string(
    value: Option<quickwit_proto::cloudprem::Value>,
    path: &str,
) -> Result<String, InvalidQuery> {
    use quickwit_proto::cloudprem::value::Value;
    match value
        .ok_or_else(|| missing_required(&format!("{path}.value")))?
        .value
        .ok_or_else(|| missing_required(&format!("{path}.value.value")))?
    {
        Value::Str(s) => Ok(s),
        Value::Int(i) => Ok(i.to_string()),
        Value::Double(d) => Ok(d.to_string()),
    }
}

fn value_to_json_literal(
    value: Option<quickwit_proto::cloudprem::Value>,
    path: &str,
) -> Result<JsonLiteral, InvalidQuery> {
    use quickwit_proto::cloudprem::value::Value;
    match value
        .ok_or_else(|| missing_required(&format!("{path}.value")))?
        .value
        .ok_or_else(|| missing_required(&format!("{path}.value.value")))?
    {
        Value::Str(s) => Ok(s.into()),
        Value::Int(i) => Ok(i.into()),
        Value::Double(d) => Ok(JsonLiteral::Number(
            Number::from_f64(d)
                .ok_or_else(|| anyhow::anyhow!("unsupported NaN or infinite f64"))?,
        )),
    }
}

// Returns a query ast that match all of the documents that DO NOT match the `ast` passed as
// argument.
fn negate(ast: QueryAst) -> QueryAst {
    BoolQuery {
        must_not: vec![ast],
        ..BoolQuery::default()
    }
    .into()
}

// Returns a query ast that match all of the `asts` passed as arguments.
fn intersection(asts: Vec<QueryAst>) -> QueryAst {
    BoolQuery {
        must: asts,
        ..BoolQuery::default()
    }
    .into()
}

// Returns a query ast that match any of `asts` passed as arguments.
fn union(asts: Vec<QueryAst>) -> QueryAst {
    BoolQuery {
        should: asts,
        ..BoolQuery::default()
    }
    .into()
}

fn build_term_query(
    term_query_node: quickwit_proto::cloudprem::AttributeTermQueryNode,
) -> Result<QueryAst, InvalidQuery> {
    let targetted_fields = expand_virtual_fields(term_query_node.attribute);
    let text = value_to_string(term_query_node.value, "term.value")?;
    let asts: Vec<QueryAst> = targetted_fields
        .into_iter()
        .map(|field| {
            crate::query_ast::FullTextQuery {
                field,
                text: text.clone(),
                params: FullTextParams {
                    tokenizer: None,
                    mode: FullTextMode::Bool {
                        operator: crate::BooleanOperand::And,
                    },
                    zero_terms_query: crate::MatchAllOrNone::MatchNone,
                },
                lenient: false,
            }
            .into()
        })
        .collect();
    Ok(union(asts))
}

fn build_range_query_helper(
    field: String,
    lower_bound: Bound<JsonLiteral>,
    upper_bound: Bound<JsonLiteral>,
) -> Result<QueryAst, InvalidQuery> {
    // We do not support our two virtual fields.
    if field == EVP_WES_FIELD || field == EVP_DEFAULT_FIELD {
        let extract_bound_type = |bound: Bound<JsonLiteral>| {
            let json_literal = match &bound {
                Bound::Included(val) | Bound::Excluded(val) => Some(val),
                Bound::Unbounded => None,
            };
            match json_literal? {
                JsonLiteral::Bool(_) => Some("bool"),
                JsonLiteral::String(_) => Some("string"),
                JsonLiteral::Number(_) => Some("number"),
            }
        };
        let value_type = extract_bound_type(lower_bound)
            .or_else(|| extract_bound_type(upper_bound))
            .unwrap_or("unknown");
        return Err(InvalidQuery::RangeQueryNotSupportedForField {
            field_name: field,
            value_type,
        });
    } else if field == EVP_RANDOM_DRAW {
        let (Some(remapped_lower_bound), Some(remapped_upper_bound)) = (
            map_bound_randomdraw_to_tiebreaker(lower_bound),
            map_bound_randomdraw_to_tiebreaker(upper_bound),
        ) else {
            return Ok(QueryAst::MatchNone);
        };
        return Ok(RangeQuery {
            field: QW_TIEBREAKER.to_string(),
            lower_bound: remapped_lower_bound,
            upper_bound: remapped_upper_bound,
        }
        .into());
    }
    Ok(RangeQuery {
        field,
        lower_bound,
        upper_bound,
    }
    .into())
}

fn map_bound_randomdraw_to_tiebreaker(bound: Bound<JsonLiteral>) -> Option<Bound<JsonLiteral>> {
    let map_literal = |literal: JsonLiteral| {
        let JsonLiteral::Number(num) = literal else {
            return None;
        };
        // this maps [0, 1) to [i32::MIN, i32::MAX)
        // we ceil so that low enough probability still allow for a non-empty range
        let int = num
            .as_f64()?
            .mul_add(2.0f64.powi(32) - 1.0, -2.0f64.powi(31))
            .ceil() as i64;

        Some(JsonLiteral::Number(int.into()))
    };
    let new_bound = match bound {
        Bound::Included(lit) => Bound::Included(map_literal(lit)?),
        Bound::Excluded(lit) => Bound::Excluded(map_literal(lit)?),
        Bound::Unbounded => Bound::Unbounded,
    };
    Some(new_bound)
}

fn build_range_query(range_query: AttributeRangeQueryNode) -> Result<QueryAst, InvalidQuery> {
    let to_bound = |value, inclusive| {
        if inclusive {
            Bound::Included(value)
        } else {
            Bound::Excluded(value)
        }
    };
    let lower_bound = to_bound(
        value_to_json_literal(range_query.lower, "range.lower")?,
        range_query.lower_inclusive,
    );
    let upper_bound = to_bound(
        value_to_json_literal(range_query.upper, "range.upper")?,
        range_query.upper_inclusive,
    );
    build_range_query_helper(range_query.attribute, lower_bound, upper_bound)
}

fn build_exists_query(field_name: String) -> QueryAst {
    let exist_queries: Vec<QueryAst> = expand_virtual_fields(field_name)
        .into_iter()
        .map(|field| { FieldPresenceQuery { field } }.into())
        .collect();
    union(exist_queries)
}

fn build_phrase_prefix_query(prefix_query_node: AttributePrefixQueryNode) -> QueryAst {
    // TODO maybe we want to make this into a wildcard query instead?
    // or give infinite expansion
    let phrase_prefix_queries: Vec<QueryAst> = expand_virtual_fields(prefix_query_node.attribute)
        .into_iter()
        .map(|field| {
            PhrasePrefixQuery {
                field,
                phrase: prefix_query_node.prefix.clone(),
                max_expansions: crate::query_ast::DEFAULT_PHRASE_QUERY_MAX_EXPANSION,
                params: FullTextParams {
                    tokenizer: None,
                    mode: FullTextMode::Phrase { slop: 0 },
                    zero_terms_query: crate::MatchAllOrNone::MatchNone,
                },
                lenient: false,
            }
            .into()
        })
        .collect();
    union(phrase_prefix_queries)
}

fn build_wildcard_query(wildcard_query: AttributeWildcardQueryNode) -> QueryAst {
    let string_wildcard = if let Some(pattern) = wildcard_query.pattern {
        wildcard_pattern_to_string(&pattern)
    } else {
        // we only do this if upstream did not provide us with a preparsed
        // (non deprecated) input
        #[allow(deprecated)]
        wildcard_query.wildcard
    };
    let wildcard_query_asts: Vec<QueryAst> = expand_virtual_fields(wildcard_query.attribute)
        .into_iter()
        .map(|field| {
            WildcardQuery {
                field,
                value: string_wildcard.clone(),
                lenient: false,
            }
            .into()
        })
        .collect();
    union(wildcard_query_asts)
}

fn build_term_in_query(term_in_query: AttributeTermInQueryNode) -> Result<QueryAst, InvalidQuery> {
    let terms: BTreeSet<String> = term_in_query
        .values
        .into_iter()
        .map(|val| value_to_string(Some(val), "termIn.values[]"))
        .collect::<Result<_, _>>()?;
    let terms_per_field: HashMap<String, BTreeSet<String>> =
        expand_virtual_fields(term_in_query.attribute)
            .into_iter()
            .map(|field| (field, terms.clone()))
            .collect();
    Ok(TermSetQuery { terms_per_field }.into())
}

fn build_quoted_query(quote_query_node: AttributeQuotedQueryNode) -> QueryAst {
    let asts: Vec<QueryAst> = expand_virtual_fields(quote_query_node.attribute)
        .into_iter()
        .map(|field| {
            FullTextQuery {
                field,
                text: quote_query_node.text.clone(),
                params: FullTextParams {
                    tokenizer: None,
                    mode: FullTextMode::Phrase { slop: 0 },
                    zero_terms_query: crate::MatchAllOrNone::MatchNone,
                },
                lenient: false,
            }
            .into()
        })
        .collect();
    union(asts)
}

fn build_search_query(search_query: AttributeSearchQueryNode) -> Result<QueryAst, InvalidQuery> {
    // this is a *:xxx query (full text on all fields)
    let mode = match search_query.mode() {
        SearchQueryMode::InvalidSearchMode => return Err(missing_required("search.mode")),

        SearchQueryMode::Wes => FullTextMode::Bool {
            operator: crate::BooleanOperand::And,
        },
        SearchQueryMode::WesQuoted => FullTextMode::Phrase { slop: 0 },
        SearchQueryMode::WesPrefix | SearchQueryMode::WesGlob => {
            return Err(unsupported_query_error("full-text search with wildcard"));
        }
    };
    let string_pattern = if let Some(pattern) = search_query.structured_text {
        wildcard_pattern_to_string(&pattern)
    } else {
        // we only do this if upstream did not provide us with a preparsed
        // (non deprecated) input
        #[allow(deprecated)]
        search_query.text
    };
    let asts: Vec<QueryAst> = expand_virtual_fields(search_query.attribute)
        .into_iter()
        .map(|field| {
            FullTextQuery {
                field,
                text: string_pattern.clone(),
                params: FullTextParams {
                    tokenizer: None,
                    mode,
                    zero_terms_query: crate::MatchAllOrNone::MatchNone,
                },
                lenient: false,
            }
            .into()
        })
        .collect();
    Ok(union(asts))
}

pub fn to_quickwit_query(cloudprem_query: QueryNode) -> Result<QueryAst, InvalidQuery> {
    let Some(node) = cloudprem_query.node else {
        return Err(missing_required("node"));
    };
    let ast = match node {
        Node::All(_) => QueryAst::MatchAll,
        Node::None(_) => QueryAst::MatchNone,
        Node::Not(not_query) => {
            let inner_query = *not_query
                .inner
                .ok_or_else(|| missing_required("not.inner"))?;
            let negated_query = to_quickwit_query(inner_query)?;
            negate(negated_query)
        }
        Node::Boolean(boolean) => {
            let operator = boolean.operator();
            let clauses: Vec<QueryAst> = boolean
                .clauses
                .into_iter()
                .map(to_quickwit_query)
                .collect::<Result<Vec<_>, _>>()?;
            match operator {
                BooleanOperator::And => intersection(clauses),
                BooleanOperator::Or => union(clauses),
                BooleanOperator::InvalidBooleanOperator => {
                    return Err(missing_required("boolean.operator"));
                }
            }
        }
        // TODO verify terms are already splited when we receive them
        Node::Term(term_query) => build_term_query(term_query)?,
        Node::Range(range_query) => build_range_query(range_query)?,
        Node::Comparison(comparison_query) => {
            use quickwit_proto::cloudprem::ComparisonOperator;
            let operator = comparison_query.operator();
            let value = value_to_json_literal(comparison_query.value, "comparison.value")?;
            let (lower_bound, upper_bound) = match operator {
                ComparisonOperator::Lt => (Bound::Unbounded, Bound::Excluded(value)),
                ComparisonOperator::Lte => (Bound::Unbounded, Bound::Included(value)),
                ComparisonOperator::Gt => (Bound::Excluded(value), Bound::Unbounded),
                ComparisonOperator::Gte => (Bound::Included(value), Bound::Unbounded),
                ComparisonOperator::InvalidComparisonOperator => {
                    return Err(missing_required("comparison.operator"));
                }
            };
            build_range_query_helper(comparison_query.attribute, lower_bound, upper_bound)?
        }
        Node::Exist(exist_query) => build_exists_query(exist_query.attribute),
        Node::Missing(missing_query) => negate(build_exists_query(missing_query.attribute)),
        Node::Prefix(prefix_query) => build_phrase_prefix_query(prefix_query),
        Node::Wildcard(wildcard_query) => build_wildcard_query(wildcard_query),
        Node::Quoted(quoted_query) => build_quoted_query(quoted_query),
        Node::TermIn(term_in_query) => build_term_in_query(term_in_query)?,
        Node::Cidr(_) => {
            // TODO we are likely to support this via an automaton matching on strings.
            // not critical on MVP, and dependant on how we tokenize => reported until later.
            return Err(unsupported_query_error("cidr query"));
        }
        Node::Search(search_query) => build_search_query(search_query)?,
    };
    Ok(ast)
}

impl From<InvalidQuery> for quickwit_proto::cloudprem::CloudPremError {
    fn from(err: InvalidQuery) -> Self {
        Self::InvalidQuery(err.to_string())
    }
}

fn wildcard_pattern_to_string(pattern: &WildcardPattern) -> String {
    let mut string_wildcard = String::new();
    for token in &pattern.tokens {
        for _ in 0..token.prefix_min_n_wild {
            string_wildcard.push('?');
        }
        if token.prefix_unbounded_n_wild {
            string_wildcard.push('*');
        }
        let mut last_pushed_pos = 0;
        for (pos, ctrl_char) in token.literal.match_indices(['?', '*', '\\']) {
            string_wildcard.push_str(&token.literal[last_pushed_pos..pos]);

            match ctrl_char {
                "?" => string_wildcard.push_str("\\?"),
                "*" => string_wildcard.push_str("\\*"),
                "\\" => string_wildcard.push_str("\\\\"),
                _ => unreachable!("{ctrl_char:?} was matched by match_indices"),
            }
            // the control char we found is considered pushed
            last_pushed_pos = pos + 1;
        }
        string_wildcard.push_str(&token.literal[last_pushed_pos..]);
    }
    string_wildcard
}

/// We have two virtual fields: `EVP_DEFAULT_FIELD` is the field used
/// targetted when the user does not target a field explicitly.
/// This is your typical full text query.
///
/// We target `message` and `error` in that case.
///
/// `EVP_WES_FIELD` on the other hand, is a whole event search.
/// This is what we get when we search using the syntax `*:something`.
///
/// In that case, we expand the request into `all` and `message`.
/// `all` is a `concatenate` field that combines several fields. We
/// excluded `message` from it:
/// - to have the raw_tokenizer for these fields
/// - because duplicating the indexing of `message` took a significant amount of space.
fn expand_virtual_fields(field_name: String) -> Vec<String> {
    if field_name == EVP_DEFAULT_FIELD {
        vec![QW_MESSAGE_FIELD.to_string(), QW_ERROR_FIELD.to_string()]
    } else if field_name == EVP_WES_FIELD {
        vec![QW_MESSAGE_FIELD.to_string(), QW_WES_FIELD.to_string()]
    } else {
        vec![field_name]
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::cloudprem::{AttributeTermQueryNode, Value, value};

    use super::*;

    #[test]
    fn test_term_query_expand_all() {
        let term_query_node = AttributeTermQueryNode {
            attribute: super::EVP_WES_FIELD.to_string(),
            value: Some(Value {
                value: Some(value::Value::Str("hello".to_string())),
            }),
        };
        let term_ast: QueryAst = super::build_term_query(term_query_node).unwrap();
        let expected_ast: QueryAst = QueryAst::Bool(BoolQuery {
            should: vec![
                QueryAst::FullText(FullTextQuery {
                    field: "message".to_string(),
                    text: "hello".to_string(),
                    params: FullTextParams {
                        tokenizer: None,
                        mode: FullTextMode::Bool {
                            operator: crate::BooleanOperand::And,
                        },
                        zero_terms_query: crate::MatchAllOrNone::MatchNone,
                    },
                    lenient: false,
                }),
                QueryAst::FullText(FullTextQuery {
                    field: "all".to_string(),
                    text: "hello".to_string(),
                    params: FullTextParams {
                        tokenizer: None,
                        mode: FullTextMode::Bool {
                            operator: crate::BooleanOperand::And,
                        },
                        zero_terms_query: crate::MatchAllOrNone::MatchNone,
                    },
                    lenient: false,
                }),
            ],
            ..Default::default()
        });
        assert_eq!(term_ast, expected_ast);
    }

    #[test]
    fn test_term_query_expand_default() {
        let term_query_node = AttributeTermQueryNode {
            attribute: super::EVP_DEFAULT_FIELD.to_string(),
            value: Some(Value {
                value: Some(value::Value::Str("hello".to_string())),
            }),
        };
        let term_ast: QueryAst = super::build_term_query(term_query_node).unwrap();
        let expected_ast: QueryAst = QueryAst::Bool(BoolQuery {
            should: vec![
                QueryAst::FullText(FullTextQuery {
                    field: "message".to_string(),
                    text: "hello".to_string(),
                    params: FullTextParams {
                        tokenizer: None,
                        mode: FullTextMode::Bool {
                            operator: crate::BooleanOperand::And,
                        },
                        zero_terms_query: crate::MatchAllOrNone::MatchNone,
                    },
                    lenient: false,
                }),
                QueryAst::FullText(FullTextQuery {
                    field: "error".to_string(),
                    text: "hello".to_string(),
                    params: FullTextParams {
                        tokenizer: None,
                        mode: FullTextMode::Bool {
                            operator: crate::BooleanOperand::And,
                        },
                        zero_terms_query: crate::MatchAllOrNone::MatchNone,
                    },
                    lenient: false,
                }),
            ],
            ..Default::default()
        });
        assert_eq!(term_ast, expected_ast);
    }

    #[test]
    fn test_range_remap_random_draw() {
        let ast = build_range_query_helper(
            "random_draw".to_string(),
            Bound::Unbounded,
            Bound::Excluded(JsonLiteral::Number(Number::from_f64(0.125).unwrap())),
        )
        .unwrap();

        let expected_ast = QueryAst::Range(RangeQuery {
            field: "tiebreaker".to_string(),
            lower_bound: Bound::Unbounded,
            upper_bound: Bound::Excluded(JsonLiteral::Number(Number::from(-1610612736))),
        });
        assert_eq!(ast, expected_ast);

        let zero_percent_bound = map_bound_randomdraw_to_tiebreaker(Bound::Excluded(
            JsonLiteral::Number(Number::from_f64(0.0).unwrap()),
        ))
        .unwrap();
        assert_eq!(
            zero_percent_bound,
            Bound::Excluded(JsonLiteral::Number(Number::from(i32::MIN)))
        );

        let everything_bound = map_bound_randomdraw_to_tiebreaker(Bound::Excluded(
            JsonLiteral::Number(Number::from_f64(1.0).unwrap()),
        ))
        .unwrap();
        assert_eq!(
            everything_bound,
            Bound::Excluded(JsonLiteral::Number(Number::from(i32::MAX)))
        );

        // anything more than zero, we want to return at least some result
        let non_zero_percent_bound = map_bound_randomdraw_to_tiebreaker(Bound::Excluded(
            JsonLiteral::Number(Number::from_f64(f64::EPSILON).unwrap()),
        ))
        .unwrap();
        assert_eq!(
            non_zero_percent_bound,
            Bound::Excluded(JsonLiteral::Number(Number::from(i32::MIN + 1)))
        );
    }
}
