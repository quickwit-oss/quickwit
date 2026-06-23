use std::collections::{BTreeSet, HashMap};
use std::ops::Bound;

use prost::{DecodeError, Message};
use quickwit_proto::cloudprem::query_node::Node;
use quickwit_proto::cloudprem::{
    AttributePrefixQueryNode, AttributeQuotedQueryNode, AttributeRangeQueryNode,
    AttributeSearchQueryNode, AttributeTermInQueryNode, AttributeWildcardQueryNode,
    BooleanOperator, QueryNode, SearchQueryMode, WildcardPattern, WildcardToken,
};
use serde_json::Number;

use super::{missing_required, unsupported_query_error};
use crate::query_ast::{
    BoolQuery, FieldPresenceQuery, FullTextMode, FullTextParams, FullTextQuery, PhrasePrefixQuery,
    QueryAst, RandomQuery, RangeQuery, TermQuery, TermSetQuery, WildcardQuery,
};
use crate::{InvalidQuery, JsonLiteral};

const EVP_DEFAULT_FIELD: &str = "_default_";
const EVP_RANDOM_DRAW: &str = "random_draw";
const EVP_WES_FIELD: &str = "*";

const QW_EXTRA_FTS: &str = "extra_fts";
const QW_MESSAGE_FIELD: &str = "message";
const QW_WES_FIELD: &str = "all";

/// Returns true for fields indexed with the DatadogTokenizer (see
/// config/cloudprem/datadog-logs.yaml).
fn is_datadog_tokenized(field: &str) -> bool {
    field == QW_MESSAGE_FIELD
        || field == QW_EXTRA_FTS
        || field.starts_with("error.")
        || field == "title"
}

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
fn intersection(mut asts: Vec<QueryAst>) -> QueryAst {
    if asts.len() == 1 {
        asts.pop().unwrap()
    } else {
        BoolQuery {
            must: asts,
            ..BoolQuery::default()
        }
        .into()
    }
}

// Returns a query ast that match any of `asts` passed as arguments.
fn union(mut asts: Vec<QueryAst>) -> QueryAst {
    if asts.len() == 1 {
        asts.pop().unwrap()
    } else {
        BoolQuery {
            should: asts,
            ..BoolQuery::default()
        }
        .into()
    }
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
        let probability = random_draw_to_probability(lower_bound, upper_bound);
        return Ok(RandomQuery { probability }.into());
    }
    Ok(RangeQuery {
        field,
        lower_bound,
        upper_bound,
    }
    .into())
}

/// Computes the sampling probability from a `random_draw` range query.
///
/// The probability is the width of the `[lower, upper]` window within `[0, 1]`.
/// Unbound lower defaults to 0.0 and unbound upper defaults to 1.0, so:
/// - `random_draw > 0.99` (lower=Excluded(0.99), upper=Unbounded): probability = 0.01
/// - `random_draw < 0.05` (lower=Unbounded, upper=Excluded(0.05)): probability = 0.05
fn random_draw_to_probability(lower: Bound<JsonLiteral>, upper: Bound<JsonLiteral>) -> f64 {
    let lower_val = bound_f64_value(&lower).unwrap_or(0.0);
    let upper_val = bound_f64_value(&upper).unwrap_or(1.0);
    (upper_val - lower_val).clamp(0.0, 1.0)
}

fn bound_f64_value(bound: &Bound<JsonLiteral>) -> Option<f64> {
    match bound {
        // continuous distribution: included or excluded is equivalent
        Bound::Included(JsonLiteral::Number(num)) | Bound::Excluded(JsonLiteral::Number(num)) => {
            num.as_f64()
        }
        _ => None,
    }
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

/// Converts the output of [`tokenize_wildcard_pattern_for_query`] into an intersected query.
/// Plain terms become [`TermQuery`]; entries with wildcards become [`WildcardQuery`].
/// An empty token list means the pattern is unconstrained and matches everything.
fn wildcard_tokens_to_query(field: String, tokens: Vec<(String, bool)>) -> QueryAst {
    if tokens.is_empty() {
        return QueryAst::MatchAll;
    }
    let queries: Vec<QueryAst> = tokens
        .into_iter()
        .map(|(value, is_wildcard)| {
            if is_wildcard {
                WildcardQuery {
                    field: field.clone(),
                    value,
                    lenient: false,
                    case_insensitive: false,
                }
                .into()
            } else {
                TermQuery {
                    field: field.clone(),
                    value,
                }
                .into()
            }
        })
        .collect();
    intersection(queries)
}

/// Returns true if the pattern represents an unconstrained match (equivalent to `*`).
/// In that case we can emit a cheaper exist query instead.
fn is_match_all_pattern(pattern: &WildcardPattern) -> bool {
    matches!(
        pattern.tokens.as_slice(),
        [WildcardToken {
            prefix_min_n_wild: 0,
            prefix_unbounded_n_wild: true,
            literal,
        }] if literal.is_empty()
    )
}

fn build_wildcard_query(wildcard_query: AttributeWildcardQueryNode) -> QueryAst {
    // A wildcard that matches everything is better expressed as an exist query.
    #[allow(deprecated)]
    let is_match_all = match &wildcard_query.pattern {
        Some(pattern) => is_match_all_pattern(pattern),
        None => wildcard_query.wildcard == "*",
    };
    if is_match_all {
        return build_exists_query(wildcard_query.attribute);
    }

    let asts: Vec<QueryAst> = expand_virtual_fields(wildcard_query.attribute)
        .into_iter()
        .map(|field| {
            if let Some(ref pattern) = wildcard_query.pattern {
                if is_datadog_tokenized(&field) {
                    wildcard_tokens_to_query(field, tokenize_wildcard_pattern_for_query(pattern))
                } else {
                    WildcardQuery {
                        field,
                        value: wildcard_pattern_to_string(pattern),
                        lenient: false,
                        case_insensitive: false,
                    }
                    .into()
                }
            } else {
                // we only do this if upstream did not provide us with a preparsed
                // (non deprecated) input
                #[allow(deprecated)]
                WildcardQuery {
                    field,
                    value: wildcard_query.wildcard.clone(),
                    lenient: false,
                    case_insensitive: false,
                }
                .into()
            }
        })
        .collect();
    union(asts)
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

fn convert_query(
    field: String,
    string_pattern: &str,
    pattern_opt: Option<&WildcardPattern>,
    mode: SearchQueryMode,
) -> QueryAst {
    use crate::{BooleanOperand, MatchAllOrNone};
    match mode {
        SearchQueryMode::Wes => FullTextQuery {
            field,
            text: string_pattern.to_string(),
            params: FullTextParams {
                tokenizer: None,
                mode: FullTextMode::Bool {
                    operator: BooleanOperand::And,
                },
                zero_terms_query: MatchAllOrNone::MatchNone,
            },
            lenient: false,
        }
        .into(),
        SearchQueryMode::WesQuoted => FullTextQuery {
            field,
            text: string_pattern.to_string(),
            params: FullTextParams {
                tokenizer: None,
                mode: FullTextMode::Phrase { slop: 0 },
                zero_terms_query: MatchAllOrNone::MatchNone,
            },
            lenient: false,
        }
        .into(),
        SearchQueryMode::WesPrefix | SearchQueryMode::WesGlob => {
            if let Some(pattern) = pattern_opt {
                if is_datadog_tokenized(&field) {
                    let mut pattern = pattern.clone();
                    if mode == SearchQueryMode::WesPrefix {
                        pattern.tokens.push(WildcardToken {
                            prefix_unbounded_n_wild: true,
                            prefix_min_n_wild: 0,
                            literal: String::new(),
                        });
                    }
                    wildcard_tokens_to_query(field, tokenize_wildcard_pattern_for_query(&pattern))
                } else {
                    let value = wildcard_pattern_to_string(pattern);
                    let value = if mode == SearchQueryMode::WesPrefix {
                        format!("{}*", value)
                    } else {
                        value
                    };
                    WildcardQuery {
                        field,
                        value,
                        lenient: false,
                        case_insensitive: false,
                    }
                    .into()
                }
            } else {
                // we only do this if upstream did not provide us with a preparsed
                // (non deprecated) input
                let value = if mode == SearchQueryMode::WesPrefix {
                    format!("{}*", string_pattern)
                } else {
                    string_pattern.to_string()
                };
                WildcardQuery {
                    field,
                    value,
                    lenient: false,
                    case_insensitive: false,
                }
                .into()
            }
        }
        SearchQueryMode::InvalidSearchMode => {
            // This shouldn't happen as we check for this before calling convert_query
            unreachable!("InvalidSearchMode should be handled before calling convert_query")
        }
    }
}

fn build_search_query(search_query: AttributeSearchQueryNode) -> Result<QueryAst, InvalidQuery> {
    // this is a *:xxx query (full text on all fields)

    let string_pattern = if let Some(ref pattern) = search_query.structured_text {
        wildcard_pattern_to_string(pattern)
    } else {
        // we only do this if upstream did not provide us with a preparsed
        // (non deprecated) input
        #[allow(deprecated)]
        search_query.text.clone()
    };

    let mode = search_query.mode();
    if mode == SearchQueryMode::InvalidSearchMode {
        return Err(missing_required("search.mode"));
    }

    let asts: Vec<QueryAst> = expand_virtual_fields(search_query.attribute)
        .into_iter()
        .map(|field| {
            convert_query(
                field,
                &string_pattern,
                search_query.structured_text.as_ref(),
                mode,
            )
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
            let ast = match operator {
                BooleanOperator::And => intersection(clauses),
                BooleanOperator::Or => union(clauses),
                BooleanOperator::InvalidBooleanOperator => {
                    return Err(missing_required("boolean.operator"));
                }
            };
            if boolean.should_cache {
                crate::query_ast::CacheNode::new(ast).into()
            } else {
                ast
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

/// Splits a [`WildcardPattern`] into per-token entries compatible with the DatadogTokenizer.
///
/// Each entry is `(value, is_wildcard)`: `is_wildcard` is `true` when `value` contains `*` or
/// `?` and needs a [`WildcardQuery`], `false` when a plain term query suffices.
fn tokenize_wildcard_pattern_for_query(pattern: &WildcardPattern) -> Vec<(String, bool)> {
    use unicode_segmentation::UnicodeSegmentation;

    let mut result: Vec<(String, bool)> = Vec::new();

    let mut current = String::new();
    let mut current_has_wildcard = false;

    for token in &pattern.tokens {
        for _ in 0..token.prefix_min_n_wild {
            current.push('?');
            current_has_wildcard = true;
        }
        if token.prefix_unbounded_n_wild {
            current.push('*');
            current_has_wildcard = true;
        }
        for segment in token.literal.split_word_bounds() {
            if segment.chars().any(|c| c.is_alphanumeric()) {
                current.push_str(segment);
            } else {
                // Separator: flush if `current` has a word, then reset.
                if current.chars().any(|c| c.is_alphanumeric()) {
                    result.push((std::mem::take(&mut current), current_has_wildcard));
                }
                current.clear();
                current_has_wildcard = false;
            }
        }
    }
    if current.chars().any(|c| c.is_alphanumeric()) {
        result.push((current, current_has_wildcard));
    }
    result
}

/// Splits a [`WildcardPattern`] into a wildcard query string suitable only for untokenized fields.
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
        // FTS: message + extra_fts (concatenate field combining error.message,
        // error.stack, and title). Keep in sync with default_search_fields
        // in config/cloudprem/datadog-logs.yaml
        vec![QW_MESSAGE_FIELD.to_string(), QW_EXTRA_FTS.to_string()]
    } else if field_name == EVP_WES_FIELD {
        vec![QW_MESSAGE_FIELD.to_string(), QW_WES_FIELD.to_string()]
    } else {
        vec![field_name]
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::cloudprem::{
        AttributeSearchQueryNode, AttributeTermQueryNode, Value, WildcardToken, query_node, value,
    };

    use super::*;
    use crate::{BooleanOperand, MatchAllOrNone};

    fn make_pattern(tokens: Vec<(i32, bool, &str)>) -> WildcardPattern {
        WildcardPattern {
            tokens: tokens
                .into_iter()
                .map(
                    |(prefix_min_n_wild, prefix_unbounded_n_wild, literal)| WildcardToken {
                        prefix_min_n_wild,
                        prefix_unbounded_n_wild,
                        literal: literal.to_string(),
                    },
                )
                .collect(),
        }
    }

    #[test]
    fn test_tokenize_wildcard_pattern_simple_word() {
        // "error" -> [("error", false)]
        let pattern = make_pattern(vec![(0, false, "error")]);
        assert_eq!(
            tokenize_wildcard_pattern_for_query(&pattern),
            vec![("error".to_string(), false)]
        );
    }

    #[test]
    fn test_tokenize_wildcard_pattern_multi_word_literal() {
        // "timeout bar" (no wildcards) -> [("timeout", false), ("bar", false)]
        let pattern = make_pattern(vec![(0, false, "timeout bar")]);
        assert_eq!(
            tokenize_wildcard_pattern_for_query(&pattern),
            vec![("timeout".to_string(), false), ("bar".to_string(), false)]
        );
    }

    #[test]
    fn test_tokenize_wildcard_pattern_glob_two_tokens() {
        // "err*timeout" -> token1={literal="err"}, token2={prefix=*, literal="timeout"}
        // The first word of token2 is stitched onto the previous entry through the *.
        let pattern = make_pattern(vec![(0, false, "err"), (0, true, "timeout")]);
        assert_eq!(
            tokenize_wildcard_pattern_for_query(&pattern),
            vec![("err*timeout".to_string(), true)]
        );
    }

    #[test]
    fn test_tokenize_wildcard_pattern_glob_multi_word() {
        // "err*timeout bar" -> token1={literal="err"}, token2={prefix=*, literal="timeout bar"}
        // "timeout" stitches onto "err" through the *, and "bar" is a separate plain term.
        let pattern = make_pattern(vec![(0, false, "err"), (0, true, "timeout bar")]);
        assert_eq!(
            tokenize_wildcard_pattern_for_query(&pattern),
            vec![
                ("err*timeout".to_string(), true),
                ("bar".to_string(), false)
            ]
        );
    }

    #[test]
    fn test_tokenize_wildcard_pattern_trailing_wildcard() {
        // "err*" -> token1={literal="err"}, token2={prefix=*, literal=""}
        let pattern = make_pattern(vec![(0, false, "err"), (0, true, "")]);
        assert_eq!(
            tokenize_wildcard_pattern_for_query(&pattern),
            vec![("err*".to_string(), true)]
        );
    }

    #[test]
    fn test_tokenize_wildcard_pattern_surround_wildcard() {
        // "*foo*" -> token1={prefix=*, literal="foo"}, token2={prefix=*, literal=""}
        let pattern = make_pattern(vec![(0, true, "foo"), (0, true, "")]);
        assert_eq!(
            tokenize_wildcard_pattern_for_query(&pattern),
            vec![("*foo*".to_string(), true)]
        );
    }

    #[test]
    fn test_tokenize_wildcard_pattern_question_mark_prefix() {
        // "?timeout" -> token={prefix_min=1, literal="timeout"}
        let pattern = make_pattern(vec![(1, false, "timeout")]);
        assert_eq!(
            tokenize_wildcard_pattern_for_query(&pattern),
            vec![("?timeout".to_string(), true)]
        );
    }

    #[test]
    fn test_tokenize_wildcard_pattern_question_mark_trailing() {
        // "err?" -> token1={literal="err"}, token2={prefix_min=1, literal=""}
        let pattern = make_pattern(vec![(0, false, "err"), (1, false, "")]);
        assert_eq!(
            tokenize_wildcard_pattern_for_query(&pattern),
            vec![("err?".to_string(), true)]
        );
    }

    #[test]
    fn test_tokenize_wildcard_pattern_pure_wildcard() {
        // "*" -> token={prefix=*, literal=""}
        // No literal constraints: treated as MatchAll by the caller.
        let pattern = make_pattern(vec![(0, true, "")]);
        assert!(tokenize_wildcard_pattern_for_query(&pattern).is_empty());
    }

    #[test]
    fn test_tokenize_wildcard_pattern_trailing_sep_before_wildcard() {
        // "err-*timeout": the "-" after "err" is a word-boundary separator, so "err" and
        // "*timeout" must not be stitched together.
        // token1={literal="err-"}, token2={prefix=*, literal="timeout"}
        let pattern = make_pattern(vec![(0, false, "err-"), (0, true, "timeout")]);
        assert_eq!(
            tokenize_wildcard_pattern_for_query(&pattern),
            vec![("err".to_string(), false), ("*timeout".to_string(), true)]
        );
    }

    #[test]
    fn test_tokenize_wildcard_pattern_leading_sep_after_wildcard() {
        // "err*-timeout": the "*" is directly after "err" (no separator between them), so it
        // becomes a suffix of "err" → "err*". The "-" then splits, leaving "timeout" separate.
        // token1={literal="err"}, token2={prefix=*, literal="-timeout"}
        let pattern = make_pattern(vec![(0, false, "err"), (0, true, "-timeout")]);
        assert_eq!(
            tokenize_wildcard_pattern_for_query(&pattern),
            vec![("err*".to_string(), true), ("timeout".to_string(), false)]
        );
    }

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
        let fts_params = FullTextParams {
            tokenizer: None,
            mode: FullTextMode::Bool {
                operator: crate::BooleanOperand::And,
            },
            zero_terms_query: crate::MatchAllOrNone::MatchNone,
        };
        let expected_ast: QueryAst = QueryAst::Bool(BoolQuery {
            should: vec![
                QueryAst::FullText(FullTextQuery {
                    field: "message".to_string(),
                    text: "hello".to_string(),
                    params: fts_params.clone(),
                    lenient: false,
                }),
                QueryAst::FullText(FullTextQuery {
                    field: "extra_fts".to_string(),
                    text: "hello".to_string(),
                    params: fts_params,
                    lenient: false,
                }),
            ],
            ..Default::default()
        });
        assert_eq!(term_ast, expected_ast);
    }

    #[test]
    fn test_random_draw_produces_random_query() {
        let ast = build_range_query_helper(
            "random_draw".to_string(),
            Bound::Excluded(JsonLiteral::Number(Number::from_f64(0.99).unwrap())),
            Bound::Unbounded,
        )
        .unwrap();
        let QueryAst::Random(q) = ast else {
            panic!("expected RandomQuery, got {ast:?}");
        };
        assert!((q.probability - 0.01).abs() < 1e-10, "probability={}", q.probability);

        let ast = build_range_query_helper(
            "random_draw".to_string(),
            Bound::Unbounded,
            Bound::Excluded(JsonLiteral::Number(Number::from_f64(0.125).unwrap())),
        )
        .unwrap();
        let QueryAst::Random(q) = ast else {
            panic!("expected RandomQuery, got {ast:?}");
        };
        assert!((q.probability - 0.125).abs() < 1e-10, "probability={}", q.probability);

        let ast = build_range_query_helper(
            "random_draw".to_string(),
            Bound::Excluded(JsonLiteral::Number(Number::from_f64(1.0).unwrap())),
            Bound::Unbounded,
        )
        .unwrap();
        let QueryAst::Random(q) = ast else {
            panic!("expected RandomQuery, got {ast:?}");
        };
        assert_eq!(q.probability, 0.0);

        let ast = build_range_query_helper(
            "random_draw".to_string(),
            Bound::Unbounded,
            Bound::Unbounded,
        )
        .unwrap();
        let QueryAst::Random(q) = ast else {
            panic!("expected RandomQuery, got {ast:?}");
        };
        assert_eq!(q.probability, 1.0);
    }

    #[test]
    fn test_wes_search_query_basic() {
        let search_query = AttributeSearchQueryNode {
            attribute: EVP_WES_FIELD.to_string(),
            structured_text: Some(WildcardPattern {
                tokens: vec![WildcardToken {
                    literal: "error".to_string(),
                    prefix_min_n_wild: 0,
                    prefix_unbounded_n_wild: false,
                }],
            }),
            mode: SearchQueryMode::Wes as i32,
            ..Default::default()
        };
        let ast = build_search_query(search_query).unwrap();

        let expected_ast = QueryAst::Bool(BoolQuery {
            should: vec![
                QueryAst::FullText(FullTextQuery {
                    field: "message".to_string(),
                    text: "error".to_string(),
                    params: FullTextParams {
                        tokenizer: None,
                        mode: FullTextMode::Bool {
                            operator: BooleanOperand::And,
                        },
                        zero_terms_query: MatchAllOrNone::MatchNone,
                    },
                    lenient: false,
                }),
                QueryAst::FullText(FullTextQuery {
                    field: "all".to_string(),
                    text: "error".to_string(),
                    params: FullTextParams {
                        tokenizer: None,
                        mode: FullTextMode::Bool {
                            operator: BooleanOperand::And,
                        },
                        zero_terms_query: MatchAllOrNone::MatchNone,
                    },
                    lenient: false,
                }),
            ],
            ..Default::default()
        });
        assert_eq!(ast, expected_ast);
    }

    #[test]
    fn test_wes_search_query_quoted() {
        let search_query = AttributeSearchQueryNode {
            attribute: EVP_WES_FIELD.to_string(),
            structured_text: Some(WildcardPattern {
                tokens: vec![WildcardToken {
                    literal: "connection timeout".to_string(),
                    prefix_min_n_wild: 0,
                    prefix_unbounded_n_wild: false,
                }],
            }),
            mode: SearchQueryMode::WesQuoted as i32,
            ..Default::default()
        };
        let ast = build_search_query(search_query).unwrap();

        let expected_ast = QueryAst::Bool(BoolQuery {
            should: vec![
                QueryAst::FullText(FullTextQuery {
                    field: "message".to_string(),
                    text: "connection timeout".to_string(),
                    params: FullTextParams {
                        tokenizer: None,
                        mode: FullTextMode::Phrase { slop: 0 },
                        zero_terms_query: MatchAllOrNone::MatchNone,
                    },
                    lenient: false,
                }),
                QueryAst::FullText(FullTextQuery {
                    field: "all".to_string(),
                    text: "connection timeout".to_string(),
                    params: FullTextParams {
                        tokenizer: None,
                        mode: FullTextMode::Phrase { slop: 0 },
                        zero_terms_query: MatchAllOrNone::MatchNone,
                    },
                    lenient: false,
                }),
            ],
            ..Default::default()
        });
        assert_eq!(ast, expected_ast);
    }

    #[test]
    fn test_wes_search_query_prefix() {
        let search_query = AttributeSearchQueryNode {
            attribute: EVP_WES_FIELD.to_string(),
            structured_text: Some(WildcardPattern {
                tokens: vec![WildcardToken {
                    literal: "err".to_string(),
                    prefix_min_n_wild: 0,
                    prefix_unbounded_n_wild: false,
                }],
            }),
            mode: SearchQueryMode::WesPrefix as i32,
            ..Default::default()
        };
        let ast = build_search_query(search_query).unwrap();

        let expected_ast = QueryAst::Bool(BoolQuery {
            should: vec![
                QueryAst::Wildcard(WildcardQuery {
                    field: "message".to_string(),
                    value: "err*".to_string(),
                    lenient: false,
                    case_insensitive: false,
                }),
                QueryAst::Wildcard(WildcardQuery {
                    field: "all".to_string(),
                    value: "err*".to_string(),
                    lenient: false,
                    case_insensitive: false,
                }),
            ],
            ..Default::default()
        });
        assert_eq!(ast, expected_ast);
    }

    #[test]
    fn test_wes_search_query_glob() {
        let search_query = AttributeSearchQueryNode {
            attribute: EVP_WES_FIELD.to_string(),
            structured_text: Some(WildcardPattern {
                tokens: vec![
                    WildcardToken {
                        literal: "err".to_string(),
                        prefix_min_n_wild: 0,
                        prefix_unbounded_n_wild: false,
                    },
                    WildcardToken {
                        literal: "timeout".to_string(),
                        prefix_min_n_wild: 0,
                        prefix_unbounded_n_wild: true,
                    },
                ],
            }),
            mode: SearchQueryMode::WesGlob as i32,
            ..Default::default()
        };
        let ast = build_search_query(search_query).unwrap();

        let expected_ast = QueryAst::Bool(BoolQuery {
            should: vec![
                QueryAst::Wildcard(WildcardQuery {
                    field: "message".to_string(),
                    value: "err*timeout".to_string(),
                    lenient: false,
                    case_insensitive: false,
                }),
                QueryAst::Wildcard(WildcardQuery {
                    field: "all".to_string(),
                    value: "err*timeout".to_string(),
                    lenient: false,
                    case_insensitive: false,
                }),
            ],
            ..Default::default()
        });
        assert_eq!(ast, expected_ast);
    }

    #[test]
    fn test_wes_via_to_quickwit_query() {
        let evp_query = QueryNode {
            node: Some(query_node::Node::Search(AttributeSearchQueryNode {
                attribute: EVP_WES_FIELD.to_string(),
                structured_text: Some(WildcardPattern {
                    tokens: vec![WildcardToken {
                        literal: "database error".to_string(),
                        prefix_min_n_wild: 0,
                        prefix_unbounded_n_wild: false,
                    }],
                }),
                mode: SearchQueryMode::Wes as i32,
                ..Default::default()
            })),
        };

        let ast = to_quickwit_query(evp_query).unwrap();

        let expected_ast = QueryAst::Bool(BoolQuery {
            should: vec![
                QueryAst::FullText(FullTextQuery {
                    field: "message".to_string(),
                    text: "database error".to_string(),
                    params: FullTextParams {
                        tokenizer: None,
                        mode: FullTextMode::Bool {
                            operator: BooleanOperand::And,
                        },
                        zero_terms_query: MatchAllOrNone::MatchNone,
                    },
                    lenient: false,
                }),
                QueryAst::FullText(FullTextQuery {
                    field: "all".to_string(),
                    text: "database error".to_string(),
                    params: FullTextParams {
                        tokenizer: None,
                        mode: FullTextMode::Bool {
                            operator: BooleanOperand::And,
                        },
                        zero_terms_query: MatchAllOrNone::MatchNone,
                    },
                    lenient: false,
                }),
            ],
            ..Default::default()
        });
        assert_eq!(ast, expected_ast);
    }

    #[test]
    fn test_expand_virtual_fields_fts_default() {
        let fields = super::expand_virtual_fields(super::EVP_DEFAULT_FIELD.to_string());
        assert_eq!(fields, vec!["message", "extra_fts"]);
    }

    #[test]
    fn test_expand_virtual_fields_wes() {
        let fields = super::expand_virtual_fields(super::EVP_WES_FIELD.to_string());
        assert_eq!(fields, vec!["message", "all"]);
    }

    #[test]
    fn test_expand_virtual_fields_regular_field() {
        let fields = super::expand_virtual_fields("service".to_string());
        assert_eq!(fields, vec!["service"]);
    }

    #[test]
    fn test_is_datadog_tokenized() {
        assert!(super::is_datadog_tokenized("message"));
        assert!(super::is_datadog_tokenized("extra_fts"));
        assert!(super::is_datadog_tokenized("error.message"));
        assert!(super::is_datadog_tokenized("error.stack"));
        assert!(super::is_datadog_tokenized("title"));
        assert!(!super::is_datadog_tokenized("custom"));
        assert!(!super::is_datadog_tokenized("service"));
        assert!(!super::is_datadog_tokenized("tag"));
    }

    #[test]
    fn test_wildcard_match_all_remaps_to_exist() {
        // `*` with structured pattern → exist query
        let wildcard_node = AttributeWildcardQueryNode {
            attribute: "custom.myfield".to_string(),
            pattern: Some(WildcardPattern {
                tokens: vec![WildcardToken {
                    prefix_min_n_wild: 0,
                    prefix_unbounded_n_wild: true,
                    literal: String::new(),
                }],
            }),
            ..Default::default()
        };
        let ast = build_wildcard_query(wildcard_node);
        let expected = QueryAst::FieldPresence(FieldPresenceQuery {
            field: "custom.myfield".to_string(),
        });
        assert_eq!(ast, expected);
    }

    #[test]
    fn test_wildcard_match_all_deprecated_remaps_to_exist() {
        // deprecated `wildcard = "*"` with no pattern → exist query
        #[allow(deprecated)]
        let wildcard_node = AttributeWildcardQueryNode {
            attribute: "custom.myfield".to_string(),
            wildcard: "*".to_string(),
            pattern: None,
        };
        let ast = build_wildcard_query(wildcard_node);
        let expected = QueryAst::FieldPresence(FieldPresenceQuery {
            field: "custom.myfield".to_string(),
        });
        assert_eq!(ast, expected);
    }

    #[test]
    fn test_wildcard_non_match_all_stays_wildcard() {
        // `?*` (at least one char) should NOT remap to exist
        let wildcard_node = AttributeWildcardQueryNode {
            attribute: "custom.myfield".to_string(),
            pattern: Some(WildcardPattern {
                tokens: vec![WildcardToken {
                    prefix_min_n_wild: 1,
                    prefix_unbounded_n_wild: true,
                    literal: String::new(),
                }],
            }),
            ..Default::default()
        };
        let ast = build_wildcard_query(wildcard_node);
        assert!(!matches!(ast, QueryAst::FieldPresence(_)));
    }
}
