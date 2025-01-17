use std::ops::Bound;

use prost::{DecodeError, Message};
use quickwit_proto::cloudprem::query_node::Node;
use quickwit_proto::cloudprem::{BooleanOperator, QueryNode};
use serde_json::Number;

use crate::query_ast::{
    BoolQuery, FieldPresenceQuery, FullTextMode, FullTextParams, FullTextQuery, PhrasePrefixQuery,
    QueryAst, RangeQuery, TermQuery, TermSetQuery, WildcardQuery,
};
use crate::{InvalidQuery, JsonLiteral};

pub fn parse_query(raw_message: prost_types::Any) -> Result<QueryNode, DecodeError> {
    // TODO validate type url?
    QueryNode::decode(raw_message.value.as_ref())
}

fn missing_required() -> InvalidQuery {
    InvalidQuery::Other(anyhow::anyhow!(
        "missing required field, this likely means a protobuf missmatch"
    ))
}

fn value_to_string(
    value: Option<quickwit_proto::cloudprem::Value>,
) -> Result<String, InvalidQuery> {
    use quickwit_proto::cloudprem::value::Value;
    match value
        .ok_or_else(missing_required)?
        .value
        .ok_or_else(missing_required)?
    {
        Value::Str(s) => Ok(s),
        Value::Int(i) => Ok(i.to_string()),
        Value::Double(d) => Ok(d.to_string()),
    }
}

fn value_to_json_literal(
    value: Option<quickwit_proto::cloudprem::Value>,
) -> Result<JsonLiteral, InvalidQuery> {
    use quickwit_proto::cloudprem::value::Value;
    match value
        .ok_or_else(missing_required)?
        .value
        .ok_or_else(missing_required)?
    {
        Value::Str(s) => Ok(s.into()),
        Value::Int(i) => Ok(i.into()),
        Value::Double(d) => Ok(JsonLiteral::Number(
            Number::from_f64(d)
                .ok_or_else(|| anyhow::anyhow!("unsupported NaN or infinite f64"))?,
        )),
    }
}

fn unsupported(feature: &str) -> InvalidQuery {
    InvalidQuery::Other(anyhow::anyhow!("unsupported feature: {feature}"))
}

pub fn to_quickwit_query(cloudprem_query: QueryNode) -> Result<QueryAst, InvalidQuery> {
    Ok(match cloudprem_query.node.ok_or_else(missing_required)? {
        Node::All(_) => QueryAst::MatchAll,
        Node::None(_) => QueryAst::MatchNone,
        Node::Not(not_query) => {
            let inner_query = *not_query.inner.ok_or_else(missing_required)?;
            BoolQuery {
                must_not: vec![to_quickwit_query(inner_query)?],
                ..BoolQuery::default()
            }
            .into()
        }
        Node::Boolean(boolean) => {
            let operator = boolean.operator();
            let clauses = boolean
                .clauses
                .into_iter()
                .map(to_quickwit_query)
                .collect::<Result<Vec<_>, _>>()?;
            match operator {
                BooleanOperator::And => BoolQuery {
                    must: clauses,
                    ..BoolQuery::default()
                }
                .into(),
                BooleanOperator::Or => BoolQuery {
                    should: clauses,
                    ..BoolQuery::default()
                }
                .into(),
                BooleanOperator::InvalidBooleanOperator => return Err(missing_required()),
            }
        }
        // TODO verify terms are already splited when we receive them
        Node::Term(term_query) => QueryAst::Term(TermQuery {
            field: term_query.attribute,
            value: value_to_string(term_query.value)?,
        }),
        Node::Range(range_query) => {
            let to_bound = |value, inclusive| {
                if inclusive {
                    Bound::Included(value)
                } else {
                    Bound::Excluded(value)
                }
            };
            let lower_bound = to_bound(
                value_to_json_literal(range_query.lower)?,
                range_query.lower_inclusive,
            );
            let upper_bound = to_bound(
                value_to_json_literal(range_query.upper)?,
                range_query.upper_inclusive,
            );
            RangeQuery {
                field: range_query.attribute,
                lower_bound,
                upper_bound,
            }
            .into()
        }
        Node::Comparison(comparison_query) => {
            use quickwit_proto::cloudprem::ComparisonOperator;
            let operator = comparison_query.operator();
            let value = value_to_json_literal(comparison_query.value)?;
            let (lower_bound, upper_bound) = match operator {
                ComparisonOperator::Lt => (Bound::Unbounded, Bound::Excluded(value)),
                ComparisonOperator::Lte => (Bound::Unbounded, Bound::Included(value)),
                ComparisonOperator::Gt => (Bound::Excluded(value), Bound::Unbounded),
                ComparisonOperator::Gte => (Bound::Included(value), Bound::Unbounded),
                ComparisonOperator::InvalidComparisonOperator => return Err(missing_required()),
            };
            RangeQuery {
                field: comparison_query.attribute,
                lower_bound,
                upper_bound,
            }
            .into()
        }
        Node::Exist(exist_query) => FieldPresenceQuery {
            field: exist_query.attribute,
        }
        .into(),
        Node::Missing(missing_query) => BoolQuery {
            must_not: vec![FieldPresenceQuery {
                field: missing_query.attribute,
            }
            .into()],
            ..BoolQuery::default()
        }
        .into(),
        Node::Prefix(prefix_query) => {
            // TODO maybe we want to make this into a wildcard query instead?
            // or give infinite expansion
            PhrasePrefixQuery {
                field: prefix_query.attribute,
                phrase: prefix_query.prefix,
                max_expansions: crate::query_ast::DEFAULT_PHRASE_QUERY_MAX_EXPANSION,
                params: FullTextParams {
                    tokenizer: None,
                    mode: FullTextMode::Phrase { slop: 0 },
                    zero_terms_query: crate::MatchAllOrNone::MatchNone,
                },
                lenient: false,
            }
            .into()
        }
        Node::Wildcard(wildcard_query) => {
            let string_wildcard = if let Some(pattern) = wildcard_query.pattern {
                let mut string_wildcard = String::new();
                for token in pattern.tokens {
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
            } else {
                // we only do this if upstream did not provide us with a preparsed
                // (non deprecated) input
                #[allow(deprecated)]
                wildcard_query.wildcard
            };
            WildcardQuery {
                field: wildcard_query.attribute,
                value: string_wildcard,
                lenient: false,
            }
            .into()
        }
        Node::Quoted(quoted_query) => FullTextQuery {
            field: quoted_query.attribute,
            text: quoted_query.text,
            params: FullTextParams {
                tokenizer: None,
                mode: FullTextMode::Phrase { slop: 0 },
                zero_terms_query: crate::MatchAllOrNone::MatchNone,
            },
            lenient: false,
        }
        .into(),
        Node::TermIn(term_in_query) => {
            let terms = term_in_query
                .values
                .into_iter()
                .map(Some)
                .map(value_to_string)
                .collect::<Result<_, _>>()?;
            TermSetQuery {
                terms_per_field: std::iter::once((term_in_query.attribute, terms)).collect(),
            }
            .into()
        }
        Node::Cidr(_) => {
            // TODO we are likely to support this via an automaton matching on strings.
            // not critical on MVP, and dependant on how we tokenize => reported until later.
            return Err(unsupported("cidr query"));
        }
        Node::Search(_) => {
            // this is a *:xxx query (full text on all fields)
            return Err(unsupported("whole event search query"));
        }
    })
}

impl From<InvalidQuery> for quickwit_proto::cloudprem::CloudPremError {
    fn from(err: InvalidQuery) -> Self {
        Self::InvalidQuery(err.to_string())
    }
}
