// Copyright (C) 2024 Quickwit, Inc.
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

use anyhow::{anyhow, bail, Context};
use serde::{Deserialize, Serialize};
use tantivy::json_utils::JsonTermWriter;
use tantivy::schema::{Field, FieldType, Schema as TantivySchema};
use tantivy::Term;

use super::{BuildTantivyAst, QueryAst};
use crate::query_ast::TantivyQueryAst;
use crate::tokenizers::TokenizerManager;
use crate::{find_field_or_hit_dynamic, InvalidQuery};

/// A Wildcard query allows to match 'bond' with a query like 'b*d'.
///
/// At the moment, only wildcard at end of term is supported.
#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
pub struct WildcardQuery {
    pub field: String,
    pub value: String,
}

impl From<WildcardQuery> for QueryAst {
    fn from(wildcard_query: WildcardQuery) -> Self {
        Self::Wildcard(wildcard_query)
    }
}

impl WildcardQuery {
    #[cfg(test)]
    pub fn from_field_value(field: impl ToString, value: impl ToString) -> Self {
        Self {
            field: field.to_string(),
            value: value.to_string(),
        }
    }
}

fn extract_unique_token(mut tokens: Vec<Term>) -> anyhow::Result<Term> {
    let term = tokens
        .pop()
        .with_context(|| "wildcard query generated no term")?;
    if !tokens.is_empty() {
        anyhow::bail!("wildcard query generated more than one term");
    }
    Ok(term)
}

fn unescape_with_final_wildcard(phrase: &str) -> anyhow::Result<String> {
    enum State {
        Normal,
        Escaped,
    }

    // we keep this state outside of scan because we want to query if after
    let mut saw_wildcard = false;
    let saw_wildcard = &mut saw_wildcard;

    let phrase = phrase
        .chars()
        .scan(State::Normal, |state, c| {
            if *saw_wildcard {
                return Some(Some(Err(anyhow!(
                    "Wildcard iquery contains wildcard in non final position"
                ))));
            }
            match state {
                State::Escaped => {
                    *state = State::Normal;
                    Some(Some(Ok(c)))
                }
                State::Normal => {
                    if c == '*' {
                        *saw_wildcard = true;
                        Some(None)
                    } else if c == '\\' {
                        *state = State::Escaped;
                        Some(None)
                    } else if c == '?' {
                        Some(Some(Err(anyhow!("Wildcard query contains `?`"))))
                    } else {
                        Some(Some(Ok(c)))
                    }
                }
            }
        })
        // we have an iterator of Option<Result<char, anyhow::Error>>
        .flatten()
        // we have an iterator of Result<char, anyhow::Error>
        .collect::<Result<String, _>>()?;
    if !*saw_wildcard {
        bail!("Wildcard query doesn't contain a wildcard");
    }
    Ok(phrase)
}

impl WildcardQuery {
    // TODO this method will probably disappear once we support the full semantic of
    // wildcard queries
    pub fn extract_prefix_term(
        &self,
        schema: &TantivySchema,
        tokenizer_manager: &TokenizerManager,
    ) -> Result<(Field, Term), InvalidQuery> {
        let (field, field_entry, json_path) = find_field_or_hit_dynamic(&self.field, schema)?;
        let field_type = field_entry.field_type();

        let prefix = unescape_with_final_wildcard(&self.value)?;

        match field_type {
            FieldType::Str(ref text_options) => {
                let text_field_indexing = text_options.get_indexing_options().ok_or_else(|| {
                    InvalidQuery::SchemaError(format!(
                        "field {} is not full-text searchable",
                        field_entry.name()
                    ))
                })?;
                let tokenizer_name = text_field_indexing.tokenizer();
                let mut normalizer = tokenizer_manager
                    .get_normalizer(tokenizer_name)
                    .with_context(|| {
                        format!("no tokenizer named `{}` is registered", tokenizer_name)
                    })?;
                let mut token_stream = normalizer.token_stream(&prefix);
                let mut tokens = Vec::new();
                token_stream.process(&mut |token| {
                    let term: Term = Term::from_field_text(field, &token.text);
                    tokens.push(term);
                });
                let term = extract_unique_token(tokens)?;
                Ok((field, term))
            }
            FieldType::JsonObject(json_options) => {
                let text_field_indexing =
                    json_options.get_text_indexing_options().ok_or_else(|| {
                        InvalidQuery::SchemaError(format!(
                            "field {} is not full-text searchable",
                            field_entry.name()
                        ))
                    })?;
                let tokenizer_name = text_field_indexing.tokenizer();
                let mut normalizer = tokenizer_manager
                    .get_normalizer(tokenizer_name)
                    .with_context(|| {
                        format!("no tokenizer named `{}` is registered", tokenizer_name)
                    })?;
                let mut token_stream = normalizer.token_stream(&prefix);
                let mut tokens = Vec::new();
                let mut term = Term::with_capacity(100);
                let mut json_term_writer = JsonTermWriter::from_field_and_json_path(
                    field,
                    json_path,
                    json_options.is_expand_dots_enabled(),
                    &mut term,
                );

                token_stream.process(&mut |token| {
                    json_term_writer.set_str(&token.text);
                    tokens.push(json_term_writer.term().clone());
                });
                let term = extract_unique_token(tokens)?;
                Ok((field, term))
            }
            _ => Err(InvalidQuery::SchemaError(
                "trying to run a Wildcard query on a non-text field".to_string(),
            )),
        }
    }
}

impl BuildTantivyAst for WildcardQuery {
    fn build_tantivy_ast_impl(
        &self,
        schema: &TantivySchema,
        tokenizer_manager: &TokenizerManager,
        _search_fields: &[String],
        _with_validation: bool,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let (_, term) = self.extract_prefix_term(schema, tokenizer_manager)?;

        let mut phrase_prefix_query =
            tantivy::query::PhrasePrefixQuery::new_with_offset(vec![(0, term)]);
        phrase_prefix_query.set_max_expansions(u32::MAX);
        Ok(phrase_prefix_query.into())
    }
}
