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

use std::borrow::Cow;
use std::sync::Arc;

use anyhow::{bail, Context};
pub use prefix::{AutomatonQuery, JsonPathPrefix};
use serde::{Deserialize, Serialize};
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

fn parse_wildcard_query(mut query: &str) -> Vec<SubQuery> {
    let mut res = Vec::new();
    while let Some(pos) = query.find(['*', '?', '\\']) {
        if pos > 0 {
            res.push(SubQuery::Text(query[..pos].to_string()));
        }
        let chr = &query[pos..pos + 1];
        query = &query[pos + 1..];
        match chr {
            "*" => res.push(SubQuery::Wildcard),
            "?" => res.push(SubQuery::QuestionMark),
            "\\" => {
                if let Some(chr) = query.chars().next() {
                    res.push(SubQuery::Text(chr.to_string()));
                    query = &query[chr.len_utf8()..];
                } else {
                    // this is invalid, but let's just ignore that escape sequence
                    break;
                }
            }
            _ => unreachable!("find shouldn't return non-matching position"),
        }
    }
    if !query.is_empty() {
        res.push(SubQuery::Text(query.to_string()));
    }
    res
}

enum SubQuery {
    Text(String),
    Wildcard,
    QuestionMark,
}

impl WildcardQuery {
    pub fn to_regex(
        &self,
        schema: &TantivySchema,
        tokenizer_manager: &TokenizerManager,
    ) -> Result<(Field, Option<Vec<u8>>, String), InvalidQuery> {
        let (field, field_entry, json_path) = find_field_or_hit_dynamic(&self.field, schema)?;
        let field_type = field_entry.field_type();

        let sub_query_parts = parse_wildcard_query(&self.value);

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

                let regex = sub_query_parts
                    .into_iter()
                    .map(|part| match part {
                        SubQuery::Text(text) => {
                            let mut token_stream = normalizer.token_stream(&text);
                            let expected_token = token_stream
                                .next()
                                .context("normalizer generated no content")?
                                .text
                                .clone();
                            if let Some(_unexpected_token) = token_stream.next() {
                                bail!("normalizer generated multiple tokens")
                            }
                            Ok(Cow::Owned(regex::escape(&expected_token)))
                        }
                        SubQuery::Wildcard => Ok(Cow::Borrowed(".*")),
                        SubQuery::QuestionMark => Ok(Cow::Borrowed(".")),
                    })
                    .collect::<Result<String, _>>()?;

                Ok((field, None, regex))
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

                let mut term_for_path = Term::from_field_json_path(
                    field,
                    json_path,
                    json_options.is_expand_dots_enabled(),
                );
                term_for_path.append_type_and_str("");

                let value = term_for_path.value();
                // We skip the 1st byte which is a marker to tell this is json. This isn't present
                // in the dictionary
                let byte_path_prefix = value.as_serialized()[1..].to_owned();
                let regex = sub_query_parts
                    .into_iter()
                    .map(|part| match part {
                        SubQuery::Text(text) => {
                            let mut token_stream = normalizer.token_stream(&text);
                            let expected_token = token_stream
                                .next()
                                .context("normalizer generated no content")?
                                .text
                                .clone();
                            if let Some(_unexpected_token) = token_stream.next() {
                                bail!("normalizer generated multiple tokens")
                            }
                            Ok(Cow::Owned(regex::escape(&expected_token)))
                        }
                        SubQuery::Wildcard => Ok(Cow::Borrowed(".*")),
                        SubQuery::QuestionMark => Ok(Cow::Borrowed(".")),
                    })
                    .collect::<Result<String, _>>()?;
                Ok((field, Some(byte_path_prefix), regex))
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
        let (field, path, regex) = self.to_regex(schema, tokenizer_manager)?;
        let regex =
            tantivy_fst::Regex::new(&regex).context("failed to parse regex built from wildcard")?;
        let regex_automaton_with_path = prefix::JsonPathPrefix {
            prefix: path.unwrap_or_default(),
            automaton: regex,
        };
        let regex_query_with_path = prefix::AutomatonQuery {
            field,
            automaton: Arc::new(regex_automaton_with_path),
        };
        Ok(regex_query_with_path.into())
    }
}

mod prefix {
    use std::sync::Arc;

    use tantivy::query::{AutomatonWeight, EnableScoring, Query, Weight};
    use tantivy::schema::Field;
    use tantivy_fst::Automaton;
    pub struct JsonPathPrefix<A> {
        pub prefix: Vec<u8>,
        pub automaton: A,
    }

    #[derive(Clone)]
    pub enum JsonPathPrefixState<A> {
        Prefix(usize),
        Inner(A),
        PrefixFailed,
    }

    impl<A: Automaton> Automaton for JsonPathPrefix<A> {
        type State = JsonPathPrefixState<A::State>;

        fn start(&self) -> Self::State {
            if self.prefix.is_empty() {
                JsonPathPrefixState::Inner(self.automaton.start())
            } else {
                JsonPathPrefixState::Prefix(0)
            }
        }

        fn is_match(&self, state: &Self::State) -> bool {
            match state {
                JsonPathPrefixState::Prefix(_) => false,
                JsonPathPrefixState::Inner(inner_state) => self.automaton.is_match(inner_state),
                JsonPathPrefixState::PrefixFailed => false,
            }
        }

        fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
            match state {
                JsonPathPrefixState::Prefix(i) => {
                    if self.prefix.get(*i) != Some(&byte) {
                        return JsonPathPrefixState::PrefixFailed;
                    }
                    let next_pos = i + 1;
                    if next_pos == self.prefix.len() {
                        JsonPathPrefixState::Inner(self.automaton.start())
                    } else {
                        JsonPathPrefixState::Prefix(next_pos)
                    }
                }
                JsonPathPrefixState::Inner(inner_state) => {
                    JsonPathPrefixState::Inner(self.automaton.accept(inner_state, byte))
                }
                JsonPathPrefixState::PrefixFailed => JsonPathPrefixState::PrefixFailed,
            }
        }

        fn can_match(&self, state: &Self::State) -> bool {
            match state {
                JsonPathPrefixState::Prefix(_) => true,
                JsonPathPrefixState::Inner(inner_state) => self.automaton.can_match(inner_state),
                JsonPathPrefixState::PrefixFailed => false,
            }
        }

        fn will_always_match(&self, state: &Self::State) -> bool {
            match state {
                JsonPathPrefixState::Prefix(_) => false,
                JsonPathPrefixState::Inner(inner_state) => {
                    self.automaton.will_always_match(inner_state)
                }
                JsonPathPrefixState::PrefixFailed => false,
            }
        }
    }

    pub struct AutomatonQuery<A> {
        pub automaton: Arc<A>,
        pub field: Field,
    }

    impl<A> std::fmt::Debug for AutomatonQuery<A> {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.debug_struct("AutomatonQuery")
                .field("field", &self.field)
                .field("automaton", &std::any::type_name::<A>())
                .finish()
        }
    }

    impl<A> Clone for AutomatonQuery<A> {
        fn clone(&self) -> Self {
            AutomatonQuery {
                automaton: self.automaton.clone(),
                field: self.field,
            }
        }
    }

    impl<A: Automaton + Send + Sync + 'static> Query for AutomatonQuery<A>
    where A::State: Clone
    {
        fn weight(&self, _enabled_scoring: EnableScoring<'_>) -> tantivy::Result<Box<dyn Weight>> {
            Ok(Box::new(AutomatonWeight::<A>::new(
                self.field,
                self.automaton.clone(),
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use tantivy::schema::{TextFieldIndexing, TextOptions};

    use super::*;
    use crate::create_default_quickwit_tokenizer_manager;

    #[test]
    fn test_wildcard_query_to_regex_on_text() {
        let query = WildcardQuery {
            field: "text_field".to_string(),
            value: "MyString Wh1ch?a.nOrMal Tokenizer would*cut".to_string(),
        };

        let tokenizer_manager = create_default_quickwit_tokenizer_manager();
        for tokenizer in ["raw", "whitespace"] {
            let mut schema_builder = TantivySchema::builder();
            let text_options = TextOptions::default()
                .set_indexing_options(TextFieldIndexing::default().set_tokenizer(tokenizer));
            schema_builder.add_text_field("text_field", text_options);
            let schema = schema_builder.build();

            let (_field, path, regex) = query.to_regex(&schema, &tokenizer_manager).unwrap();
            assert_eq!(regex, "MyString Wh1ch.a\\.nOrMal Tokenizer would.*cut");
            assert!(path.is_none());
        }

        for tokenizer in [
            "raw_lowercase",
            "lowercase",
            "default",
            "en_stem",
            "chinese_compatible",
            "source_code_default",
            "source_code_with_hex",
        ] {
            let mut schema_builder = TantivySchema::builder();
            let text_options = TextOptions::default()
                .set_indexing_options(TextFieldIndexing::default().set_tokenizer(tokenizer));
            schema_builder.add_text_field("text_field", text_options);
            let schema = schema_builder.build();

            let (_field, path, regex) = query.to_regex(&schema, &tokenizer_manager).unwrap();
            assert_eq!(regex, "mystring wh1ch.a\\.normal tokenizer would.*cut");
            assert!(path.is_none());
        }
    }

    #[test]
    fn test_wildcard_query_to_regex_on_json() {
        let query = WildcardQuery {
            // this volontarily contains uppercase and regex-unsafe char to make sure we properly
            // keep the case, but sanitize special chars
            field: "json_field.Inner.Fie*ld".to_string(),
            value: "MyString Wh1ch?a.nOrMal Tokenizer would*cut".to_string(),
        };

        let tokenizer_manager = create_default_quickwit_tokenizer_manager();
        for tokenizer in ["raw", "whitespace"] {
            let mut schema_builder = TantivySchema::builder();
            let text_options = TextOptions::default()
                .set_indexing_options(TextFieldIndexing::default().set_tokenizer(tokenizer));
            schema_builder.add_json_field("json_field", text_options);
            let schema = schema_builder.build();

            let (_field, path, regex) = query.to_regex(&schema, &tokenizer_manager).unwrap();
            assert_eq!(regex, "MyString Wh1ch.a\\.nOrMal Tokenizer would.*cut");
            assert_eq!(path.unwrap(), "Inner\u{1}Fie*ld\0s".as_bytes());
        }

        for tokenizer in [
            "raw_lowercase",
            "lowercase",
            "default",
            "en_stem",
            "chinese_compatible",
            "source_code_default",
            "source_code_with_hex",
        ] {
            let mut schema_builder = TantivySchema::builder();
            let text_options = TextOptions::default()
                .set_indexing_options(TextFieldIndexing::default().set_tokenizer(tokenizer));
            schema_builder.add_json_field("json_field", text_options);
            let schema = schema_builder.build();

            let (_field, path, regex) = query.to_regex(&schema, &tokenizer_manager).unwrap();
            assert_eq!(regex, "mystring wh1ch.a\\.normal tokenizer would.*cut");
            assert_eq!(path.unwrap(), "Inner\u{1}Fie*ld\0s".as_bytes());
        }
    }
}
