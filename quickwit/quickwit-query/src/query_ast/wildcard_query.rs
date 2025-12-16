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

use std::borrow::Cow;
use std::sync::Arc;

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};
use tantivy::Term;
use tantivy::schema::{Field, FieldType, Schema as TantivySchema};

use super::{BuildTantivyAst, QueryAst};
use crate::query_ast::{AutomatonQuery, BuildTantivyAstContext, JsonPathPrefix, TantivyQueryAst};
use crate::tokenizers::TokenizerManager;
use crate::{InvalidQuery, find_field_or_hit_dynamic};

/// A Wildcard query allows to match 'bond' with a query like 'b*d'.
#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
pub struct WildcardQuery {
    pub field: String,
    pub value: String,
    /// Support missing fields
    pub lenient: bool,
    pub case_insensitive: bool,
}

impl From<WildcardQuery> for QueryAst {
    fn from(wildcard_query: WildcardQuery) -> Self {
        Self::Wildcard(wildcard_query)
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
                    // escaping at the end is invalid, handle it as if that escape sequence wasn't
                    // present
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

fn sub_query_parts_to_regex(
    sub_query_parts: Vec<SubQuery>,
    tokenizer_name: &str,
    tokenizer_manager: &TokenizerManager,
) -> anyhow::Result<String> {
    let mut normalizer = tokenizer_manager
        .get_normalizer(tokenizer_name)
        .with_context(|| format!("no tokenizer named `{tokenizer_name}` is registered"))?;

    sub_query_parts
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
        .collect::<Result<String, _>>()
}

impl WildcardQuery {
    pub fn to_regex(
        &self,
        schema: &TantivySchema,
        tokenizer_manager: &TokenizerManager,
    ) -> Result<(Field, Option<Vec<u8>>, String), InvalidQuery> {
        let Some((field, field_entry, json_path)) = find_field_or_hit_dynamic(&self.field, schema)
        else {
            return Err(InvalidQuery::FieldDoesNotExist {
                full_path: self.field.clone(),
            });
        };
        let field_type = field_entry.field_type();

        let sub_query_parts = parse_wildcard_query(&self.value);

        match field_type {
            FieldType::Str(text_options) => {
                let text_field_indexing = text_options.get_indexing_options().ok_or_else(|| {
                    InvalidQuery::SchemaError(format!(
                        "field {} is not full-text searchable",
                        field_entry.name()
                    ))
                })?;
                let tokenizer_name = text_field_indexing.tokenizer();
                let regex =
                    sub_query_parts_to_regex(sub_query_parts, tokenizer_name, tokenizer_manager)?;
                let regex = if self.case_insensitive {
                    format!("(?i){}", regex)
                } else {
                    regex
                };

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
                let regex =
                    sub_query_parts_to_regex(sub_query_parts, tokenizer_name, tokenizer_manager)?;
                let regex = if self.case_insensitive {
                    format!("(?i){}", regex)
                } else {
                    regex
                };

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
        context: &BuildTantivyAstContext,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let (field, path, regex) = match self.to_regex(context.schema, context.tokenizer_manager) {
            Ok(res) => res,
            Err(InvalidQuery::FieldDoesNotExist { .. }) if self.lenient => {
                return Ok(TantivyQueryAst::match_none());
            }
            Err(e) => return Err(e),
        };
        let regex =
            tantivy_fst::Regex::new(&regex).context("failed to parse regex built from wildcard")?;
        let regex_automaton_with_path = JsonPathPrefix {
            prefix: path.unwrap_or_default(),
            automaton: regex.into(),
        };
        let regex_query_with_path = AutomatonQuery {
            field,
            automaton: Arc::new(regex_automaton_with_path),
        };
        Ok(regex_query_with_path.into())
    }
}

#[cfg(test)]
mod tests {
    use tantivy::schema::{TextFieldIndexing, TextOptions};

    use super::*;
    use crate::create_default_quickwit_tokenizer_manager;

    fn single_text_field_schema(field_name: &str, tokenizer: &str) -> TantivySchema {
        let mut schema_builder = TantivySchema::builder();
        let text_options = TextOptions::default()
            .set_indexing_options(TextFieldIndexing::default().set_tokenizer(tokenizer));
        schema_builder.add_text_field(field_name, text_options);
        schema_builder.build()
    }

    #[test]
    fn test_wildcard_query_to_regex_on_text() {
        let query = WildcardQuery {
            field: "text_field".to_string(),
            value: "MyString Wh1ch?a.nOrMal Tokenizer would*cut".to_string(),
            lenient: false,
            case_insensitive: false,
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
    fn test_wildcard_query_to_regex_on_escaped_text() {
        let query = WildcardQuery {
            field: "text_field".to_string(),
            value: "MyString Wh1ch\\?a.nOrMal Tokenizer would\\*cut".to_string(),
            lenient: false,
            case_insensitive: false,
        };

        let tokenizer_manager = create_default_quickwit_tokenizer_manager();
        for tokenizer in ["raw", "whitespace"] {
            let mut schema_builder = TantivySchema::builder();
            let text_options = TextOptions::default()
                .set_indexing_options(TextFieldIndexing::default().set_tokenizer(tokenizer));
            schema_builder.add_text_field("text_field", text_options);
            let schema = schema_builder.build();

            let (_field, path, regex) = query.to_regex(&schema, &tokenizer_manager).unwrap();
            assert_eq!(regex, "MyString Wh1ch\\?a\\.nOrMal Tokenizer would\\*cut");
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
            assert_eq!(regex, "mystring wh1ch\\?a\\.normal tokenizer would\\*cut");
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
            lenient: false,
            case_insensitive: false,
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

    #[test]
    fn test_extract_regex_wildcard_missing_field() {
        let query = WildcardQuery {
            field: "my_missing_field".to_string(),
            value: "My query value*".to_string(),
            lenient: false,
            case_insensitive: false,
        };
        let tokenizer_manager = create_default_quickwit_tokenizer_manager();
        let schema = single_text_field_schema("my_field", "whitespace");
        let err = query.to_regex(&schema, &tokenizer_manager).unwrap_err();
        let InvalidQuery::FieldDoesNotExist {
            full_path: missing_field_full_path,
        } = err
        else {
            panic!("unexpected error: {err:?}");
        };
        assert_eq!(missing_field_full_path, "my_missing_field");
    }

    #[test]
    fn test_wildcard_query_to_regex_on_text_case_insensitive() {
        let query = WildcardQuery {
            field: "text_field".to_string(),
            value: "MyString Wh1ch?a.nOrMal Tokenizer would*cut".to_string(),
            lenient: false,
            case_insensitive: true,
        };

        let tokenizer_manager = create_default_quickwit_tokenizer_manager();
        for tokenizer in ["raw", "whitespace"] {
            let mut schema_builder = TantivySchema::builder();
            let text_options = TextOptions::default()
                .set_indexing_options(TextFieldIndexing::default().set_tokenizer(tokenizer));
            schema_builder.add_text_field("text_field", text_options);
            let schema = schema_builder.build();

            let (_field, path, regex) = query.to_regex(&schema, &tokenizer_manager).unwrap();
            assert_eq!(regex, "(?i)MyString Wh1ch.a\\.nOrMal Tokenizer would.*cut");
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
            assert_eq!(regex, "(?i)mystring wh1ch.a\\.normal tokenizer would.*cut");
            assert!(path.is_none());
        }
    }
}
