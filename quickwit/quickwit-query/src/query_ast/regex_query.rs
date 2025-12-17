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

use std::sync::Arc;

use anyhow::Context;
pub use prefix::{AutomatonQuery, JsonPathPrefix};
use serde::{Deserialize, Serialize};
use tantivy::Term;
use tantivy::schema::{Field, FieldType, Schema as TantivySchema};

use super::{BuildTantivyAst, BuildTantivyAstContext, QueryAst};
use crate::query_ast::TantivyQueryAst;
use crate::{InvalidQuery, find_field_or_hit_dynamic};

/// A Regex query
#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
pub struct RegexQuery {
    pub field: String,
    pub regex: String,
}

impl From<RegexQuery> for QueryAst {
    fn from(regex_query: RegexQuery) -> Self {
        Self::Regex(regex_query)
    }
}

impl RegexQuery {
    #[cfg(test)]
    pub fn from_field_value(field: impl ToString, regex: impl ToString) -> Self {
        Self {
            field: field.to_string(),
            regex: regex.to_string(),
        }
    }
}

impl RegexQuery {
    pub fn to_field_and_regex(
        &self,
        schema: &TantivySchema,
    ) -> Result<(Field, Option<Vec<u8>>, String), InvalidQuery> {
        let Some((field, field_entry, json_path)) = find_field_or_hit_dynamic(&self.field, schema)
        else {
            return Err(InvalidQuery::FieldDoesNotExist {
                full_path: self.field.clone(),
            });
        };
        let field_type = field_entry.field_type();

        match field_type {
            FieldType::Str(text_options) => {
                text_options.get_indexing_options().ok_or_else(|| {
                    InvalidQuery::SchemaError(format!(
                        "field {} is not full-text searchable",
                        field_entry.name()
                    ))
                })?;

                Ok((field, None, self.regex.clone()))
            }
            FieldType::JsonObject(json_options) => {
                json_options.get_text_indexing_options().ok_or_else(|| {
                    InvalidQuery::SchemaError(format!(
                        "field {} is not full-text searchable",
                        field_entry.name()
                    ))
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
                Ok((field, Some(byte_path_prefix), self.regex.clone()))
            }
            _ => Err(InvalidQuery::SchemaError(
                "trying to run a regex query on a non-text field".to_string(),
            )),
        }
    }
}

impl BuildTantivyAst for RegexQuery {
    fn build_tantivy_ast_impl(
        &self,
        context: &BuildTantivyAstContext,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let (field, path, regex) = self.to_field_and_regex(context.schema)?;
        let regex = tantivy_fst::Regex::new(&regex).context("failed to parse regex")?;
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

mod prefix {
    use std::sync::Arc;

    use tantivy::query::{AutomatonWeight, EnableScoring, Query, Weight};
    use tantivy::schema::Field;
    use tantivy_fst::Automaton;

    pub struct JsonPathPrefix<A> {
        pub prefix: Vec<u8>,
        pub automaton: Arc<A>,
    }

    // we need to implement manually because the std adds an unnecessary bound `A: Clone`
    impl<A> Clone for JsonPathPrefix<A> {
        fn clone(&self) -> Self {
            JsonPathPrefix {
                prefix: self.prefix.clone(),
                automaton: self.automaton.clone(),
            }
        }
    }

    #[derive(Clone, Debug, PartialEq)]
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

    // we don't use RegexQuery to handle our path. We could tinker with the regex to embed
    // json field path inside, but that seems not as clean, and would prevent support of
    // case-insensitive search in the future (we would also make the path insensitive,
    // which we shouldn't)
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
    use std::sync::Arc;

    use tantivy::schema::{Schema as TantivySchema, TEXT};
    use tantivy_fst::{Automaton, Regex};

    use super::prefix::JsonPathPrefixState;
    use super::{JsonPathPrefix, RegexQuery};

    #[test]
    fn test_regex_query_text_field() {
        let mut schema_builder = TantivySchema::builder();
        schema_builder.add_text_field("field", TEXT);
        let schema = schema_builder.build();

        let query = RegexQuery {
            field: "field".to_string(),
            regex: "abc.*xyz".to_string(),
        };
        let (field, path, regex) = query.to_field_and_regex(&schema).unwrap();
        assert_eq!(field, schema.get_field("field").unwrap());
        assert!(path.is_none());
        assert_eq!(regex, query.regex);
    }

    #[test]
    fn test_regex_query_json_field() {
        let mut schema_builder = TantivySchema::builder();
        schema_builder.add_json_field("field", TEXT);
        let schema = schema_builder.build();

        let query = RegexQuery {
            field: "field.sub.field".to_string(),
            regex: "abc.*xyz".to_string(),
        };
        let (field, path, regex) = query.to_field_and_regex(&schema).unwrap();
        assert_eq!(field, schema.get_field("field").unwrap());
        assert_eq!(path.unwrap(), b"sub\x01field\0s");
        assert_eq!(regex, query.regex);

        // i believe this is how concatenated field behave
        let query_empty_path = RegexQuery {
            field: "field".to_string(),
            regex: "abc.*xyz".to_string(),
        };
        let (field, path, regex) = query_empty_path.to_field_and_regex(&schema).unwrap();
        assert_eq!(field, schema.get_field("field").unwrap());
        assert_eq!(path.unwrap(), b"\0s");
        assert_eq!(regex, query_empty_path.regex);
    }

    #[test]
    fn test_json_prefix_automaton_empty_path() {
        let regex = Arc::new(Regex::new("e(f|g.*)").unwrap());
        let empty_path_automaton = JsonPathPrefix {
            prefix: Vec::new(),
            automaton: regex.clone(),
        };

        let start = empty_path_automaton.start();
        assert_eq!(start, JsonPathPrefixState::Inner(regex.start()));
    }

    #[test]
    fn test_json_prefix_automaton() {
        let regex = Arc::new(Regex::new("e(f|g.*)").unwrap());
        let automaton = JsonPathPrefix {
            prefix: b"ab".to_vec(),
            automaton: regex.clone(),
        };

        let start = automaton.start();
        assert!(matches!(start, JsonPathPrefixState::Prefix(_)));
        assert!(automaton.can_match(&start));
        assert!(!automaton.is_match(&start));

        let miss = automaton.accept(&start, b'g');
        assert_eq!(miss, JsonPathPrefixState::PrefixFailed);
        // supporting this is important for optimisation
        assert!(!automaton.can_match(&miss));
        assert!(!automaton.is_match(&miss));

        let a = automaton.accept(&start, b'a');
        assert!(matches!(a, JsonPathPrefixState::Prefix(_)));
        assert!(automaton.can_match(&a));
        assert!(!automaton.is_match(&a));

        let ab = automaton.accept(&a, b'b');
        assert_eq!(ab, JsonPathPrefixState::Inner(regex.start()));
        assert!(automaton.can_match(&ab));
        assert!(!automaton.is_match(&ab));

        // starting here, we just take that we passthrough correctly,
        // and reply to can_match as well as possible
        // (we don't test will_always_match because Regex doesn't support it)
        let abc = automaton.accept(&ab, b'c');
        assert!(matches!(abc, JsonPathPrefixState::Inner(_)));
        assert!(!automaton.can_match(&abc));
        assert!(!automaton.is_match(&abc));

        let abe = automaton.accept(&ab, b'e');
        assert!(matches!(abe, JsonPathPrefixState::Inner(_)));
        assert!(automaton.can_match(&abe));
        assert!(!automaton.is_match(&abe));

        let abef = automaton.accept(&abe, b'f');
        assert!(matches!(abef, JsonPathPrefixState::Inner(_)));
        assert!(automaton.can_match(&abef));
        assert!(automaton.is_match(&abef));

        let abefg = automaton.accept(&abef, b'g');
        assert!(matches!(abefg, JsonPathPrefixState::Inner(_)));
        assert!(!automaton.can_match(&abefg));
        assert!(!automaton.is_match(&abefg));

        let abeg = automaton.accept(&abe, b'g');
        assert!(matches!(abeg, JsonPathPrefixState::Inner(_)));
        assert!(automaton.can_match(&abeg));
        assert!(automaton.is_match(&abeg));

        let abegh = automaton.accept(&abeg, b'h');
        assert!(matches!(abegh, JsonPathPrefixState::Inner(_)));
        assert!(automaton.can_match(&abegh));
        assert!(automaton.is_match(&abegh));
    }
}
