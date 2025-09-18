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

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use tantivy::tokenizer::{
    LowerCaser, RawTokenizer, RemoveLongFilter, TextAnalyzer,
    TokenizerManager as TantivyTokenizerManager,
};

use crate::DEFAULT_REMOVE_TOKEN_LENGTH;

pub const RAW_TOKENIZER_NAME: &str = "raw";
const LOWERCASE_TOKENIZER_NAME: &str = "lowercase";
const RAW_LOWERCASE_TOKENIZER_NAME: &str = "raw_lowercase";

#[derive(Clone)]
pub struct TokenizerManager {
    inner: TantivyTokenizerManager,
    is_lowercaser: Arc<RwLock<HashMap<String, bool>>>,
}

impl TokenizerManager {
    /// Creates an empty tokenizer manager.
    pub fn new() -> Self {
        let this = Self {
            inner: TantivyTokenizerManager::new(),
            is_lowercaser: Arc::new(RwLock::new(HashMap::new())),
        };

        // in practice these will almost always be overridden in
        // create_default_quickwit_tokenizer_manager()
        let raw_tokenizer = TextAnalyzer::builder(RawTokenizer::default())
            .filter(RemoveLongFilter::limit(DEFAULT_REMOVE_TOKEN_LENGTH))
            .build();
        this.register(RAW_TOKENIZER_NAME, raw_tokenizer, false);
        let raw_tokenizer = TextAnalyzer::builder(RawTokenizer::default())
            .filter(LowerCaser)
            .filter(RemoveLongFilter::limit(DEFAULT_REMOVE_TOKEN_LENGTH))
            .build();
        this.register(RAW_LOWERCASE_TOKENIZER_NAME, raw_tokenizer, true);
        let lower_case_tokenizer = TextAnalyzer::builder(RawTokenizer::default())
            .filter(LowerCaser)
            .filter(RemoveLongFilter::limit(DEFAULT_REMOVE_TOKEN_LENGTH))
            .build();
        this.register(LOWERCASE_TOKENIZER_NAME, lower_case_tokenizer, true);

        this
    }

    /// Registers a new tokenizer associated with a given name.
    pub fn register<T>(&self, tokenizer_name: &str, tokenizer: T, does_lowercasing: bool)
    where TextAnalyzer: From<T> {
        self.inner.register(tokenizer_name, tokenizer);
        self.is_lowercaser
            .write()
            .unwrap()
            .insert(tokenizer_name.to_string(), does_lowercasing);
    }

    /// Accessing a tokenizer given its name.
    pub fn get_tokenizer(&self, tokenizer_name: &str) -> Option<TextAnalyzer> {
        self.inner.get(tokenizer_name)
    }

    /// Query whether a given tokenizer does lowercasing
    pub fn get_normalizer(&self, tokenizer_name: &str) -> Option<TextAnalyzer> {
        let use_lowercaser = self
            .is_lowercaser
            .read()
            .unwrap()
            .get(tokenizer_name)
            .copied()?;
        let analyzer = if use_lowercaser {
            RAW_LOWERCASE_TOKENIZER_NAME
        } else {
            RAW_TOKENIZER_NAME
        };
        self.get_tokenizer(analyzer)
    }

    /// Get the inner TokenizerManager
    pub fn tantivy_manager(&self) -> &TantivyTokenizerManager {
        &self.inner
    }
}

impl Default for TokenizerManager {
    fn default() -> Self {
        Self::new()
    }
}
