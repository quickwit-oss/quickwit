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

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use tantivy::tokenizer::{
    LowerCaser, RawTokenizer, RemoveLongFilter, TextAnalyzer,
    TokenizerManager as TantivyTokenizerManager,
};

use crate::DEFAULT_REMOVE_TOKEN_LENGTH;

const RAW_TOKENIZER_NAME: &str = "raw";
const LOWERCASE_TOKENIZER_NAME: &str = "lowercase";

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

        // in practice these will almost always be overriden in
        // create_default_quickwit_tokenizer_manager()
        let raw_tokenizer = TextAnalyzer::builder(RawTokenizer::default())
            .filter(RemoveLongFilter::limit(DEFAULT_REMOVE_TOKEN_LENGTH))
            .build();
        this.register(RAW_TOKENIZER_NAME, raw_tokenizer, false);
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
            LOWERCASE_TOKENIZER_NAME
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
