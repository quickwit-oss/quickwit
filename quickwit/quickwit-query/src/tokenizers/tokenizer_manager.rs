use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use tantivy::tokenizer::{TextAnalyzer, TokenizerManager as TantivyTokenizerManager};

#[derive(Clone)]
pub struct TokenizerManager {
    inner: TantivyTokenizerManager,
    is_lowercaser: Arc<RwLock<HashMap<String, bool>>>,
}

impl TokenizerManager {
    /// Creates an empty tokenizer manager.
    pub fn new() -> Self {
        Self {
            inner: TantivyTokenizerManager::new(),
            is_lowercaser: Arc::new(RwLock::new(HashMap::new())),
        }
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
    pub fn get(&self, tokenizer_name: &str) -> Option<TextAnalyzer> {
        self.inner.get(tokenizer_name)
    }

    /// Query whether a given tokenizer does lowercasing
    pub fn get_does_lowercasing(&self, tokenizer_name: &str) -> Option<bool> {
        self.is_lowercaser
            .read()
            .unwrap()
            .get(tokenizer_name)
            .copied()
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
