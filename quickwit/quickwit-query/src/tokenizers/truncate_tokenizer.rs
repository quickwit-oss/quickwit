use tantivy::tokenizer::{Token, TokenFilter, TokenStream, Tokenizer};

#[derive(Clone)]
pub struct TruncateLongFilter {
    max_bytes: usize,
}
impl TruncateLongFilter {
    pub fn limit(max_bytes: usize) -> Self {
        Self { max_bytes }
    }
}
impl TokenFilter for TruncateLongFilter {
    type Tokenizer<T: Tokenizer> = TruncateLongWrapper<T>;
    fn transform<T: Tokenizer>(self, inner: T) -> Self::Tokenizer<T> {
        TruncateLongWrapper {
            max_bytes: self.max_bytes,
            inner,
        }
    }
}
#[derive(Clone)]
pub struct TruncateLongWrapper<T: Tokenizer> {
    max_bytes: usize,
    inner: T,
}
impl<T: Tokenizer> Tokenizer for TruncateLongWrapper<T> {
    type TokenStream<'a> = TruncateLongStream<T::TokenStream<'a>> where T: 'a;
    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        TruncateLongStream {
            max_bytes: self.max_bytes,
            tail: self.inner.token_stream(text),
        }
    }
}
pub struct TruncateLongStream<T> {
    max_bytes: usize,
    tail: T,
}
impl<T: TokenStream> TokenStream for TruncateLongStream<T> {
    fn advance(&mut self) -> bool {
        if !self.tail.advance() {
            return false;
        }
        let tok = self.tail.token_mut();
        if tok.text.len() > self.max_bytes {
            truncate_at_char_boundary(&mut tok.text, self.max_bytes);
            tok.offset_to = tok.offset_from.saturating_add(tok.text.len());
        }
        true
    }
    fn token(&self) -> &Token {
        self.tail.token()
    }
    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }
}

/// Shrinks `s` to at most `max_bytes` UTF-8 bytes without splitting a 
/// multibyte character at the given `max_bytes` index.
fn truncate_at_char_boundary(s: &mut String, max_bytes: usize) {
    if s.len() <= max_bytes {
        return;
    }
    let mut end = max_bytes;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    s.truncate(end);
}