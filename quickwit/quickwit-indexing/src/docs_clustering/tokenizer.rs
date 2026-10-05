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

//! Tokenization for fingerprint computation.
//!
//! The tokenizer turns a string field into a sequence of `TokenSpan`s. For fingerprinting,
//! tokenized fields hash the sequence of `TokenType`s instead of the original text, so values with
//! the same structure but different literals can be ordered together.
//!
//! ```text
//! "server started at 8080"
//!     |
//!     v
//! Word Gap Word Gap Word Gap Number
//! ```
//!
//! The tokenizer has two byte classes:
//! - `TOKEN`: ASCII letters, digits, selected punctuation, and all non-ASCII bytes;
//! - `GAP`: whitespace and other punctuation.
//!
//! # Invariants
//!
//! - Char-boundary safety. All non-ASCII bytes (`>= 0x80`) are `TOKEN` bytes. Since every byte of a
//!   multi-byte UTF-8 sequence is `>= 0x80`, a multi-byte character is *entirely* `TOKEN`. Cuts
//!   therefore only happen at single-byte ASCII positions, so every cut falls on a UTF-8 char
//!   boundary. Slicing `&input[start..end]` is always valid.
//! - Partitioning. The emitted spans are non-overlapping and cover the whole input: `0..len` is
//!   partitioned into the in-order half-open ranges `[start, end)`, each non-empty. Proven by
//!   proptest.
//! - Locality. Because the cut decision between two adjacent bytes depends only on those two bytes,
//!   splitting the input at any boundary we produce and tokenizing the halves independently yields
//!   the same spans (after offset shifting). Proven by proptest.
//! - ASCII coarseness. For ASCII input, every offset where we cut is also a UAX-29 word boundary.
//!   This is proven by proptest against `unicode_segmentation`. Non-ASCII bytes are deliberately
//!   treated as `TOKEN`, so mixed non-ASCII and ASCII whitespace can diverge from UAX-29 while
//!   remaining UTF-8 safe.
//! - `u32` offsets. Offsets are stored as `u32`; inputs are assumed to be smaller than 4 GiB. A log
//!   line is many orders of magnitude below this.

/// Classification of a token span.
///
/// `Gap` is produced only for inter-token byte runs (whitespace / punctuation);
/// [`classify`] never returns `Gap`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TokenType {
    /// A word: the fallback type for any token that matches no special pattern.
    Word = 1,
    /// A number: pure digits or a decimal (digits with exactly one dot).
    Number = 2,
    /// A dotted-quad IPv4 address (four octets `0..=255`, no leading zeros).
    IPv4 = 3,
    /// A UUID v4-shaped token (8-4-4-4-12 hex, 36 bytes).
    Uuid = 4,
    /// An inter-token span (whitespace or `GAP` punctuation), not indexed.
    Gap = 5,
}

/// A classified byte span ending at `end` (exclusive) into the original input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenSpan {
    /// Classification of the span.
    pub token_type: TokenType,
    /// Exclusive end byte offset into the input.
    pub end: u32,
}

/// A byte is a `TOKEN` byte (`true`) or a `GAP` byte (`false`).
///
/// The table preserves the ASCII coarseness and UTF-8 safety properties:
/// - All non-ASCII bytes (`>= 0x80`) are `TOKEN`.
/// - ASCII letters and digits are `TOKEN`.
/// - The ASCII contextual-join punctuation `.` `,` `;` `'` `_` `:` `-` is `TOKEN`.
/// - Every other ASCII byte (whitespace and always-boundary punctuation) is `GAP`.
const LOOKUP_TABLE_SIZE: usize = u8::MAX as usize + 1;

/// `true` at index `b` iff byte `b` belongs to the `TOKEN` class.
static CLASS_TABLE: [bool; LOOKUP_TABLE_SIZE] = {
    let mut table = [false; LOOKUP_TABLE_SIZE];
    let mut byte = 0usize;
    while byte < LOOKUP_TABLE_SIZE {
        let is_token = if byte >= 0x80 {
            true
        } else {
            let ch = byte as u8;
            let is_letter = ch.is_ascii_lowercase() || ch.is_ascii_uppercase();
            let is_digit = ch.is_ascii_digit();
            let is_join = matches!(ch, b'.' | b',' | b';' | b'\'' | b'_' | b':' | b'-');
            is_letter || is_digit || is_join
        };
        table[byte] = is_token;
        byte += 1;
    }
    table
};

/// `true` at index `b` iff byte `b` is an ASCII decimal digit.
static DIGIT_LOOKUP_TABLE: [bool; LOOKUP_TABLE_SIZE] = {
    let mut lookup = [false; LOOKUP_TABLE_SIZE];
    let mut i = 0;
    while i < LOOKUP_TABLE_SIZE {
        if (i as u8).is_ascii_digit() {
            lookup[i] = true;
        }
        i += 1;
    }
    lookup
};

/// `true` at index `b` iff byte `b` is an ASCII hex digit.
static HEX_DIGIT_LOOKUP_TABLE: [bool; LOOKUP_TABLE_SIZE] = {
    let mut lookup = [false; LOOKUP_TABLE_SIZE];
    let mut i = 0;
    while i < LOOKUP_TABLE_SIZE {
        if (i as u8).is_ascii_hexdigit() {
            lookup[i] = true;
        }
        i += 1;
    }
    lookup
};

/// A lossless, contiguous, zero-allocation iterator over [`TokenSpan`]s.
///
/// Concatenating the texts of all yielded spans reproduces the input exactly.
pub struct Tokenizer<'a> {
    bytes: &'a [u8],
    /// Offset of the next byte to examine. Always `<= bytes.len()`.
    pos: usize,
}

impl<'a> Tokenizer<'a> {
    #[inline]
    fn new(input: &'a str) -> Self {
        debug_assert!(
            input.len() <= u32::MAX as usize,
            "tokenizer input must be < 4 GiB to fit u32 offsets"
        );
        Tokenizer {
            bytes: input.as_bytes(),
            pos: 0,
        }
    }
}

impl Iterator for Tokenizer<'_> {
    type Item = TokenSpan;

    #[inline]
    fn next(&mut self) -> Option<TokenSpan> {
        let bytes = self.bytes;
        let start = self.pos;
        if start >= bytes.len() {
            return None;
        }

        let run_is_token = CLASS_TABLE[bytes[start] as usize];
        let len = bytes.len();
        let mut pos = start + 1;
        while pos < len && CLASS_TABLE[bytes[pos] as usize] == run_is_token {
            pos += 1;
        }
        self.pos = pos;

        let token_type = if run_is_token {
            classify_bytes(&bytes[start..pos])
        } else {
            TokenType::Gap
        };

        Some(TokenSpan {
            token_type,
            end: pos as u32,
        })
    }
}

/// Tokenize `input`, returning a lazy, zero-allocation iterator of spans.
#[inline]
pub fn tokenize(input: &str) -> Tokenizer<'_> {
    Tokenizer::new(input)
}

/// Byte-level classification core, shared by [`classify`] and the iterator.
#[inline]
fn classify_bytes(bytes: &[u8]) -> TokenType {
    let Some(&first) = bytes.first() else {
        return TokenType::Word;
    };

    if !HEX_DIGIT_LOOKUP_TABLE[first as usize] {
        return TokenType::Word;
    }

    if matches!(is_uuid_v4(bytes), Some(len) if len == bytes.len()) {
        return TokenType::Uuid;
    }

    if matches!(is_number(bytes), Some(len) if len == bytes.len()) {
        return TokenType::Number;
    }

    if is_decimal_number(bytes) {
        return TokenType::Number;
    }

    if matches!(is_ipv4(bytes), Some(len) if len == bytes.len()) {
        return TokenType::IPv4;
    }

    TokenType::Word
}

/// Quick IPv4 check: four octets 0-255, no leading zeros.
/// Returns the number of bytes consumed.
#[inline]
fn is_ipv4(bytes: &[u8]) -> Option<usize> {
    if bytes.is_empty() || !DIGIT_LOOKUP_TABLE[bytes[0] as usize] {
        return None;
    }
    let mut i = 0;

    for octet_idx in 0..4 {
        let start = i;

        if i >= bytes.len() || !DIGIT_LOOKUP_TABLE[bytes[i] as usize] {
            return None;
        }

        let mut val: u16 = 0;
        let mut digit_count = 0;
        while i < bytes.len() && DIGIT_LOOKUP_TABLE[bytes[i] as usize] {
            val = val * 10 + (bytes[i] - b'0') as u16;
            digit_count += 1;
            i += 1;
            if digit_count > 3 || val > 255 {
                return None;
            }
        }

        if digit_count > 1 && bytes[start] == b'0' {
            return None;
        }

        if octet_idx < 3 {
            if i >= bytes.len() || bytes[i] != b'.' {
                return None;
            }
            i += 1;
        }
    }

    Some(i)
}

/// Count the leading run of ASCII digits. Returns `None` if there is none.
#[inline]
fn is_number(bytes: &[u8]) -> Option<usize> {
    if bytes.is_empty() || !DIGIT_LOOKUP_TABLE[bytes[0] as usize] {
        return None;
    }
    Some(
        bytes
            .iter()
            .take_while(|&&byte| DIGIT_LOOKUP_TABLE[byte as usize])
            .count(),
    )
}

/// UUID v4-ish check (8-4-4-4-12 pattern, 36 bytes total).
/// Returns the number of bytes consumed (36) on success.
#[inline]
fn is_uuid_v4(bytes: &[u8]) -> Option<usize> {
    if bytes.len() < 36 || !HEX_DIGIT_LOOKUP_TABLE[bytes[0] as usize] {
        return None;
    }
    if bytes[8] != b'-' || bytes[13] != b'-' || bytes[18] != b'-' || bytes[23] != b'-' {
        return None;
    }
    let ranges_to_check = [0..8, 9..13, 14..18, 19..23, 24..36];
    for range in ranges_to_check.iter() {
        for &byte in &bytes[range.clone()] {
            if !HEX_DIGIT_LOOKUP_TABLE[byte as usize] {
                return None;
            }
        }
    }
    Some(36)
}

/// Decimal-number check: digits with exactly one dot (e.g. "32.3").
#[inline]
fn is_decimal_number(bytes: &[u8]) -> bool {
    if bytes.len() < 3 {
        return false;
    }
    let mut dot_count = 0;
    for &byte in bytes {
        if byte == b'.' {
            dot_count += 1;
            if dot_count > 1 {
                return false;
            }
        } else if !DIGIT_LOOKUP_TABLE[byte as usize] {
            return false;
        }
    }
    dot_count == 1
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use unicode_segmentation::UnicodeSegmentation;

    use super::{
        TokenSpan, TokenType, Tokenizer, classify_bytes, is_decimal_number, is_ipv4, is_number,
        is_uuid_v4, tokenize,
    };

    /// Tokenize `input` into `out`, clearing `out` first and reusing its capacity.
    fn tokenize_into(input: &str, out: &mut Vec<TokenSpan>) {
        out.clear();
        for span in Tokenizer::new(input) {
            out.push(span);
        }
    }

    #[inline]
    fn classify(text: &str) -> TokenType {
        classify_bytes(text.as_bytes())
    }

    fn reconstruct(input: &str) -> String {
        let mut out = String::new();
        let mut cursor = 0usize;
        for span in tokenize(input) {
            let end = span.end as usize;
            out.push_str(&input[cursor..end]);
            cursor = end;
        }
        out
    }

    fn our_cut_offsets(input: &str) -> Vec<usize> {
        let mut cuts = vec![0usize];
        for span in tokenize(input) {
            cuts.push(span.end as usize);
        }
        cuts.dedup();
        cuts
    }

    fn unicode_word_bound_offsets(input: &str) -> Vec<usize> {
        let mut offsets = Vec::new();
        let mut pos = 0usize;
        offsets.push(0);
        for piece in input.split_word_bounds() {
            pos += piece.len();
            offsets.push(pos);
        }
        offsets.dedup();
        offsets
    }

    #[test]
    fn test_lossless_empty() {
        assert_eq!(tokenize("").count(), 0);
        assert_eq!(reconstruct(""), "");
    }

    #[test]
    fn test_lossless_all_gap() {
        let input = "   \t\n  ";
        assert_eq!(reconstruct(input), input);
        let spans: Vec<TokenSpan> = tokenize(input).collect();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].token_type, TokenType::Gap);
    }

    #[test]
    fn test_lossless_all_token() {
        let input = "helloworld";
        assert_eq!(reconstruct(input), input);
        let spans: Vec<TokenSpan> = tokenize(input).collect();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].token_type, TokenType::Word);
    }

    #[test]
    fn test_lossless_leading_trailing_gaps() {
        let input = "  hello  world  ";
        assert_eq!(reconstruct(input), input);
    }

    #[test]
    fn test_lossless_multibyte() {
        for input in ["abc\u{3000}def", "café", "naïve résumé", "日本語 test"] {
            assert_eq!(reconstruct(input), input, "lossless failed for {input:?}");
        }
    }

    #[test]
    fn test_lossless_mixed_log_line() {
        let input = "2024-01-01T00:00:00Z INFO host=web-01 ip=192.168.1.1 \
                     id=550e8400-e29b-41d4-a716-446655440000 latency=3.14ms";
        assert_eq!(reconstruct(input), input);
    }

    #[test]
    fn test_contiguity() {
        let inputs = [
            "",
            "  hello  world  ",
            "192.168.1.1 foo",
            "café\u{3000}日本",
            "a.b,c-d:e",
        ];
        for input in inputs {
            let spans: Vec<TokenSpan> = tokenize(input).collect();
            if spans.is_empty() {
                assert_eq!(input.len(), 0);
                continue;
            }
            assert_eq!(reconstruct(input), input, "lossless failed for {input:?}");
            let mut cursor = 0u32;
            for span in &spans {
                assert!(span.end > cursor, "non-advancing span in {input:?}");
                cursor = span.end;
            }
            assert_eq!(
                spans.last().unwrap().end as usize,
                input.len(),
                "last end must equal len for {input:?}"
            );
        }
    }

    fn assert_partition_and_lossless(input: &str) {
        let spans: Vec<TokenSpan> = tokenize(input).collect();

        assert_eq!(reconstruct(input), input, "lossless failed for {input:?}");

        if spans.is_empty() {
            assert_eq!(input.len(), 0);
            return;
        }

        let mut cursor: u32 = 0;
        for span in &spans {
            assert!(span.end > cursor, "empty or out-of-order span in {input:?}");
            assert!(
                input.is_char_boundary(span.end as usize),
                "non-boundary end in {input:?}"
            );
            cursor = span.end;
        }
        assert_eq!(
            cursor as usize,
            input.len(),
            "last end must equal len for {input:?}"
        );
    }

    #[test]
    fn test_unicode_partition_and_lossless() {
        let inputs = [
            "café",
            "naïve résumé",
            "日本語",
            "状態。テスト",
            "ア・イ・ウ",
            "emoji 👍🏽 done",
            "family 👨‍👩‍👧 grp",
            "a\u{0301}b",
            "weird\u{3000}space",
            "Ω≈ç√∫˜µ≤",
            "",
        ];
        for input in inputs {
            assert_partition_and_lossless(input);
        }
    }

    #[test]
    fn test_unicode_no_chop_at_japanese_dot() {
        let input = "状態。テスト・データ";
        let spans: Vec<TokenSpan> = tokenize(input).collect();
        assert_eq!(spans.len(), 1, "expected one span, got {spans:?}");
        assert_eq!(spans[0].token_type, TokenType::Word);
        assert_eq!(spans[0].end as usize, input.len());

        let unicode_pieces = input.split_word_bounds().count();
        assert!(
            unicode_pieces > 1,
            "sanity: Unicode should segment {input:?} into >1 pieces, got {unicode_pieces}"
        );
    }

    #[test]
    fn test_unicode_mixed_ascii_cjk() {
        let input = "user=日本語 ip=192.168.1.1";
        assert_partition_and_lossless(input);
        let spans: Vec<TokenSpan> = tokenize(input).collect();
        let mut cursor = 0usize;
        let mut token_texts: Vec<&str> = Vec::new();
        for span in &spans {
            let end = span.end as usize;
            if span.token_type != TokenType::Gap {
                token_texts.push(&input[cursor..end]);
            }
            cursor = end;
        }
        assert_eq!(token_texts, vec!["user", "日本語", "ip", "192.168.1.1"]);
        assert!(
            spans.iter().any(|span| span.token_type == TokenType::IPv4),
            "expected an IPv4 span"
        );
    }

    #[test]
    fn test_no_cut_at_unicode_punct_or_space_stays_coarser() {
        let inputs = ["状態。テスト", "ア・イ", "weird\u{3000}space", "a\u{00A0}b"];
        for input in inputs {
            let ours: Vec<usize> = our_cut_offsets(input);
            let unicode: std::collections::HashSet<usize> =
                unicode_word_bound_offsets(input).into_iter().collect();

            for offset in &ours {
                assert!(
                    unicode.contains(offset),
                    "our cut at {offset} is not a Unicode boundary in {input:?}"
                );
            }
            assert_eq!(
                ours,
                vec![0, input.len()],
                "expected no interior cut in {input:?}"
            );
            assert!(
                unicode.len() > ours.len(),
                "Unicode should break {input:?} more than we do"
            );
        }
    }

    #[test]
    fn test_non_ascii_whitespace_may_diverge_from_unicode_boundaries() {
        let input = "\u{3000} ";
        let ours: Vec<usize> = our_cut_offsets(input);
        let unicode: std::collections::HashSet<usize> =
            unicode_word_bound_offsets(input).into_iter().collect();
        assert_eq!(ours, vec![0, 3, 4]);
        assert!(
            !ours.iter().all(|offset| unicode.contains(offset)),
            "this case is expected to not be coarser"
        );
    }

    #[test]
    fn test_classify_word() {
        assert_eq!(classify("hello"), TokenType::Word);
        assert_eq!(classify("HelloWorld"), TokenType::Word);
        assert_eq!(classify("my_service"), TokenType::Word);
        assert_eq!(classify(""), TokenType::Word);
    }

    #[test]
    fn test_classify_uuid() {
        assert_eq!(
            classify("550e8400-e29b-41d4-a716-446655440000"),
            TokenType::Uuid
        );
        assert_ne!(classify("550e8400-e29b-41d4-a716"), TokenType::Uuid);
        assert_ne!(classify("not-a-uuid-at-all"), TokenType::Uuid);
    }

    #[test]
    fn test_classify_ipv4() {
        assert_eq!(classify("192.168.1.1"), TokenType::IPv4);
        assert_eq!(classify("0.0.0.0"), TokenType::IPv4);
        assert_eq!(classify("255.255.255.255"), TokenType::IPv4);
        assert_ne!(classify("256.1.1.1"), TokenType::IPv4);
        assert_ne!(classify("192.168.1.1.1"), TokenType::IPv4);
    }

    #[test]
    fn test_classify_number() {
        assert_eq!(classify("123"), TokenType::Number);
        assert_eq!(classify("0"), TokenType::Number);
        assert_eq!(classify("01"), TokenType::Number);
        assert_eq!(classify("3.14"), TokenType::Number);
        assert_eq!(classify("0.5"), TokenType::Number);
    }

    #[test]
    fn test_classify_never_gap() {
        for text in ["", ".", "-", "hello", "1.2.3.4", "12"] {
            assert_ne!(classify(text), TokenType::Gap);
        }
    }

    #[test]
    fn test_helpers() {
        assert_eq!(
            is_uuid_v4(b"550e8400-e29b-41d4-a716-446655440000"),
            Some(36)
        );
        assert!(is_uuid_v4(b"550e8400-e29b-41d4-a716").is_none());
        assert_eq!(is_number(b"123"), Some(3));
        assert_eq!(is_number(b"12a"), Some(2));
        assert!(is_number(b"abc").is_none());
        assert!(is_ipv4(b"192.168.1.1").is_some());
        assert!(is_ipv4(b"256.1.1.1").is_none());
        assert!(is_ipv4(b"01.0.0.0").is_none());
        assert!(is_decimal_number(b"3.14"));
        assert!(!is_decimal_number(b"1.2.3"));
        assert!(!is_decimal_number(b"123"));
    }

    fn classify_via_tokenize(input: &str) -> TokenType {
        let spans: Vec<TokenSpan> = tokenize(input).collect();
        assert_eq!(spans.len(), 1, "expected a single span for {input:?}");
        spans[0].token_type
    }

    #[test]
    fn test_embedded_classification() {
        assert_eq!(classify_via_tokenize("192.168.1.1"), TokenType::IPv4);
        assert_eq!(
            classify_via_tokenize("550e8400-e29b-41d4-a716-446655440000"),
            TokenType::Uuid
        );
        assert_eq!(classify_via_tokenize("42"), TokenType::Number);
        assert_eq!(classify_via_tokenize("3.14"), TokenType::Number);
        assert_eq!(classify_via_tokenize("hello"), TokenType::Word);
    }

    #[test]
    fn test_embedded_in_context() {
        let input = "ip=192.168.1.1!";
        let spans: Vec<TokenSpan> = tokenize(input).collect();
        let ipv4_pos = spans
            .iter()
            .position(|span| span.token_type == TokenType::IPv4);
        assert!(ipv4_pos.is_some(), "expected an IPv4 span in {input:?}");
        let idx = ipv4_pos.unwrap();
        let start = if idx == 0 {
            0
        } else {
            spans[idx - 1].end as usize
        };
        let end = spans[idx].end as usize;
        assert_eq!(&input[start..end], "192.168.1.1");
    }

    #[test]
    fn test_tokenize_into_clears_and_matches() {
        let input = "hello 192.168.1.1 world";
        let mut buf: Vec<TokenSpan> = vec![TokenSpan {
            token_type: TokenType::Word,
            end: 1000,
        }];
        tokenize_into(input, &mut buf);
        let expected: Vec<TokenSpan> = tokenize(input).collect();
        assert_eq!(buf, expected);
    }

    #[test]
    fn test_tokenize_into_reuses_buffer() {
        let mut buf: Vec<TokenSpan> = Vec::new();
        tokenize_into("hello world foo bar", &mut buf);
        let cap_after_first = buf.capacity();
        tokenize_into("a b", &mut buf);
        assert!(buf.capacity() >= cap_after_first);
        assert_eq!(buf, tokenize("a b").collect::<Vec<_>>());
    }

    fn token_char() -> impl Strategy<Value = char> {
        prop_oneof![
            prop::char::range('a', 'z'),
            prop::char::range('A', 'Z'),
            prop::char::range('0', '9'),
            prop::sample::select(vec![
                '.', ',', '\'', '_', ':', '-', ' ', '\t', '\n', '\r', '(', ')', '[', ']', '{', '}',
                '<', '>', '!', '?', ';', '"', '/', '\\', '|', '@', '#', '$', '%', '^', '&', '*',
                '+', '=', '~', '`',
            ]),
        ]
    }

    fn random_ascii_string() -> impl Strategy<Value = String> {
        prop::collection::vec(token_char(), 0..64).prop_map(|chars| chars.into_iter().collect())
    }

    proptest! {
        #[test]
        fn prop_lossless_and_contiguous(input in random_ascii_string()) {
            let spans: Vec<TokenSpan> = tokenize(&input).collect();

            let mut reconstructed = String::new();
            let mut cursor = 0usize;
            for span in &spans {
                let end = span.end as usize;
                reconstructed.push_str(&input[cursor..end]);
                cursor = end;
            }
            prop_assert_eq!(&reconstructed, &input);

            if spans.is_empty() {
                prop_assert_eq!(input.len(), 0);
            } else {
                let mut prev = 0u32;
                for span in &spans {
                    prop_assert!(span.end > prev);
                    prev = span.end;
                }
                prop_assert_eq!(spans.last().unwrap().end as usize, input.len());
            }
        }

        #[test]
        fn prop_locality(input in random_ascii_string()) {
            let whole_types: Vec<TokenType> = tokenize(&input).map(|span| span.token_type).collect();

            for &cut in &our_cut_offsets(&input) {
                prop_assert!(input.is_char_boundary(cut));
                let left = &input[..cut];
                let right = &input[cut..];

                let merged_types: Vec<TokenType> = tokenize(left)
                    .chain(tokenize(right))
                    .map(|span| span.token_type)
                    .collect();
                prop_assert_eq!(&merged_types, &whole_types, "locality failed at cut {}", cut);
            }
        }

        #[test]
        fn prop_ascii_cuts_are_unicode_word_boundaries(input in random_ascii_string()) {
            let ours = our_cut_offsets(&input);
            let unicode: std::collections::HashSet<usize> =
                unicode_word_bound_offsets(&input).into_iter().collect();
            for offset in ours {
                prop_assert!(
                    unicode.contains(&offset),
                    "our cut at offset {} is not a Unicode word boundary in {:?}",
                    offset,
                    input
                );
            }
        }

        #[test]
        fn prop_partitioning(input in random_ascii_string()) {
            let spans: Vec<TokenSpan> = tokenize(&input).collect();
            if spans.is_empty() {
                prop_assert_eq!(input.len(), 0);
            } else {
                let mut cursor: u32 = 0;
                for span in &spans {
                    prop_assert!(span.end > cursor, "empty or out-of-order span");
                    cursor = span.end;
                }
                prop_assert_eq!(cursor as usize, input.len());
            }
        }
    }
}
