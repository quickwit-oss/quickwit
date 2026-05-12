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

/// Checks whether `query` search matches a `text` on a default field like `message`.
///
/// '*' is used as a wildcard character.
pub fn matches(query: &str, text: &str) -> bool {
    // Checks whether all tokens in `query` each match any token in `text`.
    let query_tokens = tokenize(query);
    let text_tokens = tokenize(text);

    for qtoken in query_tokens {
        if qtoken.contains("*") {
            let re = vrl::datadog_filter::regex::wildcard_regex(qtoken);
            if !text_tokens.iter().any(|t| re.is_match(t)) {
                return false;
            }
        } else if !text_tokens.contains(&qtoken) {
            return false;
        }
    }
    true
}

fn tokenize(input: &str) -> Vec<&str> {
    let mut tokens = Vec::new();
    let mut token_start: Option<usize> = None;

    for (i, c) in input.char_indices() {
        // Alphanumeric, underscore, and dot are in-token characters.
        // Keep * for wildcards
        if c.is_alphanumeric() || c == '_' || c == '.' || c == '*' {
            // Mark the start of a token if we're not already tracking one.
            if token_start.is_none() {
                token_start = Some(i);
            }
        } else if let Some(start) = token_start {
            // We hit a boundary (non-token char), so extract and trim the token.
            let trimmed = trim_dot_edges(input, start, i);
            if !trimmed.is_empty() {
                tokens.push(trimmed);
            }
            token_start = None;
        }
    }

    // If we ended in the middle of a token, trim and push it.
    if let Some(start) = token_start {
        let trimmed = trim_dot_edges(input, start, input.len());
        if !trimmed.is_empty() {
            tokens.push(trimmed);
        }
    }

    tokens
}

/// Removes leading and trailing '.' from the text slice
/// TODO: Check if we should also remove leading/trailing '-' characters
fn trim_dot_edges(input: &str, mut start: usize, mut end: usize) -> &str {
    // Skip leading dots.
    while start < end && input.as_bytes()[start] == b'.' {
        start += 1;
    }

    // Skip trailing dots.
    while end > start && input.as_bytes()[end - 1] == b'.' {
        end -= 1;
    }

    &input[start..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_string() {
        let input = "";
        let tokens = tokenize(input);
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_only_non_alphanumerics() {
        let input = ",.! ";
        let tokens = tokenize(input);
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_simple_sentence() {
        let input = "Hello, world! 123_test.";
        let tokens = tokenize(input);
        // Underscore is allowed, and the trailing dot is not included.
        assert_eq!(tokens, vec!["Hello", "world", "123_test"]);
    }

    #[test]
    fn test_single_token() {
        let input = "abc123";
        let tokens = tokenize(input);
        assert_eq!(tokens, vec!["abc123"]);
    }

    #[test]
    fn test_tokenization_wildcard_mix() {
        let input = "Setting Hand*...";
        let tokens = tokenize(input);
        assert_eq!(tokens, vec!["Setting", "Hand*"]);
    }

    #[test]
    fn test_multiple_tokens_with_symbols() {
        let input = "a1!b2?c3";
        let tokens = tokenize(input);
        assert_eq!(tokens, vec!["a1", "b2", "c3"]);
    }

    #[test]
    fn test_multiple_tokens_with_symbols2() {
        let input = "Hello... world. .abc. 127.0.0.1";
        let tokens = tokenize(input);
        // "Hello..." -> "Hello"
        // "world."   -> "world"
        // ".abc."    -> "abc"
        // "127.0.0.1" -> stays intact
        assert_eq!(tokens, vec!["Hello", "world", "abc", "127.0.0.1"]);
    }
    #[test]
    fn test_tokenize_ip() {
        let input = "127.0.0.1";
        let tokens = tokenize(input);
        assert_eq!(tokens, vec!["127.0.0.1"]);
    }

    #[test]
    fn test_default_field_matching() {
        let text =
            "Setting Handles in set_event_mentions for event_id:8008795072438008673, fetching.";

        assert!(!matches("Setting Handle", text));

        // Wildcards in tokens are allowed
        assert!(matches("Setting Hand*", text));

        // `_` is not filtered => No hit
        assert!(!matches("Setting Handles ___", text));

        // Some punctuation like `@`, `^`, `%`, `:` is ignored
        let filtered_chars = vec!["@", "^", "%", ":"];
        for c in filtered_chars {
            let query = format!("Setting Handles {c}");
            assert!(matches(&query, text), "Failed for: {}", &query);
        }

        // "Weird stuff, * ends the token kind of..."
        assert!(matches("Setting Hand*...", text));

        // Check that `:` and `.` can sometimes match interchangeably if your logic allows it.
        assert!(matches("\"event_id:8008795072438008673\"", text));
        // TODO: Below should match but doesn't
        //assert!(matches("\"event_id.8008795072438008673\"", text));
    }

    #[test]
    fn test_default_field_ip_addr() {
        let text = "127.0.0.1";

        assert!(matches("127.0.0.1", text));
        // If your logic sees double dot as invalid, it fails.
        assert!(!matches("127.0.0..1", text));
        // If `:` does not match `.`, this should fail.
        assert!(!matches("127:0:0:1", text));
    }

    #[test]
    fn test_default_field_matching_with_dots() {
        let text = "[RequestThrottler:o.a.z.s.q.QuorumZooKeeperServer]";

        assert!(matches(
            "\"[RequestThrottler:o.a.z.s.q.QuorumZooKeeperServer]\"",
            text
        ));

        assert!(matches(
            "RequestThrottler:o.a.z.s.q.QuorumZooKeeperServer",
            text
        ));

        assert!(!matches(
            "\"[RequestThrottler:o.a.z.s.q.QuorumZooKeeperServe]\"",
            text
        ));

        // The entire token is "RequestThrottler:o.a.z.s.q.QuorumZooKeeperServer", so partial
        // tokens won't match unless wildcards are used or partial matching is allowed.
        //assert!(!matches("RequestThrottler", text)); TODO: Behavior of `:` is unclear
        //assert!(!matches("o.a.z.s.q.QuorumZooKeeperServer", text));
        assert!(!matches("o.a.z.s.q", text));

        // Wildcard tests
        assert!(matches("*o.a.z.s.q*", text));
        assert!(matches("Request*", text));
        assert!(matches("*QuorumZooKeeperServer", text));
    }
}
