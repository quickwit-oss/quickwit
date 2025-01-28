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

use quickwit_config::validate_index_id_pattern;
use quickwit_proto::metastore::{MetastoreError, MetastoreResult};
use regex::RegexSet;
use regex_syntax::escape_into;

pub(super) type IndexIdPattern = String;

#[derive(Debug)]
pub(super) struct IndexIdMatcher {
    positive_matcher: RegexSet,
    negative_matcher: RegexSet,
}

impl IndexIdMatcher {
    /// Builds an [`IndexIdMatcher`] from an set of index ID patterns using the following rules:
    /// - If the given pattern does not contain a `*` char, it matches the exact pattern.
    /// - If the given pattern contains one or more `*`, it matches the regex built from a regex
    ///   where `*` is replaced by `.*`. All other regular expression meta characters are escaped.
    pub fn try_from_index_id_patterns(
        index_id_patterns: &[IndexIdPattern],
    ) -> MetastoreResult<Self> {
        let mut positive_patterns: Vec<&str> = Vec::new();
        let mut negative_patterns: Vec<&str> = Vec::new();

        for pattern in index_id_patterns {
            if let Some(negative_pattern) = pattern.strip_prefix('-') {
                negative_patterns.push(negative_pattern);
            } else {
                positive_patterns.push(pattern);
            }
        }
        if positive_patterns.is_empty() {
            let message = "failed to build index ID matcher: at least one positive index ID \
                           pattern must be provided"
                .to_string();
            return Err(MetastoreError::InvalidArgument { message });
        }
        let positive_matcher = build_regex_set(&positive_patterns)?;
        let negative_matcher = build_regex_set(&negative_patterns)?;

        let matcher = IndexIdMatcher {
            positive_matcher,
            negative_matcher,
        };
        Ok(matcher)
    }

    pub fn is_match(&self, index_id: &str) -> bool {
        self.positive_matcher.is_match(index_id) && !self.negative_matcher.is_match(index_id)
    }
}

fn build_regex_set(patterns: &[&str]) -> MetastoreResult<RegexSet> {
    for pattern in patterns {
        if *pattern == "*" {
            let regex_set = RegexSet::new([".*"]).expect("regular expression set should compile");
            return Ok(regex_set);
        }
        validate_index_id_pattern(pattern, false).map_err(|error| {
            let message = format!("failed to build index ID matcher: {error}");
            MetastoreError::InvalidArgument { message }
        })?;
    }
    let regexes = patterns.iter().map(|pattern| build_regex(pattern));

    let regex_set = RegexSet::new(regexes).map_err(|error| {
        let message = format!("failed to build index ID matcher: {error}");
        MetastoreError::InvalidArgument { message }
    })?;
    Ok(regex_set)
}

fn build_regex(pattern: &str) -> String {
    let mut regex = String::new();
    regex.push('^');

    for (idx, part) in pattern.split('*').enumerate() {
        if idx > 0 {
            regex.push_str(".*");
        }
        escape_into(part, &mut regex);
    }
    regex.push('$');
    regex
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_regex() {
        let regex = build_regex("");
        assert_eq!(regex, r"^$");

        let regex = build_regex("*");
        assert_eq!(regex, r"^.*$");

        let regex = build_regex("index-1");
        assert_eq!(regex, r"^index\-1$");

        let regex = build_regex("*-index-*-1");
        assert_eq!(regex, r"^.*\-index\-.*\-1$");

        let regex = build_regex("INDEX.2*-1");
        assert_eq!(regex, r"^INDEX\.2.*\-1$");
    }

    #[test]
    fn test_build_regex_set() {
        let error = build_regex_set(&["_index-1"]).unwrap_err();
        assert!(matches!(error, MetastoreError::InvalidArgument { .. }));

        let regex_set = build_regex_set(&["index-1"]).unwrap();
        assert!(regex_set.is_match("index-1"));
        assert!(!regex_set.is_match("index-2"));

        let regex_set = build_regex_set(&["index-1", "index-2"]).unwrap();
        assert!(regex_set.is_match("index-1"));
        assert!(regex_set.is_match("index-2"));
        assert!(!regex_set.is_match("index-3"));

        let regex_set = build_regex_set(&["index-1*"]).unwrap();
        assert!(regex_set.is_match("index-1"));
        assert!(regex_set.is_match("index-10"));
        assert!(!regex_set.is_match("index-2"));
    }

    #[test]
    fn test_index_id_matcher() {
        let error = IndexIdMatcher::try_from_index_id_patterns(&[]).unwrap_err();
        assert!(matches!(error, MetastoreError::InvalidArgument { .. }));

        let matcher = IndexIdMatcher::try_from_index_id_patterns(&[
            "index-foo*".to_string(),
            "-index-foobar".to_string(),
        ])
        .unwrap();
        assert!(matcher.is_match("index-foo"));
        assert!(matcher.is_match("index-fooo"));
        assert!(!matcher.is_match("index-foobar"));
    }
}
