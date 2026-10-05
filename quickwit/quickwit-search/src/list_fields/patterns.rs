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

use std::str::FromStr;
use std::sync::Arc;

/// A parsed, cheaply-cloneable set of field-name patterns. An empty set matches every field.
#[derive(Clone)]
pub struct FieldPatterns {
    patterns: Arc<[FieldPattern]>,
}

impl FieldPatterns {
    /// Parses each input string into a [`FieldPattern`]. Returns an error on invalid input
    /// (e.g. more than one wildcard).
    pub fn from_strs(field_pattern_strs: &[String]) -> crate::Result<Self> {
        let patterns: Arc<[FieldPattern]> = field_pattern_strs
            .iter()
            .map(|pattern_str| FieldPattern::from_str(pattern_str))
            .collect::<crate::Result<Vec<_>>>()?
            .into();
        Ok(Self { patterns })
    }

    pub fn is_empty(&self) -> bool {
        self.patterns.is_empty()
    }

    /// Returns true if any of the patterns match the field name.
    pub fn matches_any(&self, field_name: &str) -> bool {
        self.patterns
            .iter()
            .any(|pattern| pattern.matches(field_name))
    }
}

enum FieldPattern {
    Match { field: String },
    Wildcard { prefix: String, suffix: String },
}

impl FromStr for FieldPattern {
    type Err = crate::SearchError;

    fn from_str(field_pattern: &str) -> crate::Result<Self> {
        match field_pattern.find('*') {
            None => Ok(FieldPattern::Match {
                field: field_pattern.to_string(),
            }),
            Some(pos) => {
                let prefix = field_pattern[..pos].to_string();
                let suffix = field_pattern[pos + 1..].to_string();

                if suffix.contains("*") {
                    return Err(crate::SearchError::InvalidArgument(format!(
                        "invalid field pattern `{field_pattern}`: only one wildcard is supported"
                    )));
                }
                Ok(FieldPattern::Wildcard { prefix, suffix })
            }
        }
    }
}

impl FieldPattern {
    fn matches(&self, field_name: &str) -> bool {
        match self {
            FieldPattern::Match { field } => field == field_name,
            FieldPattern::Wildcard { prefix, suffix } => {
                field_name.starts_with(prefix) && field_name.ends_with(suffix)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_pattern() {
        let prefix_pattern = FieldPattern::from_str("toto*").unwrap();
        assert!(!prefix_pattern.matches(""));
        assert!(!prefix_pattern.matches("tot3"));
        assert!(!prefix_pattern.matches("atoto"));
        assert!(prefix_pattern.matches("toto"));
        assert!(prefix_pattern.matches("totowhatever"));

        let suffix_pattern = FieldPattern::from_str("*toto").unwrap();
        assert!(!suffix_pattern.matches(""));
        assert!(!suffix_pattern.matches("3tot"));
        assert!(!suffix_pattern.matches("totoa"));
        assert!(suffix_pattern.matches("toto"));
        assert!(suffix_pattern.matches("whatevertoto"));

        let inner_pattern = FieldPattern::from_str("to*ti").unwrap();
        assert!(!inner_pattern.matches(""));
        assert!(!inner_pattern.matches("tot"));
        assert!(!inner_pattern.matches("totia"));
        assert!(!inner_pattern.matches("atoti"));
        assert!(inner_pattern.matches("toti"));
        assert!(!inner_pattern.matches("tito"));
        assert!(inner_pattern.matches("towhateverti"));

        assert!(FieldPattern::from_str("to**").is_err());
    }

    #[test]
    fn test_field_patterns_empty_matches_nothing_via_explicit_check() {
        let patterns = FieldPatterns::from_strs(&[]).unwrap();
        assert!(patterns.is_empty());
        assert!(!patterns.matches_any("anything"));
    }

    #[test]
    fn test_field_patterns_matches_any() {
        let patterns =
            FieldPatterns::from_strs(&["service.*".to_string(), "exact".to_string()]).unwrap();
        assert!(!patterns.is_empty());
        assert!(patterns.matches_any("service.name"));
        assert!(patterns.matches_any("exact"));
        assert!(!patterns.matches_any("other"));
    }
}
