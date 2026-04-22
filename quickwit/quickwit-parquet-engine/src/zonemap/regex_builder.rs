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

//! Prefix-preserving regex builder for zonemap generation.
//!
//! Builds a regex that accepts at least all registered values while preserving
//! prefix information. Useful for query-time pruning of Parquet splits.
//!
//! Ported from Go: `logs-event-store/zonemap/regex.go`.

use super::automaton::Automaton;

/// A regex that is a superset of the set of registered values.
#[derive(Debug, Clone)]
pub struct SupersetRegex {
    /// The generated regex pattern.
    pub regex: String,
    /// Whether the regex matches exactly the registered values (no false
    /// positives). False if pruning occurred or nulls were present.
    pub is_also_subset_regex: bool,
}

/// Builds a prefix-preserving superset regex from registered string values.
///
/// The builder maintains a DFA that gets progressively pruned during
/// registration (every `prune_every` values) and at final build time.
pub(crate) struct PrefixPreservingRegexBuilder {
    automaton: Option<Automaton>,
    max_num_transitions: i32,
    prune_every: usize,
    multiplier: f32,
    count: usize,
}

impl PrefixPreservingRegexBuilder {
    /// Create a new builder (uninitialized — call `reset` before use).
    pub(crate) fn new() -> Self {
        PrefixPreservingRegexBuilder {
            automaton: None,
            max_num_transitions: -1,
            prune_every: 0,
            multiplier: 1.0,
            count: 0,
        }
    }

    /// Reset the builder, discarding all registered values.
    ///
    /// `max_num_transitions`: max transitions to keep in the final regex
    /// (`< 0` means no pruning).
    /// `prune_every`: prune every N registrations during building (0 = no
    /// periodic pruning).
    /// `multiplier`: multiplier for periodic pruning transitions (> 1.0 avoids
    /// local minima).
    pub(crate) fn reset(&mut self, max_num_transitions: i32, prune_every: usize, multiplier: f32) {
        let max_depth = if max_num_transitions >= 0 {
            (max_num_transitions as f32 * multiplier * 2.0) as usize
        } else {
            0
        };
        self.automaton = Some(Automaton::new(max_depth));
        self.max_num_transitions = max_num_transitions;
        self.prune_every = prune_every;
        self.multiplier = multiplier;
        self.count = 0;
    }

    /// Register a value so that the subsequently built regex will accept it.
    pub(crate) fn register(&mut self, value: &str) {
        if let Some(ref mut automaton) = self.automaton {
            automaton.add(value);
            self.count += 1;

            if self.prune_every > 0 && self.count.is_multiple_of(self.prune_every) {
                let periodic_limit = (self.max_num_transitions as f32 * self.multiplier) as usize;
                automaton.prune(periodic_limit);
            }
        }
    }

    /// Build and return the superset regex.
    pub(crate) fn build(&mut self) -> SupersetRegex {
        let automaton = self.automaton.as_mut().expect("reset() not called");
        if self.max_num_transitions >= 0 {
            automaton.prune(self.max_num_transitions as usize);
        }
        SupersetRegex {
            regex: automaton.regex(),
            is_also_subset_regex: automaton.is_strict_superset,
        }
    }
}
