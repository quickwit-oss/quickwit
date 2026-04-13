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

//! Deterministic finite automaton for building prefix-preserving superset regexes.
//!
//! The automaton accepts strings via [`Automaton::add`], then can be pruned to
//! bound regex size while preserving prefix information. After pruning, the
//! language accepted is a superset of the original — pruned states accept any
//! suffix (`.+` or `.*`).
//!
//! Ported from Go: `logs-event-store/zonemap/automaton.go`.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Entry in the pruning priority queue.
///
/// Ordered by weight descending, then by sequence number ascending (earlier
/// BFS visit = higher priority when weights tie). This matches Go's heap
/// traversal order, ensuring deterministic pruning.
struct PruneEntry {
    weight: u32,
    seq: u32,
    state: *mut State,
}

impl Eq for PruneEntry {}

impl PartialEq for PruneEntry {
    fn eq(&self, other: &Self) -> bool {
        self.weight == other.weight && self.seq == other.seq
    }
}

impl Ord for PruneEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher weight first, then lower seq first.
        self.weight
            .cmp(&other.weight)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

impl PartialOrd for PruneEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A deterministic finite automaton for zonemap regex generation.
pub(crate) struct Automaton {
    initial: Box<State>,
    /// Maximum depth for state transitions (pruning optimization).
    /// 0 means no depth limit.
    max_depth: usize,
    /// True iff the automaton only accepts strings that have been added.
    /// Becomes false if pruning occurs.
    pub(crate) is_strict_superset: bool,
}

/// A state in the automaton.
struct State {
    /// Transitions to next states. `None` means this state was pruned — it
    /// accepts any suffix (`.+` if non-terminal, `.*` if terminal).
    transitions: Option<Vec<(char, Box<State>)>>,
    terminal: bool,
    /// Weight is used for pruning priority (higher = keep longer).
    weight: u32,
}

impl State {
    fn new() -> Self {
        State {
            transitions: Some(Vec::new()),
            terminal: false,
            weight: 0,
        }
    }

    /// Find the index of a transition for the given character.
    fn find_transition(&self, ch: char) -> Option<usize> {
        if let Some(ref transitions) = self.transitions {
            for (i, (c, _)) in transitions.iter().enumerate() {
                if *c == ch {
                    return Some(i);
                }
            }
        }
        None
    }

    /// Count transitions in this state.
    fn num_transitions(&self) -> usize {
        match self.transitions {
            Some(ref t) => t.len(),
            None => 0,
        }
    }
}

impl Automaton {
    /// Create a new automaton with the given max depth (0 = no limit).
    pub(crate) fn new(max_depth: usize) -> Self {
        Automaton {
            initial: Box::new(State::new()),
            max_depth,
            is_strict_superset: true,
        }
    }

    /// Add a string so the automaton accepts it, in addition to previously
    /// accepted strings. States along the path have their weights incremented.
    pub(crate) fn add(&mut self, value: &str) {
        let mut st = &mut *self.initial;
        let mut depth = 0;

        let mut chars = value.chars();
        loop {
            st.weight += 1;

            let ch = match chars.next() {
                Some(c) => c,
                None => {
                    st.terminal = true;
                    return;
                }
            };
            depth += 1;

            if st.transitions.is_none() {
                // This state was pruned. We cannot make it more specific.
                return;
            }

            let idx = st.find_transition(ch);
            match idx {
                Some(i) => {
                    st = &mut st.transitions.as_mut().unwrap()[i].1;
                }
                None => {
                    let mut new_state = Box::new(State::new());
                    if self.max_depth > 0 && depth >= self.max_depth {
                        new_state.transitions = None;
                        self.is_strict_superset = false;
                    }
                    let transitions = st.transitions.as_mut().unwrap();
                    transitions.push((ch, new_state));
                    let last = transitions.len() - 1;
                    st = &mut transitions[last].1;
                }
            }
        }
    }

    /// Prune the automaton to at most `max_num_transitions` transitions.
    ///
    /// States with the lowest weights are pruned first. After pruning the
    /// automaton still accepts everything it accepted before, but may accept
    /// additional strings (superset language).
    pub(crate) fn prune(&mut self, max_num_transitions: usize) {
        if self.max_depth > 0 && max_num_transitions > self.max_depth {
            panic!(
                "bug: max_num_transitions({}) > max_depth({})",
                max_num_transitions, self.max_depth
            );
        }

        // Max-heap by (weight, insertion_order). Higher weight is popped first;
        // for equal weights, lower sequence number (earlier BFS visit) is popped
        // first, matching Go's heap traversal order.
        //
        // We use raw pointers because we need mutable access to states while
        // they're referenced in the heap. This is safe because:
        // 1. All pointers come from our owned tree
        // 2. We only mutate `transitions` on states we pop (removing from heap)
        // 3. The tree structure is never modified during iteration
        let mut heap: BinaryHeap<PruneEntry> = BinaryHeap::new();
        let mut seq: u32 = 0;
        heap.push(PruneEntry {
            weight: self.initial.weight,
            seq,
            state: &mut *self.initial as *mut State,
        });
        seq += 1;

        let mut num_transitions = 0;

        while let Some(entry) = heap.pop() {
            // SAFETY: pointer is valid and uniquely accessed (popped from heap).
            let state = unsafe { &mut *entry.state };

            let state_transitions = state.num_transitions();
            if num_transitions + state_transitions > max_num_transitions {
                state.transitions = None;
                self.is_strict_superset = false;
                continue;
            }

            num_transitions += state_transitions;

            if let Some(ref mut transitions) = state.transitions {
                // Sort transitions for deterministic pruning order.
                transitions.sort_by_key(|(ch, _)| *ch);
                for (_, child) in transitions.iter_mut() {
                    heap.push(PruneEntry {
                        weight: child.weight,
                        seq,
                        state: &mut **child as *mut State,
                    });
                    seq += 1;
                }
            }
        }
    }

    /// Generate a regex that describes the language this automaton accepts.
    ///
    /// The regex size is asymptotically linearly bounded by the number of
    /// transitions: at most `6 * num_transitions + 4` unicode characters.
    pub(crate) fn regex(&self) -> String {
        let mut sb = String::new();
        sb.push('^');
        write_regex(&self.initial, &mut sb);
        sb.push('$');
        sb
    }
}

/// Generate the regex for a state and its descendants.
fn state_regex(state: &State) -> String {
    let mut sb = String::new();
    write_regex(state, &mut sb);
    sb
}

/// Write the regex for a state into the string builder.
fn write_regex(state: &State, sb: &mut String) {
    if state.transitions.is_none() {
        // Pruned state: accept any suffix.
        sb.push('.');
        if state.terminal {
            sb.push('*');
        } else {
            sb.push('+');
        }
        return;
    }

    let transitions = state.transitions.as_ref().unwrap();
    if transitions.is_empty() && state.terminal {
        return;
    }

    // Sort transitions for deterministic output.
    let mut sorted: Vec<(char, &State)> = transitions.iter().map(|(c, s)| (*c, &**s)).collect();
    sorted.sort_by_key(|(c, _)| *c);

    // Generate regex for each transition's destination.
    let transition_regexes: Vec<String> = sorted.iter().map(|(_, s)| state_regex(s)).collect();

    // Deduplicate transition regexes: group transitions with the same
    // destination regex into character classes.
    let mut clauses: Vec<String> = Vec::new();
    if state.terminal {
        clauses.push(String::new());
    }

    let mut used = vec![false; sorted.len()];
    let mut clause_buf = String::new();
    for i in 0..sorted.len() {
        if used[i] {
            continue;
        }

        let mut same_chars: Vec<char> = vec![sorted[i].0];
        for j in (i + 1)..sorted.len() {
            if !used[j] && transition_regexes[j] == transition_regexes[i] {
                same_chars.push(sorted[j].0);
                used[j] = true;
            }
        }

        clause_buf.clear();
        write_character_class(&mut clause_buf, &same_chars);
        clause_buf.push_str(&transition_regexes[i]);
        clauses.push(clause_buf.clone());
    }

    // ".+" is a common suffix because of pruning.
    write_disjunctive_clauses_factoring_suffix(sb, &clauses, ".+");
}

/// Write characters as a regex character class.
///
/// Single char: `a`, multiple: `[abc]`, ranges collapsed: `[a-d]`.
pub(super) fn write_character_class(sb: &mut String, characters: &[char]) {
    if characters.len() == 1 {
        write_rune_escaping(sb, characters[0]);
    } else if characters.len() > 1 {
        sb.push('[');
        let mut i = 0;
        while i < characters.len() {
            write_rune_escaping_in_character_class(sb, characters[i]);

            // Collapse consecutive character ranges as "a-z".
            let mut j = i;
            while j + 1 < characters.len() && characters[j + 1] as u32 == characters[j] as u32 + 1 {
                j += 1;
            }
            if j > i + 2 {
                sb.push('-');
                write_rune_escaping_in_character_class(sb, characters[j]);
                i = j;
            }

            i += 1;
        }
        sb.push(']');
    }
}

/// Write disjunctive clauses, factoring out a common suffix.
///
/// For example, `["12abc", "34abc", "56"]` with suffix `"abc"` writes
/// `((12|34)abc|56)`.
pub(super) fn write_disjunctive_clauses_factoring_suffix(
    sb: &mut String,
    clauses: &[String],
    suffix: &str,
) {
    let num_with_suffix = clauses.iter().filter(|c| c.ends_with(suffix)).count();

    if num_with_suffix <= 1 {
        write_disjunctive_clauses(sb, clauses, 0);
        return;
    }

    if num_with_suffix == clauses.len() {
        write_disjunctive_clauses(sb, clauses, suffix.len());
        sb.push_str(suffix);
        return;
    }

    // Split clauses into two categories.
    let with_suffix: Vec<&String> = clauses.iter().filter(|c| c.ends_with(suffix)).collect();
    let without_suffix: Vec<&String> = clauses.iter().filter(|c| !c.ends_with(suffix)).collect();

    sb.push('(');
    write_disjunctive_clauses_refs(sb, &with_suffix, suffix.len());
    sb.push_str(suffix);
    sb.push('|');
    write_disjunctive_clauses_refs(sb, &without_suffix, 0);
    sb.push(')');
}

/// Write disjunctive clauses with a suffix of `stripped_suffix_len` stripped.
pub(super) fn write_disjunctive_clauses(
    sb: &mut String,
    clauses: &[String],
    stripped_suffix_len: usize,
) {
    if clauses.len() == 1 {
        let c = &clauses[0];
        sb.push_str(&c[..c.len() - stripped_suffix_len]);
    } else if clauses.len() > 1 {
        sb.push('(');
        let first = &clauses[0];
        sb.push_str(&first[..first.len() - stripped_suffix_len]);
        for c in &clauses[1..] {
            sb.push('|');
            sb.push_str(&c[..c.len() - stripped_suffix_len]);
        }
        sb.push(')');
    }
}

/// Write disjunctive clauses from references.
fn write_disjunctive_clauses_refs(
    sb: &mut String,
    clauses: &[&String],
    stripped_suffix_len: usize,
) {
    if clauses.len() == 1 {
        let c = clauses[0];
        sb.push_str(&c[..c.len() - stripped_suffix_len]);
    } else if clauses.len() > 1 {
        sb.push('(');
        let first = clauses[0];
        sb.push_str(&first[..first.len() - stripped_suffix_len]);
        for c in &clauses[1..] {
            sb.push('|');
            sb.push_str(&c[..c.len() - stripped_suffix_len]);
        }
        sb.push(')');
    }
}

/// Returns true if the character needs escaping in a regex.
fn is_special(ch: char) -> bool {
    matches!(
        ch,
        '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$'
    )
}

/// Returns true if the character needs escaping inside a character class.
fn is_special_in_character_class(ch: char) -> bool {
    matches!(ch, '\\' | '[' | ']' | '^' | '-')
}

/// Write a character, escaping it if necessary.
fn write_rune_escaping(sb: &mut String, ch: char) {
    if is_special(ch) {
        sb.push('\\');
    }
    sb.push(ch);
}

/// Write a character inside a character class, escaping if necessary.
fn write_rune_escaping_in_character_class(sb: &mut String, ch: char) {
    if is_special_in_character_class(ch) {
        sb.push('\\');
    }
    sb.push(ch);
}
