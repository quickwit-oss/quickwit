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
use std::sync::{LazyLock, Mutex};

use heck::ToSnakeCase;

/// Cache of task metrics labels, keyed by the name they were derived from.
///
/// Labels are leaked so they can be returned as `&'static str`. Keys are static strings (type names
/// or literals), so the number of entries, and therefore the leaked memory, is bounded by the
/// number of distinct actor and source names in the binary.
static TASK_METRICS_LABELS: LazyLock<Mutex<HashMap<&'static str, &'static str>>> =
    LazyLock::new(Default::default);

/// Strips the module path from every path in a type name, including in generic arguments.
///
/// For instance, `quickwit_actors::Supervisor<quickwit_indexing::IndexingPipeline>` becomes
/// `Supervisor<IndexingPipeline>`.
pub fn short_type_name(type_name: &str) -> String {
    let mut short_name = String::with_capacity(type_name.len());
    // Start, in `short_name`, of the path segment being written.
    let mut segment_start = 0;
    let mut chars = type_name.chars().peekable();

    while let Some(character) = chars.next() {
        if character == ':' && chars.peek() == Some(&':') {
            chars.next();
            // The segment written so far is a module: drop it.
            short_name.truncate(segment_start);
            continue;
        }
        short_name.push(character);

        if is_type_separator(character) {
            segment_start = short_name.len();
        }
    }
    short_name
}

/// Characters separating paths in a type name. Other characters, including the braces of
/// `{{closure}}` path segments, belong to path segments.
fn is_type_separator(character: char) -> bool {
    matches!(
        character,
        '<' | '>' | ',' | ' ' | '(' | ')' | '[' | ']' | '&' | ';' | '*'
    )
}

/// Converts a type name or an actor name into a snake_case task metrics label.
///
/// For instance, `quickwit_indexing::actors::Uploader` becomes `uploader` and `MergePackager`
/// becomes `merge_packager`.
pub fn to_task_metrics_label(name: &'static str) -> &'static str {
    let mut labels = TASK_METRICS_LABELS
        .lock()
        .expect("task metrics labels lock should not be poisoned");
    labels
        .entry(name)
        .or_insert_with(|| Box::leak(short_type_name(name).to_snake_case().into_boxed_str()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_short_type_name() {
        assert_eq!(short_type_name("Uploader"), "Uploader");
        assert_eq!(
            short_type_name("quickwit_indexing::actors::uploader::Uploader"),
            "Uploader"
        );
        assert_eq!(
            short_type_name(
                "quickwit_actors::supervisor::Supervisor<quickwit_indexing::IndexingPipeline>"
            ),
            "Supervisor<IndexingPipeline>"
        );
        assert_eq!(
            short_type_name("a::Pair<b::First, (c::Second, &d::Third)>"),
            "Pair<First, (Second, &Third)>"
        );
        assert_eq!(
            short_type_name("crate::tests::my_test::{{closure}}::LocalActor"),
            "LocalActor"
        );
    }

    #[test]
    fn test_to_task_metrics_label() {
        assert_eq!(
            to_task_metrics_label("quickwit_indexing::actors::doc_processor::DocProcessor"),
            "doc_processor"
        );
        assert_eq!(to_task_metrics_label("MergePackager"), "merge_packager");
        assert_eq!(
            to_task_metrics_label("quickwit_actors::Supervisor<quickwit_indexing::Indexer>"),
            "supervisor_indexer"
        );
        // Cached: the same name always returns the same leaked string.
        assert!(std::ptr::eq(
            to_task_metrics_label("MergePackager"),
            to_task_metrics_label("MergePackager")
        ));
    }
}
