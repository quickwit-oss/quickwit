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

use rand::distributions::Alphanumeric;
use rand::prelude::*;

const ADJECTIVES: &[&str] = &[
    "aged",
    "ancient",
    "autumn",
    "billowing",
    "bitter",
    "black",
    "blue",
    "bold",
    "broken",
    "cold",
    "cool",
    "crimson",
    "damp",
    "dark",
    "dawn",
    "delicate",
    "divine",
    "dry",
    "empty",
    "falling",
    "floral",
    "fragrant",
    "frosty",
    "green",
    "hidden",
    "holy",
    "icy",
    "late",
    "lingering",
    "little",
    "lively",
    "long",
    "misty",
    "morning",
    "muddy",
    "nameless",
    "old",
    "patient",
    "polished",
    "proud",
    "purple",
    "quiet",
    "red",
    "restless",
    "rough",
    "shy",
    "silent",
    "small",
    "snowy",
    "solitary",
    "sparkling",
    "spring",
    "still",
    "summer",
    "throbbing",
    "twilight",
    "wandering",
    "weathered",
    "white",
    "wild",
    "winter",
    "wispy",
    "withered",
    "young",
];

/// Returns a randomly generated id
pub fn new_coolid(name: &str) -> String {
    let mut rng = rand::thread_rng();
    let adjective = ADJECTIVES[rng.gen_range(0..ADJECTIVES.len())];
    let slug: String = rng
        .sample_iter(&Alphanumeric)
        .take(4)
        .map(char::from)
        .collect();
    format!("{name}-{adjective}-{slug}")
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::new_coolid;

    #[test]
    fn test_coolid() {
        let cool_ids: HashSet<String> = std::iter::repeat_with(|| new_coolid("hello"))
            .take(100)
            .collect();
        assert_eq!(cool_ids.len(), 100);
    }
}
