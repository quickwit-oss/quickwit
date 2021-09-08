// Copyright (C) 2021 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

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
    format!("{}-{}-{}", adjective, name, slug)
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
