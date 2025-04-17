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

use rand::Rng;
use rand::distributions::Alphanumeric;

/// Appends a random suffix composed of a hyphen and five random alphanumeric characters.
pub fn append_random_suffix(string: &str) -> String {
    let rng = rand::thread_rng();
    let mut randomized_string = String::with_capacity(string.len() + 6);
    randomized_string.push_str(string);
    randomized_string.push('-');

    for random_byte in rng.sample_iter(&Alphanumeric).take(5) {
        randomized_string.push(char::from(random_byte));
    }
    randomized_string
}

#[cfg(test)]
mod tests {
    use super::append_random_suffix;

    #[test]
    fn test_append_random_suffix() -> anyhow::Result<()> {
        let randomized = append_random_suffix("");
        let mut chars = randomized.chars();
        assert_eq!(chars.next(), Some('-'));
        assert_eq!(chars.clone().count(), 5);
        assert!(chars.all(|ch| ch.is_ascii_alphanumeric()));
        Ok(())
    }
}
