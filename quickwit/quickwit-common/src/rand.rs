// Copyright (C) 2022 Quickwit, Inc.
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
use rand::Rng;

/// Appends a random suffix composed of a hyphen and five random alphanumeric characters.
pub fn append_random_suffix(string: &str) -> String {
    let rng = rand::thread_rng();
    let slug: String = rng
        .sample_iter(&Alphanumeric)
        .take(5)
        .map(char::from)
        .collect();
    format!("{}-{}", string, slug)
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
