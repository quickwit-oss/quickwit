// Copyright (C) 2023 Quickwit, Inc.
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

use std::fmt;
use std::fmt::Write;

use serde::{Serialize, Deserialize};

/// Defines whether a term in a query must be present,
/// should be present or must not be present.
#[derive(Debug, Clone, Hash, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum Occur {
    /// For a given document to be considered for scoring,
    /// at least one of the terms with the Should or the Must
    /// Occur constraint must be within the document.
    Should,
    /// Document without the term are excluded from the search.
    Must,
    /// Document that contain the term are excluded from the
    /// search.
    MustNot,
}

impl Occur {
    /// Returns the one-char prefix symbol for this `Occur`.
    /// - `Should` => '?',
    /// - `Must` => '+'
    /// - `Not` => '-'
    fn to_char(self) -> char {
        match self {
            Occur::Should => '?',
            Occur::Must => '+',
            Occur::MustNot => '-',
        }
    }

    /// Compose two occur values.
    pub fn compose(left: Occur, right: Occur) -> Occur {
        match (left, right) {
            (Occur::Should, _) => right,
            (Occur::Must, Occur::MustNot) => Occur::MustNot,
            (Occur::Must, _) => Occur::Must,
            (Occur::MustNot, Occur::MustNot) => Occur::Must,
            (Occur::MustNot, _) => Occur::MustNot,
        }
    }
}

impl fmt::Display for Occur {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_char(self.to_char())
    }
}

#[cfg(test)]
mod test {
    use super::Occur;

    #[test]
    fn test_occur_compose() {
        assert_eq!(Occur::compose(Occur::Should, Occur::Should), Occur::Should);
        assert_eq!(Occur::compose(Occur::Should, Occur::Must), Occur::Must);
        assert_eq!(
            Occur::compose(Occur::Should, Occur::MustNot),
            Occur::MustNot
        );
        assert_eq!(Occur::compose(Occur::Must, Occur::Should), Occur::Must);
        assert_eq!(Occur::compose(Occur::Must, Occur::Must), Occur::Must);
        assert_eq!(Occur::compose(Occur::Must, Occur::MustNot), Occur::MustNot);
        assert_eq!(
            Occur::compose(Occur::MustNot, Occur::Should),
            Occur::MustNot
        );
        assert_eq!(Occur::compose(Occur::MustNot, Occur::Must), Occur::MustNot);
        assert_eq!(Occur::compose(Occur::MustNot, Occur::MustNot), Occur::Must);
    }
}
