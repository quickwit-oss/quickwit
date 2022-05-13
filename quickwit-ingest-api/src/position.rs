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

use std::fmt;

use crate::errors::CorruptedKey;

#[derive(Clone, Copy, Default, Ord, PartialOrd, Eq, PartialEq)]
pub struct Position([u8; 8]);

impl TryFrom<&[u8]> for Position {
    type Error = CorruptedKey;

    fn try_from(bytes: &[u8]) -> Result<Self, CorruptedKey> {
        let bytes: [u8; 8] = bytes.try_into().map_err(|_| CorruptedKey(bytes.len()))?;
        Ok(Position(bytes))
    }
}

impl From<u64> for Position {
    fn from(num: u64) -> Self {
        Position(num.to_be_bytes())
    }
}

impl From<Position> for u64 {
    fn from(pos: Position) -> u64 {
        pos.pos_val()
    }
}

impl Position {
    fn pos_val(self) -> u64 {
        u64::from_be_bytes(self.0)
    }

    pub fn inc(&self) -> Position {
        let new_val: u64 = self.pos_val() + 1u64;
        Position::from(new_val)
    }
}

impl fmt::Debug for Position {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("Position").field(&self.pos_val()).finish()
    }
}

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "#{:_>20}", self.pos_val())
    }
}

impl AsRef<[u8]> for Position {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use crate::Position;

    #[test]
    fn test_position_ordering_is_matching_natural_order() {
        for (lesser, greater) in (0..1_000).zip(1..1_001) {
            let lesser_pos = Position::from(lesser);
            let greater_pos = Position::from(greater);
            assert_eq!(lesser_pos.cmp(&greater_pos), Ordering::Less);
        }
    }

    #[test]
    fn test_from_to_u128() {
        let test_n = 20_220_303u64;
        let position = Position::from(test_n);
        let position_val: u64 = position.into();
        assert_eq!(test_n, position_val);
    }

    #[test]
    fn test_position_debug() {
        let test_n = 20_220_303u64;
        let position = Position::from(test_n);
        let position_str = format!("{position:?}");
        assert_eq!(position_str, "Position(20220303)");
    }

    #[test]
    fn test_position_display() {
        let test_n = 20_220_303u64;
        let position_str = Position::from(test_n).to_string();
        assert_eq!(position_str, "#____________20220303");
    }
}
