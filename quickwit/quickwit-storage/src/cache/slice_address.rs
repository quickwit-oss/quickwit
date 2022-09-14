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

use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::ops::Range;
use std::path::{Path, PathBuf};

use lru::KeyRef;

#[derive(Hash, Clone, Debug, Eq, PartialEq)]
pub struct SliceAddress {
    pub path: PathBuf,
    pub byte_range: Range<usize>,
}

// ------------------------------------------------------------
// The following struct exists to make it possible to
// fetch a slice from a cache without cloning PathBuf.

// The trick is described in https://github.com/sunshowers-code/borrow-complex-key-example/blob/main/src/lib.rs

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct SliceAddressRef<'a> {
    pub path: &'a Path,
    pub byte_range: Range<usize>,
}

pub(crate) trait SliceAddressKey {
    fn key(&self) -> SliceAddressRef;
}

impl SliceAddressKey for SliceAddress {
    fn key(&self) -> SliceAddressRef {
        SliceAddressRef {
            path: self.path.as_path(),
            byte_range: self.byte_range.clone(),
        }
    }
}

impl<'a> SliceAddressKey for SliceAddressRef<'a> {
    fn key(&self) -> SliceAddressRef {
        self.clone()
    }
}

impl<'a> Borrow<dyn SliceAddressKey + 'a> for KeyRef<SliceAddress> {
    fn borrow(&self) -> &(dyn SliceAddressKey + 'a) {
        let slice_address: &SliceAddress = self.borrow();
        slice_address
    }
}
impl<'a> PartialEq for (dyn SliceAddressKey + 'a) {
    fn eq(&self, other: &Self) -> bool {
        self.key().eq(&other.key())
    }
}

impl<'a> Eq for (dyn SliceAddressKey + 'a) {}

impl<'a> Hash for (dyn SliceAddressKey + 'a) {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key().hash(state)
    }
}
