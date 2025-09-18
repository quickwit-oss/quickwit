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

use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::ops::Range;
use std::path::{Path, PathBuf};

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
    fn key(&self) -> SliceAddressRef<'_>;
}

impl SliceAddressKey for SliceAddress {
    fn key(&self) -> SliceAddressRef<'_> {
        SliceAddressRef {
            path: self.path.as_path(),
            byte_range: self.byte_range.clone(),
        }
    }
}

impl SliceAddressKey for SliceAddressRef<'_> {
    fn key(&self) -> SliceAddressRef<'_> {
        self.clone()
    }
}

impl<'a> Borrow<dyn SliceAddressKey + 'a> for SliceAddress {
    fn borrow(&self) -> &(dyn SliceAddressKey + 'a) {
        self
    }
}
impl PartialEq for dyn SliceAddressKey + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.key().eq(&other.key())
    }
}

impl Eq for dyn SliceAddressKey + '_ {}

impl Hash for dyn SliceAddressKey + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key().hash(state)
    }
}
