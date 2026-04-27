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

use std::ops::Deref;

use bytes::Bytes;
use stable_deref_trait::StableDeref;

use crate::OwnedBytes;

/// `StableDeref` wrapper around [`Bytes`] so it can back an [`OwnedBytes`] without an extra copy.
///
/// `Bytes` dereferences to a heap-allocated slice whose address does not change when the `Bytes`
/// itself is moved, which is exactly the contract `StableDeref` requires.
pub(crate) struct StableDerefBytes(pub Bytes);

impl Deref for StableDerefBytes {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        &self.0
    }
}

// SAFETY: `Bytes` stores its payload behind a stable heap pointer; moving the `Bytes` (and thus
// the `StableDerefBytes`) does not invalidate the slice returned by `deref`.
unsafe impl StableDeref for StableDerefBytes {}

/// Wraps a [`Bytes`] into an [`OwnedBytes`] without copying its contents.
#[inline]
pub(crate) fn into_owned_bytes(bytes: Bytes) -> OwnedBytes {
    OwnedBytes::new(StableDerefBytes(bytes))
}
