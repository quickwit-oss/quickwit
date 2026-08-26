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

use std::mem::size_of;
use std::path::PathBuf;

/// Total number of bytes a value occupies in memory.
///
/// This is used by [`MemorySizedCache`](crate::MemorySizedCache) to charge cache keys against the
/// configured capacity. Counting `size_of::<Self>()` is deliberate: a key stored inside a cache
/// lives in a heap-allocated node, so its inline bytes are resident too.
pub trait MemUsage {
    /// `size_of::<Self>()` plus every byte this value transitively owns.
    ///
    /// Hidden contract: an implementation starts from `size_of::<Self>()` and adds only the
    /// *owned* bytes of each field, via [`owned_mem_usage`]. Adding a field's full `mem_usage()`
    /// would double-count that field's inline bytes, which `size_of::<Self>()` already covers.
    fn mem_usage(&self) -> usize;
}

/// Number of bytes `value` owns beyond its own inline footprint.
///
/// This is the building block for implementing [`MemUsage`] on a struct: sum this over the fields
/// that own heap memory and add `size_of::<Self>()`.
pub fn owned_mem_usage<T: MemUsage>(value: &T) -> usize {
    let mem_usage = value.mem_usage();
    let inline_size = size_of::<T>();
    debug_assert!(
        mem_usage >= inline_size,
        "`MemUsage` impl for `{}` returned {mem_usage}, which is below its inline size of \
         {inline_size}. An impl must start from `size_of::<Self>()`.",
        std::any::type_name::<T>(),
    );
    mem_usage.saturating_sub(inline_size)
}

impl MemUsage for String {
    fn mem_usage(&self) -> usize {
        size_of::<Self>() + self.capacity()
    }
}

impl MemUsage for PathBuf {
    fn mem_usage(&self) -> usize {
        size_of::<Self>() + self.capacity()
    }
}

/// A borrow owns nothing: only the pointer pair is accounted for.
impl MemUsage for &str {
    fn mem_usage(&self) -> usize {
        size_of::<Self>()
    }
}

impl<A: MemUsage, B: MemUsage> MemUsage for (A, B) {
    fn mem_usage(&self) -> usize {
        size_of::<Self>() + owned_mem_usage(&self.0) + owned_mem_usage(&self.1)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{MemUsage, owned_mem_usage};

    #[test]
    fn test_string_mem_usage() {
        assert_eq!(String::new().mem_usage(), size_of::<String>());
        assert_eq!("hello".to_string().mem_usage(), size_of::<String>() + 5);
    }

    #[test]
    fn test_string_mem_usage_counts_spare_capacity() {
        let mut string = String::with_capacity(100);
        string.push_str("hello");
        // We charge the allocation, not the length.
        assert_eq!(string.mem_usage(), size_of::<String>() + 100);
    }

    #[test]
    fn test_path_buf_mem_usage() {
        let path_buf = PathBuf::from("/tmp/split.split");
        assert_eq!(
            path_buf.mem_usage(),
            size_of::<PathBuf>() + path_buf.capacity()
        );
    }

    #[test]
    fn test_str_ref_mem_usage() {
        // A borrow owns nothing, however long the pointee is.
        assert_eq!("hello".mem_usage(), size_of::<&str>());
        assert_eq!("hello world, at length".mem_usage(), size_of::<&str>());
    }

    #[test]
    fn test_tuple_mem_usage() {
        let key = ("abc".to_string(), "de".to_string());
        assert_eq!(key.mem_usage(), size_of::<(String, String)>() + 3 + 2);
    }

    #[test]
    fn test_owned_mem_usage_strips_inline_size() {
        assert_eq!(owned_mem_usage(&"hello".to_string()), 5);
        assert_eq!(owned_mem_usage(&String::new()), 0);
        assert_eq!(owned_mem_usage(&"borrowed"), 0);
        let key = ("abc".to_string(), "de".to_string());
        assert_eq!(owned_mem_usage(&key), 5);
    }
}
