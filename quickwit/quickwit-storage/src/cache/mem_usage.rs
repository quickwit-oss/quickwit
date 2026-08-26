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

use std::path::PathBuf;

/// Number of bytes a value occupies in memory.
///
/// This is used by [`MemorySizedCache`](crate::MemorySizedCache) to charge cache keys against the
/// configured capacity.
pub trait MemUsage {
    /// Bytes this value owns beyond its own inline footprint.
    ///
    /// An implementation sums this over the fields that own heap memory. Because it excludes
    /// `size_of::<Self>()`, field contributions compose without double-counting inline bytes.
    fn heap_mem_usage(&self) -> usize;

    /// `size_of::<Self>()` plus every byte this value transitively owns.
    ///
    /// Counting `size_of::<Self>()` is deliberate: a key stored inside a cache lives in a
    /// heap-allocated node, so its inline bytes are resident too.
    fn mem_usage(&self) -> usize
    where Self: Sized {
        size_of::<Self>() + self.heap_mem_usage()
    }
}

impl MemUsage for String {
    fn heap_mem_usage(&self) -> usize {
        self.capacity()
    }
}

impl MemUsage for PathBuf {
    fn heap_mem_usage(&self) -> usize {
        self.capacity()
    }
}

/// A borrow owns nothing, however large the pointee is.
impl<T: ?Sized> MemUsage for &T {
    fn heap_mem_usage(&self) -> usize {
        0
    }
}

impl<A: MemUsage, B: MemUsage> MemUsage for (A, B) {
    fn heap_mem_usage(&self) -> usize {
        self.0.heap_mem_usage() + self.1.heap_mem_usage()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::MemUsage;

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
        assert_eq!(key.heap_mem_usage(), 5);
    }

    #[test]
    fn test_heap_mem_usage_excludes_inline_size() {
        assert_eq!("hello".to_string().heap_mem_usage(), 5);
        assert_eq!(String::new().heap_mem_usage(), 0);
        assert_eq!(MemUsage::heap_mem_usage(&"borrowed"), 0);
    }

    #[test]
    fn test_nested_struct_mem_usage_does_not_double_count() {
        struct Key {
            name: String,
            path: PathBuf,
        }
        impl MemUsage for Key {
            fn heap_mem_usage(&self) -> usize {
                self.name.heap_mem_usage() + self.path.heap_mem_usage()
            }
        }
        let key = Key {
            name: "hello".to_string(),
            path: PathBuf::from("/tmp/split.split"),
        };
        assert_eq!(key.mem_usage(), size_of::<Key>() + 5 + key.path.capacity());
    }
}
