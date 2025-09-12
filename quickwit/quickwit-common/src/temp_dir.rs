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

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;
use tokio::fs;

use crate::ignore_error_kind;

const MAX_LENGTH: usize = 255;

const SEPARATOR: char = '%';

const NUM_RAND_CHARS: usize = 6;

/// Creates the specified directory. If the directory already exists, deletes its contents.
pub async fn create_or_purge_directory(path: &Path) -> io::Result<PathBuf> {
    // Delete if exists and recreate scratch directory.
    ignore_error_kind!(io::ErrorKind::NotFound, fs::remove_dir_all(path).await)?;
    fs::create_dir_all(path).await?;
    Ok(path.to_path_buf())
}

/// A temporary directory. This directory is deleted when the object is dropped.
#[derive(Debug, Clone)]
pub struct TempDirectory {
    inner: Arc<TempDir>,
    _parent: Option<Arc<TempDir>>,
}

impl TempDirectory {
    /// A path where the temporary directory is pointing to.
    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    /// Creates a new temporary directory with the current temporary directory.
    /// The new directory keeps a pointer to the parent directory to perevent it
    /// from premature deletion. The directory is deleted when the object is dropped.
    pub fn named_temp_child(&self, prefix: &str) -> io::Result<TempDirectory> {
        Ok(TempDirectory {
            inner: Arc::new(
                tempfile::Builder::new()
                    .prefix(prefix)
                    .tempdir_in(self.path())?,
            ),
            _parent: Some(self.inner.clone()),
        })
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test() -> Self {
        Builder::default().tempdir().unwrap()
    }
}

/// A temporary directory builder.
#[derive(Debug)]
pub struct Builder<'a> {
    parts: Vec<&'a str>,
    max_length: usize,
    separator: char,
    num_rand_chars: usize,
}

impl Default for Builder<'_> {
    fn default() -> Self {
        Self {
            parts: Default::default(),
            max_length: MAX_LENGTH,
            separator: SEPARATOR,
            num_rand_chars: NUM_RAND_CHARS,
        }
    }
}

impl<'a> Builder<'a> {
    /// Specifies the number of random bytes to add at the end of the directory name. Default is 6.
    pub fn rand_bytes(&mut self, rand: usize) -> &mut Self {
        self.num_rand_chars = rand;
        self
    }

    /// Specifies the maximum length of the directory name in characters. Default is 255 characters.
    pub fn max_length(&mut self, max_length: usize) -> &mut Self {
        self.max_length = max_length;
        self
    }

    /// Adds a prefix to the directory name.
    pub fn join(&mut self, name: &'a str) -> &mut Self {
        if !name.is_empty() {
            self.parts.push(name.as_ref());
        }
        self
    }

    fn push_str(buffer: &mut String, addition: &'a str, size: usize) -> usize {
        let len = addition.len();
        if len <= size {
            buffer.push_str(addition);
            return len;
        } else if size < 3 {
            buffer.push_str(&addition[0..size]);
        } else {
            let half = size - size / 2;
            buffer.push_str(&addition[0..half - 1]);
            buffer.push_str("..");
            buffer.push_str(&addition[addition.len() - (size - half) + 1..]);
        }
        size
    }

    /// Constructs the prefix from the parts specified by the join function.
    /// If parts are small enough they will be simply concatenated with the
    /// separator character in between. If parts are too large they will
    /// truncated by replacing the middle of each part with "..". The resulting
    /// string will be at most max_length characters long.
    fn prefix(&self) -> io::Result<String> {
        if self.parts.is_empty() {
            return Ok(String::new());
        }
        let separator_count = if self.num_rand_chars > 0 {
            self.parts.len()
        } else {
            self.parts.len() - 1
        };
        // We want to preserve at least one letter from each part with separators.
        if self.max_length < self.parts.len() + separator_count + self.num_rand_chars {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the filename limit is too small",
            ));
        }
        // Calculate how many characters from the parts we can use in the final string.
        let len_without_separators = self.max_length - separator_count - self.num_rand_chars;
        // Calculate how many characters per part can we use.
        let average_len = len_without_separators / self.parts.len();
        // Account for the average length may not be a whole number.
        let mut leftovers = len_without_separators % self.parts.len();
        // We will have some long parts and some short parts. The short parts (part shorter
        // than average can "donate" their space to the large parts. That will allows us to
        // use all available space. In this loop we are counting how many characters large
        // parts can use in addition to the average.
        for part in &self.parts {
            if part.len() <= average_len {
                // Adjust the available length from the parts that are shorter
                leftovers += average_len - part.len();
            }
        }
        // Build the final string by cancatenating the parts while cutting the to the desired
        // length.
        let mut buf = String::new();
        for (i, part) in self.parts.iter().enumerate() {
            if part.len() <= average_len {
                // If the part is shorter than the average - we just add it
                Self::push_str(&mut buf, part, average_len);
            } else {
                // If the part is longer than the average - we can cut it down to average_len +
                // leftovers
                let pushed = Self::push_str(&mut buf, part, average_len + leftovers) - average_len;
                // We now need to adjust leftovers by the number of additional characters the we
                // pushed above average_len
                leftovers -= pushed;
            }
            // The last separator is only added if there are random bytes at the end
            if i < self.parts.len() - 1 || self.num_rand_chars > 0 {
                buf.push(self.separator)
            }
        }
        Ok(buf)
    }

    /// Creates a temporary directory in the temp directory of operation system
    pub fn tempdir(&self) -> io::Result<TempDirectory> {
        Ok(TempDirectory {
            inner: Arc::new(
                tempfile::Builder::new()
                    .rand_bytes(self.num_rand_chars)
                    .prefix(&self.prefix()?)
                    .tempdir()?,
            ),
            _parent: None,
        })
    }

    /// Creates a temporary directory in the specified directory
    pub fn tempdir_in<P: AsRef<Path>>(&self, dir: P) -> io::Result<TempDirectory> {
        Ok(TempDirectory {
            inner: Arc::new(
                tempfile::Builder::new()
                    .rand_bytes(self.num_rand_chars)
                    .prefix(&self.prefix()?)
                    .tempdir_in(dir)?,
            ),
            _parent: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::cmp;

    use rand::Rng;

    use super::*;

    #[test]
    fn test_push_str() {
        assert_truncate("abcdef", 100, "abcdef", 6);
        assert_truncate("abcdef", 6, "abcdef", 6);
        assert_truncate("abcdefghijklmnopqrstuvwxyz", 5, "ab..z", 5);
        assert_truncate("abcdefghijklmnopqrstuvwxyz", 4, "a..z", 4);
        assert_truncate("abcdefghijklmnopqrstuvwxyz", 3, "a..", 3);
        assert_truncate("abcdefghijklmnopqrstuvwxyz", 2, "ab", 2);
        assert_truncate("abcdefghijklmnopqrstuvwxyz", 1, "a", 1);
        assert_truncate("abcde", 10, "abcde", 5);
        assert_truncate("abcde", 5, "abcde", 5);
        assert_truncate("abcde", 4, "a..e", 4);
        assert_truncate("abcde", 3, "a..", 3);
        assert_truncate("abcde", 2, "ab", 2);
        assert_truncate("abcde", 1, "a", 1);
    }

    fn assert_truncate(addition: &str, size: usize, expected_addition: &str, expected_size: usize) {
        let mut buf = String::new();
        let size = Builder::push_str(&mut buf, addition, size);
        assert_eq!(expected_addition, buf);
        assert_eq!(expected_size, size);
    }

    #[test]
    fn test_random_failures() {
        assert_prefix(
            vec!["AAAAAAAAAA", "AA", "AAAAAAA", "AAAA", "AAAA", "AAAAAAAAA"],
            35,
            "AAAAAAAAAA%AA%AA..A%AAAA%AAAA%A..A%",
        );
        assert_prefix(
            vec![
                "AAAAAA",
                "AAAAAAAAA",
                "AAAAAA",
                "AAA",
                "A",
                "AAAAAAA",
                "AAAAAAAAAA",
                "AAAAA",
            ],
            55,
            "AAAAAA%AAAAAAAAA%AAAAAA%AAA%A%AAAAAAA%AAAAAAAAAA%AAAAA%",
        );
        assert_prefix(
            vec!["AAAAAAAAA", "", "AAAAAAA", "AAAAAAA"],
            25,
            "AAA..AAA%AAAAAAA%AAAAAAA%",
        );
    }

    #[test]
    fn test_prefix() {
        assert_prefix(vec!["0", "abcde", "uvwxyz"], 15, "0%abcde%uvwxyz%");

        assert_prefix(vec!["a", "b"], 100, "a%b%");
        assert_prefix(vec!["abcde", "uvwxyz"], 100, "abcde%uvwxyz%");
        assert_prefix(vec!["abcde", "uvwxyz"], 13, "abcde%uvwxyz%");
        assert_prefix(vec!["abcde", "uvwxyz"], 12, "abcde%uv..z%");
        assert_prefix(vec!["abcde", "uvwxyz"], 11, "abcde%u..z%");
        assert_prefix(vec!["abcde", "uvwxyz"], 10, "a..e%u..z%");
        assert_prefix(vec!["abcde", "uvwxyz"], 9, "a..e%u..%");
        assert_prefix(vec!["abcde", "uvwxyz"], 8, "a..%u..%");
        assert_prefix(vec!["abcde", "uvwxyz"], 7, "a..%uv%");
        assert_prefix(vec!["abcde", "uvwxyz"], 6, "ab%uv%");
        assert_prefix(vec!["abcde", "uvwxyz"], 5, "ab%u%");
        assert_prefix(vec!["abcde", "uvwxyz"], 4, "a%u%");
        assert_prefix_err(
            "the filename limit is too small",
            vec!["abcde", "uvwxyz"],
            3,
        );

        assert_prefix(vec!["0", "abcde", "uvwxyz"], 15, "0%abcde%uvwxyz%");
        assert_prefix(vec!["0", "abcde", "uvwxyz"], 14, "0%abcde%uv..z%");
        assert_prefix(vec!["0", "abcde", "uvwxyz"], 13, "0%abcde%u..z%");
        assert_prefix(vec!["0", "abcde", "uvwxyz"], 12, "0%abcde%u..%");
        assert_prefix(vec!["0", "abcde", "uvwxyz"], 11, "0%abcde%uv%");
        assert_prefix(vec!["0", "abcde", "uvwxyz"], 10, "0%a..e%uv%");
        assert_prefix(vec!["0", "abcde", "uvwxyz"], 9, "0%a..%uv%");
        assert_prefix(vec!["0", "abcde", "uvwxyz"], 8, "0%a..%u%");
        assert_prefix(vec!["0", "abcde", "uvwxyz"], 7, "0%ab%u%");
        assert_prefix(vec!["0", "abcde", "uvwxyz"], 6, "0%a%u%");
        assert_prefix_err(
            "the filename limit is too small",
            vec!["0", "abcde", "uvwxyz"],
            5,
        );
    }

    fn assert_prefix(parts: Vec<&str>, size: usize, expected_path: &str) {
        let mut builder = Builder::default();
        builder.rand_bytes(5);
        builder.max_length(size + 5); // Size of random suffix
        for part in parts.iter() {
            builder.join(part);
        }
        let prefix = builder.prefix().unwrap();
        assert_eq!(expected_path, prefix, "parts: {parts:?} len: {size:?}");
    }

    fn assert_prefix_err(expected_err: &str, parts: Vec<&str>, size: usize) {
        let mut builder = Builder::default();
        builder.rand_bytes(5);
        builder.max_length(size + 5); // Size of random suffix
        for part in parts.iter() {
            builder.join(part);
        }
        let error = builder.prefix().unwrap_err();
        assert_eq!(expected_err, error.to_string());
    }

    #[test]
    fn test_prefix_random() {
        let mut rng = rand::thread_rng();
        let template = "A".repeat(100);
        for _ in 0..10000 {
            let rand_bytes = rng.r#gen::<usize>() % 4;
            let parts_num = rng.r#gen::<usize>() % 10;
            let mut builder = Builder::default();
            builder.rand_bytes(rand_bytes);
            let mut max_size = 0;
            for _ in 0..parts_num {
                let size = 1 + rand::random::<usize>() % 10;
                builder.join(&template[0..size]);
                max_size += size + 1;
            }
            let separator_count = if rand_bytes > 0 {
                parts_num
            } else {
                // no separator at the end
                if max_size > 0 {
                    max_size -= 1;
                    parts_num - 1
                } else {
                    parts_num
                }
            };
            let limit_threshold = parts_num + separator_count + rand_bytes;
            if parts_num > 0 && rng.r#gen::<bool>() {
                builder.max_length(rand::random::<usize>() % limit_threshold);
                assert_eq!(
                    "the filename limit is too small",
                    builder.prefix().unwrap_err().to_string()
                );
            } else {
                let len = limit_threshold + rand::random::<usize>() % 100;
                builder.max_length(len);
                let builder_debug = format!("{builder:?}, len {len}");
                let builder_prefix = builder.prefix().unwrap();
                assert_eq!(
                    builder_prefix.len(),
                    cmp::min(len - rand_bytes, max_size),
                    "{builder_debug} -> {builder_prefix}"
                );
            }
        }
    }

    #[test]
    fn test_directory_creation_and_removal() {
        let directory = Builder::default()
            .join("foo")
            .join("bar")
            .join("baz")
            .rand_bytes(0)
            .tempdir()
            .unwrap();
        assert_eq!(directory.path().file_name().unwrap(), "foo%bar%baz");
        let path = directory.path().to_path_buf();
        assert!(path.try_exists().unwrap());
        drop(directory);
        assert!(!path.try_exists().unwrap());
    }

    #[test]
    fn test_directory_creation_and_removal_with_random_bytes() {
        let directory = Builder::default()
            .join("foo")
            .join("bar")
            .join("baz")
            .rand_bytes(4)
            .tempdir()
            .unwrap();
        let filename = directory.path().file_name().unwrap().to_str().unwrap();
        assert_eq!(&filename[0..filename.len() - 4], "foo%bar%baz%");
        let path = directory.path().to_path_buf();
        assert!(path.try_exists().unwrap());
        drop(directory);
        assert!(!path.try_exists().unwrap());
    }

    #[test]
    fn test_directory_randomness() {
        let mut directories = Vec::new();
        let mut paths = Vec::new();
        let temp_dir = Builder::default().tempdir().unwrap();
        // Try creating the maximum number of directories for a single random byte
        // On case-insensitive filesystems we can only have 36 different directories a-z,0-9
        for _ in 0..36 {
            let dir = Builder::default()
                .join("test")
                .rand_bytes(1)
                .tempdir_in(temp_dir.path())
                .unwrap();
            assert_eq!(dir.path().parent().unwrap(), temp_dir.path());
            paths.push(dir.path().to_path_buf());
            directories.push(dir);
        }
        for path in paths.iter() {
            assert!(path.try_exists().unwrap());
        }
        drop(directories);
        for path in paths.iter() {
            assert!(!path.try_exists().unwrap());
        }
    }
}
