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

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::fs;

use crate::ignore_error_kind;

const MAX_LENGTH: usize = 255;
const SEPARATOR: char = '%';
const NUM_RAND_CHARS: usize = 6;

/// Creates the specified directory. If the directory already exists, deletes its contents.
pub async fn create_clean_directory(path: &Path) -> io::Result<PathBuf> {
    // Delete if exists and recreate scratch directory.
    ignore_error_kind!(io::ErrorKind::NotFound, fs::remove_dir_all(&path).await)?;
    fs::create_dir_all(&path).await?;
    Ok(path.to_path_buf())
}

/// A temporary directory. This directory is deleted when the object is dropped.
#[derive(Debug, Clone)]
pub struct TempDir {
    inner: Arc<tempfile::TempDir>,
    _parent: Option<Arc<tempfile::TempDir>>,
}

impl TempDir {
    /// A path where the temporary directory is pointing to.
    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    /// Creates a new temporary directory with the current temporary directory.
    /// The new directory keeps a pointer to the parent directory to perevent it
    /// from premature deletion. The directory is deleted when the object is dropped.
    pub fn named_temp_child(&self, prefix: &str) -> io::Result<TempDir> {
        Ok(TempDir {
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
        Builder::new().tempdir().unwrap()
    }
}

/// A temporary directory builder.
#[derive(Debug)]
pub struct Builder<'a> {
    parts: Vec<&'a str>,
    max_length: usize,
    separator: char,
    rand_bytes: usize,
}

impl<'a> Default for Builder<'a> {
    fn default() -> Self {
        Self {
            parts: Default::default(),
            max_length: MAX_LENGTH,
            separator: SEPARATOR,
            rand_bytes: NUM_RAND_CHARS,
        }
    }
}

impl<'a> Builder<'a> {
    /// Create a new temporary directory builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Specifies the number of random bytes to add at the end of the directory name. Default is 6.
    pub fn rand_bytes(&mut self, rand: usize) -> &mut Self {
        self.rand_bytes = rand;
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

    pub(crate) fn push_str(buffer: &mut String, addition: &'a str, size: usize) -> usize {
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

    /// Constracts the prefix from the parts specified by the join function.
    /// If parts are small enough they will be simply contcatenated with the
    /// separator character in between. If parts are too large they will
    /// trancated by replacing the middle of each part with "..". The resulting
    /// string will be at most max_length characters long.
    pub(crate) fn prefix(&self) -> io::Result<String> {
        if self.parts.is_empty() {
            return Ok(String::new());
        }
        let separator_count = if self.rand_bytes > 0 {
            self.parts.len()
        } else {
            self.parts.len() - 1
        };
        // We want to preserve at least one letter from each part with separatos.
        if self.max_length < self.parts.len() + separator_count + self.rand_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "The filename limit is too small",
            ));
        }
        // Calculate how many characters from the parts we can use in the final string.
        let len_without_separators = self.max_length - separator_count - self.rand_bytes;
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
                // Adjust the avaible length from the parts that are shorter
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
            if i < self.parts.len() - 1 || self.rand_bytes > 0 {
                buf.push(self.separator)
            }
        }
        Ok(buf)
    }

    /// Creates a temporary directory in the temp directory of operation system
    pub fn tempdir(&self) -> io::Result<TempDir> {
        Ok(TempDir {
            inner: Arc::new(
                tempfile::Builder::new()
                    .rand_bytes(self.rand_bytes)
                    .prefix(&self.prefix()?)
                    .tempdir()?,
            ),
            _parent: None,
        })
    }

    /// Creates a temporary directory in the specified directory
    pub fn tempdir_in<P: AsRef<Path>>(&self, dir: P) -> io::Result<TempDir> {
        Ok(TempDir {
            inner: Arc::new(
                tempfile::Builder::new()
                    .rand_bytes(self.rand_bytes)
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
        assert_trancate("abcdef", 6, "abcdef", 100);
        assert_trancate("abcdef", 6, "abcdef", 6);
        assert_trancate("ab..z", 5, "abcdefghijklmnopqrstuvwxyz", 5);
        assert_trancate("a..z", 4, "abcdefghijklmnopqrstuvwxyz", 4);
        assert_trancate("a..", 3, "abcdefghijklmnopqrstuvwxyz", 3);
        assert_trancate("ab", 2, "abcdefghijklmnopqrstuvwxyz", 2);
        assert_trancate("a", 1, "abcdefghijklmnopqrstuvwxyz", 1);
        assert_trancate("abcde", 5, "abcde", 10);
        assert_trancate("abcde", 5, "abcde", 5);
        assert_trancate("a..e", 4, "abcde", 4);
        assert_trancate("a..", 3, "abcde", 3);
        assert_trancate("ab", 2, "abcde", 2);
        assert_trancate("a", 1, "abcde", 1);
    }

    fn assert_trancate(expected_addition: &str, expected_size: usize, addition: &str, size: usize) {
        let mut buf = String::new();
        let size = Builder::push_str(&mut buf, addition, size);
        assert_eq!(expected_addition, buf);
        assert_eq!(expected_size, size);
    }

    #[test]
    fn test_random_failures() {
        assert_prefix(
            "AAAAAAAAAA%AA%AA..A%AAAA%AAAA%A..A%",
            vec!["AAAAAAAAAA", "AA", "AAAAAAA", "AAAA", "AAAA", "AAAAAAAAA"],
            35,
        );
        assert_prefix(
            "AAAAAA%AAAAAAAAA%AAAAAA%AAA%A%AAAAAAA%AAAAAAAAAA%AAAAA%",
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
        );
        assert_prefix(
            "AAA..AAA%AAAAAAA%AAAAAAA%",
            vec!["AAAAAAAAA", "", "AAAAAAA", "AAAAAAA"],
            25,
        );
    }

    #[test]
    fn test_prefix() {
        assert_prefix("0%abcde%uvwxyz%", vec!["0", "abcde", "uvwxyz"], 15);

        assert_prefix("a%b%", vec!["a", "b"], 100);
        assert_prefix("abcde%uvwxyz%", vec!["abcde", "uvwxyz"], 100);
        assert_prefix("abcde%uvwxyz%", vec!["abcde", "uvwxyz"], 13);
        assert_prefix("abcde%uv..z%", vec!["abcde", "uvwxyz"], 12);
        assert_prefix("abcde%u..z%", vec!["abcde", "uvwxyz"], 11);
        assert_prefix("a..e%u..z%", vec!["abcde", "uvwxyz"], 10);
        assert_prefix("a..e%u..%", vec!["abcde", "uvwxyz"], 9);
        assert_prefix("a..%u..%", vec!["abcde", "uvwxyz"], 8);
        assert_prefix("a..%uv%", vec!["abcde", "uvwxyz"], 7);
        assert_prefix("ab%uv%", vec!["abcde", "uvwxyz"], 6);
        assert_prefix("ab%u%", vec!["abcde", "uvwxyz"], 5);
        assert_prefix("a%u%", vec!["abcde", "uvwxyz"], 4);
        assert_prefix_err(
            "The filename limit is too small",
            vec!["abcde", "uvwxyz"],
            3,
        );

        assert_prefix("0%abcde%uvwxyz%", vec!["0", "abcde", "uvwxyz"], 15);
        assert_prefix("0%abcde%uv..z%", vec!["0", "abcde", "uvwxyz"], 14);
        assert_prefix("0%abcde%u..z%", vec!["0", "abcde", "uvwxyz"], 13);
        assert_prefix("0%abcde%u..%", vec!["0", "abcde", "uvwxyz"], 12);
        assert_prefix("0%abcde%uv%", vec!["0", "abcde", "uvwxyz"], 11);
        assert_prefix("0%a..e%uv%", vec!["0", "abcde", "uvwxyz"], 10);
        assert_prefix("0%a..%uv%", vec!["0", "abcde", "uvwxyz"], 9);
        assert_prefix("0%a..%u%", vec!["0", "abcde", "uvwxyz"], 8);
        assert_prefix("0%ab%u%", vec!["0", "abcde", "uvwxyz"], 7);
        assert_prefix("0%a%u%", vec!["0", "abcde", "uvwxyz"], 6);
        assert_prefix_err(
            "The filename limit is too small",
            vec!["0", "abcde", "uvwxyz"],
            5,
        );
    }

    fn assert_prefix(expected_path: &str, parts: Vec<&str>, size: usize) {
        let mut builder = Builder::new();
        builder.rand_bytes(5);
        builder.max_length(size + 5); // Size of random suffix
        for part in parts.iter() {
            builder.join(part);
        }
        let prefix = builder.prefix().unwrap();
        assert_eq!(expected_path, prefix, "parts: {:?} len: {:?}", parts, size);
    }

    fn assert_prefix_err(expected_err: &str, parts: Vec<&str>, size: usize) {
        let mut builder = Builder::new();
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
            let rand_bytes = rng.gen::<usize>() % 4;
            let parts_num = rng.gen::<usize>() % 10;
            let mut builder = Builder::new();
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
            if parts_num > 0 && rng.gen::<bool>() {
                builder.max_length(rand::random::<usize>() % limit_threshold);
                assert_eq!(
                    "The filename limit is too small",
                    builder.prefix().unwrap_err().to_string()
                );
            } else {
                let len = limit_threshold + rand::random::<usize>() % 100;
                builder.max_length(len);
                let builder_debug = format!("{:?}, len {}", builder, len);
                let builder_prefix = builder.prefix().unwrap();
                assert_eq!(
                    builder_prefix.len(),
                    cmp::min(len - rand_bytes, max_size),
                    "{} -> {}",
                    builder_debug,
                    builder_prefix
                );
            }
        }
    }

    #[test]
    fn test_directory_creation_and_removal() {
        let directory = Builder::new()
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
        let directory = Builder::new()
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
        let temp_dir = Builder::new().tempdir().unwrap();
        // Try creating the maximum number of directories for a single random byte a-z,A-Z,0-9
        for _ in 0..62 {
            let dir = Builder::new()
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
