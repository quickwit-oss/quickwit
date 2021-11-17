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

mod coolid;
pub mod memory_usage;
pub mod metrics;

use std::fmt::Debug;
use std::ops::Range;
use std::str::FromStr;

pub use coolid::new_coolid;
use once_cell::sync::OnceCell;
use tracing::{error, info};

#[derive(Debug, PartialEq, Eq)]
pub enum QuickwitEnv {
    UNSET,
    LOCAL,
}

impl Default for QuickwitEnv {
    fn default() -> Self {
        Self::UNSET
    }
}

pub fn get_quickwit_env() -> QuickwitEnv {
    match std::env::var("QUICKWIT_ENV") {
        Ok(val) if val.to_lowercase().trim() == "local" => QuickwitEnv::LOCAL,
        Ok(val) => panic!("QUICKWIT_ENV value `{}` is not supported", val),
        Err(_) => QuickwitEnv::UNSET,
    }
}

pub fn chunk_range(range: Range<usize>, chunk_size: usize) -> impl Iterator<Item = Range<usize>> {
    range.clone().step_by(chunk_size).map(move |block_start| {
        let block_end = (block_start + chunk_size).min(range.end);
        block_start..block_end
    })
}

pub fn into_u64_range(range: Range<usize>) -> Range<u64> {
    range.start as u64..range.end as u64
}

pub fn setup_logging_for_tests() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        env_logger::builder().format_timestamp(None).init();
    });
}

pub fn split_file(split_id: &str) -> String {
    format!("{}.split", split_id)
}

pub fn get_from_env<T: FromStr + Debug>(key: &str, default_value: T) -> T {
    if let Ok(value_str) = std::env::var(key) {
        if let Ok(value) = T::from_str(&value_str) {
            info!(value=?value, "Setting `{}` from environment", key);
            return value;
        } else {
            error!(value_str=%value_str, "Failed to parse `{}` from environment", key);
        }
    }
    info!(value=?default_value, "Setting `{}` from default", key);
    default_value
}

/// Returns an instance to track the memory usage of quickwit.
pub fn quickwit_memory_usage() -> &'static memory_usage::MemoryUsage {
    static MEMORY_USAGE: OnceCell<memory_usage::MemoryUsage> = OnceCell::new();
    MEMORY_USAGE.get_or_init(Default::default)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_get_from_env() {
        const TEST_KEY: &str = "TEST_KEY";
        assert_eq!(super::get_from_env(TEST_KEY, 10), 10);
        std::env::set_var(TEST_KEY, "15");
        assert_eq!(super::get_from_env(TEST_KEY, 10), 15);
        std::env::set_var(TEST_KEY, "1invalidnumber");
        assert_eq!(super::get_from_env(TEST_KEY, 10), 10);
    }
}

pub mod fs {
    use std::path::Path;

    use tokio;

    /// Deletes the contents of a directory.
    pub async fn empty_dir<P: AsRef<Path>>(path: P) -> anyhow::Result<()> {
        let mut entries = tokio::fs::read_dir(path).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                tokio::fs::remove_dir_all(entry.path()).await?
            } else {
                tokio::fs::remove_file(entry.path()).await?;
            }
        }
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use tempfile;

        use super::*;

        #[tokio::test]
        async fn test_empty_dir() -> anyhow::Result<()> {
            let tempdir = tempfile::tempdir()?;

            let file_path = tempdir.path().join("file");
            tokio::fs::File::create(file_path).await?;

            let subdir = tempdir.path().join("subdir");
            tokio::fs::create_dir(&subdir).await?;

            let subfile_path = subdir.join("subfile");
            tokio::fs::File::create(subfile_path).await?;

            empty_dir(tempdir.path()).await?;
            assert!(tokio::fs::read_dir(tempdir.path())
                .await?
                .next_entry()
                .await?
                .is_none());
            Ok(())
        }
    }
}

pub mod net {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, ToSocketAddrs};

    /// Finds a random available port.
    pub fn find_available_port() -> anyhow::Result<u16> {
        let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let listener = TcpListener::bind(socket)?;
        let port = listener.local_addr()?.port();
        Ok(port)
    }

    /// Converts this string to a resolved `SocketAddr`.
    pub fn socket_addr_from_str(addr_str: &str) -> anyhow::Result<SocketAddr> {
        addr_str
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| anyhow::anyhow!("Failed to resolve address `{}`.", addr_str))
    }
}

pub mod rand {
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
}
