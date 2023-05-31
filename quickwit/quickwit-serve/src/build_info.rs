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

use once_cell::sync::OnceCell;
use quickwit_common::runtimes::RuntimesConfig;
use serde::Serialize;

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct BuildInfo {
    pub build_date: &'static str,
    pub build_profile: &'static str,
    pub build_target: &'static str,
    pub cargo_pkg_version: &'static str,
    pub commit_date: &'static str,
    pub commit_hash: &'static str,
    pub commit_short_hash: &'static str,
    pub commit_tags: Vec<String>,
    pub version: String,
}

impl BuildInfo {
    /// Returns the properties of the binary.
    pub fn get() -> &'static Self {
        const UNKNOWN: &str = "unknown";

        static INSTANCE: OnceCell<BuildInfo> = OnceCell::new();

        INSTANCE.get_or_init(|| {
            let commit_date = option_env!("QW_COMMIT_DATE")
                .filter(|commit_date| !commit_date.is_empty())
                .unwrap_or(UNKNOWN);
            let commit_hash = option_env!("QW_COMMIT_HASH")
                .filter(|commit_hash| !commit_hash.is_empty())
                .unwrap_or(UNKNOWN);
            let commit_short_hash = option_env!("QW_COMMIT_HASH")
                .filter(|commit_hash| commit_hash.len() >= 7)
                .map(|commit_hash| &commit_hash[..7])
                .unwrap_or(UNKNOWN);
            let mut commit_tags: Vec<String> = option_env!("QW_COMMIT_TAGS")
                .map(|tags| {
                    tags.split(',')
                        .map(|tag| tag.trim().to_string())
                        .filter(|tag| !tag.is_empty())
                        .collect()
                })
                .unwrap_or_default();
            commit_tags.sort();

            let version = commit_tags
                .iter()
                .find(|tag| tag.starts_with('v'))
                .cloned()
                .unwrap_or_else(|| concat!(env!("CARGO_PKG_VERSION"), "-nightly").to_string());

            Self {
                build_date: env!("BUILD_DATE"),
                build_profile: env!("BUILD_PROFILE"),
                build_target: env!("BUILD_TARGET"),
                cargo_pkg_version: env!("CARGO_PKG_VERSION"),
                commit_date,
                commit_hash,
                commit_short_hash,
                commit_tags,
                version,
            }
        })
    }
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct RuntimeInfo {
    pub num_cpus_logical: usize,
    pub num_cpus_physical: usize,
    pub num_threads_blocking: usize,
    pub num_threads_non_blocking: usize,
}

impl RuntimeInfo {
    /// Returns the properties of the node.
    pub fn get() -> &'static Self {
        static INSTANCE: OnceCell<RuntimeInfo> = OnceCell::new();

        INSTANCE.get_or_init(|| {
            let num_cpus_logical = num_cpus::get();
            let runtimes_config = RuntimesConfig::with_num_cpus(num_cpus_logical);

            Self {
                num_cpus_logical,
                num_cpus_physical: num_cpus::get_physical(),
                num_threads_blocking: runtimes_config.num_threads_blocking,
                num_threads_non_blocking: runtimes_config.num_threads_non_blocking,
            }
        })
    }
}
