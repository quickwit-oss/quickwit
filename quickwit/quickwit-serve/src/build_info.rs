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

use once_cell::sync::OnceCell;
use quickwit_common::runtimes::RuntimesConfig;
use serde::Serialize;

#[derive(Debug, Eq, PartialEq, Serialize, utoipa::ToSchema)]
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

    pub fn get_version_text() -> String {
        let build_info = Self::get();
        format!(
            "{} ({} {} {})",
            build_info.cargo_pkg_version,
            build_info.build_target,
            build_info.commit_date,
            build_info.commit_short_hash
        )
    }
}

#[derive(Debug, Eq, PartialEq, Serialize, utoipa::ToSchema)]
pub struct RuntimeInfo {
    // This is a number of logical cpus: vCPU or hyperthread depending on where you are running.
    // This is usually NOT necessarily the number of cores.
    pub num_cpus: usize,
    pub num_threads_blocking: usize,
    pub num_threads_non_blocking: usize,
}

impl RuntimeInfo {
    /// Returns the properties of the node.
    pub fn get() -> &'static Self {
        static INSTANCE: OnceCell<RuntimeInfo> = OnceCell::new();

        INSTANCE.get_or_init(|| {
            let num_cpus = quickwit_common::num_cpus();
            let runtimes_config = RuntimesConfig::with_num_cpus(num_cpus);
            Self {
                num_cpus,
                num_threads_blocking: runtimes_config.num_threads_blocking,
                num_threads_non_blocking: runtimes_config.num_threads_non_blocking,
            }
        })
    }
}
