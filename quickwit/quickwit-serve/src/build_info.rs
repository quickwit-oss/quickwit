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

use std::sync::LazyLock;

use quickwit_common::runtimes::RuntimesConfig;
use serde::Serialize;

/// Build metadata supplied by the executable and shared with its server services.
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
    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_tests() -> &'static Self {
        static BUILD_INFO: LazyLock<BuildInfo> = LazyLock::new(|| BuildInfo {
            build_date: "2025-01-01T00:00:00Z",
            build_profile: "test",
            build_target: "test-target",
            cargo_pkg_version: "0.0.0",
            commit_date: "2025-01-01T00:00:00Z",
            commit_hash: "0123456789abcdef0123456789abcdef01234567",
            commit_short_hash: "0123456",
            commit_tags: Vec::new(),
            version: "0.0.0-nightly".to_string(),
        });
        &BUILD_INFO
    }

    pub fn get_version_text(&self) -> String {
        format!(
            "{} ({} {} {})",
            self.cargo_pkg_version, self.build_target, self.commit_date, self.commit_short_hash
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
        static INSTANCE: LazyLock<RuntimeInfo> = LazyLock::new(|| {
            let num_cpus = quickwit_common::num_cpus();
            let runtimes_config = RuntimesConfig::get();
            RuntimeInfo {
                num_cpus,
                num_threads_blocking: runtimes_config.num_threads_blocking,
                num_threads_non_blocking: runtimes_config.num_threads_non_blocking,
            }
        });

        &INSTANCE
    }
}
