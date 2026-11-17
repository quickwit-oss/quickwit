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

use quickwit_serve::BuildInfo;

// Keep volatile metadata in the executable so library artifacts remain cacheable.
pub static BUILD_INFO: LazyLock<BuildInfo> = LazyLock::new(build_info);

fn build_info() -> BuildInfo {
    const UNKNOWN: &str = "unknown";

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

    BuildInfo {
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
}

#[cfg(test)]
mod tests {
    use super::BUILD_INFO;

    #[test]
    fn test_build_info() {
        let build_info = &*BUILD_INFO;
        assert_eq!(build_info.build_date, env!("BUILD_DATE"));
        assert_eq!(build_info.build_profile, env!("BUILD_PROFILE"));
        assert_eq!(build_info.build_target, env!("BUILD_TARGET"));
        assert_eq!(build_info.cargo_pkg_version, env!("CARGO_PKG_VERSION"));
        assert_eq!(
            build_info.commit_date,
            option_env!("QW_COMMIT_DATE")
                .filter(|date| !date.is_empty())
                .unwrap_or("unknown")
        );
        assert_eq!(
            build_info.commit_hash,
            option_env!("QW_COMMIT_HASH")
                .filter(|hash| !hash.is_empty())
                .unwrap_or("unknown")
        );
        assert_eq!(
            build_info.commit_short_hash,
            build_info.commit_hash.get(..7).unwrap_or("unknown")
        );
        let mut expected_tags: Vec<String> = option_env!("QW_COMMIT_TAGS")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|tag| !tag.is_empty())
            .map(str::to_owned)
            .collect();
        expected_tags.sort();
        assert_eq!(build_info.commit_tags, expected_tags);
        assert_eq!(
            build_info.version,
            build_info
                .commit_tags
                .iter()
                .find(|tag| tag.starts_with('v'))
                .cloned()
                .unwrap_or_else(|| format!("{}-nightly", env!("CARGO_PKG_VERSION")))
        );
    }
}
