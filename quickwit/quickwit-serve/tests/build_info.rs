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

use quickwit_serve::BuildInfo;

fn build_info() -> BuildInfo {
    BuildInfo {
        build_date: "2025-01-02T00:00:00Z",
        build_profile: "release",
        build_target: "x86_64-unknown-linux-gnu",
        cargo_pkg_version: "1.2.3",
        commit_date: "2025-01-01T00:00:00Z",
        commit_hash: "0123456789abcdef0123456789abcdef01234567",
        commit_short_hash: "0123456",
        commit_tags: vec!["v1.2.3".to_string()],
        version: "v1.2.3".to_string(),
    }
}

#[test]
fn test_build_info_version_text() {
    assert_eq!(
        build_info().get_version_text(),
        "1.2.3 (x86_64-unknown-linux-gnu 2025-01-01T00:00:00Z 0123456)"
    );
    let mut other_build_info = build_info();
    other_build_info.cargo_pkg_version = "2.0.0";
    other_build_info.commit_short_hash = "abcdef0";
    assert_eq!(
        other_build_info.get_version_text(),
        "2.0.0 (x86_64-unknown-linux-gnu 2025-01-01T00:00:00Z abcdef0)"
    );
}

#[test]
fn test_build_info_serialization() {
    let serialized = serde_json::to_value(build_info()).unwrap();
    assert_eq!(
        serialized,
        serde_json::json!({
            "build_date": "2025-01-02T00:00:00Z",
            "build_profile": "release",
            "build_target": "x86_64-unknown-linux-gnu",
            "cargo_pkg_version": "1.2.3",
            "commit_date": "2025-01-01T00:00:00Z",
            "commit_hash": "0123456789abcdef0123456789abcdef01234567",
            "commit_short_hash": "0123456",
            "commit_tags": ["v1.2.3"],
            "version": "v1.2.3",
        })
    );
}
