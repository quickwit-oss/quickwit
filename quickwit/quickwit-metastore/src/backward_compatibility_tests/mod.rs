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

use std::fs;
use std::path::Path;

use anyhow::{bail, Context};
use quickwit_config::{IndexConfig, IndexTemplate, SourceConfig, TestableForRegression};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::file_backed::file_backed_index::FileBackedIndex;
use crate::file_backed::manifest::Manifest;
use crate::{IndexMetadata, SplitMetadata};

/// In order to avoid confusion, we need to make sure that the
/// resource versions is the same for all resources.
///
/// We don't want to confuse quickwit users with different source_config /
/// index_config versions.
///
/// If you bump this version, makes sure to update all resources.
/// Of course some resource may not have any config change.
///
/// You can just reuse the same versioned object in that case.
/// ```
/// enum MyResource {
///     #[serde(rename="0.1")]
///     V0_1(MyResourceV1),
///     #[serde(rename="0.2")]
///     V0_2(MyResourceV1) //< there was no change in this version.
/// }
const GLOBAL_QUICKWIT_RESOURCE_VERSION: &str = "0.9";

/// This test makes sure that the resource is using the current `GLOBAL_QUICKWIT_RESOURCE_VERSION`.
fn test_global_version<T: Serialize>(serializable: &T) -> anyhow::Result<()> {
    let json = serde_json::to_value(serializable).unwrap();
    let version_value = json.get("version").context("no version tag")?;
    let version_str = version_value.as_str().context("version should be a str")?;
    if version_str != GLOBAL_QUICKWIT_RESOURCE_VERSION {
        bail!(
            "version `{version_str}` is not the global quickwit resource version \
             ({GLOBAL_QUICKWIT_RESOURCE_VERSION})"
        );
    }
    Ok(())
}

fn deserialize_json_file<T>(path: &Path) -> anyhow::Result<T>
where for<'a> T: Deserialize<'a> {
    let payload = std::fs::read(path)?;
    let deserialized: T = serde_json::from_slice(&payload)?;
    Ok(deserialized)
}

fn test_backward_compatibility_single_case<T>(path: &Path) -> anyhow::Result<()>
where T: TestableForRegression + std::fmt::Debug {
    println!("---\nTest deserialization of {}", path.display());
    let deserialized: T = deserialize_json_file(path)?;
    let expected_path = path.to_string_lossy().replace(".json", ".expected.json");
    let expected: T = deserialize_json_file(Path::new(&expected_path))?;
    println!("---\nTest equality of {:?}", expected);
    println!("---\nwith {:?}", deserialized);
    deserialized.assert_equality(&expected);
    Ok(())
}

fn test_backward_compatibility<T>(test_dir: &Path) -> anyhow::Result<()>
where T: TestableForRegression + std::fmt::Debug {
    for entry in
        fs::read_dir(test_dir).with_context(|| format!("failed to read {}", test_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.to_string_lossy().ends_with(".expected.json") {
            continue;
        }
        test_backward_compatibility_single_case::<T>(&path)
            .with_context(|| format!("test path {}", path.display()))?;
    }
    Ok(())
}

fn test_and_update_expected_files_single_case<T>(expected_path: &Path) -> anyhow::Result<bool>
where for<'a> T: std::fmt::Debug + Serialize + Deserialize<'a> {
    let expected: T = deserialize_json_file(Path::new(&expected_path))?;
    let expected_old_json_value: JsonValue = deserialize_json_file(Path::new(&expected_path))?;
    let expected_new_json_value: JsonValue = serde_json::to_value(&expected)?;
    // We compare json Value, so we don't detect format change like a change in the field order.
    if expected_old_json_value == expected_new_json_value {
        // No modification
        return Ok(false);
    }
    println!("---\nTest deserialization of {}", expected_path.display());
    println!("---\nexpected {:?}", expected);
    println!("---\nwith {:?}", expected_new_json_value);
    let mut expected_new_json = serde_json::to_string_pretty(&expected_new_json_value)?;
    expected_new_json.push('\n');
    std::fs::write(expected_path, expected_new_json.as_bytes())?;
    Ok(true)
}

fn test_and_update_expected_files<T>(test_dir: &Path) -> anyhow::Result<()>
where for<'a> T: std::fmt::Debug + Deserialize<'a> + Serialize {
    let mut updated_expected_files = Vec::new();
    for entry in fs::read_dir(test_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.to_string_lossy().ends_with(".expected.json") {
            continue;
        }
        if test_and_update_expected_files_single_case::<T>(&path)
            .with_context(|| format!("test filepath {}", path.display()))?
        {
            updated_expected_files.push(path);
        }
    }
    assert!(
        updated_expected_files.is_empty(),
        "The following expected files need to be updated. {updated_expected_files:?}"
    );
    Ok(())
}

fn test_and_create_new_test<T>(test_dir: &Path, sample: T) -> anyhow::Result<()>
where for<'a> T: Serialize {
    let sample_json_value = serde_json::to_value(&sample)?;
    let version: &str = sample_json_value
        .as_object()
        .unwrap()
        .get("version")
        .expect("missing version")
        .as_str()
        .expect("version should be a string");
    let mut sample_json = serde_json::to_string_pretty(&sample_json_value)?;
    sample_json.push('\n');
    let test_name = format!("v{version}");
    let file_regression_test_path_str = format!("{}/{}.json", test_dir.display(), test_name);
    let file_regression_expected_path_str =
        format!("{}/{}.expected.json", test_dir.display(), test_name);

    let file_regression_test_path = Path::new(&file_regression_test_path_str);
    let (changes_detected, file_created) = if file_regression_test_path.try_exists()? {
        let expected_old_json_value: JsonValue = deserialize_json_file(file_regression_test_path)?;
        let expected_new_json_value: JsonValue = serde_json::from_str(&sample_json)?;
        (expected_old_json_value != expected_new_json_value, false)
    } else {
        (false, true)
    };

    if changes_detected || file_created {
        std::fs::write(
            file_regression_test_path_str.clone(),
            sample_json.as_bytes(),
        )?;
        std::fs::write(
            file_regression_expected_path_str.clone(),
            sample_json.as_bytes(),
        )?;
        if file_created {
            panic!(
                "The following files need to be added: {file_regression_test_path_str:?} and \
                 {file_regression_expected_path_str:?}"
            )
        } else {
            panic!(
                "The following files need to be updated: {file_regression_test_path_str:?} and \
                 {file_regression_expected_path_str:?}"
            )
        }
    }
    Ok(())
}

/// This helper function scans the `test-data/{test_name}`
/// for JSON deserialization regression tests and runs them sequentially.
///
/// - `test_name` is just the subdirectory name, for the type being test.
/// - `test` is a function asserting the equality of the deserialized version and the expected
///   version.
pub(crate) fn test_json_backward_compatibility_helper<T>(test_name: &str) -> anyhow::Result<()>
where T: TestableForRegression + std::fmt::Debug {
    let sample_instance: T = T::sample_for_regression();
    test_global_version(&sample_instance).unwrap();

    let test_dir = Path::new("test-data").join(test_name);
    test_backward_compatibility::<T>(&test_dir).context("backward-compatibility")?;
    test_and_update_expected_files::<T>(&test_dir).context("test-and-update")?;

    test_and_create_new_test::<T>(&test_dir, sample_instance)
        .context("test-and-create-new-test")?;
    Ok(())
}

#[test]
fn test_split_metadata_backward_compatibility() {
    test_json_backward_compatibility_helper::<SplitMetadata>("split-metadata").unwrap();
}

#[test]
fn test_index_metadata_backward_compatibility() {
    test_json_backward_compatibility_helper::<IndexMetadata>("index-metadata").unwrap();
}

#[test]
fn test_index_config_global_version() {
    let sample_instance = IndexConfig::sample_for_regression();
    test_global_version(&sample_instance).unwrap();
}

#[test]
fn test_source_config_global_version() {
    let sample_instance = SourceConfig::sample_for_regression();
    test_global_version(&sample_instance).unwrap();
}

#[test]
fn test_file_backed_index_backward_compatibility() {
    test_json_backward_compatibility_helper::<FileBackedIndex>("file-backed-index").unwrap();
}

#[test]
fn test_file_backed_metastore_manifest_backward_compatibility() {
    test_json_backward_compatibility_helper::<Manifest>("manifest").unwrap();
}

#[test]
fn test_index_template_global_version() {
    let sample_instance = IndexTemplate::sample_for_regression();
    test_global_version(&sample_instance).unwrap();
}
