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
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
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
    println!("---\nTest equality of {expected:?}");
    println!("---\nwith {deserialized:?}");
    deserialized.assert_equality(&expected);
    Ok(())
}

/// For each pair of `x.json` and `x.expected.json` in `test_dir`, assert that the deserialized
/// versions are equal according to `T::assert_equality`.
fn test_backward_compatibility<T>(test_dir: &Path) -> anyhow::Result<()>
where T: TestableForRegression + std::fmt::Debug {
    for entry in
        fs::read_dir(test_dir).with_context(|| format!("failed to read {}", test_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.to_string_lossy().ends_with(".expected.json")
            || path.to_string_lossy().ends_with(".modified.json")
        {
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
    println!("---\nexpected {expected:?}");
    println!("---\nwith {expected_new_json_value:?}");
    let mut expected_new_json = serde_json::to_string_pretty(&expected_new_json_value)?;
    expected_new_json.push('\n');
    std::fs::write(
        expected_path.with_extension("modified.json"),
        expected_new_json.as_bytes(),
    )?;
    Ok(true)
}

/// For versions different (older) than the current [GLOBAL_QUICKWIT_RESOURCE_VERSION],
/// assert whether the expected.json files need to be changed.
///
/// Returns the proposed updated files (xxx.expected.modified.json).
fn test_and_update_old_expected_files<T>(test_dir: &Path) -> anyhow::Result<Vec<PathBuf>>
where for<'a> T: std::fmt::Debug + Deserialize<'a> + Serialize {
    let mut updated_expected_files = Vec::new();
    for entry in fs::read_dir(test_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.to_string_lossy().ends_with(".expected.json") {
            continue;
        }
        if path.to_string_lossy().ends_with(&format!(
            "v{GLOBAL_QUICKWIT_RESOURCE_VERSION}.expected.json"
        )) {
            continue;
        }
        if test_and_update_expected_files_single_case::<T>(&path)
            .with_context(|| format!("test filepath {}", path.display()))?
        {
            updated_expected_files.push(path.with_extension("modified.json"));
        }
    }
    Ok(updated_expected_files)
}

/// Asserts whether the serialized version of the `sample` is the same as the existing
/// `v{GLOBAL_QUICKWIT_RESOURCE_VERSION}.json`.
///
/// Returns the created serialized files if they didn't exist (x.json and x.expected.json) or the
/// proposed updated files (.modified.json) if they changed.
///
/// Both generated files have identical contents.
fn test_and_create_new_test<T>(test_dir: &Path, sample: T) -> anyhow::Result<Vec<PathBuf>>
where for<'a> T: Serialize {
    let sample_json_value = serde_json::to_value(&sample)?;
    let mut sample_json = serde_json::to_string_pretty(&sample_json_value)?;
    sample_json.push('\n');

    let file_regression_test_path_str = format!(
        "{}/v{GLOBAL_QUICKWIT_RESOURCE_VERSION}.json",
        test_dir.display()
    );
    let mut file_regression_test_path = PathBuf::from(file_regression_test_path_str);

    let (changes_detected, file_created) = if file_regression_test_path.try_exists()? {
        let expected_old_json_value: JsonValue = deserialize_json_file(&file_regression_test_path)?;
        let expected_new_json_value: JsonValue = serde_json::from_str(&sample_json)?;
        (expected_old_json_value != expected_new_json_value, false)
    } else {
        (false, true)
    };

    let mut file_regression_expected_path =
        file_regression_test_path.with_extension("expected.json");

    if !file_created {
        file_regression_test_path = file_regression_test_path.with_extension("modified.json");
        file_regression_expected_path =
            file_regression_expected_path.with_extension("modified.json")
    }

    if changes_detected || file_created {
        std::fs::write(&file_regression_test_path, sample_json.as_bytes())?;
        std::fs::write(&file_regression_expected_path, sample_json.as_bytes())?;
        Ok(vec![
            file_regression_test_path,
            file_regression_expected_path,
        ])
    } else {
        Ok(vec![])
    }
}

/// This helper function scans the `test-data/{test_name}`
/// for JSON deserialization regression tests and runs them sequentially.
///
/// - `test_name` is just the subdirectory name, for the type being test.
pub(crate) fn test_json_backward_compatibility_helper<T>(test_name: &str) -> anyhow::Result<()>
where T: TestableForRegression + std::fmt::Debug {
    let sample_instance: T = T::sample_for_regression();
    test_global_version(&sample_instance).unwrap();

    let test_dir = Path::new("test-data").join(test_name);
    test_backward_compatibility::<T>(&test_dir).context("backward-compatibility")?;
    let updated_files =
        test_and_update_old_expected_files::<T>(&test_dir).context("test-and-update")?;

    let mut updated_or_new_files = test_and_create_new_test::<T>(&test_dir, sample_instance)
        .context("test-and-create-new-test")?;

    updated_or_new_files.extend(updated_files);

    if !updated_or_new_files.is_empty() {
        panic!(
            "Some files have been updated or created. Please check the diff and replace their \
             counterparts when appropriate: {updated_or_new_files:?}"
        );
    }

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

/// Testing the tests
///
/// A simplified example that helps understanding the backward compatibility tests.
#[cfg(test)]
mod tests {
    use std::panic::catch_unwind;

    use serde_json::json;

    use super::*;

    #[derive(Serialize, Deserialize, Debug, Clone)]
    #[serde(into = "VersionedTestEntity")]
    #[serde(from = "VersionedTestEntity")]
    struct TestEntity {
        field_already_in_0_7: u16,
        field_added_in_0_8: u16,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct TestEntityV0_8 {
        field_already_in_0_7: u16,
        field_added_in_0_8: u16,
    }

    #[derive(Deserialize, Debug, Clone)]
    struct TestEntityV0_7 {
        field_already_in_0_7: u16,
    }

    #[derive(Clone, Debug, Serialize, Deserialize, utoipa::ToSchema)]
    #[serde(tag = "version")]
    enum VersionedTestEntity {
        #[serde(rename = "0.9")]
        #[serde(alias = "0.8")]
        V0_8(TestEntityV0_8),
        #[serde(alias = "0.7", skip_serializing)]
        V0_7(TestEntityV0_7),
    }

    impl From<VersionedTestEntity> for TestEntity {
        fn from(versioned_test_entity: VersionedTestEntity) -> Self {
            match versioned_test_entity {
                VersionedTestEntity::V0_8(v0_9) => TestEntity {
                    field_added_in_0_8: v0_9.field_added_in_0_8,
                    field_already_in_0_7: v0_9.field_already_in_0_7,
                },
                VersionedTestEntity::V0_7(v0_7) => TestEntity {
                    field_already_in_0_7: v0_7.field_already_in_0_7,
                    field_added_in_0_8: 1,
                },
            }
        }
    }

    impl From<TestEntity> for VersionedTestEntity {
        fn from(test_entity: TestEntity) -> Self {
            VersionedTestEntity::V0_8(TestEntityV0_8 {
                field_added_in_0_8: test_entity.field_added_in_0_8,
                field_already_in_0_7: test_entity.field_already_in_0_7,
            })
        }
    }

    impl TestableForRegression for TestEntity {
        fn sample_for_regression() -> Self {
            TestEntity {
                field_added_in_0_8: 43,
                field_already_in_0_7: 42,
            }
        }

        fn assert_equality(&self, other: &Self) {
            assert_eq!(self.field_added_in_0_8, other.field_added_in_0_8);
            assert_eq!(self.field_already_in_0_7, other.field_already_in_0_7);
        }
    }

    #[test]
    fn test_test_json_backward_compatibility_helper_create() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_path = temp_dir.path();

        let json_sample_0_7 = json!({"version": "0.7", "field_already_in_0_7": 42});
        let json_sample_0_8 = json!({"version": GLOBAL_QUICKWIT_RESOURCE_VERSION,
"field_already_in_0_7": 42, "field_added_in_0_8": 43});

        let json_sample_0_7_str = serde_json::to_string_pretty(&json_sample_0_7).unwrap();
        let json_sample_0_8_str = serde_json::to_string_pretty(&json_sample_0_8).unwrap();

        std::fs::write(temp_path.join("v0.7.json"), json_sample_0_7_str.as_bytes()).unwrap();
        std::fs::write(
            temp_path.join("v0.7.expected.json"),
            json_sample_0_7_str.as_bytes(),
        )
        .unwrap();
        std::fs::write(temp_path.join("v0.8.json"), json_sample_0_8_str.as_bytes()).unwrap();
        std::fs::write(
            temp_path.join("v0.8.expected.json"),
            json_sample_0_8_str.as_bytes(),
        )
        .unwrap();

        let test_panic = catch_unwind(|| {
            test_json_backward_compatibility_helper::<TestEntity>(&temp_path.to_string_lossy())
                .unwrap();
        });
        let test_panic_msg = format!(
            "{:?}",
            test_panic.unwrap_err().downcast::<String>().unwrap()
        );
        let latest_version_filename = format!("v{GLOBAL_QUICKWIT_RESOURCE_VERSION}.json");
        let latest_version_expected_filename =
            format!("v{GLOBAL_QUICKWIT_RESOURCE_VERSION}.expected.json");
        assert!(test_panic_msg.contains(&latest_version_filename));
        assert!(test_panic_msg.contains(&latest_version_expected_filename));
        assert!(test_panic_msg.contains("v0.7.expected.modified.json"));

        // assert on the directory
        let nb_files = fs::read_dir(temp_path).unwrap().count();
        assert_eq!(nb_files, 4 + 3);
        let created_last_version =
            deserialize_json_file::<JsonValue>(&temp_path.join(latest_version_filename)).unwrap();
        assert_eq!(created_last_version, json_sample_0_8);
        let created_expected_last_version =
            deserialize_json_file::<JsonValue>(&temp_path.join(latest_version_expected_filename))
                .unwrap();
        assert_eq!(created_expected_last_version, json_sample_0_8);
        let created_expected_modified_0_7 =
            deserialize_json_file::<JsonValue>(&temp_path.join("v0.7.expected.modified.json"))
                .unwrap();
        assert_eq!(
            created_expected_modified_0_7,
            json!({
                "version": GLOBAL_QUICKWIT_RESOURCE_VERSION,
                "field_already_in_0_7": 42,
                // use TestEntity::From<VersionedTestEntity>
                "field_added_in_0_8": 1,
            })
        );

        // assert idempotency
        let test_panic = catch_unwind(|| {
            test_json_backward_compatibility_helper::<TestEntity>(&temp_path.to_string_lossy())
                .unwrap();
        });
        test_panic.unwrap_err();
        let nb_files = fs::read_dir(temp_path).unwrap().count();
        assert_eq!(nb_files, 4 + 3);
    }
}
