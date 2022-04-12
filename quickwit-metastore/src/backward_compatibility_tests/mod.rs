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

mod file_backed_index;
mod index_metadata;
mod split_metadata;

use std::fs;
use std::path::Path;

use anyhow::Context;
use serde::{Deserialize, Serialize};

fn deserialize_json_file<T>(path: &Path) -> anyhow::Result<T>
where for<'a> T: Deserialize<'a> {
    let payload = std::fs::read(path)?;
    let deserialized: T = serde_json::from_slice(&payload)?;
    Ok(deserialized)
}

fn test_backward_compatibility_single_case<T>(
    path: &Path,
    test: impl Fn(&T, &T),
) -> anyhow::Result<()>
where
    for<'a> T: Deserialize<'a>,
{
    println!("---\nTest deserialization of {}", path.display());
    let deserialized = deserialize_json_file(path)?;
    let expected_path = path.to_string_lossy().replace(".json", ".expected.json");
    let expected = deserialize_json_file(Path::new(&expected_path))?;
    test(&deserialized, &expected);
    Ok(())
}

fn test_backward_compatibility<T>(test_dir: &Path, test: impl Fn(&T, &T)) -> anyhow::Result<()>
where for<'a> T: Deserialize<'a> {
    for entry in
        fs::read_dir(&test_dir).with_context(|| format!("Failed to read {}", test_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.to_string_lossy().ends_with(".expected.json") {
            continue;
        }
        test_backward_compatibility_single_case(&path, &test)
            .with_context(|| format!("test path {}", path.display()))?;
    }
    Ok(())
}

fn test_and_update_expected_files_single_case<T>(expected_path: &Path) -> anyhow::Result<bool>
where for<'a> T: Serialize + Deserialize<'a> {
    let expected: T = deserialize_json_file(Path::new(&expected_path))?;
    let expected_old_json_value: serde_json::Value =
        deserialize_json_file(Path::new(&expected_path))?;
    let expected_new_json_value: serde_json::Value = serde_json::to_value(&expected)?;
    // We compare json Value, so we don't detect format change like a change in the field order.
    if expected_old_json_value == expected_new_json_value {
        // No modification
        return Ok(false);
    }
    let mut expected_new_json = serde_json::to_string_pretty(&expected_new_json_value)?;
    expected_new_json.push('\n');
    std::fs::write(expected_path, expected_new_json.as_bytes())?;
    Ok(true)
}

fn test_and_update_expected_files<T>(test_dir: &Path) -> anyhow::Result<()>
where for<'a> T: Deserialize<'a> + Serialize {
    let mut updated_expected_files = Vec::new();
    for entry in fs::read_dir(&test_dir)? {
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
        "The following expected files need to be updated. {:?}",
        updated_expected_files
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
    let md5_digest = md5::compute(&sample_json);
    let test_name = format!("v{}-{:x}", version, md5_digest);
    let file_regression_test_path = format!("{}/{}.json", test_dir.display(), test_name);
    let file_regression_expected_path =
        format!("{}/{}.expected.json", test_dir.display(), test_name);
    std::fs::write(&file_regression_test_path, sample_json.as_bytes())?;
    std::fs::write(&file_regression_expected_path, sample_json.as_bytes())?;
    Ok(())
}

/// This helper function scans the `test-data/{test_name}`
/// for JSON deserialization regression tests and runs them sequentially.
///
/// - `test_name` is just the subdirectory name, for the type being test.
/// - `test` is a function asserting the equality of the deserialized version
/// and the expected version.
pub(crate) fn test_json_backward_compatibility_helper<T>(
    test_name: &str,
    test: impl Fn(&T, &T),
    sample_instance: T,
) -> anyhow::Result<()>
where
    for<'a> T: Deserialize<'a> + Serialize,
{
    let test_dir = Path::new("test-data").join(test_name);
    test_backward_compatibility(&test_dir, test).context("backward-compatiblitity")?;
    test_and_update_expected_files::<T>(&test_dir).context("test-and-update")?;
    test_and_create_new_test(&test_dir, sample_instance).context("test-and-create-new-test")?;
    Ok(())
}
