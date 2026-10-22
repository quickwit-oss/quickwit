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

use std::io;
use std::path::Path;

use uuid::Uuid;

const INSTALL_ID_FILE_NAME: &str = "telemetry_install_id";

/// Returns the install ID stored in `data_dir`, creating it if needed, and whether it was just
/// created. An invalid ID file is an error rather than being replaced.
pub fn load_or_create_install_id(data_dir: &Path) -> io::Result<(Uuid, bool)> {
    let install_id_path = data_dir.join(INSTALL_ID_FILE_NAME);
    match std::fs::read_to_string(&install_id_path) {
        Ok(install_id_str) => {
            let install_id = Uuid::parse_str(install_id_str.trim()).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "invalid install ID in `{}`: {error}",
                        install_id_path.display()
                    ),
                )
            })?;
            return Ok((install_id, false));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let install_id = Uuid::new_v4();
    std::fs::create_dir_all(data_dir)?;
    // Write to a temporary file, then rename it, so that a crash never leaves a truncated ID.
    let tmp_install_id_path = data_dir.join(format!("{INSTALL_ID_FILE_NAME}.tmp"));
    std::fs::write(&tmp_install_id_path, install_id.to_string())?;
    std::fs::rename(&tmp_install_id_path, &install_id_path)?;
    Ok((install_id, true))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_or_create_install_id() {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path().join("qwdata");

        let (install_id, created) = load_or_create_install_id(&data_dir).unwrap();
        assert!(created);

        let (reloaded_install_id, created) = load_or_create_install_id(&data_dir).unwrap();
        assert!(!created);
        assert_eq!(reloaded_install_id, install_id);
    }

    #[test]
    fn test_load_or_create_install_id_rejects_invalid_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        std::fs::write(temp_dir.path().join(INSTALL_ID_FILE_NAME), "not-a-uuid").unwrap();

        let error = load_or_create_install_id(temp_dir.path()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
}
