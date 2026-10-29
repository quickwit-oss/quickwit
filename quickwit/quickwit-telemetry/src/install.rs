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

use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

use uuid::Uuid;

const INSTALL_ID_FILE_NAME: &str = "telemetry_install_id";
const INSTALL_ID_TEMP_FILE_NAME: &str = "telemetry_install_id.tmp";

/// Returns the install ID stored in `data_dir`, creating it if needed, and whether it was just
/// created. Returns an error if the stored install ID is invalid.
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
    let temp_install_id_path = data_dir.join(INSTALL_ID_TEMP_FILE_NAME);
    let mut temp_install_id_file = File::create(&temp_install_id_path)?;
    temp_install_id_file.write_all(install_id.to_string().as_bytes())?;
    temp_install_id_file.sync_all()?;
    std::fs::rename(&temp_install_id_path, &install_id_path)?;
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
    fn test_load_or_create_install_id_rejects_invalid_install_id() {
        let temp_dir = tempfile::tempdir().unwrap();
        let install_id_path = temp_dir.path().join(INSTALL_ID_FILE_NAME);
        std::fs::write(&install_id_path, "not-a-uuid").unwrap();

        let error = load_or_create_install_id(temp_dir.path()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            std::fs::read_to_string(&install_id_path).unwrap(),
            "not-a-uuid"
        );
    }
}
