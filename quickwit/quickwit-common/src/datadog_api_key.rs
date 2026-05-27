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

use std::fmt::{Display, Formatter};
use std::path::PathBuf;

use secrecy::SecretString;

use crate::get_from_env_opt;

const DD_API_KEY_ENV_KEY: &str = "DD_API_KEY";
const DD_API_KEY_FILE_ENV_KEY: &str = "DD_API_KEY_FILE";

#[derive(Clone, Copy)]
enum DdApiKeySource {
    Env,
    File,
}

impl Display for DdApiKeySource {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Env => formatter.write_str("env"),
            Self::File => formatter.write_str("file"),
        }
    }
}

pub fn resolve_dd_api_key_from_env() -> Option<SecretString> {
    resolve_dd_api_key(
        get_from_env_opt::<String>(DD_API_KEY_ENV_KEY, true),
        get_from_env_opt::<PathBuf>(DD_API_KEY_FILE_ENV_KEY, false),
    )
}

fn resolve_dd_api_key(
    api_key: Option<String>,
    api_key_file: Option<PathBuf>,
) -> Option<SecretString> {
    api_key
        .and_then(|api_key| normalize_api_key(DdApiKeySource::Env, api_key))
        .or_else(|| {
            api_key_file
                .and_then(resolve_dd_api_key_file)
                .and_then(|api_key| normalize_api_key(DdApiKeySource::File, api_key))
        })
}

fn resolve_dd_api_key_file(path: PathBuf) -> Option<String> {
    match std::fs::read_to_string(&path) {
        Ok(api_key) => Some(api_key),
        Err(_) => {
            let path = path.display();
            tracing::warn!(
                %path,
                "failed to read DD_API_KEY_FILE"
            );
            None
        }
    }
}

fn normalize_api_key(source: DdApiKeySource, api_key: String) -> Option<SecretString> {
    let api_key = api_key.trim();
    if api_key.is_empty() {
        tracing::warn!(
            %source,
            "Datadog API key is configured but empty"
        );
        None
    } else {
        Some(SecretString::from(api_key.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;

    use super::*;

    fn expose_api_key(api_key: Option<SecretString>) -> Option<String> {
        api_key.map(|api_key| api_key.expose_secret().to_string())
    }

    #[test]
    fn test_resolve_dd_api_key_uses_env() {
        assert_eq!(
            expose_api_key(resolve_dd_api_key(Some(" env-api-key\n".to_string()), None)).as_deref(),
            Some("env-api-key")
        );
    }

    #[test]
    fn test_resolve_dd_api_key_ignores_empty_env() {
        assert!(resolve_dd_api_key(Some(" \n".to_string()), None).is_none());
    }

    #[test]
    fn test_resolve_dd_api_key_falls_back_to_file_when_env_is_empty() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("api-key");
        std::fs::write(&path, "file-api-key\n").unwrap();

        assert_eq!(
            expose_api_key(resolve_dd_api_key(Some(" \n".to_string()), Some(path))).as_deref(),
            Some("file-api-key")
        );
    }

    #[test]
    fn test_resolve_dd_api_key_reads_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("api-key");
        std::fs::write(&path, " file-api-key\n").unwrap();

        assert_eq!(
            expose_api_key(resolve_dd_api_key(None, Some(path))).as_deref(),
            Some("file-api-key")
        );
    }

    #[test]
    fn test_resolve_dd_api_key_ignores_empty_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("api-key");
        std::fs::write(&path, " \n").unwrap();

        assert!(resolve_dd_api_key(None, Some(path)).is_none());
    }

    #[test]
    fn test_resolve_dd_api_key_prefers_env_over_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("api-key");
        std::fs::write(&path, "file-api-key\n").unwrap();

        assert_eq!(
            expose_api_key(resolve_dd_api_key(
                Some("env-api-key".to_string()),
                Some(path)
            ))
            .as_deref(),
            Some("env-api-key")
        );
    }

    #[test]
    fn test_resolve_dd_api_key_ignores_unreadable_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("missing-api-key");

        assert!(resolve_dd_api_key(None, Some(path)).is_none());
    }
}
