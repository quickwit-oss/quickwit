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

use secrecy::SecretString;

use crate::get_from_env_opt;

const DD_API_KEY_ENV_KEY: &str = "DD_API_KEY";
const DD_API_KEY_FILE_ENV_KEY: &str = "DD_API_KEY_FILE";

#[derive(Clone, Copy)]
enum DdApiKeySource {
    Env,
    File,
}

impl DdApiKeySource {
    const LOAD_ORDER: [Self; 2] = [Self::Env, Self::File];

    fn load(self) -> Option<SecretString> {
        match self {
            Self::Env => get_from_env_opt::<String>(DD_API_KEY_ENV_KEY, true)
                .and_then(|api_key| self.normalize_api_key(api_key)),
            Self::File => {
                get_from_env_opt::<String>(DD_API_KEY_FILE_ENV_KEY, false).and_then(|path| {
                    std::fs::read_to_string(&path)
                        .map_err(|error| {
                            tracing::warn!(
                                path = %path,
                                error = %error,
                                "failed to read DD_API_KEY_FILE"
                            );
                            error
                        })
                        .ok()
                        .and_then(|api_key| self.normalize_api_key(api_key))
                })
            }
        }
    }

    fn normalize_api_key(self, api_key: String) -> Option<SecretString> {
        let api_key = api_key.trim();
        if api_key.is_empty() {
            tracing::warn!(
                source = %self,
                "Datadog API key is configured but empty"
            );
            None
        } else {
            Some(SecretString::from(api_key.to_string()))
        }
    }
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
    DdApiKeySource::LOAD_ORDER
        .into_iter()
        .find_map(DdApiKeySource::load)
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::path::PathBuf;
    use std::sync::{Mutex, MutexGuard};

    use secrecy::ExposeSecret;

    use super::*;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn lock_env() -> MutexGuard<'static, ()> {
        ENV_LOCK.lock().unwrap()
    }

    fn temp_file_path(test_name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "quickwit-common-datadog-api-key-{test_name}-{}",
            std::process::id()
        ))
    }

    fn dd_api_key() -> Option<String> {
        resolve_dd_api_key_from_env().map(|api_key| api_key.expose_secret().to_string())
    }

    struct EnvVarGuard {
        key: &'static str,
        previous_value: Option<OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let guard = Self {
                key,
                previous_value: std::env::var_os(key),
            };
            unsafe { std::env::set_var(key, value) };
            guard
        }

        fn remove(key: &'static str) -> Self {
            let guard = Self {
                key,
                previous_value: std::env::var_os(key),
            };
            unsafe { std::env::remove_var(key) };
            guard
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(previous_value) = &self.previous_value {
                unsafe { std::env::set_var(self.key, previous_value) };
            } else {
                unsafe { std::env::remove_var(self.key) };
            }
        }
    }

    #[test]
    fn test_resolve_dd_api_key_uses_env() {
        let _lock = lock_env();
        let _dd_api_key_guard = EnvVarGuard::set(DD_API_KEY_ENV_KEY, " env-api-key\n");
        let _dd_api_key_file_guard = EnvVarGuard::remove(DD_API_KEY_FILE_ENV_KEY);

        assert_eq!(dd_api_key().as_deref(), Some("env-api-key"));
    }

    #[test]
    fn test_resolve_dd_api_key_ignores_empty_env() {
        let _lock = lock_env();
        let _dd_api_key_guard = EnvVarGuard::set(DD_API_KEY_ENV_KEY, " \n");
        let _dd_api_key_file_guard = EnvVarGuard::remove(DD_API_KEY_FILE_ENV_KEY);

        assert_eq!(dd_api_key(), None);
    }

    #[test]
    fn test_resolve_dd_api_key_falls_back_to_file_when_env_is_empty() {
        let _lock = lock_env();
        let path = temp_file_path("empty-env-fallback");
        std::fs::remove_file(&path).ok();
        std::fs::write(&path, "file-api-key\n").unwrap();

        let _dd_api_key_guard = EnvVarGuard::set(DD_API_KEY_ENV_KEY, " \n");
        let _dd_api_key_file_guard =
            EnvVarGuard::set(DD_API_KEY_FILE_ENV_KEY, path.to_str().unwrap());

        assert_eq!(dd_api_key().as_deref(), Some("file-api-key"));

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_resolve_dd_api_key_reads_file() {
        let _lock = lock_env();
        let path = temp_file_path("file");
        std::fs::remove_file(&path).ok();
        std::fs::write(&path, " file-api-key\n").unwrap();

        let _dd_api_key_guard = EnvVarGuard::remove(DD_API_KEY_ENV_KEY);
        let _dd_api_key_file_guard =
            EnvVarGuard::set(DD_API_KEY_FILE_ENV_KEY, path.to_str().unwrap());

        assert_eq!(dd_api_key().as_deref(), Some("file-api-key"));

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_resolve_dd_api_key_ignores_empty_file() {
        let _lock = lock_env();
        let path = temp_file_path("empty-file");
        std::fs::remove_file(&path).ok();
        std::fs::write(&path, " \n").unwrap();

        let _dd_api_key_guard = EnvVarGuard::remove(DD_API_KEY_ENV_KEY);
        let _dd_api_key_file_guard =
            EnvVarGuard::set(DD_API_KEY_FILE_ENV_KEY, path.to_str().unwrap());

        assert_eq!(dd_api_key(), None);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_resolve_dd_api_key_prefers_env_over_file() {
        let _lock = lock_env();
        let path = temp_file_path("env-over-file");
        std::fs::remove_file(&path).ok();
        std::fs::write(&path, "file-api-key\n").unwrap();

        let _dd_api_key_guard = EnvVarGuard::set(DD_API_KEY_ENV_KEY, "env-api-key");
        let _dd_api_key_file_guard =
            EnvVarGuard::set(DD_API_KEY_FILE_ENV_KEY, path.to_str().unwrap());

        assert_eq!(dd_api_key().as_deref(), Some("env-api-key"));

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_resolve_dd_api_key_ignores_unreadable_file() {
        let _lock = lock_env();
        let path = temp_file_path("missing-file");
        std::fs::remove_file(&path).ok();

        let _dd_api_key_guard = EnvVarGuard::remove(DD_API_KEY_ENV_KEY);
        let _dd_api_key_file_guard =
            EnvVarGuard::set(DD_API_KEY_FILE_ENV_KEY, path.to_str().unwrap());

        assert_eq!(dd_api_key(), None);
    }
}
