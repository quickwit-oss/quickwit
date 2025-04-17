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

use std::collections::HashMap;
use std::io::BufRead;

use anyhow::{Context, Result, bail};
use new_string_template::template::Template;
use once_cell::sync::Lazy;
use regex::Regex;
use tracing::debug;

// Matches `${value}` if value is formatted as:
// `ENV_VAR` or `ENV_VAR:DEFAULT`
// Ignores whitespaces in curly braces
static TEMPLATE_ENV_VAR_CAPTURE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\$\{\s*([A-Za-z0-9_]+)\s*(?::\-\s*([^\s\}]+)\s*)?}")
        .expect("regular expression should compile")
});

pub fn render_config(config_content: &[u8]) -> Result<String> {
    let template_str = std::str::from_utf8(config_content)
        .context("config file contains invalid UTF-8 characters")?;

    let mut values = HashMap::new();

    for (line_no, line_result) in config_content.lines().enumerate() {
        let line = line_result?;

        for captures in TEMPLATE_ENV_VAR_CAPTURE.captures_iter(&line) {
            let env_var_key = captures
                .get(1)
                .expect("captures should always have at least one match")
                .as_str();
            let substitution_value = {
                if line.trim_start().starts_with('#') {
                    debug!(
                        env_var_name=%env_var_key,
                        "config file line #{line_no} is commented out, skipping"
                    );
                    // This line is commented out, return the line as is.
                    captures
                        .get(0)
                        .expect("0th capture should always be set")
                        .as_str()
                        .to_string()
                } else if let Ok(env_var_value) = std::env::var(env_var_key) {
                    debug!(
                        env_var_name=%env_var_key,
                        env_var_value=%env_var_value,
                        "environment variable is set, substituting with environment variable value"
                    );
                    env_var_value
                } else if let Some(default_match) = captures.get(2) {
                    let default_value = default_match.as_str().to_string();
                    debug!(
                        env_var_name=%env_var_key,
                        default_value=%default_value,
                        "environment variable is not set, substituting with default value"
                    );
                    default_value
                } else {
                    bail!(
                        "failed to render config file template: environment variable \
                         `{env_var_key}` is not set and no default value is provided"
                    );
                }
            };
            values.insert(env_var_key.to_string(), substitution_value);
        }
    }
    let template = Template::new(template_str).with_regex(&TEMPLATE_ENV_VAR_CAPTURE);
    let rendered = template
        .render_string(&values)
        .context("failed to render config file template")?;
    Ok(rendered)
}

#[cfg(test)]
mod test {
    use std::env;

    use super::render_config;

    #[test]
    fn test_template_render() {
        // SAFETY: this test may not be entirely sound if not run with nextest or --test-threads=1
        // as this is only a test, and it would be extremly inconvenient to run it in a different
        // way, we are keeping it that way

        let config_content = b"metastore_uri: ${TEST_TEMPLATE_RENDER_ENV_VAR_PLEASE_DONT_NOTICE}";
        unsafe {
            env::set_var(
                "TEST_TEMPLATE_RENDER_ENV_VAR_PLEASE_DONT_NOTICE",
                "s3://test-bucket/metastore",
            )
        };
        let rendered = render_config(config_content).unwrap();
        unsafe { std::env::remove_var("TEST_TEMPLATE_RENDER_ENV_VAR_PLEASE_DONT_NOTICE") };
        assert_eq!(rendered, "metastore_uri: s3://test-bucket/metastore");
    }

    #[test]
    fn test_template_render_supports_whitespaces() {
        // SAFETY: this test may not be entirely sound if not run with nextest or --test-threads=1
        // as this is only a test, and it would be extremly inconvenient to run it in a different
        // way, we are keeping it that way

        unsafe {
            env::set_var(
                "TEST_TEMPLATE_RENDER_WHITESPACE_QW_TEST",
                "s3://test-bucket/metastore",
            )
        };
        {
            let config_content = b"metastore_uri: ${  TEST_TEMPLATE_RENDER_WHITESPACE_QW_TEST  }";
            let rendered = render_config(config_content).unwrap();
            assert_eq!(rendered, "metastore_uri: s3://test-bucket/metastore");
        }
    }

    #[test]
    fn test_template_render_with_default_value() {
        {
            let config_content =
                b"metastore_uri: ${QW_ENV_VAR_DOES_NOT_EXIST:-s3://test-bucket/metastore}";
            let rendered = render_config(config_content).unwrap();
            assert_eq!(rendered, "metastore_uri: s3://test-bucket/metastore");
        }
        {
            let config_content =
                b"metastore_uri: ${  QW_ENV_VAR_DOES_NOT_EXIST  :-  s3://test-bucket/metastore  }";
            let rendered = render_config(config_content).unwrap();
            assert_eq!(rendered, "metastore_uri: s3://test-bucket/metastore");
        }
    }

    #[test]
    fn test_template_render_should_panic() {
        let config_content = b"metastore_uri: ${QW_ENV_VAR_DOES_NOT_EXIST}";
        render_config(config_content).unwrap_err();
    }

    #[test]
    fn test_template_render_with_default_use_env() {
        // SAFETY: this test may not be entirely sound if not run with nextest or --test-threads=1
        // as this is only a test, and it would be extremly inconvenient to run it in a different
        // way, we are keeping it that way

        let config_content =
            b"metastore_uri: ${TEST_TEMPLATE_RENDER_ENV_VAR_DEFAULT_USE_ENV:-s3://test-bucket/wrongbucket}";
        unsafe {
            env::set_var(
                "TEST_TEMPLATE_RENDER_ENV_VAR_DEFAULT_USE_ENV",
                "s3://test-bucket/metastore",
            )
        };
        let rendered = render_config(config_content).unwrap();
        unsafe { std::env::remove_var("TEST_TEMPLATE_RENDER_ENV_VAR_DEFAULT_USE_ENV") };
        assert_eq!(rendered, "metastore_uri: s3://test-bucket/metastore");
    }

    #[test]
    fn test_template_render_with_multiple_vars_per_line() {
        // SAFETY: this test may not be entirely sound if not run with nextest or --test-threads=1
        // as this is only a test, and it would be extremly inconvenient to run it in a different
        // way, we are keeping it that way

        let config_content =
            b"metastore_uri: s3://${RENDER_MULTIPLE_BUCKET}/${RENDER_MULTIPLE_PREFIX:-index}#polling_interval=${RENDER_MULTIPLE_INTERVAL}s";
        unsafe {
            env::set_var("RENDER_MULTIPLE_BUCKET", "test-bucket");
            env::set_var("RENDER_MULTIPLE_PREFIX", "metastore");
            env::set_var("RENDER_MULTIPLE_INTERVAL", "30");
        }
        let rendered = render_config(config_content).unwrap();
        unsafe {
            std::env::remove_var("RENDER_MULTIPLE_BUCKET");
            std::env::remove_var("RENDER_MULTIPLE_PREFIX");
            std::env::remove_var("RENDER_MULTIPLE_INTERVAL");
        }
        assert_eq!(
            rendered,
            "metastore_uri: s3://test-bucket/metastore#polling_interval=30s"
        );
    }

    #[test]
    fn test_template_render_ignores_commented_lines() {
        {
            let config_content = b"# metastore_uri: ${QW_ENV_VAR_DOES_NOT_EXIST}";
            let rendered = render_config(config_content).unwrap();
            assert_eq!(rendered, "# metastore_uri: ${QW_ENV_VAR_DOES_NOT_EXIST}");
        }
        {
            let config_content =
                b" # metastore_uri: ${ QW_ENV_VAR_DOES_NOT_EXIST :- default-value }";
            let rendered = render_config(config_content).unwrap();
            assert_eq!(
                rendered,
                " # metastore_uri: ${ QW_ENV_VAR_DOES_NOT_EXIST :- default-value }"
            );
        }
    }
}
