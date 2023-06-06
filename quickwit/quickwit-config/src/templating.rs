// Copyright (C) 2023 Quickwit, Inc.
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

use std::collections::HashMap;
use std::io::BufRead;

use anyhow::{bail, Context, Result};
use new_string_template::template::Template;
use once_cell::sync::Lazy;
use regex::Regex;
use tracing::debug;

// Matches ${value} if value is in format of:
// ENV_VAR or ENV_VAR:DEFAULT
// Ignores whitespaces in curly braces
static TEMPLATE_ENV_VAR_CAPTURE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\$\{\s*([A-Za-z0-9_]+)\s*(?::\-\s*([\S]+)\s*)?}")
        .expect("The regular expression should compile.")
});

pub fn render_config(config_content: &[u8]) -> Result<String> {
    let template_str = std::str::from_utf8(config_content)
        .context("Config file contains invalid UTF-8 characters.")?;

    let mut values = HashMap::new();

    for (line_no, line_res) in config_content.lines().enumerate() {
        let line = line_res?;

        for captures in TEMPLATE_ENV_VAR_CAPTURE.captures_iter(&line) {
            let env_var_key = captures
                .get(1)
                .expect("Captures should always have at least one match.")
                .as_str();
            let substitution_value = {
                if line.trim_start().starts_with('#') {
                    debug!(
                        env_var_name=%env_var_key,
                        "Config file line #{line_no} is commented out, skipping."
                    );
                    // This line is commented out, return the line as is.
                    captures
                        .get(0)
                        .expect("The 0th capture should aways be set.")
                        .as_str()
                        .to_string()
                } else if let Ok(env_var_value) = std::env::var(env_var_key) {
                    debug!(
                        env_var_name=%env_var_key,
                        env_var_value=%env_var_value,
                        "Environment variable is set, substituting with environment variable value."
                    );
                    env_var_value
                } else if let Some(default_match) = captures.get(2) {
                    let default_value = default_match.as_str().to_string();
                    debug!(
                        env_var_name=%env_var_key,
                        default_value=%default_value,
                        "Environment variable is not set, substituting with default value."
                    );
                    default_value
                } else {
                    bail!(
                        "Failed to render config file template: environment variable \
                         `{env_var_key}` is not set and no default value is provided."
                    );
                }
            };
            values.insert(env_var_key.to_string(), substitution_value);
        }
    }
    let template = Template::new(template_str).with_regex(&TEMPLATE_ENV_VAR_CAPTURE);
    let rendered = template
        .render_string(&values)
        .context("Failed to render config file template.")?;
    Ok(rendered)
}

#[cfg(test)]
mod test {
    use std::env;

    use super::render_config;

    #[test]
    fn test_template_render() {
        let config_content = b"metastore_uri: ${TEST_TEMPLATE_RENDER_ENV_VAR_PLEASE_DONT_NOTICE}";
        env::set_var(
            "TEST_TEMPLATE_RENDER_ENV_VAR_PLEASE_DONT_NOTICE",
            "s3://test-bucket/metastore",
        );
        let rendered = render_config(config_content).unwrap();
        std::env::remove_var("TEST_TEMPLATE_RENDER_ENV_VAR_PLEASE_DONT_NOTICE");
        assert_eq!(rendered, "metastore_uri: s3://test-bucket/metastore");
    }

    #[test]
    fn test_template_render_supports_whitespaces() {
        env::set_var(
            "TEST_TEMPLATE_RENDER_WHITESPACE_QW_TEST",
            "s3://test-bucket/metastore",
        );
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
        let config_content =
            b"metastore_uri: ${TEST_TEMPLATE_RENDER_ENV_VAR_DEFAULT_USE_ENV:-s3://test-bucket/wrongbucket}";
        env::set_var(
            "TEST_TEMPLATE_RENDER_ENV_VAR_DEFAULT_USE_ENV",
            "s3://test-bucket/metastore",
        );
        let rendered = render_config(config_content).unwrap();
        std::env::remove_var("TEST_TEMPLATE_RENDER_ENV_VAR_DEFAULT_USE_ENV");
        assert_eq!(rendered, "metastore_uri: s3://test-bucket/metastore");
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
