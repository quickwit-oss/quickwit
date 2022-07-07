// Copyright (C) 2022 Quickwit, Inc.
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

use anyhow::{bail, Context, Result};
use new_string_template::template::Template;
use once_cell::sync::Lazy;
use quickwit_common::uri::Uri;
use regex::Regex;
use tracing::debug;

// Matches ${value} if value is in format of:
// ENV_VAR or ENV_VAR:DEFAULT
// Ignores whitespaces in curly braces
static TEMPLATE_ENV_VAR_CAPTURE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\$\{\s*([A-Za-z0-9_]+)(?:(?::\-)([\S]+))?\s*}").expect("Failed to compile regular expression. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.")
});

pub fn render_config_file(contents: &[u8], uri: &Uri) -> Result<String> {
    let contents_as_string = String::from_utf8(contents.to_vec())
        .with_context(|| format!("Config file `{uri}` contains invalid UTF-8 characters."))?;
    let template = Template::new(&contents_as_string).with_regex(&TEMPLATE_ENV_VAR_CAPTURE);
    let mut data = HashMap::new();

    for captured in TEMPLATE_ENV_VAR_CAPTURE.captures_iter(&contents_as_string) {
        let env_var_name = captured.get(1).unwrap().as_str(); // Captures always have at least one match
        let subst_val = {
            if let Ok(env_var_value) = std::env::var(env_var_name) {
                debug!(
                    env_var_name,
                    env_var_value, "Found ENV_VAR: {} with value: {}", env_var_name, env_var_value
                );
                env_var_value
            } else if let Some(default_val) = captured.get(2) {
                let default_val = default_val.as_str();
                debug!(
                    env_var_name,
                    default_val,
                    "Unable to get ENV_VAR specified: {}, Using default value instead: {}",
                    env_var_name,
                    default_val
                );
                default_val.to_string()
            } else {
                bail!(
                    "Couldn't find ENV_VAR: {env_var_name} and the default value for the given \
                     template"
                );
            }
        };
        data.insert(env_var_name, subst_val);
    }

    let rendered = template
        .render(&data)
        .with_context(|| format!("Failed to render templated config file `{uri}`."))?;
    Ok(rendered)
}

#[cfg(test)]
mod test {
    use std::env;

    use once_cell::sync::Lazy;
    use quickwit_common::uri::Uri;

    use super::render_config_file;

    static TEST_URI: Lazy<Uri> = Lazy::new(|| Uri::new("file://config.yaml".to_string()));

    #[test]
    fn test_template_render() {
        let mock_config = b"metastore_uri: ${TEST_TEMPLATE_RENDER_ENV_VAR_PLEASE_DONT_NOTICE}";
        env::set_var(
            "TEST_TEMPLATE_RENDER_ENV_VAR_PLEASE_DONT_NOTICE",
            "s3://test-bucket/metastore",
        );
        let rendered = render_config_file(mock_config, &TEST_URI).unwrap();
        std::env::remove_var("TEST_TEMPLATE_RENDER_ENV_VAR_PLEASE_DONT_NOTICE");
        assert_eq!(rendered, "metastore_uri: s3://test-bucket/metastore");
    }

    #[test]
    fn test_template_render_whitespaces() {
        env::set_var(
            "TEST_TEMPLATE_RENDER_WHITESPACE_QW_TEST",
            "s3://test-bucket/metastore",
        );

        {
            let mock_config = b"metastore_uri: ${TEST_TEMPLATE_RENDER_WHITESPACE_QW_TEST}";
            let rendered = render_config_file(mock_config, &TEST_URI).unwrap();
            assert_eq!(rendered, "metastore_uri: s3://test-bucket/metastore");
        }
        {
            let mock_config = b"metastore_uri: ${TEST_TEMPLATE_RENDER_WHITESPACE_QW_TEST  }";
            let rendered = render_config_file(mock_config, &TEST_URI).unwrap();
            assert_eq!(rendered, "metastore_uri: s3://test-bucket/metastore");
        }
        {
            let mock_config = b"metastore_uri: ${   TEST_TEMPLATE_RENDER_WHITESPACE_QW_TEST}";
            let rendered = render_config_file(mock_config, &TEST_URI).unwrap();
            assert_eq!(rendered, "metastore_uri: s3://test-bucket/metastore");
        }
        {
            let mock_config = b"metastore_uri: ${  TEST_TEMPLATE_RENDER_WHITESPACE_QW_TEST    }";
            let rendered = render_config_file(mock_config, &TEST_URI).unwrap();
            assert_eq!(rendered, "metastore_uri: s3://test-bucket/metastore");
        }
    }

    #[test]
    fn test_template_render_default_value() {
        let mock_config = b"metastore_uri: ${QW_NO_ENV_WITH_THIS_NAME:-s3://test-bucket/metastore}";
        let rendered = render_config_file(mock_config, &TEST_URI).unwrap();
        assert_eq!(rendered, "metastore_uri: s3://test-bucket/metastore");
    }

    #[test]
    fn test_template_render_should_panic() {
        let mock_config = b"metastore_uri: ${QW_NO_ENV_WITH_THIS_NAME}";
        render_config_file(mock_config, &TEST_URI).unwrap_err();
    }

    #[test]
    fn test_template_render_with_default_use_env() {
        let mock_config =
            b"metastore_uri: ${TEST_TEMPLATE_RENDER_ENV_VAR_DEFAULT_USE_ENV:-s3://test-bucket/wrongbucket}";
        env::set_var(
            "TEST_TEMPLATE_RENDER_ENV_VAR_DEFAULT_USE_ENV",
            "s3://test-bucket/metastore",
        );
        let rendered = render_config_file(mock_config, &TEST_URI).unwrap();
        std::env::remove_var("TEST_TEMPLATE_RENDER_ENV_VAR_DEFAULT_USE_ENV");
        assert_eq!(rendered, "metastore_uri: s3://test-bucket/metastore");
        assert_ne!(rendered, "metastore_uri: s3://test-bucket/wrongbucket");
    }
}
