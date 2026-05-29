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

/// Normalizes a Datadog site URL by stripping the optional `https://` scheme
/// and trailing slash, and resolving bare-domain aliases (e.g. `datadoghq.com`
/// → `app.datadoghq.com`) the same way the Datadog Agent does.
///
/// See <https://docs.datadoghq.com/agent/troubleshooting/site/?site=us>.
pub fn normalize_site_url(site: &str) -> String {
    let site_no_scheme = site.strip_prefix("https://").unwrap_or(site);
    let site_no_scheme_no_slash = site_no_scheme.strip_suffix('/').unwrap_or(site_no_scheme);
    let normalized = match site_no_scheme_no_slash {
        "datadoghq.com" => "app.datadoghq.com",
        "datadoghq.eu" => "app.datadoghq.eu",
        // we hardly care about fed, but let's have it for completeness
        "ddog-gov.com" => "app.ddog-gov.com",
        site => site,
    };
    normalized.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_site_url_resolves_bare_domain() {
        assert_eq!(normalize_site_url("datadoghq.com"), "app.datadoghq.com");
        assert_eq!(normalize_site_url("datadoghq.eu"), "app.datadoghq.eu");
        assert_eq!(normalize_site_url("ddog-gov.com"), "app.ddog-gov.com");
    }

    #[test]
    fn test_normalize_site_url_strips_scheme_and_trailing_slash() {
        assert_eq!(
            normalize_site_url("https://datadoghq.com"),
            "app.datadoghq.com"
        );
        assert_eq!(
            normalize_site_url("https://datadoghq.com/"),
            "app.datadoghq.com"
        );
        assert_eq!(normalize_site_url("datadoghq.com/"), "app.datadoghq.com");
    }

    #[test]
    fn test_normalize_site_url_preserves_regional_subdomain() {
        assert_eq!(normalize_site_url("us5.datadoghq.com"), "us5.datadoghq.com");
        assert_eq!(
            normalize_site_url("https://us5.datadoghq.com/"),
            "us5.datadoghq.com"
        );
    }

    #[test]
    fn test_normalize_site_url_preserves_staging() {
        assert_eq!(normalize_site_url("datad0g.com"), "datad0g.com");
        assert_eq!(normalize_site_url("dd.datad0g.com"), "dd.datad0g.com");
    }
}
