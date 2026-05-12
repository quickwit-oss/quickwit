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

use super::LogRouter;

/// Mock document structure for testing without quickwit-processing dependency.
/// Contains tags (flat key-value) and custom fields (nested JSON-like structure).
#[derive(Default)]
struct MockDoc {
    tags: HashMap<String, String>,
    custom_fields: HashMap<Vec<String>, String>,
}

impl MockDoc {
    fn new() -> Self {
        Self::default()
    }

    fn tag(mut self, key: &str, value: &str) -> Self {
        self.tags.insert(key.to_string(), value.to_string());
        self
    }

    fn custom_field(mut self, path: &[&str], value: &str) -> Self {
        let path_vec = path.iter().map(|s| s.to_string()).collect();
        self.custom_fields.insert(path_vec, value.to_string());
        self
    }

    fn get_tag(&self, key: &str) -> Option<&str> {
        self.tags.get(key).map(|s| s.as_str())
    }

    fn get_custom_field(&self, path: &[String]) -> Option<&str> {
        self.custom_fields.get(path).map(|s| s.as_str())
    }

    fn route<'a>(&'a self, router: &'a LogRouter) -> Option<&'a str> {
        router.resolve_index(&|key| self.get_tag(key), &|path| {
            self.get_custom_field(path)
        })
    }
}

/// Creates a Vec<IndexRoutingRule> from filter:index_id pairs.
macro_rules! routing_rules {
    ($($filter:expr => $index_id:expr),* $(,)?) => {
        vec![
            $(
                quickwit_proto::metastore::IndexRoutingRule {
                    filter: $filter.to_string(),
                    index_id: $index_id.to_string(),
                },
            )*
        ]
    };
}

#[test]
fn test_pattern_exact() {
    let router = LogRouter::create_from_rules(routing_rules![
        "service:api-gateway" => "exact-match",
        "service:*" => "wildcard",
        "*" => "default",
    ])
    .unwrap();

    assert_eq!(
        MockDoc::new().tag("service", "api-gateway").route(&router),
        Some("exact-match")
    );
    assert_eq!(
        MockDoc::new().tag("service", "api-gateways").route(&router),
        Some("wildcard")
    );
    assert_eq!(
        MockDoc::new().tag("service", "api").route(&router),
        Some("wildcard")
    );
    assert_eq!(MockDoc::new().route(&router), Some("default"));
}

#[test]
fn test_pattern_prefix() {
    let router = LogRouter::create_from_rules(routing_rules![
        "service:payment-*" => "prefix-match",
        "*" => "default",
    ])
    .unwrap();

    assert_eq!(
        MockDoc::new()
            .tag("service", "payment-processor")
            .route(&router),
        Some("prefix-match")
    );
    assert_eq!(
        MockDoc::new()
            .tag("service", "payment-gateway")
            .route(&router),
        Some("prefix-match")
    );
    assert_eq!(
        MockDoc::new().tag("service", "payment").route(&router),
        Some("default")
    );
    assert_eq!(
        MockDoc::new().tag("service", "my-payment").route(&router),
        Some("default")
    );
}

#[test]
fn test_pattern_suffix() {
    let router = LogRouter::create_from_rules(routing_rules![
        "service:*-service" => "suffix-match",
        "*" => "default",
    ])
    .unwrap();

    assert_eq!(
        MockDoc::new().tag("service", "auth-service").route(&router),
        Some("suffix-match")
    );
    assert_eq!(
        MockDoc::new().tag("service", "user-service").route(&router),
        Some("suffix-match")
    );
    assert_eq!(
        MockDoc::new().tag("service", "-service").route(&router),
        Some("suffix-match")
    );
    assert_eq!(
        MockDoc::new().tag("service", "service").route(&router),
        Some("default")
    );
    assert_eq!(
        MockDoc::new()
            .tag("service", "my-service-extra")
            .route(&router),
        Some("default")
    );
}

#[test]
fn test_pattern_contains() {
    let router = LogRouter::create_from_rules(routing_rules![
        "service:*-api-*" => "contains-match",
        "*" => "default",
    ])
    .unwrap();

    assert_eq!(
        MockDoc::new()
            .tag("service", "web-api-gateway")
            .route(&router),
        Some("contains-match")
    );
    assert_eq!(
        MockDoc::new()
            .tag("service", "mobile-api-service")
            .route(&router),
        Some("contains-match")
    );
    assert_eq!(
        MockDoc::new().tag("service", "-api-").route(&router),
        Some("contains-match")
    );
    assert_eq!(
        MockDoc::new().tag("service", "api").route(&router),
        Some("default")
    );
    assert_eq!(
        MockDoc::new().tag("service", "web-gateway").route(&router),
        Some("default")
    );
}

#[test]
fn test_weird_keys() {
    let router = LogRouter::create_from_rules(routing_rules![
        r"@app\.version:1.0.0" => "escaped-dot-key",
        r"@trace\.span\.id:abc" => "multi-escaped-dots",
        r"@metadata.file\.name:test.txt" => "nested-escaped-dot",
        "@café.name:value" => "unicode-attr-key",
        "@用户.id:123" => "chinese-attr-key",
        "@user.email-address:test" => "dash-in-key",
        "@user.first_name:test" => "underscore-in-key",
        "*" => "default",
    ])
    .unwrap();

    assert_eq!(
        MockDoc::new()
            .custom_field(&["app.version"], "1.0.0")
            .route(&router),
        Some("escaped-dot-key")
    );
    assert_eq!(
        MockDoc::new()
            .custom_field(&["app", "version"], "1.0.0")
            .route(&router),
        Some("default")
    );
    assert_eq!(
        MockDoc::new()
            .custom_field(&["trace.span.id"], "abc")
            .route(&router),
        Some("multi-escaped-dots")
    );
    assert_eq!(
        MockDoc::new()
            .custom_field(&["metadata", "file.name"], "test.txt")
            .route(&router),
        Some("nested-escaped-dot")
    );
    assert_eq!(
        MockDoc::new()
            .custom_field(&["café", "name"], "value")
            .route(&router),
        Some("unicode-attr-key")
    );
    assert_eq!(
        MockDoc::new()
            .custom_field(&["用户", "id"], "123")
            .route(&router),
        Some("chinese-attr-key")
    );
    assert_eq!(
        MockDoc::new()
            .custom_field(&["user", "email-address"], "test")
            .route(&router),
        Some("dash-in-key")
    );
    assert_eq!(
        MockDoc::new()
            .custom_field(&["user", "first_name"], "test")
            .route(&router),
        Some("underscore-in-key")
    );
}

#[test]
fn test_weird_tag_values() {
    let router = LogRouter::create_from_rules(routing_rules![
        "service:" => "empty-value",
        r#"service:my\ service\ name"# => "spaces-value",
        r#"hostname:server@prod-01"# => "at-sign",
        r#"ddsource:app#v1.0"# => "hash",
        r#"service:payment$processor"# => "dollar",
        r#"ddsource:nginx/1.0"# => "slash",
        r#"service:auth::v2"# => "double-colon",
        "service:José's-Service" => "unicode-tag",
        "hostname:北京-server" => "chinese-tag",
        "service:café-api" => "accented-tag",
        "*" => "default",
    ])
    .unwrap();

    assert_eq!(
        MockDoc::new().tag("service", "").route(&router),
        Some("empty-value")
    );
    assert_eq!(
        MockDoc::new()
            .tag("service", "my service name")
            .route(&router),
        Some("spaces-value")
    );
    assert_eq!(
        MockDoc::new()
            .tag("hostname", "server@prod-01")
            .route(&router),
        Some("at-sign")
    );
    assert_eq!(
        MockDoc::new().tag("ddsource", "app#v1.0").route(&router),
        Some("hash")
    );
    assert_eq!(
        MockDoc::new()
            .tag("service", "payment$processor")
            .route(&router),
        Some("dollar")
    );
    assert_eq!(
        MockDoc::new().tag("ddsource", "nginx/1.0").route(&router),
        Some("slash")
    );
    assert_eq!(
        MockDoc::new().tag("service", "auth::v2").route(&router),
        Some("double-colon")
    );
    assert_eq!(
        MockDoc::new()
            .tag("service", "José's-Service")
            .route(&router),
        Some("unicode-tag")
    );
    assert_eq!(
        MockDoc::new().tag("hostname", "北京-server").route(&router),
        Some("chinese-tag")
    );
    assert_eq!(
        MockDoc::new().tag("service", "café-api").route(&router),
        Some("accented-tag")
    );
}

#[test]
fn test_nesting_levels() {
    let router = LogRouter::create_from_rules(routing_rules![
        "@status:active" => "level-1",
        "@request.headers.host:example.com" => "level-3",
        "@metrics.perf.http.response.time:100ms" => "level-5",
        "*" => "default",
    ])
    .unwrap();

    assert_eq!(
        MockDoc::new()
            .custom_field(&["status"], "active")
            .route(&router),
        Some("level-1")
    );
    assert_eq!(
        MockDoc::new()
            .custom_field(&["request", "headers", "host"], "example.com")
            .route(&router),
        Some("level-3")
    );
    assert_eq!(
        MockDoc::new()
            .custom_field(&["metrics", "perf", "http", "response", "time"], "100ms")
            .route(&router),
        Some("level-5")
    );
    assert_eq!(
        MockDoc::new()
            .custom_field(&["metrics.perf.http.response.time"], "100ms")
            .route(&router),
        Some("default")
    );
}

#[test]
fn test_escaped_dots() {
    let router = LogRouter::create_from_rules(routing_rules![
        r"@app\.version:1.0.0" => "escaped-top-level",
        r"@metadata.file\.name:test.txt" => "escaped-nested",
        "*" => "default",
    ])
    .unwrap();

    assert_eq!(
        MockDoc::new()
            .custom_field(&["app.version"], "1.0.0")
            .route(&router),
        Some("escaped-top-level")
    );
    assert_eq!(
        MockDoc::new()
            .custom_field(&["app", "version"], "1.0.0")
            .route(&router),
        Some("default")
    );
    assert_eq!(
        MockDoc::new()
            .custom_field(&["metadata", "file.name"], "test.txt")
            .route(&router),
        Some("escaped-nested")
    );
}

#[test]
fn test_operator_or() {
    let router = LogRouter::create_from_rules(routing_rules![
        "service:auth OR service:login" => "or-match",
        "*" => "default",
    ])
    .unwrap();

    assert_eq!(
        MockDoc::new().tag("service", "auth").route(&router),
        Some("or-match")
    );
    assert_eq!(
        MockDoc::new().tag("service", "login").route(&router),
        Some("or-match")
    );
    assert_eq!(
        MockDoc::new().tag("service", "other").route(&router),
        Some("default")
    );
}

#[test]
fn test_operator_and() {
    let router = LogRouter::create_from_rules(routing_rules![
        "service:api status:error" => "and-match",
        "*" => "default",
    ])
    .unwrap();

    assert_eq!(
        MockDoc::new()
            .tag("service", "api")
            .tag("status", "error")
            .route(&router),
        Some("and-match")
    );
    assert_eq!(
        MockDoc::new()
            .tag("service", "api")
            .tag("status", "warning")
            .route(&router),
        Some("default")
    );
    assert_eq!(
        MockDoc::new()
            .tag("service", "other")
            .tag("status", "error")
            .route(&router),
        Some("default")
    );
}

#[test]
fn test_operator_not() {
    let router = LogRouter::create_from_rules(routing_rules![
        "service:* -service:test" => "not-match",
        "*" => "default",
    ])
    .unwrap();

    assert_eq!(
        MockDoc::new().tag("service", "production").route(&router),
        Some("not-match")
    );
    assert_eq!(
        MockDoc::new().tag("service", "staging").route(&router),
        Some("not-match")
    );
    assert_eq!(
        MockDoc::new().tag("service", "test").route(&router),
        Some("default")
    );
}

#[test]
fn test_operator_combined() {
    let router = LogRouter::create_from_rules(routing_rules![
        "(service:api OR service:web) status:error" => "or-and",
        "service:backend -(status:debug OR status:trace)" => "and-not-or",
        "service:db hostname:prod-* -@db.replica:true" => "and-and-not",
        "*" => "default",
    ])
    .unwrap();

    assert_eq!(
        MockDoc::new()
            .tag("service", "api")
            .tag("status", "error")
            .route(&router),
        Some("or-and")
    );
    assert_eq!(
        MockDoc::new()
            .tag("service", "web")
            .tag("status", "error")
            .route(&router),
        Some("or-and")
    );
    assert_eq!(
        MockDoc::new()
            .tag("service", "api")
            .tag("status", "warning")
            .route(&router),
        Some("default")
    );
    assert_eq!(
        MockDoc::new()
            .tag("service", "backend")
            .tag("status", "error")
            .route(&router),
        Some("and-not-or")
    );
    assert_eq!(
        MockDoc::new()
            .tag("service", "backend")
            .tag("status", "debug")
            .route(&router),
        Some("default")
    );
    assert_eq!(
        MockDoc::new()
            .tag("service", "db")
            .tag("hostname", "prod-db-01")
            .custom_field(&["db", "replica"], "false")
            .route(&router),
        Some("and-and-not")
    );
    assert_eq!(
        MockDoc::new()
            .tag("service", "db")
            .tag("hostname", "prod-db-01")
            .custom_field(&["db", "replica"], "true")
            .route(&router),
        Some("default")
    );
}

#[test]
fn test_pattern_or() {
    let router = LogRouter::create_from_rules(routing_rules![
        "service:(api OR web)" => "or-pattern",
        "*" => "default",
    ])
    .unwrap();

    assert_eq!(
        MockDoc::new().tag("service", "api").route(&router),
        Some("or-pattern")
    );
    assert_eq!(
        MockDoc::new().tag("service", "web").route(&router),
        Some("or-pattern")
    );
    assert_eq!(
        MockDoc::new().tag("service", "database").route(&router),
        Some("default")
    );
}

#[test]
fn test_pattern_and() {
    let router = LogRouter::create_from_rules(routing_rules![
        "service:(prod* AND *gateway)" => "and-pattern",
        "*" => "default",
    ])
    .unwrap();

    assert_eq!(
        MockDoc::new()
            .tag("service", "prod-api-gateway")
            .route(&router),
        Some("and-pattern")
    );
    assert_eq!(
        MockDoc::new().tag("service", "prod-gateway").route(&router),
        Some("and-pattern")
    );
    assert_eq!(
        MockDoc::new().tag("service", "prod-api").route(&router),
        Some("default")
    );
    assert_eq!(
        MockDoc::new().tag("service", "gateway").route(&router),
        Some("default")
    );
}

#[test]
fn test_pattern_not() {
    let router = LogRouter::create_from_rules(routing_rules![
        "service:(NOT test*)" => "not-pattern",
        "*" => "default",
    ])
    .unwrap();

    assert_eq!(
        MockDoc::new().tag("service", "api").route(&router),
        Some("not-pattern")
    );
    assert_eq!(
        MockDoc::new().tag("service", "production").route(&router),
        Some("not-pattern")
    );
    assert_eq!(
        MockDoc::new().tag("service", "test-api").route(&router),
        Some("default")
    );
    assert_eq!(
        MockDoc::new().tag("service", "testing").route(&router),
        Some("default")
    );
}

#[test]
fn test_pattern_combined() {
    let router = LogRouter::create_from_rules(routing_rules![
        "service:api-gateway" => "exact",
        "service:((*prod* OR *dev*) AND NOT *test*)" => "complex-pattern",
        "service:(*api* AND *gateway*)" => "and-pattern",
        "service:prod-*" => "prefix",
        "service:*-service" => "suffix",
        "service:*" => "wildcard",
        "*" => "default",
    ])
    .unwrap();

    assert_eq!(
        MockDoc::new().tag("service", "api-gateway").route(&router),
        Some("exact")
    );
    assert_eq!(
        MockDoc::new().tag("service", "prod-web").route(&router),
        Some("complex-pattern")
    );
    assert_eq!(
        MockDoc::new().tag("service", "dev-api").route(&router),
        Some("complex-pattern")
    );
    assert_eq!(
        MockDoc::new()
            .tag("service", "prod-test-web")
            .route(&router),
        Some("prefix")
    );
    assert_eq!(
        MockDoc::new()
            .tag("service", "web-api-gateway")
            .route(&router),
        Some("and-pattern")
    );
    assert_eq!(
        MockDoc::new().tag("service", "auth-service").route(&router),
        Some("suffix")
    );
    assert_eq!(
        MockDoc::new()
            .tag("service", "anything-else")
            .route(&router),
        Some("wildcard")
    );
    assert_eq!(MockDoc::new().route(&router), Some("default"));
}
