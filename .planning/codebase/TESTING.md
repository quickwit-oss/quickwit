# Testing Patterns

**Analysis Date:** 2026-01-22

## Test Framework

**Runner:**
- Rust built-in `test` framework with `#[test]` attribute
- `tokio` runtime for async tests: `#[tokio::test]` attribute
- Config: None required; built into cargo

**Assertion Library:**
- Rust standard library `assert_eq!`, `assert_ne!`, `assert!` macros
- Standard pattern: `assert_eq!(actual, expected)` with optional message

**Run Commands:**
```bash
cargo test                          # Run all tests
cargo test -p <crate-name>         # Test specific crate
cargo test -- --test-threads=1    # Single-threaded (for serial tests)
cargo test --doc                   # Run doc tests
cargo test -- --nocapture          # Show print output
cargo test <test_name>              # Run specific test
```

## Test File Organization

**Location:**
- Co-located: Tests in same file within `#[cfg(test)]` module (preferred pattern)
- Separate: `tests/` directory at crate root for integration tests
- Both patterns used; see examples in `quickwit-common/src/`, `quickwit-proto/src/`

**Naming:**
- Test files: lowercase snake_case ending in `_test.rs`
- Test functions: `test_<description>()` pattern
- Examples: `test_single_value()`, `test_doc_uid_json_serde_roundtrip()`, `test_position_ord()`

**Structure (typical):**
```
src/
├── lib.rs
├── module.rs              # Contains:
│   ├── fn my_function()   //   #[cfg(test)]
│   └── #[cfg(test)]       //   mod tests {
│       mod tests {        //       #[test] fn test_...
│           ...           //   }
│       }
└── tests/ (integration)
    ├── lib.rs
    └── integration_test.rs
```

## Test Structure

**Suite Organization:**
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::other_module::*;

    #[test]
    fn test_basic_scenario() {
        // Arrange
        let input = vec!["color:blue".to_string(), "size:medium".to_string()];

        // Act
        let result = convert_tags(&input);

        // Assert
        assert_eq!(
            result.get("color"),
            Some(&StringOrVec::String("blue".to_string()))
        );
    }
}
```

**Patterns:**
- Setup: Create inputs and test data at beginning
- Action: Call function/method being tested
- Assertion: Use `assert_eq!`, `assert!` to verify results
- Naming: Descriptive test names indicate what is being tested

## Mocking

**Framework:** `mockall` crate for struct/trait mocking

**Dependencies:**
- `mockall = "0.11"` in workspace dependencies
- Enable via `testsuite` feature in crates that use it
- Example in `quickwit-proto/Cargo.toml`: `testsuite = ["mockall", "futures"]`

**Patterns from codebase:**
```rust
use mockall::Sequence;

#[test]
fn test_with_mock() {
    let mut mock = MockService::new();
    mock.expect_method()
        .times(1)
        .returning(|| Ok(result));
    // Use mock in test
}
```

**HTTP Mocking:** `wiremock` crate for HTTP endpoints

**Usage pattern:**
```rust
#[tokio::test]
async fn test_search_endpoint() {
    let mock_server = MockServer::start().await;
    let server_url = Url::parse(&mock_server.uri()).unwrap();
    let qw_client = QuickwitClientBuilder::new(server_url).build();

    // Configure mock response
    Mock::given(method("POST"))
        .and(path("/api/v1/search"))
        .respond_with(ResponseTemplate::new(200).set_body_json(...))
        .mount(&mock_server)
        .await;

    // Test client behavior
    let response = qw_client.search(...).await;
    assert_eq!(response.num_hits, 0);
}
```

**What to Mock:**
- External HTTP services: Use `wiremock`
- Complex traits/structs: Use `mockall` with `#[automock]`
- Expensive operations: Use mocks to avoid actual I/O

**What NOT to Mock:**
- Simple data structures: Test with real values
- Core business logic: Test actual implementation
- Serialization/deserialization: Use roundtrip tests instead

## Fixtures and Factories

**Test Data Creation:**
- Helper functions create test instances
- Factory pattern for complex objects
- Examples from codebase:
  ```rust
  // In tests module
  fn create_test_input() -> Vec<String> {
      vec!["color:blue".to_string(), "color:red".to_string()]
  }

  // For builders
  QuickwitClientBuilder::new(url)
      .connect_timeout(Timeout::from_secs(5))
      .timeout(Timeout::from_secs(10))
      .build()
  ```

**Location:**
- Inline in test module (preferred for small tests)
- Separate `fixtures.rs` module (for shared test data)
- Builder pattern on types themselves

## Coverage

**Requirements:** Not enforced; development team assesses on case-by-case basis

**View Coverage:**
```bash
# Requires tarpaulin or llvm-cov installed
cargo tarpaulin --out Html
# or
cargo llvm-cov --html
```

## Test Types

**Unit Tests:**
- Scope: Single function or small module
- Approach: Test with various inputs, edge cases, error conditions
- Location: Co-located in `#[cfg(test)]` modules within source files
- Examples: `quickwit-doc-transforms/src/flatten_tags.rs` contains `test_single_value()`, `test_multi_value()`

**Integration Tests:**
- Scope: Multiple components working together
- Approach: Test roundtrip operations (ingest→process→query→verify)
- Location: Separate `tests/` directories or `*_test.rs` files
- Examples: `quickwit-integration-tests/src/tests/basic_tests.rs`, `quickwit-indexing/tests/metrics_infra_e2e_test.rs`

**E2E Tests:**
- Framework: Actor system tests using `quickwit-actors`
- Approach: Full pipeline tests with all components
- Location: Separate files like `quickwit-indexing/src/actors/metrics_e2e_test.rs`
- Examples in codebase:
  - `quickwit-integration-tests/src/tests/ingest_v1_tests.rs`
  - `quickwit-integration-tests/src/tests/otlp_tests.rs`
  - `quickwit-indexing/src/actors/metrics_e2e_test.rs`

## Common Patterns

**Async Testing:**
```rust
#[tokio::test]
async fn test_client_no_server() {
    let port = quickwit_common::net::find_available_tcp_port().unwrap();
    let server_url = Url::parse(&format!("http://127.0.0.1:{port}")).unwrap();
    let qw_client = QuickwitClientBuilder::new(server_url).build();
    let error = qw_client.indexes().list().await.unwrap_err();
    assert!(matches!(error, Error::Middleware(_)));
}
```

**Roundtrip Tests (Serialization):**
```rust
#[test]
fn test_doc_uid_json_serde_roundtrip() {
    let doc_uid = DocUid::default();
    let serialized = serde_json::to_string(&doc_uid).unwrap();
    let deserialized: DocUid = serde_json::from_str(&serialized).unwrap();
    assert_eq!(deserialized, doc_uid);
}

#[test]
fn test_doc_uid_prost_serde_roundtrip() {
    let doc_uid = DocUid::random();
    let encoded = doc_uid.encode_to_vec();
    assert_eq!(DocUid::decode(Bytes::from(encoded)).unwrap(), doc_uid);
}
```

**Error Testing:**
```rust
#[test]
fn test_parse_invalid_input() {
    let tag = "invalid";  // Missing colon separator
    let result = TagKV::parse_tag(tag);
    assert_eq!(result, None);
}
```

**Property-Based Testing:**
```rust
#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_property(val in any::<u64>()) {
            // Property must hold for all generated values
            assert!(val >= 0);
        }
    }
}
```

**Serial Test Execution:**
```rust
#[serial_test::file_serial]
#[tokio::test]
async fn test_file_access() {
    // This test runs serially to avoid file conflicts
    // Used in quickwit-metastore tests
}
```

**Test Constants:**
```rust
// Workspace-level test utilities
#[cfg(any(test, feature = "testsuite"))]
pub mod test_utils;

// In tests, features gate test code
#[cfg(any(test, feature = "testsuite"))]
impl Default for RateLimiterSettings {
    fn default() -> Self { ... }
}
```

## Test Statistics

- **Framework:** Rust built-in + tokio for async
- **Mocking:** mockall (trait mocking), wiremock (HTTP mocking)
- **Property testing:** proptest library
- **Test count:** Varies by crate
  - `quickwit-doc-transforms`: 8+ tests
  - `quickwit-proto/src/types`: 10+ roundtrip tests
  - `quickwit-integration-tests`: 50+ E2E tests

## Example: Complete Test Module

From `quickwit-doc-transforms/src/flatten_tags.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_value() {
        let input = vec!["color:blue".to_string(), "size:medium".to_string()];
        let result = convert_tags(&input);

        assert_eq!(
            result.get("color"),
            Some(&StringOrVec::String("blue".to_string()))
        );
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_multi_value() {
        let input = vec![
            "color:blue".to_string(),
            "color:red".to_string(),
            "size:medium".to_string(),
        ];
        let result = convert_tags(&input);

        // Multi-value check: same key appears twice
        assert_eq!(
            result.get("color"),
            Some(&StringOrVec::Vec(vec![
                "blue".to_string(),
                "red".to_string(),
            ]))
        );
        assert_eq!(result.len(), 2);
    }
}
```

---

*Testing analysis: 2026-01-22*
