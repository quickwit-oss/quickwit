# Quickwit Rust Style Guide

Supplements [CODE_STYLE.md](../../CODE_STYLE.md) with additional reliability patterns for the Quickwit project. CODE_STYLE.md is the **primary style reference** — this document adds patterns that improve reliability and error detection.

Influences:
- [Quickwit CODE_STYLE.md](../../CODE_STYLE.md) - Primary: proofreadability, naming, hidden contracts
- [Apache DataFusion](https://github.com/apache/datafusion) - Query engine patterns
- [TigerBeetle](https://github.com/tigerbeetle/tigerbeetle) - Assertion discipline for reliability

## Quick Reference

| Pattern | Rule | Source |
|---------|------|--------|
| Error handling | Propagate via `?`, never `unwrap()` in library code | Quickwit |
| Assertions | Use `debug_assert!` to document invariants for proofreading | Quickwit CODE_STYLE + TigerBeetle |
| Iterator style | Prefer procedural loops when error handling makes chains unreadable | Quickwit |
| Naming | Long descriptive names, standard Rust snake_case | Quickwit |
| Hidden contracts | Avoid them; use types/Result/Option or add `debug_assert!` | Quickwit |
| File size | Maximum 500 lines per file; split by responsibility | Project policy |
| Clone avoidance | Prefer `&self` / `Arc<T>`; cloning OK in actor/async code for clarity | Project policy |
| Async safety | No blocking > 500us; forbidden patterns listed below | Quickwit |
| Disallowed methods | `Path::exists`, `Option::is_some_and`, etc. (see `clippy.toml`) | Quickwit |

---

## 1. Error Handling

### Rule: Library code MUST propagate errors, NEVER panic

```rust
// BAD - panics in library code
fn parse_timestamp(nanos: i64) -> NaiveDateTime {
    DateTime::from_timestamp(secs, nsecs).unwrap()  // PANIC!
}

// GOOD - propagates errors
fn parse_timestamp(nanos: i64) -> Result<NaiveDateTime> {
    DateTime::from_timestamp(secs, nsecs)
        .ok_or_else(|| anyhow::anyhow!("invalid timestamp: {}", nanos))
}
```

### Error Style

Quickwit uses `anyhow` extensively. Error messages follow the format from [CODE_STYLE.md](../../CODE_STYLE.md):
- Concise, lowercase (except proper names), no trailing punctuation
- Examples: `"failed to open split"`, `"unknown output format {:?}"`

### Custom Error Types

Use `thiserror` for domain error types with structured variants:

```rust
#[derive(Debug, thiserror::Error)]
pub enum MetastoreError {
    #[error("invalid metastore config: `{0}`")]
    InvalidConfig(String),
    #[error("split `{split_id}` not found")]
    SplitNotFound { split_id: String },
}
```

### Allowed Exceptions

- `unwrap()` in tests is acceptable
- `expect()` only when the condition is provably impossible
- `unwrap_or()` / `unwrap_or_default()` are acceptable (have fallbacks)

---

## 2. Assertions for Reliability

### Rule: Use `debug_assert!` to express invariants and help reviewers proofread

From [CODE_STYLE.md](../../CODE_STYLE.md): *"A good idea to help reviewers proofread your code is to identify invariants and express them as `debug_assert`."*

This is especially valuable for:
- Preconditions that aren't enforced by the type system
- Invariants that should hold after state transitions
- Hidden contracts that can't be eliminated

```rust
pub fn publish_splits(&self, splits: &[SplitMetadata]) -> Result<()> {
    // Assert invariant: all splits must be known before publishing
    debug_assert!(
        splits.iter().all(|s| self.known_splits.contains(&s.split_id)),
        "publishing unknown split"
    );

    self.metastore.publish(splits)?;
    Ok(())
}

fn merge_candidates(splits: &mut [SplitMetadata]) -> Vec<SplitMetadata> {
    splits.sort_by_key(|s| s.time_range.end);
    // Assert postcondition: result is sorted
    debug_assert!(splits.is_sorted_by_key(|s| s.time_range.end));
    // ...
}
```

### When to use `debug_assert!` vs `Result`

| Scenario | Use |
|----------|-----|
| API boundary / user input | `Result` — always validate properly |
| Internal invariant | `debug_assert!` — documents the expectation |
| Should never happen in correct code | `debug_assert!` — catches bugs during testing |
| Could happen due to external state | `Result` — handle gracefully |

---

## 3. Type Aliases and Domain Types

### Rule: Use Quickwit's existing type aliases for domain concepts

Quickwit defines key type aliases in `quickwit-proto`:

```rust
use quickwit_proto::types::{
    IndexId,     // Index identifier
    IndexUid,    // Unique index identifier
    SplitId,     // Split identifier
    SourceId,    // Source identifier
    ShardId,     // Shard identifier
    NodeId,      // Node identifier
    PipelineUid, // Pipeline unique identifier
};
```

When adding new domain concepts, prefer creating type aliases or newtypes over using raw strings.

---

## 4. Iterator Patterns (Quickwit Style)

### Rule: Choose readability over dogma

Quickwit's [CODE_STYLE.md](../../CODE_STYLE.md) explicitly allows procedural loops when iterator chains become hard to read, especially with error handling:

```rust
// GOOD - simple chain, easy to read
let results: Vec<_> = items
    .iter()
    .filter_map(|item| item.value())
    .map(|val| val.to_uppercase())
    .collect();

// ALSO GOOD - procedural loop when error handling makes chains unreadable
let mut results = Vec::new();
for item in items {
    let value = item.value().map_err(|e| {
        warn!(error=%e, item_id=%item.id, "failed to extract value");
        e
    })?;
    if value.is_valid() {
        results.push(transform(value)?);
    }
}
```

### Disallowed Option Methods (`clippy.toml`)

These are banned for readability reasons:
- `Option::is_some_and` — use `matches!` or `if let`
- `Option::is_none_or` — use explicit match
- `Option::xor` — use explicit logic
- `Option::map_or` — use `.map(..).unwrap_or(..)`
- `Option::map_or_else` — use `.map(..).unwrap_or_else(..)` or `let Some(..) else {..}`

---

## 5. File Size Limits

### Rule: Maximum 500 lines per new file

Large files indicate mixed concerns. Split at logical boundaries.

| File Type | Split Strategy |
|-----------|---------------|
| Functions | By category (extract, arithmetic, format) |
| Handlers | By protocol (HTTP, gRPC, native) |
| Types | By domain (query, ingest, storage) |

### Example: large handler file -> module directory

```
serve/elasticsearch_api/
├── mod.rs           # Re-exports, router setup
├── search.rs        # _search endpoint
├── bulk.rs          # _bulk endpoint
├── scroll.rs        # _scroll endpoint
└── model.rs         # Request/response types
```

**Note**: Some existing Quickwit files exceed this limit. The 500-line rule applies to *new* code we write — don't refactor existing files just to hit the target.

---

## 6. Clone Avoidance

### Rule: Prefer references and Arc over cloning, except in concurrent code

```rust
// BAD - clones entire collection
impl State {
    fn splits(&self) -> Vec<String> {
        self.splits.clone()  // Allocates!
    }
}

// GOOD - returns reference
impl State {
    fn splits(&self) -> &[String] {
        &self.splits
    }
}

// GOOD - shared ownership when needed
impl State {
    fn splits(&self) -> Arc<Vec<String>> {
        Arc::clone(&self.splits)
    }
}
```

### When Cloning is Acceptable

- **Actor/async code**: Cloning for ownership transfer into actors, closures, or across `.await` points is expected and preferred over complex lifetime management
- Small types (< 64 bytes) and `Copy` types
- `Arc::clone()` (cheap reference count bump)
- In test code

The goal is to avoid *unnecessary* allocations in hot paths, not to eliminate `.clone()` everywhere. When cloning makes concurrent code simpler and less error-prone, clone freely.

---

## 7. From/Into Implementations

### Rule: Implement From for natural conversions

```rust
// BAD - manual conversion everywhere
let id = IndexId::new(string.clone());

// GOOD - From implementation
impl From<String> for IndexId {
    fn from(s: String) -> Self { Self(s) }
}

// Usage
let id: IndexId = string.into();
```

### Naming Convention

| Method | Returns | Use Case |
|--------|---------|----------|
| `as_str()` | `&str` | Borrowed view, no allocation |
| `to_string()` | `String` | Owned copy, allocates |
| `into_inner()` | Inner type | Consumes self |

---

## 8. Module Organization

### Rule: One responsibility per module

Quickwit organizes larger crates into directory modules with `mod.rs`:

```
quickwit-indexing/src/actors/
├── mod.rs           # Re-exports
├── indexer.rs       # Indexing actor
├── uploader.rs      # Upload actor
├── packager.rs      # Packaging actor
└── ...
```

Files can be long when they cover a single cohesive responsibility — Quickwit doesn't enforce a strict line limit. If a file is growing unwieldy, split at logical boundaries (by handler, by protocol, by domain).

---

## 9. Documentation (Quickwit Style)

### Rule: Document "why", not "what"

```rust
// BAD - restates the code
/// Returns the split id
fn split_id(&self) -> &str { &self.split_id }

// GOOD - explains why/when
/// Returns the canonical split ID used for deduplication during merge.
/// Use this when comparing splits across nodes.
fn split_id(&self) -> &str { &self.split_id }
```

From [CODE_STYLE.md](../../CODE_STYLE.md):
- Comments should convey **intent**, **context** (links to issues, papers), and **hidden contracts**
- No rustdoc in Quickwit private API is OK
- Inline comments are encouraged for thorny code

---

## 10. Structured Logging (Quickwit Style)

### Rule: Use `tracing` structured fields over string interpolation

```rust
// BAD - string interpolation
warn!("split {} failed to compact ({} attempts remaining)", split_id, remaining);

// GOOD - structured fields
warn!(split_id=%split_id, remaining=remaining, "split compaction failed");
```

Error and log messages: concise, lowercase, no trailing punctuation.

---

## 11. Hidden Contracts (Quickwit Style)

### Rule: Avoid hidden contracts; enforce constraints through types

From [CODE_STYLE.md](../../CODE_STYLE.md): A "hidden contract" is a precondition not enforced by the type system.

```rust
// BAD - hidden contract: splits must be sorted
fn merge_candidates(splits: &[SplitMetadata]) -> Vec<SplitMetadata> { ... }

// GOOD - internalize the sort (timsort is linear if already sorted)
fn merge_candidates(splits: &mut [SplitMetadata]) -> Vec<SplitMetadata> {
    splits.sort_by_key(|s| s.time_range.end);
    // ...
}

// ALSO GOOD - use types to prevent invalid states
fn min(values: &[usize]) -> Option<usize> {
    // Returns None instead of panicking on empty input
    values.iter().copied().min()
}
```

When a hidden contract is unavoidable, add a `debug_assert!` to check it.

---

## 12. Async Patterns (Quickwit Style)

### Rule: Async code must not block for more than 500 microseconds

From [CODE_STYLE.md](../../CODE_STYLE.md):

```rust
// BAD - blocks the async runtime
async fn process() {
    let result = expensive_computation(); // Blocks!
    send(result).await;
}

// GOOD - offload blocking work
async fn process() {
    let result = tokio::task::spawn_blocking(|| expensive_computation()).await?;
    send(result).await;
}
```

### Async Safety (from Known Pitfalls)

| Forbidden | Use Instead |
|-----------|-------------|
| `tokio::sync::Mutex` | Actor model with message passing |
| `JoinHandle::abort()` | `CancellationToken` |
| Recreating futures in `select!` | `&mut fut` to resume |
| Holding locks across `.await` | Message passing or synchronous critical sections |

---

## Checklist

Before committing, verify:

- [ ] No `unwrap()` in library code (use `?` or proper error types)
- [ ] `debug_assert!` for non-obvious invariants and hidden contracts
- [ ] New files under 500 lines (split by responsibility if larger)
- [ ] No unnecessary `.clone()` (OK in actor/async code for clarity)
- [ ] Readable iterator patterns (procedural loops for complex error handling)
- [ ] Structured logging with `tracing` fields
- [ ] No disallowed methods from `clippy.toml`
- [ ] Follows Quickwit naming conventions (standard Rust snake_case)
- [ ] Hidden contracts documented or eliminated

---

## References

- [CODE_STYLE.md](../../CODE_STYLE.md) - Quickwit coding style (proofreadability) — **primary reference**
- [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/) - Official Rust guidelines
- [DataFusion Contributing](https://datafusion.apache.org/contributor-guide/) - Query engine patterns
- [TigerBeetle TIGER_STYLE.md](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md) - Assertion discipline inspiration
