# Simulation-First Development Workflow

This document defines the mandatory workflow for all Quickwit development,
following the verification pyramid philosophy.

## The Verification Pyramid

```
                    ^
                   /|\
                  / | \
                 /  |  \      TLA+ Specs (docs/internals/specs/tla/)
                /   |   \     - Mathematical model
               /----+----\    - Defines "what is correct"
              /     |     \
             /      |      \  Stateright Models
            /       |       \ - Rust-native model checking
           /--------+--------\- Verifies state space
          /         |         \
         /          |          \ DST Tests
        /           |           \- Deterministic simulation
       /------------+------------\- Fault injection
      /             |             \
     /              |              \ Unit/Integration Tests
    /               |               \- Fast feedback
   /----------------+----------------\
  /                 |                 \
 /                  |                  \ Production Monitoring
/-------------------+-------------------\- production invariant metrics
```

## Mandatory Workflow

### Phase 1: Specification (BEFORE any code)

1. **Check existing TLA+ spec** in `docs/internals/specs/tla/`
   - If exists: Review invariants that apply to your change
   - If not: Write one (for significant features)

2. **Check existing Stateright model**
   - If exists: Understand the state machine and properties
   - If not: Consider if model checking is needed

### Phase 2: Write Tests FIRST (still no implementation)

3. **Write DST tests**
   ```rust
   #[test]
   fn test_feature_invariant_holds() {
       let config = SimConfig::new(SEED);
       let mut sim = Simulation::new(config);

       sim.run(|env| async move {
           // Setup
           let component = create_component();

           // Exercise (with fault injection)
           for _ in 0..iterations {
               perform_operation(&component).await?;
           }

           // Verify invariants from TLA+ spec
           verify_invariant_1(&component)?;  // Maps to TLA+ line X
           verify_invariant_2(&component)?;  // Maps to TLA+ line Y

           Ok(())
       });
   }
   ```

4. **Run tests - EXPECT FAILURE**
   ```bash
   cargo test -p quickwit-dst -- your_feature_tests
   # Should fail: component doesn't exist yet
   ```

### Phase 3: Implement (make tests pass)

5. **Write minimal implementation** to make DST tests pass
   - Add `debug_assert!` for invariants that match TLA+ properties
   - Follow the coding style in [CODE_STYLE.md](../../CODE_STYLE.md) and [RUST_STYLE.md](RUST_STYLE.md)

6. **Run tests - EXPECT PASS**
   ```bash
   cargo test -p quickwit-dst -- your_feature_tests
   # Should pass now
   ```

### Phase 4: Verify All Layers

7. **Run Stateright model** (if applicable)
   ```bash
   cargo test -p quickwit-dst -- stateright_your_feature
   ```

8. **Run unit tests**
   ```bash
   cargo nextest run -p your-crate -- your_feature
   ```

9. **Run integration tests**
   ```bash
   # Rust integration tests
   cargo nextest run -p quickwit-integration-tests

   # REST API tests (if touching API surface)
   cd rest-api-tests && ./run_tests.py --engine quickwit
   ```

10. **Full verification**
    ```bash
    cargo nextest run --all-features
    cargo clippy --workspace --all-features --tests
    ```

### Phase 5: PR (only after all verification passes)

11. **Create PR** with evidence of verification
    - DST test results
    - Stateright exploration stats (if applicable)
    - Link to TLA+ spec (if applicable)
    - Integration test results

---

## Example: Adding a New Feature

### Bad (what NOT to do):
```
1. Write implementation
2. Create PR
3. "Oh, should I write tests?" (asked by reviewer)
4. Write tests after the fact
```

### Good (simulation-first):
```
1. Read TLA+ spec for invariants
2. Write DST test that verifies invariant
3. Run test -> FAILS (no implementation)
4. Write implementation
5. Run test -> PASSES
6. Run Stateright -> PASSES
7. Run unit + integration tests -> PASSES
8. Create PR with test evidence
```

---

## When Simulation-First Applies

### MUST use simulation-first for:
- Stateful components (metastore, ingest pipeline, shard management)
- Concurrency protocols (locks, transactions, atomic operations)
- Distributed coordination (control plane, cluster membership)
- Data lifecycle (ingest -> index -> compact -> GC)
- Recovery paths (crash recovery, WAL replay)

### MAY skip simulation-first for:
- Pure functions (parsing, formatting, serialization)
- Simple CRUD endpoints
- UI changes
- Configuration changes
- Documentation

Even when skipping DST, still write tests before implementation when practical.

---

## Adapting the Workflow for Quickwit's Actor Model

Quickwit uses an actor framework (`quickwit-actors`) for concurrent components. When applying simulation-first to actors:

1. **Actors are natural DST targets**: Each actor has a mailbox, message types, and state transitions — perfect for model checking
2. **Test through the mailbox**: Send messages, verify state after processing
3. **Fault inject at actor boundaries**: Simulate message drops, slow processing, actor crashes
4. **Verify supervisor behavior**: Test that supervisors correctly restart failed actors

```rust
// Example: Testing an actor with DST
#[test]
fn test_indexer_actor_handles_storage_fault() {
    let config = SimConfig::new(SEED);
    let mut sim = Simulation::new(config)
        .with_fault(FaultConfig::new(FaultType::StorageWriteFail, 0.1));

    sim.run(|env| async move {
        let indexer = IndexerActor::new(env.storage());

        // Send messages through the mailbox
        indexer.send(IndexMessage::IndexBatch(batch)).await?;

        // Verify invariants hold despite faults
        assert!(indexer.state().splits_published >= expected_min);
        assert!(indexer.state().no_data_loss());

        Ok(())
    });
}
```

---

## Checklist for Every PR

- [ ] Identified relevant TLA+ invariants
- [ ] DST tests written BEFORE implementation (for stateful components)
- [ ] DST tests verify TLA+ properties
- [ ] Stateright model passes (if applicable)
- [ ] Unit + integration tests pass
- [ ] `cargo clippy --workspace --all-features --tests` passes
- [ ] `cargo +nightly fmt -- --check` passes
- [ ] PR includes verification evidence

---

## References

- [Verification Guide](./VERIFICATION.md)
- [Verification Stack](./VERIFICATION_STACK.md)
- [Rust Style Guide](./RUST_STYLE.md)
- [Pierre Zemb: Simulation-Driven Development](https://pierrezemb.fr/posts/simulation-driven-development/)
- [TigerBeetle: Simulation Testing](https://tigerbeetle.com/blog/2023-07-06-simulation-testing)
