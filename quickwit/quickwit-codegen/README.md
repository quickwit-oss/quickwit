# Quickwit codegen

## Getting Started

1. Describe your service in a proto file.

2. Define an error and a result type for your service. The error type must implement `From<tonic::Status>` and `Into<tonic::Status>`.

3. Add the following dependencies to your project:

```toml
[dependencies]
async-trait = { workspace = true }
dyn-clone = { workspace = true }
prost = { workspace = true }
serde = { workspace = true }
thiserror = { workspace = true }
tokio = { workspace = true }
tonic = { workspace = true }
tower = { workspace = true }
utoipa = { workspace = true }

[dev-dependencies]
mockall = { workspace = true }

[build-dependencies]
quickwit-codegen = { workspace = true }
```

4. Run the code generation logic as part of a Cargo build script:

```rust
use quickwit_codegen::Codegen;

fn main() {
    // Proto file describing the service.
    let proto = Path::new("src/hello.proto");
    // Output directory for the generated code.
    let out_dir = Path::new("src/");
    Codegen::run(proto, out_dir, "crate::HelloResult", "crate::HelloError").unwrap();
}
```

5. Import and implement the generated service trait and use the various generated adapters to instantiate a gRPC server, or use a local or remote gRPC implementation with the same client interface.

Checkout the complete working example in the `quickwit-codegen-example` crate.
