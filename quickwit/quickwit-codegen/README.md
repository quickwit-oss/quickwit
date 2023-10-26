# Quickwit codegen

## Getting Started

1. Describe your service in a proto file.

2. Define an error and a result type for your service. The error type must implement `From<tonic::Status>` and `Into<tonic::Status>`.

3. Add the following dependencies to your project:

```toml
[dependencies]
async-trait = { workspace = true }
bytes = { workspace = true }
dyn-clone = { workspace = true }
http = { workspace = true }
hyper = { workspace = true }
prost = { workspace = true }
serde = { workspace = true }
thiserror = { workspace = true }
tokio = { workspace = true }
tonic = { workspace = true }
tower = { workspace = true }
utoipa = { workspace = true }

quickwit-actors = { workspace = true }

[dev-dependencies]
mockall = { workspace = true }

[build-dependencies]
quickwit-codegen = { workspace = true }
```

4. Run the code generation logic as part of a Cargo build script:

```rust
use quickwit_codegen::Codegen;

fn main() {
    Codegen::builder()
        .with_protos(&["src/hello.proto"])
        .with_output_dir("src/")
        .with_result_type_path("crate::HelloResult")
        .with_error_type_path("crate::HelloError")
        .run()
        .unwrap();
}
```

5. If additional prost settings need to be configured they can be provided the following way:

```rust
use quickwit_codegen::Codegen;

fn main() {
    let mut config = prost_build::Config::default();
    config.bytes(["PingRequest.name", "PingResponse.name"]);
    Codegen::builder()
        .with_protos(&["src/hello.proto"])
        .with_output_dir("src/codegen/")
        .with_result_type_path("crate::HelloResult")
        .with_error_type_path("crate::HelloError")
        .with_prost_config(config)
        .run()
        .unwrap();
}
```


6. Import and implement the generated service trait and use the various generated adapters to instantiate a gRPC server, or use a local or remote gRPC implementation with the same client interface.

Checkout the complete working example in the `quickwit-codegen-example` crate.
