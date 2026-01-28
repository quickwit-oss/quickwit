# Quickwit Lambda

This crate provides AWS Lambda support for Quickwit's leaf search operations, enabling serverless execution of search queries across splits.

## Overview

Quickwit can offload leaf search operations to AWS Lambda functions. This is useful for:
- Scaling search capacity elastically
- Reducing infrastructure costs for sporadic workloads
- Isolating search execution from the main Quickwit cluster

## Features

### Lambda Invoker

The `RemoteFunctionInvoker` trait is meant to allow implementing a custom invoker for other cloud providers,
but at the moment only AWS is supported. 

### Auto-Deploy (Optional)

When the `auto-deploy` feature is enabled, Quickwit can automatically create or update the Lambda function at startup. This eliminates the need to manually deploy the Lambda function.

## Configuration

Add the following to your Quickwit configuration:

```yaml
searcher:
  lambda:
    enabled: true
    # Auto-deploy settings (requires auto-deploy feature)
    auto_deploy: true
    execution_role_arn: "arn:aws:iam::123456789:role/quickwit-lambda-role"
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | bool | `false` | Enable Lambda execution for leaf search |
| `function_name` | string | `"quickwit-lambda-search"` | Lambda function name or ARN |
| `function_qualifier` | string | `null` | Optional alias or version qualifier |
| `max_splits_per_invocation` | int | `10` | Maximum splits per Lambda invocation |
| `invocation_timeout_secs` | int | `30` | Client-side timeout for invocations |
| `max_concurrent_invocations` | int | `100` | Maximum concurrent Lambda invocations |
| `auto_deploy` | bool | `false` | Enable automatic Lambda deployment |
| `execution_role_arn` | string | `null` | IAM role ARN (required for auto-deploy) |
| `memory_size_mb` | int | `1024` | Lambda memory allocation in MB |
| `timeout_secs` | int | `30` | Lambda function timeout in seconds |

## Auto-Deploy Feature

The auto-deploy feature allows Quickwit to automatically manage the Lambda function lifecycle.

### Build-Time Binary Packaging

When building Quickwit with the `auto-deploy` feature enabled, the build script (`build.rs`) packages the Lambda binary into a zip file that gets embedded in the Quickwit binary:

```bash
cargo build --release -p quickwit-lambda --features auto-deploy
```

The build script looks for the Lambda binary in the following locations (in order):

1. `QUICKWIT_LAMBDA_BINARY_PATH` environment variable
2. `target/aarch64-unknown-linux-musl/release/quickwit-lambda-leaf-search`
3. `target/lambda/quickwit-lambda-leaf-search/bootstrap`
4. `lambda_bootstrap` in the workspace root

If no binary is found, a placeholder is created. This allows development builds to succeed, but the placeholder will fail if actually deployed.

**For production builds**, you must first cross-compile the Lambda binary for ARM64 Linux:

```bash
# Install the target
rustup target add aarch64-unknown-linux-musl

# Build the Lambda binary
cargo build --release --target aarch64-unknown-linux-musl -p quickwit-lambda --bin quickwit-lambda-leaf-search

# Then build Quickwit with auto-deploy
cargo build --release --features auto-deploy
```

### Startup Behavior

When `auto_deploy: true` is configured, the following happens at Quickwit startup:

1. **Function Check**: Quickwit checks if the Lambda function exists
2. **Create or Update**:
   - If the function doesn't exist, it creates it
   - If the function exists but has a different version tag, it updates the code and configuration
   - If the function exists with the same version, no action is taken
3. **Validation**: After deployment, Quickwit validates the function is invocable

**Important**: If deployment fails for any reason, Quickwit startup fails immediately. This fail-fast behavior ensures you don't run a Quickwit cluster that expects Lambda execution but can't actually invoke the function.

### Multi-Node Coordination

The auto-deploy feature is safe for concurrent execution from multiple Quickwit nodes:

- AWS Lambda's `CreateFunction` API is idempotent - if multiple nodes try to create the same function simultaneously, one succeeds and others receive a `ResourceConflictException`
- When a conflict is detected, the node falls back to updating the existing function
- Version tracking via tags ensures updates only happen when necessary

### Version Management

Quickwit tracks the deployed Lambda version using a `quickwit_version` tag on the function. On startup:

- If the tag matches the current Quickwit version, no update is performed
- If the tag differs or is missing, the function is updated

This means upgrading Quickwit automatically updates the Lambda function to match.

## IAM Permissions

### For the Quickwit Process (Auto-Deploy)

The Quickwit process needs these permissions to auto-deploy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "lambda:CreateFunction",
        "lambda:UpdateFunctionCode",
        "lambda:UpdateFunctionConfiguration",
        "lambda:GetFunction",
        "lambda:TagResource"
      ],
      "Resource": "arn:aws:lambda:*:*:function:quickwit-*"
    },
    {
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "arn:aws:iam::*:role/quickwit-lambda-*"
    }
  ]
}
```

### For Invoking the Lambda

The Quickwit process needs permission to invoke the function:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "lambda:InvokeFunction",
      "Resource": "arn:aws:lambda:*:*:function:quickwit-*"
    }
  ]
}
```

### Lambda Execution Role

The Lambda function itself needs an execution role with:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-index-bucket",
        "arn:aws:s3:::your-index-bucket/*"
      ]
    }
  ]
}
```

## Architecture

The Lambda function:
- Runs on ARM64 (Graviton2) for cost efficiency
- Uses the `provided.al2023` runtime
- Receives protobuf-encoded `LeafSearchRequest` payloads (base64 in JSON)
- Returns protobuf-encoded `LeafSearchResponse` payloads (base64 in JSON)

## Troubleshooting

### Startup Failures

If Quickwit fails to start with Lambda enabled:

1. **"execution_role_arn required for auto_deploy"**: You enabled `auto_deploy` but didn't provide an IAM role ARN
2. **"failed to create function"**: Check IAM permissions for the Quickwit process
3. **"Lambda function validation failed"**: The function exists but isn't invocable - check the function's execution role

### Runtime Issues

1. **Timeouts**: Increase `invocation_timeout_secs` and/or `timeout_secs`
2. **Memory errors**: Increase `memory_size_mb`
3. **Throttling**: AWS Lambda has concurrency limits - check your account limits

### Build Issues

If you see "No Lambda binary found, creating placeholder zip":
- This is expected for development builds without cross-compilation
- For production, ensure the Lambda binary is built for `aarch64-unknown-linux-musl`
- Set `QUICKWIT_LAMBDA_BINARY_PATH` to point to your pre-built binary
