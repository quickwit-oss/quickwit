# Quickwit Lambda

Quickwit supports offloading leaf search to AWS Lambda for horizontal scaling.
The Lambda function is built separately and embedded into Quickwit's binary,
allowing Quickwit to auto-deploy the function at startup.

## Architecture

- **quickwit-lambda-server**: The Lambda function binary that executes leaf searches
- **quickwit-lambda-client**: The client that invokes Lambda and embeds the Lambda zip for auto-deployment

## Release Process

### 1. Tag the release

Push a tag with the `lambda-` prefix:

```bash
git tag lambda-v0.8.0
git push origin lambda-v0.8.0
```

This triggers the `publish_lambda.yaml` GitHub Action which:
- Cross-compiles the Lambda binary for ARM64
- Creates a zip file named `quickwit-aws-lambda-v0.8.0-aarch64.zip`
- Uploads it as a **draft** GitHub release

### 2. Publish the release

Go to GitHub releases and manually publish the draft release to make the
artifact URL publicly accessible.

### 3. Update the embedded Lambda URL

Update `LAMBDA_ZIP_URL` in `quickwit-lambda-client/build.rs` to point to the
new release:

```rust
const LAMBDA_ZIP_URL: &str = "https://github.com/quickwit-oss/quickwit/releases/download/lambda-v0.8.0/quickwit-aws-lambda-v0.8.0-aarch64.zip";
```

### 4. Versioning

The Lambda client uses content-based versioning:
- A SHA1 hash of the Lambda zip is computed at build time
- This hash is embedded in the Lambda function description as `quickwit:{version}-{sha1_short}`
- When Quickwit starts, it checks if a matching version exists before deploying

This ensures that:
- Different Quickwit builds with the same Lambda binary share the same Lambda version
- Updating the Lambda binary automatically triggers a new deployment

## Local Development

You can override the Lambda zip URL at build time:

```bash
QUICKWIT_LAMBDA_ZIP_URL=file:///path/to/local.zip cargo build -p quickwit-lambda-client
```
