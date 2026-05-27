# Quickwit Lambda

Quickwit supports offloading leaf search to AWS Lambda for horizontal scaling.
The Lambda function is built separately and embedded into Quickwit's binary,
allowing Quickwit to auto-deploy the function at startup.

## Architecture

- **quickwit-lambda-server**: The Lambda function binary that executes leaf searches
- **quickwit-lambda-client**: The client that invokes Lambda and embeds the Lambda zip for auto-deployment

## Release Process

### Automated (recommended)

Use the `/update-lambda` Claude Code skill from the repo root:

```
/update-lambda
```

This skill automates the full process end-to-end:
1. Creates and pushes a `lambda-<commit>` tag, triggering the `publish_lambda.yaml` workflow
2. Polls until the cross-compilation build completes (~15–20 min)
3. Publishes the draft GitHub release
4. Updates `LAMBDA_ZIP_URL` and `LAMBDA_ZIP_SHA256` in `build.rs`
5. Opens a PR with the artifact bump

### Manual fallback

**1. Tag the release**

Push a tag with the `lambda-` prefix:

```bash
git tag lambda-<commit>
git push origin lambda-<commit>
```

This triggers the `publish_lambda.yaml` GitHub Action which:
- Cross-compiles the Lambda binary for ARM64
- Creates a zip named `quickwit-aws-lambda-<commit>-aarch64.zip`
- Uploads it as a **draft** GitHub release tagged `lambda-<commit>`

**2. Publish the release**

Make the artifact URL publicly accessible:

```bash
gh release edit lambda-<commit> --repo quickwit-oss/quickwit --draft=false
```

**3. Update the embedded Lambda URL**

Get the SHA256 from the release metadata (no download needed):

```bash
gh release view lambda-<commit> --repo quickwit-oss/quickwit \
  --json assets --jq '.assets[0] | {url, digest}'
```

Update both constants in `quickwit-lambda-client/build.rs`:

```rust
const LAMBDA_ZIP_URL: &str = "https://github.com/quickwit-oss/quickwit/releases/download/lambda-<commit>/quickwit-aws-lambda-<commit>-aarch64.zip";
const LAMBDA_ZIP_SHA256: &str = "<hex from digest field, strip sha256: prefix>";
```

### Versioning

The Lambda client uses content-based versioning:
- The SHA256 of the Lambda zip is computed at build time
- The first 8 hex chars are embedded in the Lambda function description as `quickwit:{version}-{hash_short}`
- When Quickwit starts, it checks if a matching version exists before deploying

This ensures that:
- Different Quickwit builds with the same Lambda binary share the same Lambda version
- Updating the Lambda binary automatically triggers a new deployment
