---
name: update-lambda
description: Trigger a new lambda release, update build.rs with the new artifact URL and SHA256, and open a PR
disable-model-invocation: true
---

# Update Lambda Artifact

This skill triggers the `Release Lambda binary` GitHub Actions workflow by pushing a
`lambda-<commit>` tag, waits for the artifact to be published, then updates
`quickwit-lambda-client/build.rs` with the new URL and SHA256 hash, and opens a PR.

## Step 1: Verify we are on main

Run: `git branch --show-current`

If not on `main`, abort and ask the user to switch branches first.

## Step 2: Pull latest main

Run: `git pull origin main`

This ensures the lambda tag points to the latest code.

## Step 3: Get the commit short hash

Run: `git rev-parse --short HEAD`

Save this as `COMMIT_HASH` (e.g. `dd6442e`). The tag that will be pushed is
`lambda-{COMMIT_HASH}`.

## Step 4: Check the tag does not already exist

Run:
```bash
git tag --list "lambda-{COMMIT_HASH}"
```

If the tag already exists locally or on the remote, skip straight to Step 6 to check
whether the corresponding release already exists. Do not push a duplicate tag.

## Step 5: Push the tag to trigger the workflow

```bash
git tag lambda-{COMMIT_HASH}
git push origin lambda-{COMMIT_HASH}
```

This triggers the `Release Lambda binary` GitHub Actions workflow
(`.github/workflows/publish_lambda.yaml`).

After pushing, wait ~10 seconds for GitHub to register the workflow run before polling.

## Step 6: Wait for the workflow run to complete

Poll the workflow runs until the one for this tag completes:

```bash
gh run list --repo quickwit-oss/quickwit \
  --workflow=publish_lambda.yaml \
  --limit 10 \
  --json headBranch,status,conclusion,databaseId,url
```

Look for the run where `headBranch` equals `lambda-{COMMIT_HASH}`.

Note: the GitHub release will be tagged `{COMMIT_HASH}` (not `lambda-{COMMIT_HASH}`)
because the workflow strips the `lambda-` prefix when setting `ASSET_VERSION`.

- If `status` is `completed` and `conclusion` is `success`: proceed to Step 7.
- If `status` is `completed` and `conclusion` is not `success`: abort and report
  the failure URL to the user.
- If no matching run is found or `status` is still in-progress: wait 30 seconds and
  retry. The build typically takes 15–20 minutes.

## Step 7: Fetch the draft release asset info

Once the workflow succeeds:

```bash
gh release view {COMMIT_HASH} \
  --repo quickwit-oss/quickwit \
  --json assets,isDraft
```

From the first element of `assets`, extract:
- `url`: the download URL (e.g.
  `https://github.com/quickwit-oss/quickwit/releases/download/lambda-{COMMIT_HASH}/quickwit-aws-lambda--aarch64.zip`)
- `digest`: the SHA256 in `sha256:<hex>` format — strip the `sha256:` prefix to get
  the bare hex string.

Abort if the release has no assets or is not found.

## Step 8: Publish the draft release

The release is created as a draft by the workflow. Make it publicly accessible so
`build.rs` can download the artifact:

```bash
gh release edit {COMMIT_HASH} \
  --repo quickwit-oss/quickwit \
  --draft=false
```

## Step 9: Update build.rs

Edit `quickwit/quickwit-lambda-client/build.rs` and update the two constants:

```rust
const LAMBDA_ZIP_URL: &str = "<new URL from Step 7>";
const LAMBDA_ZIP_SHA256: &str = "<bare SHA256 hex from Step 7>";
```

These are the only two lines that should change.

## Step 10: Create a working branch

Get the git username: `git config user.name | tr ' ' '-' | tr '[:upper:]' '[:lower:]'`
Get today's date: `date +%Y-%m-%d`

Create and checkout a new branch:
```
git checkout -b {username}/update-lambda-{COMMIT_HASH}
```

## Step 11: Commit the changes

Stage the modified file and commit:
```bash
git add quickwit/quickwit-lambda-client/build.rs
git commit -m "Update lambda artifact to {COMMIT_HASH}"
```

## Step 12: Push and open a PR

```bash
git push origin {username}/update-lambda-{COMMIT_HASH}

gh pr create \
  --repo quickwit-oss/quickwit \
  --title "Update lambda artifact to {COMMIT_HASH}" \
  --body "$(cat <<'EOF'
Updates `quickwit-lambda-client/build.rs` to point to the new lambda artifact
built from commit {COMMIT_HASH}.

- Release: https://github.com/quickwit-oss/quickwit/releases/tag/lambda-{COMMIT_HASH}
EOF
)"
```

Report the PR URL to the user.
