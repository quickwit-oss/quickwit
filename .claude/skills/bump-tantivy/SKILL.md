---
name: bump-tantivy
description: Bump tantivy to the latest commit on main branch, fix compilation issues, and open a PR
disable-model-invocation: true
---

# Bump Tantivy

Follow these steps to bump tantivy to its latest version:

## Step 1: Check that we are on the main branch

Run: `git branch --show-current`

If the current branch is not `main`, abort and ask the user to switch to the main branch first.

## Step 2: Ensure main is up to date

Run: `git pull origin main`

This ensures we're working from the latest code.

## Step 3: Get the latest tantivy SHA

Run: `gh api repos/quickwit-oss/tantivy/commits/main --jq '.sha'`

Extract the first 7 characters as the short SHA.

## Step 4: Update Cargo.toml

Edit `quickwit/Cargo.toml` and update the `rev` field in the tantivy dependency to the new short SHA.

The line looks like:
```toml
tantivy = { git = "https://github.com/quickwit-oss/tantivy/", rev = "XXXXXXX", ... }
```

## Step 5: Run cargo check and fix compilation errors

Run `cargo check` in the `quickwit` directory to verify compilation.

If there are compilation errors:
- If the fix is straightforward (simple API changes, renames, etc.), fix them without asking
- If the fix is complex or unclear, ask the user before proceeding

Repeat until cargo check passes.

## Step 6: Format code

Run `make fmt` from the `quickwit/` directory to format the code.

## Step 7: Update licenses

Run `make update-licenses` from the `quickwit/` directory, then move the generated file:
```
mv quickwit/LICENSE-3rdparty.csv ./LICENSE-3rdparty.csv
```

## Step 8: Create a new branch

Get the git username: `git config user.name | tr ' ' '-' | tr '[:upper:]' '[:lower:]'`

Get today's date: `date +%Y-%m-%d`

Create and checkout a new branch named: `{username}/bump-tantivy-{date}`

Example: `paul/bump-tantivy-2024-03-15`

## Step 9: Commit changes

Stage all modified files and create a commit with message:
```
Bump tantivy to {short-sha}
```

## Step 10: Push and open a PR

Push the branch and open a PR using:
```
gh pr create --title "Bump tantivy to {short-sha}" --body "Updates tantivy dependency to the latest commit on main."
```

Report the PR URL to the user when complete.
