---
name: merge-oss
description: Merge latest changes from the upstream OSS repository (oss/main), resolve conflicts, and regenerate licenses
---

# Merge OSS

Follow these steps to merge the latest changes from the upstream open-source repository into this fork.

## Step 1: Check that we are on the main branch

Run: `git branch --show-current`

If the current branch is not `main`, abort and ask the user to switch to the main branch first.

## Step 2: Ensure main is up to date

Run: `git pull origin main`

This ensures we're working from the latest code.

## Step 3: Create a merge branch

Get the git username: `git config user.name | tr ' ' '-' | tr '[:upper:]' '[:lower:]'`

Get today's date: `date +%Y-%m-%d`

Create and checkout a new branch: `git checkout -b {username}/merge-oss-{date}`

## Step 4: Fetch the latest from oss remote

Run: `git fetch oss`

This fetches the latest commits from the upstream OSS repository (`ssh://git@github.com/quickwit-oss/quickwit.git`).

## Step 5: Attempt the merge

Run: `git merge oss/main --no-edit`

If the merge completes without conflicts, skip to Step 7.

## Step 6: Resolve conflicts

If there are merge conflicts:

1. Run `git diff --name-only --diff-filter=U` to list all conflicted files.
2. For each conflicted file:
   - Read the file and examine the conflict markers (`<<<<<<<`, `=======`, `>>>>>>>`).
   - Analyze both sides of the conflict (ours = private fork, theirs = OSS upstream).
   - Attempt an automatic resolution:
     - If the conflict is in generated files (e.g., `Cargo.lock`), prefer regeneration later.
     - If the conflict is in code, try to reconcile both changes logically.
     - If the conflict is in `Cargo.toml` dependencies, merge both sets of changes.
   - **Auto-resolve without asking** for the following cases:
     - **GitHub Actions version bumps**: If OSS updated action versions (e.g., checkout, setup-python, setup-node), take the OSS version. Keep any fork-specific steps (ADMS override, DD STS token, etc.) intact.
     - **Tantivy rev updates**: Always take the OSS tantivy rev.
     - **scorecard.yml (modify/delete)**: Always delete — we don't use it in the fork.
   - For all other conflicts, **show the user your proposed resolution** using `AskUserQuestion`. Display the conflicting sections and your proposed fix, and ask the user to confirm or provide an alternative.
   - After resolution (auto or confirmed), apply the fix and run `git add {file}` to mark it resolved.
3. After all conflicts are resolved, finalize the merge with `git commit --no-edit`.

If a conflict is too complex to resolve automatically, show it to the user and ask for guidance.

## Step 7: Regenerate the lock file if needed

If `Cargo.lock` was conflicted or if dependencies changed, run from the `quickwit/` directory:

```
cargo check
```

This regenerates `Cargo.lock` and verifies compilation. Fix any compilation errors.

## Step 8: Regenerate 3rd-party licenses

**IMPORTANT**: This must run AFTER `cargo check` (Step 7), since the license tool needs the up-to-date `Cargo.lock`.

Always run this step, even if `Cargo.lock` was not conflicted — upstream dependency changes may introduce new licenses.

Run from the `quickwit/` directory:

```
make update-licenses
```

This runs `dd-rust-license-tool` and moves the generated `LICENSE-3rdparty.csv` to the repo root.

Check if the file actually changed with `git diff --stat LICENSE-3rdparty.csv`.

## Step 9: Commit license updates

Stage and commit the license changes:

```
git add LICENSE-3rdparty.csv quickwit/Cargo.lock
git commit -m "Regenerate 3rd-party licenses after OSS merge"
```

(Only commit if there are actual changes to these files.)

## Step 10: Push and open a PR

Push the branch:

```
git push -u origin {branch-name}
```

Open a PR:

```
gh pr create --title "Merge oss/main ({date})" --body "Merges latest upstream OSS changes into the private fork and regenerates 3rd-party licenses."
```

Report the PR URL to the user when complete.
