---
name: fix-clippy
description: Fix all clippy lint warnings in the project
---

# Fix Clippy

Clippy issues are **warnings**, not errors. Never grep for `error` when looking for clippy issues.

## Step 1: Auto-fix

Run `make fix` to automatically fix clippy warnings:

```
make fix
```

## Step 2: Fix remaining warnings manually

Check for remaining warnings that couldn't be auto-fixed:

```
cargo clippy --tests 2>&1 | grep "^warning:" | sort -u
```

For each remaining warning, find the exact location and fix it manually.
