---
name: pomsky-changelog
description: "Generate a changelog entry for the latest Pomsky release and prepend it to POMSKY_CHANGELOG.md. Use when cutting a release or when asked to document what changed since the last release."
user-invocable: true
---

# Pomsky Changelog Generator

Investigate the commits since the previous Pomsky release, categorize them, and prepend a
formatted entry to `POMSKY_CHANGELOG.md` at the repo root.

## Step 1 — Identify the release range

Pomsky releases are tagged `vX.Y.Z` with an annotated tag message of the form
`"Release pomsky vX.Y.Z"` (set by `scripts/release_pomsky.sh`). Use the tag message to
distinguish them from OSS Quickwit tags that also live in this repo:

```bash
git tag --sort=-version:refname \
  --format='%(refname:short)|%(contents:subject)' \
  | grep "|Release pomsky" \
  | head -2
```

The first line is the release to document (`THIS_TAG`), the second is the previous one
(`PREV_TAG`). Extract the commit hash and date for the release tag:

```bash
git show <THIS_TAG> --format="%H %ad" --date=short | head -1
```

## Step 2 — Collect commits in the range

```bash
git log <PREV_TAG>..<THIS_TAG> --oneline \
  | grep -v "^[a-f0-9]* Merge remote-tracking\|^[a-f0-9]* Merge oss/main\|^[a-f0-9]* build(deps)\|^[a-f0-9]* chore(deps)\|^[a-f0-9]* docs(claude)\|^[a-f0-9]* ci:"
```

## Step 3 — Categorize commits

Split commits into three buckets:

**Pomsky-specific (DataDog)** — commits that are DataDog work, not from upstream Quickwit.
Signals:
- `CLOUDPREM-XXX` ticket reference
- Low pomsky PR number in parens, e.g. `(#612)` — pomsky internal PRs are typically < 1000
- Branch prefix like `DataDog/` in the merge commit title
- No Quickwit issue number (Quickwit upstream PRs are typically `(#5xxx)` or `(#6xxx)`)

**From Quickwit upstream** — changes merged via `Merge oss/main` PRs. Do not list individual
upstream commits; summarise thematically (metrics, search, storage, etc.) and cite the
upstream PR numbers.

**From dependency bumps** — tantivy, chitchat, mrecordlog. Handle each separately in Step 4.

## Step 4 — Investigate dependency bumps

For each of tantivy, chitchat, and mrecordlog, check whether the pinned rev or version changed
between the two releases.

```bash
# Find the old rev/version
git show <PREV_TAG>:quickwit/Cargo.toml | grep -E "^tantivy|^chitchat|^mrecordlog"

# Find the new rev/version
git show <THIS_TAG>:quickwit/Cargo.toml | grep -E "^tantivy|^chitchat|^mrecordlog"
```

If a dep changed, investigate the commits between the old and new revs:

### Tantivy (local clone at `~/git/tantivy`)

```bash
cd ~/git/tantivy && git log <old_rev>..<new_rev> --oneline
```

For each non-trivial commit, check the message:
```bash
git show <hash> --format="%B" | head -20
```

Include in the changelog only changes relevant to Pomsky's use of tantivy:
- Bug fixes in full-text search, fast fields, aggregations, sstable/dictionary
- Performance improvements (skip-lists, block skipping, etc.)
- Changes to the HyperLogLog / datasketches integration
- Any fix explicitly referencing a CLOUDPREM ticket

Skip: benchmark tooling updates, doc-only changes, dependency bumps inside tantivy.

### Chitchat (gossip library)

Chitchat is version-pinned in Cargo.toml (e.g. `chitchat = "0.10.1"`). If the version changed,
fetch the changelog from GitHub:

```bash
gh api repos/quickwit-oss/chitchat/releases --jq '.[].tag_name' | head -10
gh api repos/quickwit-oss/chitchat/releases/tags/<version> --jq '.body'
```

Or compare commits between tags:
```bash
gh api "repos/quickwit-oss/chitchat/compare/<old_tag>...<new_tag>" \
  --jq '.commits[].commit.message' | head -40
```

Include only changes affecting gossip correctness or cluster membership.

### mrecordlog (WAL library)

mrecordlog is git-rev-pinned. Compare commits between old and new rev:

```bash
gh api "repos/quickwit-oss/mrecordlog/compare/<old_rev>...<new_rev>" \
  --jq '.commits[].commit.message' | head -40
```

Include only correctness fixes or relevant performance changes.

## Step 5 — Write the changelog entry

Format the entry as follows. Use the exact release version from the commit message and the
commit hash of the release commit (first 9 characters).

```markdown
## [vX.Y.Z] — YYYY-MM-DD — `<short_hash>`

### <Category>

- **Short title**: One-sentence description. Gate/ticket reference if applicable.
...
```

Section order (omit empty sections):
1. Search
2. Intake — Traces / Spans
3. Metrics
4. Observability & Diagnostics
5. Build & Infrastructure
6. From Quickwit Upstream (OSS merges up to YYYY-MM-DD)
7. From Tantivy (`<old_rev>` → `<new_rev>`) — only if tantivy changed
8. From Chitchat (`<old_ver>` → `<new_ver>`) — only if chitchat changed
9. From mrecordlog (`<old_rev>` → `<new_rev>`) — only if mrecordlog changed

Keep bullet points concise. For upstream sections, group by theme rather than listing every
commit. For Pomsky-specific sections, one bullet per logical change (not per commit).

## Step 6 — Prepend to POMSKY_CHANGELOG.md

Read the current `POMSKY_CHANGELOG.md`, then write it back with the new entry inserted
immediately after the file header (the block of lines before the first `## [v` entry).

The header block is:
```
# Pomsky Changelog

All notable changes to Pomsky are documented here.
...

---
```

Insert the new `## [vX.Y.Z]` entry right after the `---` separator, with a blank line between
the separator and the new entry.
