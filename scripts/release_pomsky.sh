#!/usr/bin/env bash
set -euo pipefail

cargo install cargo-edit

cd quickwit
echo "Reset state of branch to main and pull latest changes"
# Ask for confirmation
read -p "This will reset any local changes and pull the latest changes from origin/main. Do you want to continue? (y/n) " -n 1 -r
echo    # move to a new line
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborting."
    exit 1
fi
git fetch origin main
git switch main
git reset --hard origin/main
git pull origin main

echo "Bumping version..."
cargo set-version
# Get the new version
VERSION=$(cargo metadata --no-deps --format-version 1 \
| jq -r '(.workspace_default_members[0] // .workspace_members[0]) as $m | .packages[] | select(.id==$m) | .version')

# ask if we should continue
read -p "Bumped version to v$VERSION. Do you want to continue? (y/n) " -n 1 -r
echo    # move to a new line
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborting."
    exit 1
fi

# Create release branch
BRANCH="release-v$VERSION"
echo "Creating release branch '$BRANCH'..."
git switch -c "$BRANCH"

# Commit and tag the release version, e.g. "v0.1.16"
git add Cargo.toml Cargo.lock
git commit -a -m "Release pomsky v$VERSION"
git tag -f "v$VERSION" -m "Release pomsky v$VERSION"

# Bump patch, append -dev (e.g. 0.1.25 -> 0.1.26-dev).
DEV_VERSION="${VERSION%.*}.$((${VERSION##*.}+1))-dev"

echo "Bumping to v$DEV_VERSION..."
cargo set-version "$DEV_VERSION"
git add Cargo.toml Cargo.lock
git commit -a -m "Bump pomsky to v$DEV_VERSION"

echo "Created on branch '$BRANCH':"
echo "  1. Release pomsky v$VERSION (tagged v$VERSION)"
echo "  2. Bump pomsky to v$DEV_VERSION"

# Ask if we should continue
read -p "Push branch '$BRANCH' and tag v$VERSION to origin? (y/n) " -n 1 -r
echo    # move to a new line
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborting push."
    exit 1
fi

echo "Pushing branch '$BRANCH'..."
git push -u origin "$BRANCH"

echo "Pushing tag v$VERSION..."
git push origin "v$VERSION"

PR_TITLE="Release pomsky v$VERSION"
PR_BODY="Release pomsky **v$VERSION**.

Contains:
- \`Release pomsky v$VERSION\` (tagged \`v$VERSION\`)
- \`Bump pomsky to v$DEV_VERSION\`

⚠️ **Merge as a merge commit (not squash)** so tag \`v$VERSION\` keeps pointing at the release commit on \`main\`.

If the PR is squashed, the tag must be moved to the resulting merge commit:

\`\`\`bash
git fetch origin main
git tag -f v$VERSION <merge-commit-sha>
git push -f origin v$VERSION
\`\`\`
"

if command -v gh >/dev/null 2>&1; then
    echo "Opening PR via gh..."
    gh pr create \
        --base main \
        --head "$BRANCH" \
        --title "$PR_TITLE" \
        --body "$PR_BODY"
else
    REMOTE_URL=$(git config --get remote.origin.url)
    REPO=$(echo "$REMOTE_URL" | sed -E 's#(git@github.com:|https://github.com/)([^/]+/[^/.]+?)(\.git)?$#\2#')
    echo "gh CLI not found. Open PR manually:"
    echo "  https://github.com/$REPO/compare/main...$BRANCH?expand=1&title=$(echo "$PR_TITLE" | sed 's/ /%20/g')"
fi

echo "Done."
echo "Pushed branch '$BRANCH' and tag v$VERSION."
echo
echo "REMINDER: merge the PR with a merge commit (not squash) to preserve tag v$VERSION."
echo "If squashed, re-tag the merge commit on main and force-push:"
echo "  git fetch origin main && git tag -f v$VERSION <merge-sha> && git push -f origin v$VERSION"

cd ..
