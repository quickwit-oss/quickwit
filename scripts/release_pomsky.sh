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
git fetch origin
git reset --hard origin/main
git pull origin main

echo "Bumping version..."
cargo set-version
# Get the new version
VERSION=$(cargo metadata --no-deps --format-version 1 \
| jq -r '(.workspace_default_members[0] // .workspace_members[0]) as $m | .packages[] | select(.id==$m) | .version')

# ask if we should continue
read -p "Bumped version to v$(echo $VERSION). Do you want to continue? (y/n) " -n 1 -r
echo    # move to a new line
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborting."
    exit 1
fi

# Commit and tag the new version, e.g. "v0.1.16"
git add Cargo.toml Cargo.lock
git commit -a -m "Release pomsky v$(echo $VERSION)"
git tag -f "v$(echo $VERSION)" -m "Release pomsky v$(echo $VERSION)"

echo "Bumped version, created commit and set tag v$(echo $VERSION)"
# Ask if we should continue
read -p "Do you want to push the changes to the remote repository? (y/n) " -n 1 -r
echo    # move to a new line
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborting push."
    exit 1
fi
# Push changes and tag to remote
echo "Pushing changes to remote repository..."
echo "Push to the main branch:"
git push origin HEAD:main

echo "Push the tag v$(echo $VERSION):"
git push origin "v$(echo $VERSION)"
echo "Done."
echo "Pushed changes and tag v$(echo $VERSION) to remote repository."

echo "Remember to bump the version to -dev"

cd ..

