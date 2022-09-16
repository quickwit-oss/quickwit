#!/usr/bin/env bash

set -x

# npm install prettier prettier-plugin-toml
find . -maxdepth 4 -name "Cargo.toml" | xargs -P 4 npx prettier --write

find . -type f -name 'Cargo.toml' -exec sed -i '1,5 s/^version\ =\ "'"$1"'"/version\ =\ "'"$2"'"/g' {} +
find . -type f -name 'Cargo.toml' -exec sed -i 's/^quickwit-\(.*\)\ =\ {\ version\ =\ "'"$1"'"/quickwit-\1\ =\ {\ version\ =\ "'"$2"'"/g' {} +
