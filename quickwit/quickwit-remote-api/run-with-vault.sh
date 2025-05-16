#!/bin/bash

set -euo pipefail

SOURCE="${BASH_SOURCE[0]}"

while [ -h "$SOURCE" ]; do
  DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
done

SCRIPT_DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"

cd "$SCRIPT_DIR"

export VAULT_ADDR=https://vault.us1.staging.dog/

KV_PATH=kv/data/k8s/rapid-event-platform/cloudprem-bridge/dev-tls-cert

ddtool auth login
KV_VAL=$(vault read -format json "$KV_PATH")

trap 'kill $(jobs -p)' EXIT

cargo run --bin remote-api -- --key <(echo "$KV_VAL" | jq -r '.data.data.key') --cert <(echo "$KV_VAL" | jq -r '.data.data.cert') "$@"
