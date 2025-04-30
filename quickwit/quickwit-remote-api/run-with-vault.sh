#!/bin/bash

set -euo pipefail

export VAULT_ADDR=https://vault.us1.staging.dog/

KV_PATH=kv/data/k8s/rapid-event-platform/cloudprem-bridge/dev-tls-cert

ddtool auth login
KV_VAL=$(vault read -format json "$KV_PATH")

cargo run -- --key <(echo "$KV_VAL" | jq -r '.data.data.key') --cert <(echo "$KV_VAL" | jq -r '.data.data.cert') "$@"
