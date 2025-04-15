#!/bin/bash

set -euo pipefail

export VAULT_ADDR=https://vault.us1.staging.dog/

SOURCE="${BASH_SOURCE[0]}"

while [ -h "$SOURCE" ]; do
  DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
done

SCRIPT_DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"

PROTO_DIR="${SCRIPT_DIR}/../quickwit/quickwit-proto/protos/cloudprem"

KV_PATH=kv/data/k8s/rapid-event-platform/cloudprem-bridge/dev-tls-cert


ddtool auth login
KV_VAL=$(vault read -format json "$KV_PATH")

PROTO_FILES_ARG=$(ls $PROTO_DIR/ | grep .proto | sed "s/^/-proto /" | xargs)


grpcurl -key <(echo "$KV_VAL" | jq -r '.data.data.key') -cert <(echo "$KV_VAL" | jq -r '.data.data.cert') -import-path "$PROTO_DIR" $PROTO_FILES_ARG "$@"
