#!/usr/bin/env bash

set -euo pipefail

SOURCE="${BASH_SOURCE[0]}"

while [ -h "$SOURCE" ]; do
  DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
done

cd "$(dirname "$SOURCE")"

TARGET="$1"
if [[ $(echo "$TARGET" | tr -cd .|wc -c) = 2 ]]; then
  CLUSTER_TARGET=$(ddtool clusters list --selector \
        'cluster.datacenter == "'"$TARGET"'" && "rapid" in cluster.workloads && cluster.zonal_cluster_set != []' \
        | jq -r .[0].name)
else
  CLUSTER_TARGET="$TARGET"
fi
CLUSTER_BEFORE=$(kubectl config current-context)

# restore env after running
trap 'ddtool clusters use $CLUSTER_BEFORE' EXIT
trap 'kill $(jobs -p)' EXIT

ddtool clusters use "$CLUSTER_TARGET"

POD_TARGET=$(kubectl get pods -n rapid-event-platform -l 'service=cloudprem-bridge' -o json | jq -r .items[0].metadata.name)

echo "connecting to pod $POD_TARGET"

# to update run `crane ls registry.ddbuild.io/traffic-toolbox | grep -E '^v\d+\.\d+\.\d+$' |sort -rV | head -n1`
# this takes a good 20s to run, so running it interactively isn't really an option
VERSION="v1.144.1"

TARGET=${2:-}

TOOLBOX_CMD="ddtool clusters debug --image traffic-toolbox:$VERSION $POD_TARGET -n rapid-event-platform"

PORT=$(shuf -i 2000-8000 -n 1)

if [[ -n "$TARGET" ]]; then
  # setup a forwarding from dc to customer
  $TOOLBOX_CMD -- socat "TCP4-LISTEN:$PORT,fork,reuseaddr" "TCP4:$TARGET" &
  # setup a forwarding from here to dc
  kubectl port-forward -n rapid-event-platform "$POD_TARGET" "$PORT:$PORT" &

  sleep 5

  # setup a CloudPrem to Jaeger adapter
  ../quickwit/quickwit-remote-api/run-with-vault.sh -p "127.0.0.1:$PORT" "$TARGET" &
  # and finally run jaeger
  docker run --rm --name jaeger-qw -e SPAN_STORAGE_TYPE=grpc -e GRPC_STORAGE_SERVER=docker.for.mac.host.internal:7381 -p 16686:16686 jaegertracing/jaeger-query:latest &

  wait
else
  $TOOLBOX_CMD
fi
