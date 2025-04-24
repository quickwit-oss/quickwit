#!/usr/bin/env bash

set -euo pipefail

CLUSTER_TARGET="$1"
CLUSTER_BEFORE=$(kubectl config current-context)

# restore env after running
trap "ddtool clusters use $CLUSTER_BEFORE" EXIT
ddtool clusters use "$CLUSTER_TARGET"

POD_TARGET=$(kubectl get pods -n rapid-event-platform -l 'service=cloudprem-bridge' -o json | jq -r .items[0].metadata.name)

echo "connecting to pod $POD_TARGET"

# to update run `crane ls registry.ddbuild.io/traffic-toolbox | grep -E '^v\d+\.\d+\.\d+$' |sort -rV | head -n1`
# this takes a good 20s to run, so running it interactively isn't really an option
VERSION="v1.139.0"

ddtool clusters debug --image "traffic-toolbox:$VERSION" "$POD_TARGET" -n rapid-event-platform
# we could run a port-forward such as
# kubectl port-forward -n rapid-event-platform "$POT_TARGET" 9443
