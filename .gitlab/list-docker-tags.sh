#!/bin/bash

# this file defines how we tag new releases
#
# - we always push to edge
# - if there is a `cp*` tag, we push to `devel`
# - if there is a `cp*` tag, we push to `cp*`
# - if there is a `v<version>-<rc-marker>` tag, we push to `v<version>-<rc-marker>`, but not latest or semver-compat (do we push devel?)
# - if there is a `v<version>` tag, we push to latest
# - if there is a `v<version>` tag, we push to `v*`, and the semver-corresponding shorter versions

# TODO we could have safety against re-ran pipelines (if we rerun v1.2.3, we don't want to downgrade v1.2 from .4 to .3)

set -euo pipefail

if [ -z "$IMG_DESTINATION_BASE" ]; then
  echo "Error: IMG_DESTINATION_BASE. This should be set to the destination docker image, excluding the tag name, e.g. dd-lib-dotnet-init"
  exit 1
fi

IMG_DESTINATIONS="${IMG_DESTINATION_BASE}:edge"

if [ ! -z "${CI_COMMIT_TAG-}" ]; then
  if echo "$CI_COMMIT_TAG" | grep -q "^cp"; then
    # cp*
    IMG_DESTINATIONS="${IMG_DESTINATIONS},${IMG_DESTINATION_BASE}:devel"
    IMG_DESTINATIONS="${IMG_DESTINATIONS},${IMG_DESTINATION_BASE}:${CI_COMMIT_TAG}"
  fi
  if echo "$CI_COMMIT_TAG" | grep -qE "^v\d+\.\d+\.\d+-.+$"; then
    # rc
    IMG_DESTINATIONS="${IMG_DESTINATIONS},${IMG_DESTINATION_BASE}:${CI_COMMIT_TAG}"
  fi
  if echo "$CI_COMMIT_TAG" | grep -qE "^v\d+\.\d+\.\d+$"; then
    # true release
    MAJOR_MINOR_VERSION="$(echo ${CI_COMMIT_TAG} | sed -nE 's/^(v[0-9]+\.[0-9]+)\.[0-9]+$/\1/p')"
    MAJOR_VERSION="$(echo ${CI_COMMIT_TAG} | sed -nE 's/^(v[0-9]+)\.[0-9]+\.[0-9]+$/\1/p')"

    IMG_DESTINATIONS="${IMG_DESTINATIONS},${IMG_DESTINATION_BASE}:latest"
    IMG_DESTINATIONS="${IMG_DESTINATIONS},${IMG_DESTINATION_BASE}:${CI_COMMIT_TAG}"
    IMG_DESTINATIONS="${IMG_DESTINATIONS},${IMG_DESTINATION_BASE}:$MAJOR_MINOR_VERSION"
    IMG_DESTINATIONS="${IMG_DESTINATIONS},${IMG_DESTINATION_BASE}:$MAJOR_VERSION"
  fi
fi

echo "IMG_DESTINATIONS=${IMG_DESTINATIONS}" | tee -a build.env
