#!/bin/bash
set -euo pipefail

# Default values
DEFAULT_REGISTRY="registry.ddbuild.io"
DEFAULT_REPO="pomsky"
DEFAULT_TAG="edge"
DEFAULT_TARGET_ENV="staging"
DEFAULT_PLATFORM="linux/arm64"

# Allow override through environment variables
REGISTRY="${REGISTRY:-$DEFAULT_REGISTRY}"
REPO="${REPO:-$DEFAULT_REPO}"
TAG="${TAG:-$DEFAULT_TAG}"
TARGET_ENV="${TARGET_ENV:-$DEFAULT_TARGET_ENV}"
PLATFORM="${PLATFORM:-$DEFAULT_PLATFORM}"

# Construct Docker image name
DOCKER_IMAGE="${REGISTRY}/${REPO}:${TAG}"
METADATA_FILE=$(mktemp)

# Build args
QW_COMMIT_DATE=$(TZ=UTC0 git log -1 --format=%cd --date=format-local:%Y-%m-%dT%H:%M:%SZ)
QW_COMMIT_HASH=$(git rev-parse HEAD)
QW_COMMIT_TAGS=$(git tag --points-at HEAD | tr '\n' ',')

# Build and push the Docker image
docker buildx build \
  -f Dockerfile \
  -t "$DOCKER_IMAGE" \
  --build-arg QW_COMMIT_DATE="$QW_COMMIT_DATE" \
  --build-arg QW_COMMIT_HASH="$QW_COMMIT_HASH" \
  --build-arg QW_COMMIT_TAGS="$QW_COMMIT_TAGS" \
  --platform "$PLATFORM" \
  --label target="$TARGET_ENV" \
  --metadata-file "$METADATA_FILE" \
  --push .

# Sign the Docker image
ddsign sign "$DOCKER_IMAGE" --docker-metadata-file "$METADATA_FILE"
