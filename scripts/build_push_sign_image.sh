#!/bin/bash
set -euo pipefail

# Variables
DOCKER_IMAGE="registry.ddbuild.io/pomsky:edge"
DOCKER_PLATFORM="linux/arm64"
DOCKER_METADATA_FILE="metadata.json"

# Build and push the Docker image
docker buildx build \
  --platform "$DOCKER_PLATFORM" \
  -t "$DOCKER_IMAGE" \
  -f Dockerfile \
  --label target=staging \
  --metadata-file "$DOCKER_METADATA_FILE" \
  --push .

# Sign the Docker image
ddsign sign "$DOCKER_IMAGE" --docker-metadata-file "$DOCKER_METADATA_FILE"
