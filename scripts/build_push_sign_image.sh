#!/bin/bash
set -euo pipefail

# Variables
DOCKER_IMAGE="registry.ddbuild.io/logs-clouprem/pomsky:edge"
DOCKER_PLATFORM="linux/arm64"
DOCKER_METADATA_FILE="metadata.json"

# Build and push the Docker image
ls -la
docker buildx build \
  --platform "$DOCKER_PLATFORM" \
  -t "$DOCKER_IMAGE" \
  -f Dockerfile \
  --metadata-file "$DOCKER_METADATA_FILE" \
  --push .

# Sign the Docker image
ddsign sign "$DOCKER_IMAGE" --docker-metadata-file "$DOCKER_METADATA_FILE"
