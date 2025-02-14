#!/bin/bash
set -euo pipefail

# Default values
DEFAULT_REGISTRY="registry.ddbuild.io"
DEFAULT_REPO="pomsky"
DEFAULT_TAG="edge"
DEFAULT_TARGET_ENV="staging"
DEFAULT_PLATFORM="linux/arm64"

# Initialize variables with defaults
REGISTRY="$DEFAULT_REGISTRY"
REPO="$DEFAULT_REPO"
TAG="$DEFAULT_TAG"
TARGET_ENV="$DEFAULT_TARGET_ENV"
PLATFORM="$DEFAULT_PLATFORM"
DO_PUSH=false
DO_SIGN=false
METADATA_FILE=""

usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  --registry <registry>          Docker registry (default: $DEFAULT_REGISTRY)"
    echo "  --repo <repository>            Docker repository (default: $DEFAULT_REPO)"
    echo "  --tag <tag>                    Image tag (default: $DEFAULT_TAG)"
    echo "  --target-env <env>             Target environment (default: $DEFAULT_TARGET_ENV)"
    echo "  --platform <platform>          Build platform (default: $DEFAULT_PLATFORM)"
    echo "  --metadata-file <file>         Path to save metadata file (optional)"
    echo "  --push                         Push the image after building"
    echo "  --sign                         Sign the image after building"
    exit 1
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --registry)
            if [ -n "${2:-}" ]; then
                REGISTRY="$2"
                shift 2
            else
                echo "Error: --registry requires a value"
                usage
            fi
            ;;
        --repo)
            if [ -n "${2:-}" ]; then
                REPO="$2"
                shift 2
            else
                echo "Error: --repo requires a value"
                usage
            fi
            ;;
        --tag)
            if [ -n "${2:-}" ]; then
                TAG="$2"
                shift 2
            else
                echo "Error: --tag requires a value"
                usage
            fi
            ;;
        --target-env)
            if [ -n "${2:-}" ]; then
                TARGET_ENV="$2"
                shift 2
            else
                echo "Error: --target-env requires a value"
                usage
            fi
            ;;
        --platform)
            if [ -n "${2:-}" ]; then
                PLATFORM="$2"
                shift 2
            else
                echo "Error: --platform requires a value"
                usage
            fi
            ;;
        --metadata-file)
            if [ -n "${2:-}" ]; then
                METADATA_FILE="$2"
                shift 2
            else
                echo "Error: --metadata-file requires a value"
                usage
            fi
            ;;
        --push)
            DO_PUSH=true
            shift
            ;;
        --sign)
            DO_SIGN=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown parameter: $1"
            usage
            ;;
    esac
done

# Construct Docker image name
DOCKER_IMAGE="${REGISTRY}/${REPO}:${TAG}"

# Set up metadata file
if [ -z "$METADATA_FILE" ]; then
    METADATA_FILE=$(mktemp)
    CLEANUP_METADATA_FILE=true
else
    CLEANUP_METADATA_FILE=false
fi

# Build args
QW_COMMIT_DATE=$(TZ=UTC0 git log -1 --format=%cd --date=format-local:%Y-%m-%dT%H:%M:%SZ)
QW_COMMIT_HASH=$(git rev-parse HEAD)
QW_COMMIT_TAGS=$(git tag --points-at HEAD | tr '\n' ',')

# Function to build the image
build_image() {
    local push_flag=""
    if [ "$DO_PUSH" = true ]; then
        push_flag="--push"
    fi

    echo "Building Docker image..."
    echo "Registry: $REGISTRY"
    echo "Repository: $REPO"
    echo "Tag: $TAG"
    echo "Target Environment: $TARGET_ENV"
    echo "Platform: $PLATFORM"
    echo "Metadata File: $METADATA_FILE"
    
    docker buildx build \
        -f Dockerfile \
        -t "$DOCKER_IMAGE" \
        --build-arg QW_COMMIT_DATE="$QW_COMMIT_DATE" \
        --build-arg QW_COMMIT_HASH="$QW_COMMIT_HASH" \
        --build-arg QW_COMMIT_TAGS="$QW_COMMIT_TAGS" \
        --platform "$PLATFORM" \
        --label target="$TARGET_ENV" \
        --metadata-file "$METADATA_FILE" \
        $push_flag .
}

# Function to sign the image
sign_image() {
    echo "Signing Docker image..."
    ddsign sign "$DOCKER_IMAGE" --docker-metadata-file "$METADATA_FILE"
}

# Main execution logic
build_image

if [ "$DO_SIGN" = true ]; then
    if [ "$DO_PUSH" = false ]; then
        echo "Warning: Signing an image that wasn't pushed"
    fi
    sign_image
fi

# Cleanup only if we created a temporary file
if [ "$CLEANUP_METADATA_FILE" = true ]; then
    rm -f "$METADATA_FILE"
else
    echo "Metadata file saved to: $METADATA_FILE"
fi
