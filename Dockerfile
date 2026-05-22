FROM registry.ddbuild.io/images/pomsky/node:24@sha256:b2b2184ba9b78c022e1d6a7924ec6fba577adf28f15c9d9c457730cc4ad3807a AS ui-builder

COPY quickwit/quickwit-ui /quickwit/quickwit-ui

WORKDIR /quickwit/quickwit-ui

RUN touch .gitignore_for_build_directory \
    && NODE_ENV=production make install build

FROM registry.ddbuild.io/images/pomsky/node:20@sha256:8cdc6b9b711af0711cc6139955cc1331fab5e0a995afd3260c52736fbc338059 AS cloudprem-ui-loader
COPY quickwit/cloudprem-ui /quickwit/cloudprem-ui
WORKDIR /quickwit/cloudprem-ui

ARG CLOUDPREM_UI_ENV=prod
ARG CLOUDPREM_UI_VERSION=0.1.0
ENV CLOUDPREM_UI_ENV=$CLOUDPREM_UI_ENV
ENV CLOUDPREM_UI_VERSION=$CLOUDPREM_UI_VERSION

RUN touch .gitignore_for_build_directory \
    && make load-cloudprem-ui

FROM registry.ddbuild.io/images/pomsky/rust:bookworm@sha256:b5efaabfd787a695d2e46b37d3d9c54040e11f4c10bc2e714bbadbfcc0cd6c39 AS bin-builder

ARG CARGO_FEATURES=release-feature-set
ARG CARGO_FEATURES_METRICS=release-feature-set-metrics
ARG CARGO_PROFILE=release
ARG INCLUDE_POMSKY_INTAKE=true
ARG INCLUDE_QUICKWIT_METRICS=true
ARG QW_COMMIT_DATE
ARG QW_COMMIT_HASH
ARG QW_COMMIT_TAGS
ARG LAMBDA_ZIP_PATH

ENV QW_COMMIT_DATE=$QW_COMMIT_DATE
ENV QW_COMMIT_HASH=$QW_COMMIT_HASH
ENV QW_COMMIT_TAGS=$QW_COMMIT_TAGS
ENV LAMBDA_ZIP_PATH=$LAMBDA_ZIP_PATH

# dd-octo-sts CLI copied from its published scratch image.
COPY --from=registry.ddbuild.io/images/dd-octo-sts-ci-base:v107310663-4dd9003-2026.04-2 /usr/local/bin/dd-octo-sts /usr/local/bin/dd-octo-sts

RUN apt-get -y update \
    && apt-get -y install \
        ca-certificates \
        clang \
        cmake \
        libssl-dev \
        llvm \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Install the pinned toolchain before copying full source so this layer is cached
# unless rust-toolchain.toml itself changes.
COPY quickwit/rust-toolchain.toml /quickwit/rust-toolchain.toml
WORKDIR /quickwit
RUN rustup toolchain install

COPY quickwit /quickwit
COPY .cargo/config.toml /quickwit/.cargo/config.toml

COPY config/cloudprem/datadog-logs.yaml /config/cloudprem/datadog-logs.yaml
COPY config/cloudprem/datadog-metrics.yaml /config/cloudprem/datadog-metrics.yaml
COPY config/cloudprem/datadog-sketches.yaml /config/cloudprem/datadog-sketches.yaml
COPY config/cloudprem/datadog-spans.yaml /config/cloudprem/datadog-spans.yaml
COPY config/quickwit.yaml /quickwit/config/quickwit.yaml

COPY --from=ui-builder /quickwit/quickwit-ui/build /quickwit/quickwit-ui/build
COPY --from=cloudprem-ui-loader /quickwit/cloudprem-ui/cloudprem_ui_build /quickwit/cloudprem-ui/cloudprem_ui_build

# Mint dd-octo-sts tokens via OIDC secret and configure git URL rewrites.
# Optional: if no OIDC secret is provided (e.g. local image-tool.sh invocation),
# the mint is skipped and cargo will fall back to SSH for the private repos.
# Placed after the source COPY so the cached token layer is invalidated whenever
# source changes — keeping minted tokens fresh on every meaningful rebuild.
RUN --mount=type=secret,id=ddoctosts_oidc,required=false \
    if [ -s /run/secrets/ddoctosts_oidc ]; then \
      export DDOCTOSTS_ID_TOKEN=$(cat /run/secrets/ddoctosts_oidc) \
      && EVENT_PERCOLATION_TOKEN=$(dd-octo-sts token --disable-tracing --scope DataDog/event-percolation --policy access_pomsky_gitlab) \
      && git config --global url."https://x-access-token:${EVENT_PERCOLATION_TOKEN}@github.com/DataDog/event-percolation".insteadOf "ssh://git@github.com/DataDog/event-percolation"; \
    fi
# Fall back to CI_JOB_TOKEN via GitLab mirror for remaining DataDog repos
RUN --mount=type=secret,id=ci_job_token,required=false \
    if [ -s /run/secrets/ci_job_token ]; then \
      CI_JOB_TOKEN=$(cat /run/secrets/ci_job_token) \
      && git config --global url."https://gitlab-ci-token:${CI_JOB_TOKEN}@gitlab.ddbuild.io/DataDog/".insteadOf "ssh://git@github.com/DataDog/"; \
    fi

# Cache mounts persist the cargo registry, git checkouts, and target/ across builds.
# These live in the local BuildKit cache — prune periodically with `docker buildx prune`
# if disk usage grows too large.
# Because target/ is a cache mount, the produced binaries are NOT in the image layer —
# we must explicitly copy each one out into /quickwit/bin/.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/quickwit/target,sharing=locked \
    set -eu; \
    echo "Building binaries with profile '$CARGO_PROFILE'"; \
    mkdir -p /quickwit/bin; \
    if [ "$CARGO_PROFILE" = "dev" ]; then \
        TARGET_DIR="debug"; \
        RELEASE_FLAG=""; \
    else \
        TARGET_DIR="$CARGO_PROFILE"; \
        RELEASE_FLAG="--release"; \
    fi; \
    echo "Building quickwit with feature(s) '$CARGO_FEATURES'"; \
    RUSTFLAGS="--cfg tokio_unstable" \
        cargo build -p quickwit-cli --features "$CARGO_FEATURES" --bin quickwit $RELEASE_FLAG; \
    cp "target/$TARGET_DIR/quickwit" /quickwit/bin/quickwit; \
    if [ "$INCLUDE_QUICKWIT_METRICS" = "true" ]; then \
        echo "Building quickwit-metrics with feature(s) '$CARGO_FEATURES_METRICS'"; \
        RUSTFLAGS="--cfg tokio_unstable" \
            cargo build -p quickwit-cli --features "$CARGO_FEATURES_METRICS" --bin quickwit --target-dir target/metrics $RELEASE_FLAG; \
        cp "target/metrics/$TARGET_DIR/quickwit" /quickwit/bin/quickwit-metrics; \
    fi; \
    if [ "$INCLUDE_POMSKY_INTAKE" = "true" ]; then \
        echo "Building pomsky-intake"; \
        RUSTFLAGS="--cfg tokio_unstable" \
            cargo build -p pomsky-intake --bin pomsky-intake $RELEASE_FLAG; \
        cp "target/$TARGET_DIR/pomsky-intake" /quickwit/bin/pomsky-intake; \
    else \
        echo "Skipping pomsky-intake (INCLUDE_POMSKY_INTAKE=$INCLUDE_POMSKY_INTAKE)"; \
    fi; \
    rm -f /root/.gitconfig

FROM registry.ddbuild.io/images/base/gbi-ubuntu_2404:latest AS quickwit

LABEL org.opencontainers.image.title="Datadog CloudPrem"
LABEL maintainer="Datadog, Inc."
LABEL org.opencontainers.image.vendor="Datadog, Inc."
LABEL org.opencontainers.image.licenses="Datadog EULA"

COPY NOTICE /quickwit/
COPY LICENSE /quickwit/
COPY LICENSE-3rdparty.csv /quickwit/

# Switch to root so we can install dependencies
USER root

RUN apt-get -y update \
    && apt-get -y install \
        ca-certificates \
        libssl3 \
        tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /quickwit
RUN mkdir config qwdata
COPY --from=bin-builder /quickwit/bin/ /usr/local/bin/
COPY --from=bin-builder /quickwit/config/quickwit.yaml /quickwit/config/quickwit.yaml

ENV QW_CONFIG=/quickwit/config/quickwit.yaml
ENV QW_DATA_DIR=/quickwit/qwdata
ENV QW_LISTEN_ADDRESS=0.0.0.0

USER dog

RUN quickwit --version
RUN if [ -x /usr/local/bin/quickwit-metrics ]; then quickwit-metrics --version; fi
RUN if [ -x /usr/local/bin/pomsky-intake ]; then pomsky-intake --help > /dev/null; fi

ENTRYPOINT ["quickwit"]
