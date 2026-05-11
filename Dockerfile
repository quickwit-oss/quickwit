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
ARG CARGO_PROFILE=release
ARG QW_COMMIT_DATE
ARG QW_COMMIT_HASH
ARG QW_COMMIT_TAGS
# it's dangerous to expose tokens in ARGs like this, but this is an intermediate build container, so its arguments are not stored in the final image
ARG CI_JOB_TOKEN

ENV QW_COMMIT_DATE=$QW_COMMIT_DATE
ENV QW_COMMIT_HASH=$QW_COMMIT_HASH
ENV QW_COMMIT_TAGS=$QW_COMMIT_TAGS

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

COPY quickwit /quickwit
COPY .cargo/config.toml /quickwit/.cargo/config.toml

COPY config/cloudprem/datadog-logs.yaml /config/cloudprem/datadog-logs.yaml
COPY config/cloudprem/datadog-metrics.yaml /config/cloudprem/datadog-metrics.yaml
COPY config/cloudprem/datadog-sketches.yaml /config/cloudprem/datadog-sketches.yaml
COPY config/cloudprem/datadog-spans.yaml /config/cloudprem/datadog-spans.yaml
COPY config/quickwit.yaml /quickwit/config/quickwit.yaml

COPY --from=ui-builder /quickwit/quickwit-ui/build /quickwit/quickwit-ui/build
COPY --from=cloudprem-ui-loader /quickwit/cloudprem-ui/cloudprem_ui_build /quickwit/cloudprem-ui/cloudprem_ui_build

WORKDIR /quickwit

# Mint dd-octo-sts tokens via OIDC secret and configure git URL rewrites.
# Optional: if no OIDC secret is provided (e.g. local image-tool.sh invocation),
# the mint is skipped and cargo will fall back to SSH for the private repos.
RUN --mount=type=secret,id=ddoctosts_oidc,required=false \
    if [ -s /run/secrets/ddoctosts_oidc ]; then \
      export DDOCTOSTS_ID_TOKEN=$(cat /run/secrets/ddoctosts_oidc) \
      && POMCHI_TOKEN=$(dd-octo-sts token --disable-tracing --scope DataDog/PomChi --policy access_pomsky_gitlab) \
      && EVENT_PERCOLATION_TOKEN=$(dd-octo-sts token --disable-tracing --scope DataDog/event-percolation --policy access_pomsky_gitlab) \
      && git config --global url."https://x-access-token:${POMCHI_TOKEN}@github.com/DataDog/PomChi".insteadOf "ssh://git@github.com/DataDog/PomChi" \
      && git config --global url."https://x-access-token:${EVENT_PERCOLATION_TOKEN}@github.com/DataDog/event-percolation".insteadOf "ssh://git@github.com/DataDog/event-percolation"; \
    fi
# Fall back to CI_JOB_TOKEN via GitLab mirror for remaining DataDog repos
RUN if [ -n "$CI_JOB_TOKEN" ]; then \
      git config --global url."https://gitlab-ci-token:${CI_JOB_TOKEN}@gitlab.ddbuild.io/DataDog/".insteadOf "ssh://git@github.com/DataDog/"; \
    fi

RUN rustup toolchain install

RUN echo "Building binaries with feature(s) '$CARGO_FEATURES' and profile '$CARGO_PROFILE'" \
    && echo "Building pomsky" \
    && RUSTFLAGS="--cfg tokio_unstable" \
    cargo build \
    -p quickwit-cli \
    --features $CARGO_FEATURES \
    --bin quickwit \
    $(test "$CARGO_PROFILE" = "release" && echo "--release") \
    && echo "Building pomsky-intake" \
    && RUSTFLAGS="--cfg tokio_unstable" \
    cargo build \
    -p pomsky-intake \
    --bin pomsky-intake \
    $(test "$CARGO_PROFILE" = "release" && echo "--release") \
    && echo "Copying binaries to /quickwit/bin" \
    && mkdir -p /quickwit/bin \
    && TARGET_DIR=$(test "$CARGO_PROFILE" = "dev" && echo "debug" || echo "$CARGO_PROFILE") \
    && find target/$TARGET_DIR -maxdepth 1 -perm /a+x -type f -exec mv {} /quickwit/bin \;

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
COPY --from=bin-builder /quickwit/bin/pomsky-intake /usr/local/bin/pomsky-intake
COPY --from=bin-builder /quickwit/bin/quickwit /usr/local/bin/quickwit
COPY --from=bin-builder /quickwit/config/quickwit.yaml /quickwit/config/quickwit.yaml

ENV QW_CONFIG=/quickwit/config/quickwit.yaml
ENV QW_DATA_DIR=/quickwit/qwdata
ENV QW_LISTEN_ADDRESS=0.0.0.0

USER dog

RUN pomsky-intake --help > /dev/null
RUN quickwit --version

ENTRYPOINT ["quickwit"]
