FROM node:20@sha256:8cdc6b9b711af0711cc6139955cc1331fab5e0a995afd3260c52736fbc338059 AS ui-builder

COPY quickwit/quickwit-ui /quickwit/quickwit-ui

WORKDIR /quickwit/quickwit-ui

RUN touch .gitignore_for_build_directory \
    && NODE_ENV=production make install build


FROM rust:bookworm@sha256:3914072ca0c3b8aad871db9169a651ccfce30cf58303e5d6f2db16d1d8a7e58f AS bin-builder

ARG CARGO_FEATURES=release-feature-set
ARG CARGO_PROFILE=release
ARG QW_COMMIT_DATE
ARG QW_COMMIT_HASH
ARG QW_COMMIT_TAGS
ARG CI_JOB_TOKEN

ENV QW_COMMIT_DATE=$QW_COMMIT_DATE
ENV QW_COMMIT_HASH=$QW_COMMIT_HASH
ENV QW_COMMIT_TAGS=$QW_COMMIT_TAGS

RUN git config --global url."https://gitlab-ci-token:${CI_JOB_TOKEN}@gitlab.ddbuild.io/DataDog/".insteadOf "ssh://git@github.com/DataDog/"

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
COPY config/quickwit.yaml /quickwit/config/quickwit.yaml
COPY config/cloudprem/datadog.yaml /config/cloudprem/datadog.yaml
COPY --from=ui-builder /quickwit/quickwit-ui/build /quickwit/quickwit-ui/build

WORKDIR /quickwit

RUN rustup toolchain install

RUN echo "Building workspace with feature(s) '$CARGO_FEATURES' and profile '$CARGO_PROFILE'" \
    && RUSTFLAGS="--cfg tokio_unstable" \
    cargo build \
    -p quickwit-cli \
    --features $CARGO_FEATURES \
    --bin quickwit \
    $(test "$CARGO_PROFILE" = "release" && echo "--release") \
    && echo "Copying binaries to /quickwit/bin" \
    && mkdir -p /quickwit/bin \
    && find target/$CARGO_PROFILE -maxdepth 1 -perm /a+x -type f -exec mv {} /quickwit/bin \;


FROM registry.ddbuild.io/images/base/gbi-ubuntu_2204:latest AS quickwit

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
COPY --from=bin-builder /quickwit/bin/quickwit /usr/local/bin/quickwit
COPY --from=bin-builder /quickwit/config/quickwit.yaml /quickwit/config/quickwit.yaml

ENV QW_CONFIG=/quickwit/config/quickwit.yaml
ENV QW_DATA_DIR=/quickwit/qwdata
ENV QW_LISTEN_ADDRESS=0.0.0.0

USER dog

RUN quickwit --version

ENTRYPOINT ["quickwit"]
