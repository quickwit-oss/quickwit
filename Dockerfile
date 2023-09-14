FROM node:18 as ui-builder

COPY quickwit/quickwit-ui /quickwit/quickwit-ui

WORKDIR /quickwit/quickwit-ui

RUN touch .gitignore_for_build_directory \
    && make install build


FROM rust:bullseye AS bin-builder-base

ARG CARGO_PROFILE=release
ARG QW_COMMIT_DATE
ARG QW_COMMIT_HASH
ARG QW_COMMIT_TAGS

ENV QW_COMMIT_DATE=$QW_COMMIT_DATE
ENV QW_COMMIT_HASH=$QW_COMMIT_HASH
ENV QW_COMMIT_TAGS=$QW_COMMIT_TAGS

RUN apt-get -y update \
    && apt-get -y install ca-certificates \
                          clang \
                          cmake \
                          libssl-dev \
                          llvm \
                          protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Required by tonic
RUN rustup component add rustfmt

# Build workspace
COPY quickwit /quickwit
COPY config/quickwit.yaml /quickwit/config/quickwit.yaml

WORKDIR /quickwit


FROM  bin-builder-base AS bin-builder-cli

ARG CARGO_FEATURES=release-feature-set

COPY --from=ui-builder /quickwit/quickwit-ui/build /quickwit/quickwit-ui/build

RUN echo "Building workspace with feature(s) '$CARGO_FEATURES' and profile '$CARGO_PROFILE'" \
    && cargo build \
        --features $CARGO_FEATURES \
        $(test "$CARGO_PROFILE" = "release" && echo "--release") \
    && echo "Copying binaries to /quickwit/bin" \
    && mkdir -p /quickwit/bin \
    && find target/$CARGO_PROFILE -maxdepth 1 -perm /a+x -type f -exec mv {} /quickwit/bin \;


FROM  bin-builder-base AS bin-builder-lambda

ARG CARGO_FEATURES=lambda

# empty dir to avoid packaging the frontend to the Lambda binary
RUN mkdir -p /quickwit/quickwit-ui/build

RUN echo "Building workspace with feature(s) '$CARGO_FEATURES' and profile '$CARGO_PROFILE'" \
    && cargo build \
        --features $CARGO_FEATURES \
        $(test "$CARGO_PROFILE" = "release" && echo "--release") \
    && echo "Copying binaries to /quickwit/bin" \
    && mkdir -p /quickwit/bin \
    && find target/$CARGO_PROFILE -maxdepth 1 -perm /a+x -type f -exec mv {} /quickwit/bin \;


FROM debian:bullseye-slim AS quickwit-base

LABEL org.opencontainers.image.title="Quickwit"
LABEL maintainer="Quickwit, Inc. <hello@quickwit.io>"
LABEL org.opencontainers.image.vendor="Quickwit, Inc."
LABEL org.opencontainers.image.licenses="AGPL-3.0"

RUN apt-get -y update \
    && apt-get -y install ca-certificates \
                          libssl1.1 \
    && rm -rf /var/lib/apt/lists/*


FROM quickwit-base AS quickwit-lambda

WORKDIR /quickwit
RUN mkdir config qwdata
COPY --from=bin-builder-lambda /quickwit/bin/quickwit-lambda /usr/local/bin/quickwit-lambda
COPY --from=bin-builder-lambda /quickwit/config/quickwit.yaml /quickwit/config/quickwit.yaml

ENV QW_CONFIG=/quickwit/config/quickwit.yaml
ENV QW_DATA_DIR=/quickwit/qwdata
ENV QW_LISTEN_ADDRESS=0.0.0.0

ENTRYPOINT ["quickwit-lambda"]


FROM quickwit-base AS quickwit

WORKDIR /quickwit
RUN mkdir config qwdata
COPY --from=bin-builder-cli /quickwit/bin/quickwit /usr/local/bin/quickwit
COPY --from=bin-builder-cli /quickwit/config/quickwit.yaml /quickwit/config/quickwit.yaml

ENV QW_CONFIG=/quickwit/config/quickwit.yaml
ENV QW_DATA_DIR=/quickwit/qwdata
ENV QW_LISTEN_ADDRESS=0.0.0.0

RUN quickwit --version

ENTRYPOINT ["quickwit"]
