FROM rust:bullseye AS builder

ARG CARGO_FEATURES=release-feature-set
ARG CARGO_PROFILE=release

RUN echo "Adding Node.js PPA" \
    && curl -s https://deb.nodesource.com/setup_16.x | bash

RUN apt-get -y update \
    && apt-get -y install ca-certificates \
                          clang \
                          cmake \
                          libssl-dev \
                          libsasl2-dev \
                          llvm \
                          nodejs \
                          protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Required by tonic
RUN rustup component add rustfmt

# Build UI
COPY quickwit/quickwit-ui /quickwit/quickwit-ui

WORKDIR /quickwit/quickwit-ui

RUN echo "Building Quickwit UI" \
    && touch .gitignore_for_build_directory \
    && npm install --location=global yarn \
    && make install build

# Build workspace
COPY quickwit /quickwit

WORKDIR /quickwit

RUN echo "Building workspace with feature(s) '$CARGO_FEATURES' and profile '$CARGO_PROFILE'" \
    && cargo build \
        --features $CARGO_FEATURES \
        $(test "$CARGO_PROFILE" = "release" && echo "--release") \
    && echo "Copying binaries to /quickwit/bin" \
    && mkdir -p /quickwit/bin \
    && find target/$CARGO_PROFILE -maxdepth 1 -perm /a+x -type f -exec mv {} /quickwit/bin \;

# Change the default configuration file in order to make the REST,
# gRPC, and gossip services accessible outside of Docker container.
COPY config/quickwit.yaml /quickwit/config/quickwit.yaml
RUN sed -i 's/#[ ]*listen_address: 127.0.0.1/listen_address: 0.0.0.0/g' config/quickwit.yaml


FROM debian:bullseye-slim AS quickwit

LABEL org.opencontainers.image.title="Quickwit"
LABEL maintainer="Quickwit, Inc. <hello@quickwit.io>"
LABEL org.opencontainers.image.vendor="Quickwit, Inc."
LABEL org.opencontainers.image.licenses="AGPL-3.0"

RUN apt-get -y update \
    && apt-get -y install ca-certificates \
                          libssl1.1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /quickwit
RUN mkdir config qwdata
COPY --from=builder /quickwit/bin/quickwit /usr/local/bin/quickwit
COPY --from=builder /quickwit/config/quickwit.yaml /quickwit/config/quickwit.yaml

ENV QW_CONFIG=/quickwit/config/quickwit.yaml
ENV QW_DATA_DIR=/quickwit/qwdata

ENTRYPOINT ["/usr/local/bin/quickwit"]
