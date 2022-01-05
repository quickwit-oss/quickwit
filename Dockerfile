FROM rust:bullseye AS builder

ARG CARGO_FEATURES=release-feature-set
ARG CARGO_PROFILE=release

# Labels
LABEL org.opencontainers.image.title="Quickwit"
LABEL maintainer="Quickwit, Inc. <hello@quickwit.io>"
LABEL org.opencontainers.image.vendor="Quickwit, Inc."
LABEL org.opencontainers.image.licenses="AGPL-3.0"

RUN apt-get -y update \
    && apt-get -y install ca-certificates \
                          cmake \
                          libpq-dev \
                          libpq5  \
                          libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Required by tonic
RUN rustup component add rustfmt

COPY . /quickwit

WORKDIR /quickwit

RUN echo "Building workspace with feature(s) '$CARGO_FEATURES' and profile '$CARGO_PROFILE'" \
    && cargo build \
        --features $CARGO_FEATURES \
        $(test "$CARGO_PROFILE" = "release" && echo "--release") \
    && echo "Copying binaries to /quickwit/bin" \
    && mkdir -p /quickwit/bin \
    && find target/$CARGO_PROFILE -maxdepth 1 -perm /a+x -type f -exec mv {} /quickwit/bin \;

# Change the default configuration file in order to make the rest,
# grpc servers and gossip accessible from outside of docker.
COPY ./config/quickwit.yaml ./config/quickwit.yaml
RUN sed -i 's/#listen_address: 127.0.0.1/listen_address: 0.0.0.0/g' ./config/quickwit.yaml

FROM debian:bullseye-slim AS quickwit

RUN apt-get -y update \
    && apt-get -y install ca-certificates \
                          libpq5  \
                          libssl1.1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /quickwit
RUN mkdir config qwdata
COPY --from=builder /quickwit/bin/quickwit /usr/local/bin/quickwit
COPY --from=builder /quickwit/config/quickwit.yaml /quickwit/config/quickwit.yaml

ENV QW_CONFIG=/quickwit/config/quickwit.yaml
ENV QW_DATA_DIR=/quickwit/qwdata

ENTRYPOINT ["/usr/local/bin/quickwit"]
