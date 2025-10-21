FROM ubuntu:noble@sha256:66460d557b25769b102175144d538d88219c077c678a49af4afca6fbfc1b5252 AS builder

RUN apt-get update && apt-get install -y curl
RUN curl -L https://install.quickwit.io | sh


FROM ubuntu:noble@sha256:66460d557b25769b102175144d538d88219c077c678a49af4afca6fbfc1b5252 AS quickwit

LABEL org.opencontainers.image.title="Quickwit"
LABEL maintainer="Quickwit, Inc. <hello@quickwit.io>"
LABEL org.opencontainers.image.vendor="Quickwit, Inc."
LABEL org.opencontainers.image.licenses="Apache-2.0"

RUN apt-get -y update \
    && apt-get -y install ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /quickwit
RUN mkdir config qwdata
COPY --from=builder /quickwit-v*/quickwit /usr/local/bin/quickwit
COPY --from=builder /quickwit-v*/config/quickwit.yaml /quickwit/config/quickwit.yaml

ENV QW_CONFIG=/quickwit/config/quickwit.yaml
ENV QW_DATA_DIR=/quickwit/qwdata
ENV QW_LISTEN_ADDRESS=0.0.0.0

RUN quickwit --version

ENTRYPOINT ["quickwit"]
