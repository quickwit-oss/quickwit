FROM alpine:3.15 AS builder

# Install tools
RUN apk add --no-cache curl
RUN curl -L https://install.quickwit.io | sh
RUN mv  ./quickwit-*/* ./ && ls ./config/

# Change the default configuration file in order to make the searcher
# and indexer services accessible from outside of docker.
RUN sed -i 's/#indexer/indexer/g' ./config/quickwit.yaml \
    && sed -i 's/#searcher/searcher/g' ./config/quickwit.yaml  \
    && sed -i 's/#  rest_listen_address: 127.0.0.1/ rest_listen_address: 0.0.0.0/g' ./config/quickwit.yaml

FROM alpine:3.15
LABEL org.opencontainers.image.title="Quickwit"
LABEL maintainer="Quickwit, Inc. <hello@quickwit.io>"
LABEL org.opencontainers.image.vendor="Quickwit, Inc."
LABEL org.opencontainers.image.licenses="AGPL-3.0"

WORKDIR /quickwit
RUN mkdir config qwdata
COPY --from=builder /quickwit /usr/local/bin/
COPY --from=builder /config/quickwit.yaml /quickwit/config/quickwit.yaml

ENV QW_CONFIG=/quickwit/config/quickwit.yaml
ENV QW_DATA_DIR=/quickwit/qwdata

ENTRYPOINT ["/usr/local/bin/quickwit"]
