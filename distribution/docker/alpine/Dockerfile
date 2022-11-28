FROM alpine:3 AS builder

COPY quickwit-*-unknown-linux-musl.tar.gz ./
RUN tar -xzf quickwit-*-$(cat /etc/apk/arch)-unknown-linux-musl.tar.gz
RUN mv ./quickwit-*/* ./
RUN chmod 744 ./quickwit


FROM alpine:3

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
ENV QW_LISTEN_ADDRESS=0.0.0.0

ENTRYPOINT ["/usr/local/bin/quickwit"]
