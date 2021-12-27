FROM alpine:3.14 AS builder

# Install tools
RUN apk add --no-cache curl
RUN curl -L https://install.quickwit.io | sh
RUN mv  ./quickwit-*/* ./

FROM alpine:3.14
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
