FROM alpine:3.14
LABEL org.opencontainers.image.title="Quickwit"
LABEL maintainer="Quickwit, Inc. <hello@quickwit.io>"
LABEL org.opencontainers.image.vendor="Quickwit, Inc."
LABEL org.opencontainers.image.licenses="AGPL-3.0"

# Default environment variables
ENV QW_CONFIG=/config/quickwit.yaml
ENV QW_DATA_DIR=/qwdata

# Install tools
RUN apk add --no-cache curl

# Install Quickwit
WORKDIR /
RUN curl -L https://install.quickwit.io | sh
RUN mv  quickwit-*/quickwit /usr/local/bin
RUN mkdir /config && mv quickwit-*/config/quickwit.yaml /config
RUN rm -rf quickwit-*/ && mkdir /qwdata

WORKDIR ${QW_DATA_DIR}

EXPOSE 8080 

ENTRYPOINT [ "quickwit" ]
