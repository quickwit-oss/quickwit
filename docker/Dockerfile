FROM alpine:3.14
LABEL maintainer="Quickwit, Inc. <hello@quickwit.io>"

# Install tool
RUN apk add --no-cache curl

# Create working directory
RUN mkdir -p /usr/quickwit
WORKDIR /usr/quickwit
EXPOSE 8080

RUN curl -L https://install.quickwit.io | sh

ENTRYPOINT [ "ash" ]
