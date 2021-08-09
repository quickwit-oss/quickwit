FROM alpine:3.14
LABEL maintainer="Quickwit, Inc. <hello@quickwit.io>"

# Install tools
RUN apk add --no-cache curl

# Install Quickwit
RUN curl -L https://install.quickwit.io | sh
RUN mv ./quickwit /usr/local/bin/quickwit

WORKDIR /usr/quickwit
EXPOSE 8080

ENTRYPOINT [ "quickwit" ]
