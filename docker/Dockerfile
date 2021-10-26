FROM alpine:3.14
LABEL maintainer="Quickwit, Inc. <hello@quickwit.io>"

# Install tools
RUN apk add --no-cache curl

# Install Quickwit
# RUN curl -L https://install.quickwit.io | sh
# RUN mv ./quickwit /usr/local/bin/quickwit
# postgresql-dev
#RUN apk add libpq 

COPY ./quickwit-musl /usr/local/bin/quickwit

WORKDIR /usr/quickwit
EXPOSE 8080
ENTRYPOINT [ "ash" ]

# RUN curl -L https://github.com/evanxg852000/hello/releases/download/latest-draft-release/quickwit-aarch64-unknown-linux-gnu > quickwit
# RUN curl -L https://github.com/evanxg852000/hello/releases/download/latest-draft-release/quickwit-aarch64-unknown-linux-musl > quickwit-musl

# RUN curl -L https://ughe.github.io/data/2018/ld-linux-aarch64.so.1 > /lib/ld-linux-aarch64.so.1
# RUN curl -L https://ughe.github.io/data/2018/libc.so.6 > libc.so.6



# WORKDIR /usr/quickwit
# EXPOSE 8080

# ENTRYPOINT [ "quickwit" ]
