FROM quickwit/cross-base:x86_64-unknown-linux-gnu
# See https://github.com/quickwit-inc/cross

ARG PBC_URL="https://github.com/protocolbuffers/protobuf/releases/download/v21.5/protoc-21.5-linux-x86_64.zip"

RUN apt-get update && \
    apt-get install -y zlib1g-dev \
        unzip \
        libssl-dev && \
    rm -rf /var/lib/apt/lists/*

RUN curl -LO $PBC_URL && \
    unzip protoc-21.5-linux-x86_64.zip -d ./protobuf && \
    mv ./protobuf/bin/protoc /usr/bin/ && \
    rm -rf ./protobuf
