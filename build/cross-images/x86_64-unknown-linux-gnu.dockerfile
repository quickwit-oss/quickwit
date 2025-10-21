FROM ghcr.io/cross-rs/x86_64-unknown-linux-gnu:0.2.4@sha256:7c9067212c2283be2a1d5585af5ecebd4c4a2e18091e2a6aafd23f9b4b81d496

ARG PBC_URL="https://github.com/protocolbuffers/protobuf/releases/download/v21.5/protoc-21.5-linux-x86_64.zip"

RUN apt-get update && \
    apt-get install -y clang-3.9 \
        libclang-3.9-dev \
        libsasl2-dev \
        unzip && \
    rm -rf /var/lib/apt/lists/*

RUN curl -fLO $PBC_URL && \
    unzip protoc-21.5-linux-x86_64.zip -d ./protobuf && \
    mv ./protobuf/bin/protoc /usr/bin/ && \
    rm -rf ./protobuf protoc-21.5-linux-x86_64.zip
