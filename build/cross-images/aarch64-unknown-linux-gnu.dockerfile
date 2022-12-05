FROM ghcr.io/cross-rs/aarch64-unknown-linux-gnu:0.2.4

ARG PBC_URL="https://github.com/protocolbuffers/protobuf/releases/download/v21.5/protoc-21.5-linux-x86_64.zip"

#TODO: 
# We can switch to static linking (remove `libsasl2-dev:arm64`) using 
# `rdkafka/gssapi-vendored` feature when there is a release incuding: 
# https://github.com/MaterializeInc/rust-sasl/pull/48

RUN dpkg --add-architecture arm64 && \
    apt-get update && \
    apt-get install -y clang-3.9 \
        libclang-3.9-dev \
        binutils-aarch64-linux-gnu \
        libsasl2-dev:arm64 \
        unzip && \
    rm -rf /var/lib/apt/lists/*

RUN curl -fLO $PBC_URL && \
    unzip protoc-21.5-linux-x86_64.zip -d ./protobuf && \
    mv ./protobuf/bin/protoc /usr/bin/ && \
    rm -rf ./protobuf protoc-21.5-linux-x86_64.zip

ENV LIBZ_SYS_STATIC=1 \
    PKG_CONFIG_ALLOW_CROSS=true \
    PKG_CONFIG_ALL_STATIC=true \
    X86_64_UNKNOWN_LINUX_MUSL_OPENSSL_STATIC=1 \
    X86_64_UNKNOWN_LINUX_MUSL_OPENSSL_DIR=/usr/local/musl/
