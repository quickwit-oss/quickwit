# cross-rs hasn't cut a release after 0.2.5 (which is Ubuntu 16.04 / xenial,
# whose default clang is too old for bindgen >= 0.72 used by zstd-sys >= 2.0.16).
# The `main` tag tracks Ubuntu 20.04 (focal) and ships libclang-10, which
# satisfies bindgen's libclang >= 9.0 requirement. Pinned by digest for
# reproducibility.
FROM ghcr.io/cross-rs/x86_64-unknown-linux-gnu:main@sha256:2431cbfcf2499f8a00570e864c5f5d3c81363dbdc23c92c9c1f9331484a44365

ARG PBC_URL="https://github.com/protocolbuffers/protobuf/releases/download/v21.5/protoc-21.5-linux-x86_64.zip"

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libsasl2-dev \
        unzip && \
    rm -rf /var/lib/apt/lists/*

RUN curl -fLO $PBC_URL && \
    unzip protoc-21.5-linux-x86_64.zip -d ./protobuf && \
    mv ./protobuf/bin/protoc /usr/bin/ && \
    rm -rf ./protobuf protoc-21.5-linux-x86_64.zip
