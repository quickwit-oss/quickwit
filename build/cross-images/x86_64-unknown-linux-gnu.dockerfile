# cross-rs hasn't cut a release after 0.2.5 (which is Ubuntu 16.04 / xenial,
# whose default clang is too old for bindgen >= 0.72 used by zstd-sys >= 2.0.16).
# The `main` tag tracks Ubuntu 20.04 (focal) and ships libclang-10, which
# satisfies bindgen's libclang >= 9.0 requirement. Pinned by digest for
# reproducibility.
FROM ghcr.io/cross-rs/x86_64-unknown-linux-gnu:main@sha256:2431cbfcf2499f8a00570e864c5f5d3c81363dbdc23c92c9c1f9331484a44365

ARG PBC_URL="https://github.com/protocolbuffers/protobuf/releases/download/v21.5/protoc-21.5-linux-x86_64.zip"

# librdkafka 2.12.1 includes curl/curl.h even when OIDC support is disabled.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        g++-10 \
        gcc-10 \
        libcurl4-openssl-dev \
        libsasl2-dev \
        unzip && \
    rm -rf /var/lib/apt/lists/*

# GCC 9.4 is affected by https://gcc.gnu.org/bugzilla/show_bug.cgi?id=95189,
# which aws-lc correctly rejects because it can miscompile memcmp at -O3.
ENV CC_x86_64_unknown_linux_gnu=gcc-10 \
    CXX_x86_64_unknown_linux_gnu=g++-10 \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=gcc-10

RUN curl -fLO $PBC_URL && \
    unzip protoc-21.5-linux-x86_64.zip -d ./protobuf && \
    mv ./protobuf/bin/protoc /usr/bin/ && \
    rm -rf ./protobuf protoc-21.5-linux-x86_64.zip
