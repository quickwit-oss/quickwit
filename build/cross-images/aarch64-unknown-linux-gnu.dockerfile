FROM rustembedded/cross:aarch64-unknown-linux-gnu

RUN apt-get update -y && \
    apt-get install -y libclang-3.9-dev \
        clang-3.9 \
        gcc-aarch64-linux-gnu \
        g++-aarch64-linux-gnu \
        g++-4.9-multilib && \
    rm -rf /var/lib/apt/lists/*

ENV LIBZ_SYS_STATIC=1 \
    PKG_CONFIG_ALLOW_CROSS=true \
    PKG_CONFIG_ALL_STATIC=true \
    X86_64_UNKNOWN_LINUX_MUSL_OPENSSL_STATIC=1 \
    X86_64_UNKNOWN_LINUX_MUSL_OPENSSL_DIR=/usr/local/musl/
