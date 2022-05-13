FROM rustembedded/cross:aarch64-unknown-linux-gnu

COPY libpq.sh /
RUN bash /libpq.sh aarch64-linux-gnu

RUN apt-get update -y && \
    apt-get install -y libpq-dev \
        libclang-3.9-dev \
        clang-3.9 \
        gcc-aarch64-linux-gnu \
        g++-aarch64-linux-gnu \
        g++-4.9-multilib && \
    rm -rf /var/lib/apt/lists/*

ENV LIBZ_SYS_STATIC=1 \
    PG_CONFIG_X86_64_UNKNOWN_LINUX_GNU=/usr/bin/pg_config \
    PKG_CONFIG_ALLOW_CROSS=true \
    PKG_CONFIG_ALL_STATIC=true \
    PQ_LIB_DIR=/libpq/lib \
    PQ_LIB_STATIC_X86_64_UNKNOWN_LINUX_MUSL=1 \
    X86_64_UNKNOWN_LINUX_MUSL_OPENSSL_STATIC=1 \
    X86_64_UNKNOWN_LINUX_MUSL_OPENSSL_DIR=/usr/local/musl/
