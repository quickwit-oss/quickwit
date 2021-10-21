FROM rustembedded/cross:aarch64-unknown-linux-musl


COPY libpq.sh /
RUN bash /libpq.sh aarch64-linux-musl

RUN apt-get update -y && \
    apt-get install -y libpq-dev

ENV X86_64_UNKNOWN_LINUX_MUSL_OPENSSL_DIR=/usr/local/musl/ \
    X86_64_UNKNOWN_LINUX_MUSL_OPENSSL_STATIC=1 \
    PQ_LIB_STATIC_X86_64_UNKNOWN_LINUX_MUSL=1 \
    PG_CONFIG_X86_64_UNKNOWN_LINUX_GNU=/usr/bin/pg_config \
    PKG_CONFIG_ALLOW_CROSS=true \
    PKG_CONFIG_ALL_STATIC=true \
    LIBZ_SYS_STATIC=1 \
    PQ_LIB_DIR=/libpq/lib
