FROM rustembedded/cross:aarch64-unknown-linux-musl


# The Rust toolchain to use when building our image.  Set by `hooks/build`.
# ARG TOOLCHAIN=stable

# The OpenSSL version to use. Here is the place to check for new releases:
#
# - https://www.openssl.org/source/
#
# ALSO UPDATE hooks/build!
ARG OPENSSL_VERSION=1.1.1i
ARG ZLIB_VERSION=1.2.11

RUN echo "Building OpenSSL" && \
    cd /tmp && \
    short_version="$(echo "$OPENSSL_VERSION" | sed s'/[a-z]$//' )" && \
    curl -fLO "https://www.openssl.org/source/openssl-$OPENSSL_VERSION.tar.gz" || \
        curl -fLO "https://www.openssl.org/source/old/$short_version/openssl-$OPENSSL_VERSION.tar.gz" && \
    tar xvzf "openssl-$OPENSSL_VERSION.tar.gz" && cd "openssl-$OPENSSL_VERSION" && \
    AR=aarch64-linux-musl-ar CC=aarch64-linux-musl-gcc ./Configure no-zlib -fPIC --prefix=/usr/local/aarch64-linux-musl -DOPENSSL_NO_SECURE_MEMORY linux-aarch64 && \
    env C_INCLUDE_PATH=/usr/local/aarch64-linux-musl/include/ make depend && \
    env C_INCLUDE_PATH=/usr/local/aarch64-linux-musl/include/ make && \
    make install && \
    rm -r /tmp/*

RUN echo "Building zlib" && \
    cd /tmp && \
    curl -fLO "https://zlib.net/fossils/zlib-$ZLIB_VERSION.tar.gz" && \
    tar xzf "zlib-$ZLIB_VERSION.tar.gz" && cd "zlib-$ZLIB_VERSION" && \
    AR=aarch64-linux-musl-ar CC=aarch64-linux-musl-gcc ./configure --static --prefix=/usr/local/aarch64-linux-musl && \
    make && make install && \
    rm -r /tmp/*

ENV AARCH64_UNKNOWN_LINUX_MUSL_OPENSSL_STATIC=1 \
    CC=aarch64-linux-musl-gcc \
    CFLAGS=-I/usr/local/aarch64-linux-musl/include \
    LIBZ_SYS_STATIC=1 \
    LIB_LDFLAGS=-L/usr/local/aarch64-linux-musl/lib \
    OPENSSL_INCLUDE_DIR=/usr/local/aarch64-linux-musl/include/openssl \
    OPENSSL_LIB_DIR=/usr/local/aarch64-linux-musl/lib \
    PKG_CONFIG_ALLOW_CROSS=true \
    PKG_CONFIG_ALL_STATIC=true \
    TARGET=aarch64-unknown-linux-musl \
    AARCH64_UNKNOWN_LINUX_MUSL_OPENSSL_DIR=/usr/local/aarch64-linux-musl \
    OPENSSL_ROOT_DIR=/usr/local/aarch64-linux-musl
