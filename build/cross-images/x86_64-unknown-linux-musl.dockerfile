FROM quickwit/cross-base:x86_64-unknown-linux-musl@sha256:5bcc7843aab64f89bf85c464fa2c5a00ecc634a8b1ac88c84a864f60054450cb
# See https://github.com/quickwit-inc/rust-musl-builder

RUN echo "Upgrading CMake" && \
    sudo apt-get remove cmake -y && \
    curl -fLO https://www.cmake.org/files/v3.12/cmake-3.12.1.tar.gz && \
    tar -xvzf cmake-3.12.1.tar.gz && \
    cd cmake-3.12.1/ && ./configure && \
    sudo make install
    
ENV CC=musl-gcc \
    CFLAGS=-I/usr/local/musl/include \
    LIB_LDFLAGS=-L/usr/lib/x86_64-linux-gnu
