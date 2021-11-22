FROM ekidd/rust-musl-builder:latest

RUN echo "Upgrading CMake" && \
    sudo apt-get remove cmake -y && \
    curl -fLO https://www.cmake.org/files/v3.12/cmake-3.12.1.tar.gz && \
    tar -xvzf cmake-3.12.1.tar.gz && \
    cd cmake-3.12.1/ && ./configure && \
    sudo make install
    
ENV CC=musl-gcc \
    CFLAGS=-I/usr/local/musl/include \
    LIB_LDFLAGS=-L/usr/lib/x86_64-linux-gnu
