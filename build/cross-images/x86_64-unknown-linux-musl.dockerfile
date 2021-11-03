FROM ekidd/rust-musl-builder:latest
    
ENV CC=musl-gcc \
    CFLAGS=-I/usr/local/musl/include \
    LIB_LDFLAGS=-L/usr/lib/x86_64-linux-gnu
