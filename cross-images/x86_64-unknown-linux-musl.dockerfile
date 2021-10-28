FROM ekidd/rust-musl-builder:latest
    
ENV LIB_LDFLAGS=-L/usr/lib/x86_64-linux-gnu \
    CFLAGS=-I/usr/local/musl/include \
    PQ_LIB_STATIC=1 \
    CC=musl-gcc

