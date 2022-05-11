FROM quickwit/cross-base:x86_64-unknown-linux-gnu
# See https://github.com/quickwit-inc/cross

RUN apt-get update && \
    apt-get install -y zlib1g-dev libssl-dev \
        libpq-dev libclang-3.9-dev clang-3.9 && \
    rm -rf /var/lib/apt/lists/*
