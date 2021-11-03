FROM evanxg852000/cross-x86_64-unknown-linux-gnu:latest

RUN apt-get update && \
    apt-get install -y zlib1g-dev libssl-dev libpq-dev && \
    rm -rf /var/lib/apt/lists/*
