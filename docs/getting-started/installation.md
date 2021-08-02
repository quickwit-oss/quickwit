---
title: Installation
sidebar_position: 2
---

Quickwit compiles to a single binary, we provide different methods to install it.
We notably provide musl builds to provide static binaries with no dependencies. 


## Download

Version: 0.1 - [Release note](https://github.com/quickwit-inc/quickwit/releases/tag/v0.1.0)
License: [AGPL V3](https://github.com/quickwit-inc/quickwit/blob/main/LICENSE.md)
Downloads `.tar.gz`:
- [Linux ARM64](https://github.com/quickwit-inc/quickwit/releases/download/v0.1.0/quickwit-v0.1.0-aarch64-unknown-linux-gnu.tar.gz)
- [Linux ARMv7](https://github.com/quickwit-inc/quickwit/releases/download/v0.1.0/quickwit-v0.1.0-armv7-unknown-linux-gnueabihf.tar.gz)
- [Linux x86_64](https://github.com/quickwit-inc/quickwit/releases/download/v0.1.0/quickwit-v0.1.0-x86_64-unknown-linux-gnu.tar.gz)
- [macOS x86_64](https://github.com/quickwit-inc/quickwit/releases/download/v0.1.0/quickwit-v0.1.0-x86_64-apple-darwin.tar.gz)

Checkout all builds on [github](https://github.com/quickwit-inc/quickwit/releases)

## Install script

Quickwit's script detects the architecture and then downloads the correct binary for it.

```bash
curl -L https://install.quickwit.io | sh
./quickwit --version
```

## Build with cargo

If cargo is on your system, you can install the package from crates.io.

```bash
cargo install quickwit-cli
```

## Use the docker image

If you use docker, this might be one of the quickest way to get going. 
The following command will pull the image from [dockerhub](https://hub.docker.com/r/quickwit/quickwit-cli)
and gets you right in the shell of the running container ready to execute Quickwit commands. 
Note that we are also mounting the working directory as volume. This is useful when you already have your dataset ready on your machine and want to work with Quickwit docker image.

```bash
docker run -it -v "$(pwd)":"/usr/quickwit" --entrypoint ash quickwit/quickwit-cli
quickwit --version
```

To get the full gist of this, let's run a minified version of the - [Quickstart guide](./quickstart.md).

```bash
# let's create a `data` directory
mkdir data && cd data

# download wikipedia dataset files
curl -o wikipedia_index_config.json https://raw.githubusercontent.com/quickwit-inc/quickwit/main/examples/index_configs/wikipedia_index_config.json
curl -o wiki-articles-10000.json https://quickwit-datasets-public.s3.amazonaws.com/wiki-articles-10000.json

# create, index and search using the container 
docker run -v "$(pwd)":"/usr/quickwit" quickwit/quickwit-cli new --index-uri file:///usr/quickwit/wikipedia --index-config-path wikipedia_index_config.json
docker run -v "$(pwd)":"/usr/quickwit" quickwit/quickwit-cli index --index-uri file:///usr/quickwit/wikipedia --input-path wiki-articles-10000.json
docker run -v "$(pwd)":"/usr/quickwit" quickwit/quickwit-cli search --index-uri file:///usr/quickwit/wikipedia --query "barack obama"
docker run -v "$(pwd)":"/usr/quickwit" -p 8080:8080 quickwit/quickwit-cli serve --index-uri file:///usr/quickwit/wikipedia 
```

Alternatively, you can run a container shell session with `data` folder mounted and execute the commands from within that session.

```bash
# start the shell session
docker run -it -v "$(pwd)":"/usr/quickwit" --entrypoint ash quickwit/quickwit-cli
# create, index and search from inside the container 
quickwit new --index-uri file:///$(pwd)/wikipedia --index-config-path wikipedia_index_config.json
quickwit index --index-uri file:///$(pwd)/wikipedia --input-path wiki-articles-10000.json
quickwit search --index-uri file:///$(pwd)/wikipedia --query "barack AND obama"
quickwit serve --index-uri file:///$(pwd)/wikipedia
```

*Note: Since the search API runs by default on port `:8080`, we already expose this port. 
You can choose another port based on your needs and map it to a host port.*
