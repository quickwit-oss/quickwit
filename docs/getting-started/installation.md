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

Quickwit's script detect the architecture and then download the right binary for it.

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

Coming soon.




