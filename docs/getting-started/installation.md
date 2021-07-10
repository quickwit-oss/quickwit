---
title: Installation
sidebar_position: 2
---

Quickwit compiles to a single binary, we provide different methods to install it.
We notably provide musl builds to provide static binaries with no dependencies. 
You can checkout all available artefacts in on [github](https://github.com/quickwit-inc/quickwit/releases).



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


