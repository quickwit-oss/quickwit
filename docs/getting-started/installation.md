---
title: Installation
sidebar_position: 2
---

Quickwit compiles to a single binary, we provide different methods to install it.
We notably provide musl builds to provide static binaries with no dependencies. 


## Download

Version: 0.2.0 - [Release note](https://github.com/quickwit-inc/quickwit/releases/tag/v0.2.0)
License: [AGPL V3](https://github.com/quickwit-inc/quickwit/blob/main/LICENSE.md)
Downloads `.tar.gz`:
- [Linux ARM64](https://github.com/quickwit-inc/quickwit/releases/download/v0.2.0/quickwit-v0.2.0-aarch64-unknown-linux-gnu.tar.gz)
- [Linux x86_64](https://github.com/quickwit-inc/quickwit/releases/download/v0.2.0/quickwit-v0.2.0-x86_64-unknown-linux-gnu.tar.gz)
- [macOS x86_64](https://github.com/quickwit-inc/quickwit/releases/download/v0.2.0/quickwit-v0.2.0-x86_64-apple-darwin.tar.gz)

Checkout all builds on [github](https://github.com/quickwit-inc/quickwit/releases)

### Note on external dependencies

Quickwit depends on the following external libraries to work correctly:
- `libpq`: the Postgres client library.
- `libssl`: the industry defacto cryptography library. 
These libraries can be installed on your system using the native package manager. 
On Ubuntu for instance, you can install these dependencies using the following command:

```bash
apt-get -y update && apt-get -y install libpq-dev libssl-dev
```

:::note

Quickwit static binary packages are also provided as `musl` builds. These packages don't require you to install any external library and can be automatically picked during installation on your system if the required libc version is not present. You can also download and manually install a static binary package. 

:::


## Install script

To easily install Quickwit on your machine, just run the command below from your preferred shell.
The script detects the architecture and then downloads the correct binary archive for the machine.

```bash
curl -L https://install.quickwit.io | sh
```

All this script does is download the correct binary archive for your machine and extract it in the current working directory. This means you can download any desired archive from [github](https://github.com/quickwit-inc/quickwit/releases) that match your OS architecture and manually extract it anywhere.

Once installed or extracted, all Quickwit's installation files can be found in a directory named `quickwit-{version}` where `version` is the corresponding version of Quickwit. This directory has the following layout:

```bash
quickwit-{version}
    ├── config
    │   └── quickwit.yaml
    ├── LICENSE_AGPLv3.0.txt
    ├── quickwit
    └── qwdata
```

- `config/quickwit.yaml`: is the default configuration file.
- `LICENSE_AGPLv3.0.txt`: the license file.
- `quickwit`: the quickwit executable binary.
- `qwdata/`: the default data directory. 


## Use the docker image

If you use docker, this might be one of the quickest way to get going. 
The following command will pull the image from [dockerhub](https://hub.docker.com/r/quickwit/quickwit)
and gets you right in the shell of the running container ready to execute Quickwit commands. 
Note that we are also mounting the working directory as volume. This is useful when you already have your dataset ready on your machine and want to work with Quickwit docker image.

```bash
docker run -it -v "$(pwd)":"/quickwit/files" --entrypoint ash quickwit/quickwit
quickwit --version
```

To get the full gist of this, let's run a minified version of the - [Quickstart guide](./quickstart.md).

```bash
# let's create a `data` directory
mkdir data && cd data

# download wikipedia dataset files
curl -o wikipedia_index_config.yaml https://raw.githubusercontent.com/quickwit-inc/quickwit/main/config/tutorials/wikipedia/index-config.yaml
curl -o wiki-articles-10000.json https://quickwit-datasets-public.s3.amazonaws.com/wiki-articles-10000.json

# create, index and search using the container 
docker run -v "$(pwd)":"/quickwit/qwdata" quickwit/quickwit index create --index wikipedia --index-config ./qwdata/wikipedia_index_config.yaml

docker run -v "$(pwd)":"/quickwit/qwdata" quickwit/quickwit index ingest --index wikipedia --input-path ./qwdata/wiki-articles-10000.json

docker run -v "$(pwd)":"/quickwit/qwdata" quickwit/quickwit index search --index wikipedia --query "barack obama"

docker run -v "$(pwd)":"/quickwit/qwdata" --expose 7280 -p 7280:7280 quickwit/quickwit service run searcher
```

Now you can make HTTP requests to the searcher service API.

```bash
curl http://127.0.0.1:7280/api/v1/wikipedia/search?query=obama
```

Alternatively, you can run a container shell session with your `data` folder mounted and execute the commands from within that session.
