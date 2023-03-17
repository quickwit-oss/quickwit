---
title: Installation
sidebar_position: 2
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Quickwit compiles to a single binary and we provide different methods to install it:

- Linux/MacOS binaries that you can[download manually](#download) or with the [install script](#install-script)
- [Docker image](#use-the-docker-image)
- [Helm chart](/docs/deployment/kubernetes.md)

## Prerequisites

Quickwit is officially only supported for Linux. Freebsd and MacOS are not officially supported, but should work as well.

Quickwit supplies binaries for x86-64 and aarch64. No special instruction set is required, but on x86-64 SSE3 is recommended.
Support of aarch64 is currently experimental.

## Download

version: 0.5.0 - [Release note](https://github.com/quickwit-oss/quickwit/releases/tag/v0.5.0)
License: [AGPL V3](https://github.com/quickwit-oss/quickwit/blob/main/LICENSE.md)
Downloads `.tar.gz`:
- [Linux ARM64](https://github.com/quickwit-oss/quickwit/releases/download/v0.5.0/quickwit-v0.5.0-aarch64-unknown-linux-gnu.tar.gz)
- [Linux x86_64](https://github.com/quickwit-oss/quickwit/releases/download/v0.5.0/quickwit-v0.5.0-x86_64-unknown-linux-gnu.tar.gz)
- [macOS aarch64](https://github.com/quickwit-oss/quickwit/releases/download/v0.5.0/quickwit-v0.5.0-aarch64-apple-darwin.tar.gz)
- [macOS x86_64](https://github.com/quickwit-oss/quickwit/releases/download/v0.5.0/quickwit-v0.5.0-x86_64-apple-darwin.tar.gz)


Check out the available builds in greater detail on [GitHub](https://github.com/quickwit-oss/quickwit/releases)

### Note on external dependencies

Quickwit depends on the following external libraries to work correctly:
- `libssl`: the industry defacto cryptography library.
These libraries can be installed on your system using the native package manager.
You can install these dependencies using the following command:

<Tabs>

<TabItem value="ubuntu" label="Ubuntu">

```bash
apt-get -y update && apt-get -y install libssl
```

</TabItem>

<TabItem value="aws-linux" label="AWS Linux">

```bash
yum -y update && yum -y install openssl
```

</TabItem>

<TabItem value="arch-linux" label="Arch Linux">

```bash
pacman -S openssl
```

</TabItem>

</Tabs>

Additionally it requires a few more dependencies to compile it. These dependencies are not required on production system:
- `clang`: used to compile some dependencies.
- `protobuf-compiler`: used to compile protobuf definitions.
- `libssl-dev`: headers for libssl.
- `pkg-config`: used to locate libssl.
- `cmake`: used to build librdkafka, for kafka support.
These dependencies can be installed on your system using the native package manager.
You can install these dependencies using the following command:

<Tabs>

<TabItem value="ubuntu" label="Ubuntu">

```bash
apt install -y clang protobuf-compiler libssl-dev pkg-config cmake
```

</TabItem>

<TabItem value="aws-linux" label="AWS Linux">

```bash
yum -y update && yum -y install clang openssl-devel pkgconfig cmake3
# amazonlinux only has protobuf-compiler 2.5, we need something much more up to date.
wget https://github.com/protocolbuffers/protobuf/releases/download/v21.9/protoc-21.9-linux-x86_64.zip
sudo unzip protoc-21.9-linux-x86_64.zip -d /usr/local
# amazonlinux use cmake2 as cmake, we need cmake3
ln -s /usr/bin/cmake3 /usr/bin/cmake
```

</TabItem>

<TabItem value="arch-linux" label="Arch Linux">

```bash
pacman -S clang protobuf openssl pkg-config cmake make
```

</TabItem>

</Tabs>

## Install script

To easily install Quickwit on your machine, just run the command below from your preferred shell.
The script detects the architecture and then downloads the correct binary archive for the machine.

```bash
curl -L https://install.quickwit.io | sh
```

All this script does is download the correct binary archive for your machine and extract it in the current working directory. This means you can download any desired archive from [github](https://github.com/quickwit-oss/quickwit/releases) that match your OS architecture and manually extract it anywhere.

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


## Use the Docker image

If you use Docker, this might be one of the quickest way to get going.
The following command will pull the image from [Docker Hub](https://hub.docker.com/r/quickwit/quickwit)
and start a container ready to execute Quickwit commands.

```bash
docker run --rm quickwit/quickwit --version

# If you are using Apple silicon based macOS system you might need to specify the platform.
# You can also safely ignore jemalloc warnings.
docker run --rm --platform linux/amd64 quickwit/quickwit --version
```

To get the full gist of this, follow the [Quickstart guide](./quickstart.md).
