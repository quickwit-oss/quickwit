# Contributing to Quickwit
There are many ways to contribute to Quickwit.
Code contributions are welcome of course, but also
bug reports, feature requests, and evangelizing are as valuable.

# Submitting a PR
Check if your issue is already listed on [github](https://github.com/quickwit-oss/quickwit/issues).
If it is not, create your own issue.

Please add the following phrase at the end of your commit `Closes #<Issue Number>`.
It will automatically link your PR in the issue page. Also, once your PR is merged, it will
close the issue. If your PR only partially addresses the issue and you would like to
keep it open, just write `See #<Issue Number>`.

Feel free to send your contribution in an unfinished state to get early feedback.
In that case, simply mark the PR with the tag [WIP] (standing for work in progress).

## PR verification checks
When you submit a pull request to the project, the CI system runs several verification checks. After your PR is merged, a more exhaustive list of tests will be run.

You will be notified by email from the CI system if any issues are discovered, but if you want to run these checks locally before submitting PR or in order to verify changes you can use the following commands in the root directory:
1. To verify that all tests are passing, run `make test-all`.
2. To fix code style and format as well as catch common mistakes run `make fix`. Alternatively, run `make -k test-all docker-compose-down` to tear down the Docker services after running all the tests.
3. To build docs run `make build-rustdoc`.

# Development

## Setup & run tests

### Local Development

1. Install Rust, CMake, Docker (https://docs.docker.com/engine/install/) and Docker Compose (https://docs.docker.com/compose/install/)
2. Install node@20 and `npm install -g yarn`
3. Install awslocal https://github.com/localstack/awscli-local
4. Install protoc https://grpc.io/docs/protoc-installation/ (you may need to install the latest binaries rather than your distro's flavor)
5. Install nextest https://nexte.st/docs/installation/pre-built-binaries/

### GitHub Codespaces

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/quickwit-oss/quickwit?devcontainer_path=.devcontainer/devcontainer.json)

GitHub Codespaces provides a fully configured development environment in the cloud, making it easy to get started with Quickwit development. By clicking the badge above, you can create a codespace with all the necessary tools installed and configured.

### Running tests
Run `make test-all` to run all tests.

## Useful commands
* `make test-all` - starts necessary Docker services and runs all tests.
* `make -k test-all docker-compose-down` - the same as above, but tears down the Docker services after running all the tests.
* `make fmt` - runs formatter, this command requires the nightly toolchain to be installed by running `rustup toolchain install nightly`.
* `make fix` - runs formatter and clippy checks.
* `make typos` - runs the spellcheck tool over the codebase. (Install by running `cargo install typos-cli`)
* `make doc` - builds docs.
* `make docker-compose-up` - starts Docker services.
* `make docker-compose-down` - stops Docker services.
* `make docker-compose-logs` - shows Docker logs.

## Start the UI
1. Switch to the `quickwit` subdirectory of the project and create a data directory `qwdata` there if it doesn't exist
2. Start a server `cargo r run --config ../config/quickwit.yaml`
3. `yarn --cwd quickwit-ui install` and `yarn --cwd quickwit-ui start`
4. Open your browser at `http://localhost:3000/ui` if it doesn't open automatically

## Running UI Tests
1. Run `yarn --cwd quickwit-ui install` and `yarn --cwd quickwit-ui test` in the `quickwit` directory

## Running UI e2e tests
1. Ensure to run a searcher `cargo r run --service searcher --config ../config/quickwit.yaml`
2. Run `yarn --cwd quickwit-ui e2e-test`

## Running services such as Amazon Kinesis or S3, Kafka, or PostgreSQL locally.
1. Ensure Docker and Docker Compose are correctly installed on your machine (see above)
2. Run `make docker-compose-up` to launch all the services or `make docker-compose-up DOCKER_SERVICES=kafka,postgres` to launch a subset of services.

## Tracing with Jaeger
1. Ensure Docker and Docker Compose are correctly installed on your machine (see above)
2. Start the Jaeger services (UI, collector, agent, ...) running the command `make docker-compose-up DOCKER_SERVICES=jaeger`
3. Start Quickwit with the following environment variables:

```
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
QW_ENABLE_OPENTELEMETRY_OTLP_EXPORTER=true
```

4. Open your browser and visit [localhost:16686](http://localhost:16686/)

## Using tokio console
1. Install tokio-console by running `cargo install tokio-console`.
2. Install the quickwit binary in the quickwit-cli folder `RUSTFLAGS="--cfg tokio_unstable" cargo install --path . --features tokio-console`
3. Launch a long running command such as index and activate tokio with the: `QW_ENABLE_TOKIO_CONSOLE=1 quickwit index ...`
4. Run `tokio-console`.

## Building binaries

Currently, we use [cross](https://github.com/rust-embedded/cross) to build Quickwit binaries for different architectures.
For this to work, we've had to customize the docker images cross uses. These customizations can be found in docker files located in the `./cross-images` folder. To make cross take into account any change on those
docker files, you will need to build and push the images on Docker Hub by running `make cross-images`.
We also have nightly builds that are pushed to Docker Hub. This helps continuously check that our binaries are still built even with external dependency updates. Successful builds let you access the artifacts for the next three days. Release builds always have their artifacts attached to the release.

## Docker images

Each merge on the `main` branch triggers the build of a new Docker image available on DockerHub at `quickwit/quickwit:edge`. Tagging a commit also creates a new image `quickwit/quickwit:<tag name>` if the tag name starts with `v*` or `qw*`. The Docker images are based on Debian.

### Notes on the embedded UI
As the react UI is embedded in the rust binary, we need to build the react app before building the binary. Hence `make cross-image` depends on the command `build-ui`.

## Testing release (alpha, beta, rc)

The following Quickwit installation command `curl -L https://install.quickwit.io | sh` always installs the latest stable version of quickwit. To make it easier in installing and testing new (alpha, beta, rc) releases, you can manually pull and execute the script as `./install.sh --allow-any-latest-version`. This will force the script to install any latest available release package.

## Tracking licenses

We keep track of the licenses used by the open source crates used by this project using
[`rust-license-tool`](https://github.com/DataDog/rust-license-tool). The listing is checked every
time CI is run. To update the listing, install the tool with `cargo install --git
https://github.com/DataDog/rust-license-tool` and then run `dd-rust-license-tool write`. If there are
any errors, you may need to update the listing of exceptions in `license-tool.toml`.

# Documentation

Quickwit documentation is located in the docs directory.

## Generating the CLI docs.

The [CLI doc page](docs/reference/cli.md) is partly generated by a script.
To update it, first run the script:

```bash
cargo run --bin generate_markdown > ../docs/reference/cli_insert.md
```

Then manually edit the [doc page](docs/reference/cli.md) to update it and delete the generated file.
There are two comments to indicate where you want to insert the new docs and where it ends:

```markdown
[comment]: <> (Insert auto generated CLI docs from here.)

...docs to insert...

[comment]: <> (End of auto generated CLI docs.)
```
