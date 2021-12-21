# Contributing to Quickwit
There are many ways to contribute to Quickwit.
Code contribution are welcome of course, but also
bug reports, feature request, and evangelizing are as valuable.

# Submitting a PR
Check if your issue is already listed [github](https://github.com/quickwit-inc/quickwit/issues).
If it is not, create your own issue.

Please add the following phrase at the end of your commit.  `Closes #<Issue Number>`.
It will automatically link your PR in the issue page. Also, once your PR is merged, it will
closes the issue. If your PR only partially addresses the issue and you would like to
keep it open, just write `See #<Issue Number>`.

Feel free to send your contribution in an unfinished state to get early feedback.
In that case, simply mark the PR with the tag [WIP] (standing for work in progress).

# Signing the CLA
Quickwit is an opensource project licensed a AGPLv3.
It is also distributed under a commercial license by Quickwit, Inc.

Contributors are required to sign a Contributor License Agreement.
The process is simple and fast. Upon your first pull request, you will be prompted to
[sign our CLA by visiting this link](https://cla-assistant.io/quickwit-inc/quickwit).

# Development
## Setup & run tests
1. Install Docker (https://docs.docker.com/engine/install/) and Docker Compose (https://docs.docker.com/compose/install/)
2. Install awslocal https://github.com/localstack/awscli-local
3. Start the external services with `make docker-compose-up`
5. Run `QUICKWIT_ENV=LOCAL cargo test --all-features`

## Running services such as Amazon Kinesis or S3, Kafka, or PostgreSQL locally.
1. Ensure Docker and Docker Compose are correctly installed on your machine (see above)
2. Run `make docker-compose-up` to launch all the services or `make docker-compose-up DOCKER_SERVICES=kafka,postgres` to launch a subset of services.

## Tracing with Jaeger
1. Ensure Docker and Docker Compose are correctly installed on your machine (see above)
2. Start the Jaeger services (UI, collector, agent, ...) running the command `make docker-compose-up DOCKER_SERVICES=jaeger`
3. Open your browser and visit [localhost:16686](http://localhost:16686/)

## Using tokio console
1. Install tokio-console by running `cargo install tokio-console`.
2. Install the quickwit binary in the quickwit-cli folder `RUSTFLAGS="--cfg tokio_unstable" cargo install --path . --features tokio-console`
3. Launch a long running command such as index and activate tokio with the: `QUICKWIT_TOKIO_CONSOLE_ENABLED=1 quickwit index ...`
4. Run `tokio-console`.

## Building binaries

Currently, we use [cross](https://github.com/rust-embedded/cross) to build Quickwit binaries for different architectures.
For this to work, we've had to customize the docker images cross uses. These customizations can be found in docker files located in `./cross-images` folder. To make cross take into account any change on those 
docker files, you will need to build and push the images on dockerhub by running `make cross-images`. 
Also, rather than invoking cross locally to build the binaries, you can build the binaries from our CI pipeline. This has two advantages: 
- One can easily build and share binaries from his working branch without having to setup cross.
- We can also make sure our binaries build correctly without going through a release process.
To build the binaries from our CI, just create a branch from your working branch, name it `make-binaries-from-ci` and push. Once the CI passes, you should find all the binary artifacts under the corresponding `Build release packages` run on the `Artifacts` tab. 
Please note:
- The branch `make-binaries-from-ci` can be used by any other contributor, therefore should only be used for creating Quickwit binaries.
- The artifacts for your CI run are only kept for three days.
