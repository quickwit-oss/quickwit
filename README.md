[![codecov](https://codecov.io/gh/quickwit-inc/quickwit/branch/main/graph/badge.svg?token=06SRGAV5SS)](https://codecov.io/gh/quickwit-inc/quickwit) [![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.0-4baaaa.svg)](CODE_OF_CONDUCT.md) 

# Quickwit

This repository will host Quickwit, the big data search engine developed by Quickwit Inc.
We will progressively polish and opensource our code in the next months.

Stay tuned.


### Setup - Run Tests

1. Install docker https://docs.docker.com/engine/install/
2. Install localstack https://github.com/localstack/localstack#installing
3. Install awslocal https://github.com/localstack/awscli-local
4. Prepare s3 bucket used in tests, execute `./quickwit-cli/tests/prepare_tests.sh`
5. `QUICKWIT_ENV=LOCAL cargo test --all-features`
