[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.0-4baaaa.svg)](CODE_OF_CONDUCT.md)
[![codecov](https://codecov.io/gh/quickwit/branch/main/graph/badge.svg)](https://codecov.io/gh/tantivy-search/tantivy)

<p align="center">
  <img src="docs/assets/images/logo_horizontal.svg" alt="Quickwit">
</p>

# What is Quickwit?

Quickwit is a distributed search engine built from the ground up to achieve high performance, high reliability while keeping its architecture simple and manageable for the mere mortals.

# Why Quickwit?

Quickwit is born from the idea that today's search engines are hard to manage and too costly on specific use cases like analytics in a multitenancy environment or low QPS use cases.


How can this dream comes true? By searching data straight from object storage like AWS S3, achieving true decoupled compute and storage. Not only is Quickwit more cost-efficient, but search clusters are also easier to operate. One can add or remove search instances in seconds. Multi-tenant search becomes trivial.


# Documentation
- [Introduction](docs/introduction.md)

## Getting started
- [Quickstart](docs/getting-started/quickstart.md)
- [Installation](docs/getting-started/installation.md)

## Overview
- [Features](docs/overview/features.md)
- [Architecture](docs/overview/architecture.md)

## Administration
- [Operating in the cloud](docs/administration/cloud-env.md)

## Tutorials
- [Search on logs with timestamp pruning](docs/tutorials/tutorial-hdfs-logs.md)
- [Setup a distributed search on AWS S3](docs/tutorials/tutorial-hdfs-logs-distributed-search-aws-s3.md)

## Reference
- [Quickwit CLI](docs/reference/cli.md)
- [Index Config](docs/reference/index-config.md)
- [Search API](docs/reference/search-api.md)
- [Query language](docs/reference/query-language.md)
- [Telemetry](docs/reference/telemetry.md)

## Meta
- [Explore further](docs/meta/explore-further.md)
- [Release notes](docs/meta/release-notes.md)
- [Code of conduct](CODE_OF_CONDUCT.md)
- [Contributing](CONTRIBUTING.md)