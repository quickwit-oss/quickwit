<p align="center">
  <img src="docs/assets/images/logo_horizontal.svg" alt="Quickwit" height="100">
</p>
<h4 align="center">
  <a href="https://quickwit.io/docs/getting-started/quickstart">Quickwstart</a> |
  <a href="https://quickwit.io/docs/">Docs</a> |
  <a href="https://quickwit.io/blog">Tutorials</a> |
  <a href="https://quickwit.io/blog">Documentation</a> |
  <a href="https://quickwit.io/docs/getting-started/installation">Download</a>
</h4>
<p align="center">
[![codecov](https://codecov.io/gh/quickwit-inc/quickwit/branch/main/graph/badge.svg?token=06SRGAV5SS)](https://codecov.io/gh/quickwit-inc/quickwit) [![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.0-4baaaa.svg)](CODE_OF_CONDUCT.md) [![License: AGPL V3](https://img.shields.io/badge/license-AGPL%20V3-blue)](LICENCE.md)
</p>


Quickwit is a distributed search engine built from the ground up to offer cost-efficiency and high reliability. By mere mortals for mere mortals, Quickwit's architecture is as simple as possible[^1].

Quickwit is written in Rust and built on top of the mighty [tantivy](https://github.com/tantivy-search/tantivy) library. We designed it to index big datasets.

## Why Quickwit?

Quickwit is born from the idea that today's search engines are hard to manage and uneconomical when dealing with large datasets and a low QPS[^2] rate. Its benefits are most apparent in a multitenancy or a multi-index setting.

Quickwit allows true decoupled compute and storage.
We designed it to search straight from object storage like AWS S3 in a stateless manner.

Imagine hosting an arbitrary amount of indices on S3 for $25/TB.month and querying them with the same pool of search servers and with a subsecond latency.

Not only is Quickwit more cost-efficient, but search clusters are also easier to operate. One can add or remove search instances in seconds. You can also effortlessly index a massive amount of historical data using your favorite batch technology. Last but not least, Multi-tenant search is now cheap and painless.

- [Look at the feature list](overview/features.md)
- [Get started](getting-started/quickstart.md)


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



---
[^1] ... But not one bit simpler.
[^2] QPS stands for Queries per second. It is a standard measure of the amount of search traffic.