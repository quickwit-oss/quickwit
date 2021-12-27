[![codecov](https://codecov.io/gh/quickwit-inc/quickwit/branch/main/graph/badge.svg?token=06SRGAV5SS)](https://codecov.io/gh/quickwit-inc/quickwit)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.0-4baaaa.svg)](CODE_OF_CONDUCT.md)
[![License: AGPL V3](https://img.shields.io/badge/license-AGPL%20V3-blue)](LICENCE.md)
[![Join the discord chat](https://shields.io/discord/908281611840282624?label=chat%20on%20discord)](https://discord.gg/rpRRTezWhW)
<br/>
<br/>
<br/>
<p align="center">
  <img src="docs/assets/images/logo_horizontal.svg" alt="Quickwit" height="100">
</p>
<h4 align="center">
  <a href="https://quickwit.io/docs/getting-started/quickstart">Quickstart</a> |
  <a href="https://quickwit.io/docs/">Docs</a> |
  <a href="https://quickwit.io/docs/tutorials/tutorial-hdfs-logs">Tutorials</a> |
  <a href="https://discord.gg/rpRRTezWhW">Chat</a> |
  <a href="https://quickwit.io/docs/getting-started/installation">Download</a>
</h4>
<br/>

### Quickwit 0.2 is out! Check out our [blog post](https://quickwit.io/blog/quickwit-0.2-release/) to discover the new features.

<br/>

Quickwit is a distributed search engine built from the ground up to offer cost-efficiency and high reliability. By mere mortals for mere mortals, Quickwit's architecture is as simple as possible <sup>[1](#footnote1)</sup>.

Quickwit is written in Rust and built on top of the mighty [tantivy](https://github.com/quickwit-inc/tantivy) library. We designed it to index large datasets.

## Why Quickwit?

Quickwit is born from the idea that today's search engines are hard to manage and uneconomical when dealing with large datasets and a low QPS<sup>[2](#footnote2)</sup> rate. Its benefits are most apparent in a multitenancy or a multi-index setting.

Quickwit allows true decoupled compute and storage.
We designed it to search straight from object storage like AWS S3 in a stateless manner.

Imagine hosting an arbitrary amount of indices on S3 for $25/TB.month and querying them with the same pool of search servers and with a subsecond latency.

Not only is Quickwit more cost-efficient, but search clusters are also easier to operate. One can add or remove search instances in seconds. You can also effortlessly index a massive amount of historical data using your favorite batch technology. Last but not least, Multi-tenant search is now cheap and painless.


- [Get started](https://quickwit.io/docs/getting-started/quickstart)
- [Look at the feature set](https://quickwit.io/docs/overview/features)


<p align="center">
  <img src="docs/assets/images/quickstart_terminal_screenshot.png" alt="Quickstart">
</p>


# Documentation
- [Introduction](https://quickwit.io/docs/)

## Getting started
- [Quickstart](https://quickwit.io/docs/getting-started/quickstart)
- [Installation](https://quickwit.io/docs/getting-started/installation)

## Overview
- [Features](https://quickwit.io/docs/overview/features)
- [Architecture](https://quickwit.io/docs/overview/architecture)

## Tutorials
- [Search on logs with timestamp pruning](https://quickwit.io/docs/tutorials/tutorial-hdfs-logs)
- [Setup a distributed search on AWS S3](https://quickwit.io/docs/tutorials/tutorial-hdfs-logs-distributed-search-aws-s3)
- [Set up your AWS S3 environment](https://quickwit.io/docs/tutorials/configure-aws-env)

## Administration
- [Operating in the cloud](https://quickwit.io/docs/administration/cloud-env)

## Reference
- [Quickwit CLI](https://quickwit.io/docs/reference/cli)
- [Index Config](https://quickwit.io/docs/reference/index-config)
- [Search API](https://quickwit.io/docs/reference/search-api)
- [Query language](https://quickwit.io/docs/reference/query-language)
- [Telemetry](https://quickwit.io/docs/reference/telemetry)

## Meta
- [Explore further](https://quickwit.io/docs/meta/explore-further)
- [Release notes](https://quickwit.io/docs/meta/release-notes)
- [Code of conduct](CODE_OF_CONDUCT.md)
- [Contributing](CONTRIBUTING.md)



---
<a name="footnote1">1.</a>: ... But not one bit simpler.

<a name="footnote2">2.</a>: QPS stands for Queries per second. It is a standard measure of the amount of search traffic.
